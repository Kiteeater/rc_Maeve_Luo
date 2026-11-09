// Package biz_operations 实现 S07 人工重投与 UNKNOWN 核对结案：当前授权、操作幂等、风险确认与证据校验。
// 不删除或重写原尝试，不重投成功任务，不使用新路由重分发旧事件。
package biz_operations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// Principal 是操作者身份。Permissions 来自当前认证，不缓存。
type Principal struct {
	ActorID     string
	Scopes      []string
	Permissions []string
}

func (p Principal) allows(perm string) bool { return contains(p.Permissions, perm) }

// Config 是 S07 §9 参数与当前允许的轮次上限。
type Config struct {
	MaxEvidenceAttempts int
	ConfirmTimeout      time.Duration
	MaxAllowedAttempts  uint32
	MaxRoundTimeout     time.Duration
}

// UseCase 编排人工操作。
type UseCase struct {
	Tasks       data_interface.DeliveryStore
	Ops         data_interface.OperationStore
	Constraints data_interface.ConstraintChecker
	Config      Config
	Now         func() time.Time
	NewID       func() (string, error)
}

// RetryRequest 是 POST /v1/deliveries/{id}/retry。
type RetryRequest struct {
	Key         string
	ExpectedRow uint32
	Reason      string
	RiskAck     bool
}

// ResolveRequest 是 POST /v1/deliveries/{id}/resolve。
type ResolveRequest struct {
	Key         string
	ExpectedRow uint32
	Resolution  string
	Reason      string
	Evidence    domain.Evidence
}

// Result 是操作结果；Replayed 表示同键同参数返回原快照。
type Result struct {
	Body     []byte
	Replayed bool
}

// TaskSnapshot 是操作完成时的脱敏任务摘要（不含固定载荷）。
type TaskSnapshot struct {
	DeliveryID        string `json:"delivery_id"`
	Status            string `json:"status"`
	ResultCertainty   string `json:"result_certainty"`
	RoundNo           uint32 `json:"round_no"`
	RowVersion        uint32 `json:"row_version"`
	ScheduleVersion   uint32 `json:"schedule_version"`
	RoundAttemptCount uint32 `json:"round_attempt_count"`
	TotalAttemptCount uint32 `json:"total_attempt_count"`
	MaxAttempts       uint32 `json:"max_attempts"`
	ScheduledAt       string `json:"scheduled_at,omitempty"`
	RoundDeadlineAt   string `json:"round_deadline_at"`
	RoundFinishedAt   string `json:"round_finished_at,omitempty"`
	UnresolvedCount   int    `json:"unresolved_attempt_count"`
	RiskAuthorized    bool   `json:"risk_authorized"`
}

type resultBody struct {
	OperationID   string       `json:"operation_id"`
	OperationType string       `json:"operation_type"`
	DeliveryID    string       `json:"delivery_id"`
	CompletedAt   string       `json:"completed_at"`
	PreviousRound uint32       `json:"previous_round_no"`
	PolicyNarrow  bool         `json:"policy_narrowed,omitempty"`
	Task          TaskSnapshot `json:"task"`
}

func errOf(code, msg string) error {
	e, _ := domain.NewError(code, msg, "", nil)
	return e
}

// loadScoped 读取任务并校验调用方范围；无范围返回 404，不暴露其他调用方数据（OPS-10）。
func (u *UseCase) loadScoped(ctx context.Context, p Principal, id uint64) (data_interface.TaskRecord, error) {
	task, err := u.Tasks.LoadTask(ctx, id)
	if err != nil {
		return task, err
	}
	if !contains(p.Scopes, task.CallerID) {
		return data_interface.TaskRecord{}, errOf(constant.CodeNotFound, "任务不存在")
	}
	return task, nil
}

func validReason(reason string) bool {
	return strings.TrimSpace(reason) != "" && len(reason) <= 1024 && !domain.LooksSecret(reason)
}

// Retry 实现 OPS-R01..R03。
func (u *UseCase) Retry(ctx context.Context, p Principal, id uint64, req RetryRequest) (Result, error) {
	if !p.allows(constant.PermDeliveriesRetry) {
		return Result{}, errOf(constant.CodeForbidden, "无权限")
	}
	if req.Key == "" || len(req.Key) > 191 || req.ExpectedRow == 0 || !validReason(req.Reason) {
		return Result{}, errOf(constant.CodeInvalidRequest, "请求参数不合法")
	}
	task, err := u.loadScoped(ctx, p, id)
	if err != nil {
		return Result{}, err
	}
	fp, err := domain.OperationFingerprint(constant.OpManualRetry, id, req.ExpectedRow, req.Reason, req.RiskAck, "", nil)
	if err != nil {
		return Result{}, err
	}
	if r, done, err := u.replayed(ctx, task.CallerID, constant.OpManualRetry, req.Key, fp); done || err != nil {
		return r, err
	}
	cr, err := u.Constraints.CheckConstraints(ctx, task, u.now())
	if err != nil {
		return Result{}, err
	}
	risk, code := domain.CheckRetry(domain.RetryCheck{
		Task: task.Facts(), Now: u.now(), ExpectedRow: req.ExpectedRow, Constraint: cr.Code, Covers: cr.Covers,
		RiskPermission: p.allows(constant.PermDeliveriesRiskRetry), RiskAck: req.RiskAck,
	})
	if code != "" {
		return Result{}, errOf(code, "当前不能重投")
	}
	orig, err := domain.ParseFrozenPolicy(task.RoundPolicy)
	if err != nil {
		return Result{}, errOf(constant.CodeConfigurationUnavailable, "原轮次策略不可读")
	}
	policy, narrowed, err := domain.NarrowPolicy(orig, u.Config.MaxAllowedAttempts, u.Config.MaxRoundTimeout)
	if err != nil {
		return Result{}, err
	}
	opID, err := u.newID()
	if err != nil {
		return Result{}, err
	}
	var evidence []byte
	if risk || narrowed {
		evidence, _ = json.Marshal(map[string]any{
			"risk_acknowledged": risk, "risk_category": riskCategory(risk), "actor": p.ActorID,
			"original_max_attempts": orig.MaxAttempts, "new_max_attempts": policy.MaxAttempts,
			"original_round_timeout_seconds": int64(orig.RoundTimeout / time.Second), "new_round_timeout_seconds": int64(policy.RoundTimeout / time.Second),
		})
	}
	prevRound := task.RoundNo
	meta := u.meta(p, task, constant.OpManualRetry, opID, req.Key, fp, req.Reason, req.ExpectedRow, evidence, prevRound, narrowed)
	body, err := u.Ops.ReplayTask(ctx, data_interface.ReplayInput{OperationMeta: meta, Policy: policy, PolicyRaw: domain.EncodeFrozenPolicy(policy), Risk: risk})
	return u.settle(ctx, task.CallerID, constant.OpManualRetry, req.Key, fp, body, err)
}

// Resolve 实现 OPS-V01/V02。
func (u *UseCase) Resolve(ctx context.Context, p Principal, id uint64, req ResolveRequest) (Result, error) {
	if !p.allows(constant.PermDeliveriesResolve) {
		return Result{}, errOf(constant.CodeForbidden, "无权限")
	}
	if req.Key == "" || len(req.Key) > 191 || req.ExpectedRow == 0 || !validReason(req.Reason) ||
		(req.Resolution != "SUCCEEDED" && req.Resolution != "FAILED") {
		return Result{}, errOf(constant.CodeInvalidRequest, "请求参数不合法")
	}
	if req.Evidence.Scope == "" {
		if len(req.Evidence.CoveredAttemptIDs) > 0 {
			req.Evidence.Scope = domain.EvidenceScopeAttempts
		} else if req.Evidence.IdempotencyKey != "" {
			req.Evidence.Scope = domain.EvidenceScopeBusiness
		}
	}
	if code := domain.ValidateEvidenceShape(req.Evidence, u.maxEvidence()); code != "" {
		return Result{}, errOf(code, "核对证据不合法")
	}
	task, err := u.loadScoped(ctx, p, id)
	if err != nil {
		return Result{}, err
	}
	fp, err := domain.OperationFingerprint(constant.OpResolve, id, req.ExpectedRow, req.Reason, false, req.Resolution, &req.Evidence)
	if err != nil {
		return Result{}, err
	}
	if r, done, err := u.replayed(ctx, task.CallerID, constant.OpResolve, req.Key, fp); done || err != nil {
		return r, err
	}
	refs := make([]domain.AttemptRef, len(task.Attempts))
	for i, a := range task.Attempts {
		refs[i] = domain.AttemptRef{AttemptID: a.AttemptID}
	}
	if code := domain.CheckResolve(task.Facts(), task.IdempotencyKey, refs, req.ExpectedRow, req.Resolution, req.Evidence); code != "" {
		return Result{}, errOf(code, "核对条件不满足")
	}
	opID, err := u.newID()
	if err != nil {
		return Result{}, err
	}
	evidence, _ := json.Marshal(req.Evidence)
	meta := u.meta(p, task, constant.OpResolve, opID, req.Key, fp, req.Reason, req.ExpectedRow, evidence, task.RoundNo, false)
	body, err := u.Ops.ResolveTask(ctx, data_interface.ResolveInput{OperationMeta: meta, Resolution: req.Resolution, Ev: req.Evidence, MaxEvidence: u.maxEvidence()})
	return u.settle(ctx, task.CallerID, constant.OpResolve, req.Key, fp, body, err)
}

// replayed 先读唯一键：同键同参数返回原结果；同键不同参数 409。
func (u *UseCase) replayed(ctx context.Context, scope, opType, key, fp string) (Result, bool, error) {
	op, found, err := u.Ops.ReadOperation(ctx, scope, opType, key)
	if err != nil || !found {
		return Result{}, false, err
	}
	if op.Fingerprint != fp {
		return Result{}, true, errOf(constant.CodeIdempotencyConflict, "相同操作键的参数不一致")
	}
	return Result{Body: op.Result, Replayed: true}, true, nil
}

// settle 处理提交不确定与并发占键：先读唯一键核对，不可确认返回 503（OPS-09、OPS-11）。
func (u *UseCase) settle(ctx context.Context, scope, opType, key, fp string, body []byte, err error) (Result, error) {
	switch {
	case err == nil:
		return Result{Body: body}, nil
	case errors.Is(err, data_interface.ErrCommitUncertain), errors.Is(err, data_interface.ErrOperationKeyTaken):
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), u.confirmTimeout())
		defer cancel()
		r, done, rerr := u.replayed(cctx, scope, opType, key, fp)
		if done {
			return r, rerr
		}
		if errors.Is(err, data_interface.ErrOperationKeyTaken) {
			return Result{}, errOf(constant.CodeIdempotencyConflict, "操作键已被占用")
		}
		return Result{}, errOf(constant.CodeOperationUnconfirmed, "操作结果无法确认，请用原键原参数重试")
	default:
		return Result{}, err
	}
}

func (u *UseCase) meta(p Principal, task data_interface.TaskRecord, opType, opID, key, fp, reason string, expected uint32, evidence []byte, prevRound uint32, narrowed bool) data_interface.OperationMeta {
	return data_interface.OperationMeta{
		OperationID: opID, OperationTy: opType, Key: key, CallerScope: task.CallerID, Fingerprint: fp,
		Actor: p.ActorID, Reason: reason, Evidence: evidence, DeliveryID: task.DeliveryID, ExpectedRow: expected,
		Build: func(t data_interface.TaskRecord, at time.Time) ([]byte, error) {
			return json.Marshal(resultBody{
				OperationID: opID, OperationType: opType, DeliveryID: strconv.FormatUint(t.DeliveryID, 10),
				CompletedAt: at.UTC().Format(time.RFC3339Nano), PreviousRound: prevRound, PolicyNarrow: narrowed, Task: Snapshot(t),
			})
		},
	}
}

// Snapshot 生成脱敏任务快照。
func Snapshot(t data_interface.TaskRecord) TaskSnapshot {
	s := TaskSnapshot{
		DeliveryID: strconv.FormatUint(t.DeliveryID, 10), Status: string(t.Status), ResultCertainty: string(t.ResultCertainty),
		RoundNo: t.RoundNo, RowVersion: t.RowVersion, ScheduleVersion: t.ScheduleVersion, RoundAttemptCount: t.RoundAttemptCount,
		TotalAttemptCount: t.TotalAttemptCount, MaxAttempts: t.MaxAttempts, RoundDeadlineAt: t.RoundDeadlineAt.UTC().Format(time.RFC3339Nano),
		UnresolvedCount: len(t.Unresolved), RiskAuthorized: t.RiskOperationID != "",
	}
	if t.ScheduledAt != nil {
		s.ScheduledAt = t.ScheduledAt.UTC().Format(time.RFC3339Nano)
	}
	if t.RoundFinishedAt != nil {
		s.RoundFinishedAt = t.RoundFinishedAt.UTC().Format(time.RFC3339Nano)
	}
	return s
}

func riskCategory(risk bool) string {
	if risk {
		return constant.RiskCategoryDuplicateExec
	}
	return ""
}

func (u *UseCase) now() time.Time {
	if u.Now != nil {
		return u.Now().UTC()
	}
	return time.Now().UTC()
}

func (u *UseCase) newID() (string, error) {
	if u.NewID != nil {
		return u.NewID()
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (u *UseCase) maxEvidence() int {
	if u.Config.MaxEvidenceAttempts > 0 {
		return u.Config.MaxEvidenceAttempts
	}
	return 256
}

func (u *UseCase) confirmTimeout() time.Duration {
	if u.Config.ConfirmTimeout > 0 {
		return u.Config.ConfirmTimeout
	}
	return 2 * time.Second
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
