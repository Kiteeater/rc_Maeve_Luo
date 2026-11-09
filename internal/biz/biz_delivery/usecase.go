// Package biz_delivery 编排一次投递指令：核对、准备、领取、单次调用和结果计划。
// 真实 MQ 由 S04 完成，真实受控 HTTP 由 S05 完成。
package biz_delivery

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// Config 是执行预算。轮次次数和退避不在这里，它们来自任务冻结的 round_policy。
type Config struct {
	HTTPTimeout    time.Duration
	BuildTimeout   time.Duration
	LeaseTimeout   time.Duration
	ResultReserve  time.Duration
	CredentialWait time.Duration
	BlockedCheck   time.Duration
	SummaryMax     int
	WorkerID       string
	Deployment     string
}

// UseCase 是 worker 调用的执行编排。
type UseCase struct {
	Store    data_interface.DeliveryStore
	Runtime  data_interface.ProviderRuntime
	Schedule data_interface.ScheduleOriginal
	Now      func() time.Time
	Jitter   func() (uint32, error)
	// LateConflicts 是迟到证据与当前结论冲突的告警计数（S06 RCV-08）。
	LateConflicts atomic.Int64
	WaitJitter    func() (time.Duration, error)
	Allow         func(callerID string) bool
	Config        Config
}

// Execute 处理一条指令，并返回 ACK 结论和最新任务。
func (u *UseCase) Execute(ctx context.Context, cmd domain.DeliveryCommand) (domain.Step, data_interface.TaskRecord, error) {
	return u.execute(ctx, cmd, 0)
}

// ExecuteInstruction 供后台入口调用。receipt 不进入本方法。
func (u *UseCase) ExecuteInstruction(ctx context.Context, cmd domain.DeliveryCommand) (domain.Step, error) {
	step, _, err := u.Execute(ctx, cmd)
	return step, err
}

// ApplyResult 提交一次已经产生的尝试结果。旧领取走迟到证据，不覆盖当前任务。
func (u *UseCase) ApplyResult(ctx context.Context, own domain.Ownership, in ResultInput) (domain.Step, data_interface.TaskRecord, error) {
	task, err := u.Store.LoadTask(ctx, own.DeliveryID)
	if err != nil {
		return domain.Step{}, data_interface.TaskRecord{}, err
	}
	return u.finish(ctx, task, own, in, u.now())
}

// Resume 在阻塞条件解除时做一次条件恢复。重复调用只保留一个新计划。扫描循环由 S06 完成。
func (u *UseCase) Resume(ctx context.Context, deliveryID uint64) (domain.Step, data_interface.TaskRecord, error) {
	task, err := u.Store.LoadTask(ctx, deliveryID)
	if err != nil {
		return domain.Step{}, data_interface.TaskRecord{}, err
	}
	if task.Status != enums.TaskBlocked {
		return domain.Step{ACK: true}, task, nil
	}
	prep, err := u.prepare(ctx, task)
	if err != nil {
		return domain.Step{}, task, err
	}
	allowed := u.allow(task.CallerID) && (len(task.Unresolved) == 0 || prep.IdempotencyCovers || domain.RiskAuthAvailable(factsOf(task)))
	if prep.Kind == domain.PrepareBlock || prep.Kind == domain.PrepareUnsafe {
		allowed = false
	}
	plan, err := domain.ResumePlan(factsOf(task), u.now(), allowed)
	if err != nil {
		return domain.Step{}, task, err
	}
	rec, err := u.Store.ResumeBlocked(ctx, data_interface.ResumeInput{
		DeliveryID: task.DeliveryID, ExpectedRowVersion: task.RowVersion,
		ScheduleVersion: task.ScheduleVersion, RoundNo: task.RoundNo, Plan: plan,
	})
	return u.afterWrite(ctx, task, 0, plan, rec, err)
}

// ResultInput 是一次调用或明确未发送的事实。
type ResultInput struct {
	SendState         enums.SendState
	Outcome           enums.Outcome
	Retryable         bool
	ErrorCode         string
	ErrorReason       string
	RequestSummary    string
	ResponseSummary   string
	RequestTruncated  bool
	ResponseTruncated bool
	HTTPStatus        *int
	ProviderRequestID string
	Evidence          []byte
	HTTPDuration      *time.Duration
	AttemptDuration   *time.Duration
	ConfirmedNotSent  bool
	Constraint        string
	CoversBusinessKey bool
	CoveredAttemptIDs []uint64
	Conflict          bool
	IdempotencyCovers bool
	SupplierNotBefore time.Time
	RoundAttemptCount uint32
	RoundNo           uint32
	Prior             []domain.UnresolvedRef
}

func (u *UseCase) execute(ctx context.Context, cmd domain.DeliveryCommand, depth int) (domain.Step, data_interface.TaskRecord, error) {
	if err := ctx.Err(); err != nil {
		return domain.Step{}, data_interface.TaskRecord{}, err
	}
	if err := domain.ValidateDeliveryCommand(cmd); err != nil {
		return domain.Step{}, data_interface.TaskRecord{}, err
	}
	task, err := u.Store.LoadTask(ctx, cmd.DeliveryID)
	if err != nil {
		return domain.Step{}, data_interface.TaskRecord{}, err
	}
	kind, err := domain.InspectInstruction(cmd, factsOf(task), u.now())
	if err != nil {
		return domain.Step{}, task, err
	}
	switch kind {
	case domain.InspectStale, domain.InspectActiveOwner:
		return domain.Step{ACK: true}, task, nil
	case domain.InspectRecovery:
		return domain.Step{}, task, nil
	case domain.InspectEarly:
		when := u.now()
		if task.ScheduledAt != nil {
			when = *task.ScheduledAt
		}
		if err := u.Schedule.ScheduleOriginal(ctx, cmd, when); err != nil {
			return domain.Step{}, task, err
		}
		return domain.Step{ACK: true}, task, nil
	case domain.InspectPrepare:
		return u.prepareAndMaybeStart(ctx, cmd, task, depth)
	default:
		return domain.Step{}, task, domainErr(constant.CodeInternal, "指令处置不合法")
	}
}

func (u *UseCase) prepareAndMaybeStart(ctx context.Context, cmd domain.DeliveryCommand, task data_interface.TaskRecord, depth int) (domain.Step, data_interface.TaskRecord, error) {
	policy, err := domain.ParseFrozenPolicy(task.RoundPolicy)
	if err != nil {
		return domain.Step{}, task, err
	}
	prep, err := u.prepare(ctx, task)
	if err != nil {
		return domain.Step{}, task, err
	}
	jitter, err := u.waitJitter()
	if err != nil {
		return domain.Step{}, task, err
	}
	before := domain.BeforeFacts{
		Now: u.now(), Deadline: task.RoundDeadlineAt, Status: task.Status,
		RoundAttemptCount: task.RoundAttemptCount, MaxAttempts: policy.MaxAttempts,
		HasUnresolved: len(task.Unresolved) > 0, IdempotencyCovers: prep.IdempotencyCovers,
		RiskAuthAvailable: domain.RiskAuthAvailable(factsOf(task)) && u.allow(task.CallerID),
		Prepare:           prep.Kind, Reason: prep.Reason, ErrorCode: prep.ErrorCode,
		RateLimitUntil: prep.RateLimitUntil, SupplierNotBefore: prep.SupplierNotBefore,
		CredentialWait: credentialWait(prep, u.Config.CredentialWait), CredentialJitter: jitter,
		BlockedCheck: u.Config.BlockedCheck, ScheduledAt: derefTime(task.ScheduledAt),
	}
	if !u.allow(task.CallerID) && prep.Kind == domain.PrepareReady {
		before.Prepare = domain.PrepareBlock
		before.ErrorCode = constant.ErrCallerForbidden
		before.Reason = "调用方已失去发送权限"
	}
	plan, ready, err := domain.DecideBeforeAttempt(before)
	if err != nil {
		return domain.Step{}, task, err
	}
	if !ready {
		rec, werr := u.Store.WaitOrBlock(ctx, data_interface.WaitOrBlockInput{
			DeliveryID: task.DeliveryID, EventID: task.EventID, RoundNo: task.RoundNo,
			ScheduleVersion: task.ScheduleVersion, ExpectedRowVersion: task.RowVersion,
			OutboxID: task.OutboxID, Plan: plan,
		})
		return u.afterWrite(ctx, task, 0, plan, rec, werr)
	}
	started, serr := u.Store.StartAttempt(ctx, data_interface.StartAttemptInput{
		DeliveryID: task.DeliveryID, EventID: task.EventID, RoundNo: task.RoundNo,
		ScheduleVersion: task.ScheduleVersion, ExpectedRowVersion: task.RowVersion, OutboxID: task.OutboxID,
		WorkerID: u.Config.WorkerID, DeploymentVersion: u.Config.Deployment, TraceID: "",
		ExpectedChannelVersion: prep.ChannelVersion, ExpectedReleaseID: prep.ReleaseID,
		LeaseTimeout: u.Config.LeaseTimeout, SafetyNotAfter: prep.SafeUntil,
		UseRiskAuthorization: task.RiskOperationID != "" && task.RiskAttemptID == nil && task.RoundAttemptCount == 0,
	})
	if errors.Is(serr, data_interface.ErrPreconditionChanged) {
		if depth >= 1 {
			return domain.Step{}, task, serr
		}
		return u.execute(ctx, cmd, depth+1)
	}
	if errors.Is(serr, data_interface.ErrCommitUncertain) || errors.Is(serr, data_interface.ErrUnconfirmed) {
		loaded, rerr := u.Store.ReadDisposition(ctx, task.DeliveryID, 0)
		if rerr != nil || loaded.CurrentAttemptID == nil || loaded.Status != enums.TaskProcessing {
			return domain.Step{Unconfirmed: true}, loaded, nil
		}
		started = data_interface.StartAttemptResult{
			Ownership: domain.Ownership{
				DeliveryID: loaded.DeliveryID, RoundNo: loaded.RoundNo, ScheduleVersion: loaded.ScheduleVersion,
				AttemptID: *loaded.CurrentAttemptID, LeaseToken: loaded.LeaseToken, LeaseExpiresAt: derefTime(loaded.LeaseExpiresAt),
			},
			AttemptNo: loaded.RoundAttemptCount, StartedAt: u.now(), RowVersion: loaded.RowVersion,
			RoundAttemptCount: loaded.RoundAttemptCount, TotalAttemptCount: loaded.TotalAttemptCount,
			ChannelVersion: prep.ChannelVersion,
		}
		task = loaded
	} else if errors.Is(serr, data_interface.ErrOwnershipLost) {
		loaded, rerr := u.Store.LoadTask(ctx, task.DeliveryID)
		if rerr != nil {
			return domain.Step{}, task, nil
		}
		kind, ierr := domain.InspectInstruction(cmd, factsOf(loaded), u.now())
		if ierr == nil && (kind == domain.InspectStale || kind == domain.InspectActiveOwner) {
			return domain.Step{ACK: true}, loaded, nil
		}
		return domain.Step{}, loaded, nil
	} else if serr != nil {
		return domain.Step{}, task, serr
	}
	if ctx.Err() != nil {
		return domain.Step{}, task, ctx.Err()
	}
	return u.afterLease(ctx, task, prep, started)
}

func (u *UseCase) afterLease(ctx context.Context, task data_interface.TaskRecord, prep data_interface.PrepareResponse, started data_interface.StartAttemptResult) (domain.Step, data_interface.TaskRecord, error) {
	own := started.Ownership
	recheck, err := u.prepare(ctx, task)
	if err != nil {
		return domain.Step{}, task, err
	}
	switch {
	case !u.allow(task.CallerID):
		return u.finishNotSent(ctx, task, own, started, constant.ErrCallerForbidden, "调用方已失去发送权限", constant.ErrCallerForbidden, recheck, time.Time{})
	case recheck.ChannelVersion != prep.ChannelVersion || recheck.ReleaseID != prep.ReleaseID:
		return u.finishNotSent(ctx, task, own, started, constant.ErrConfigurationUnavail, "通道或发布版本已变化", "", recheck, time.Time{})
	case recheck.Kind == domain.PrepareBlock || blocking(recheck.ErrorCode):
		code := recheck.ErrorCode
		if !blocking(code) {
			code = constant.ErrChannelDisabled
		}
		return u.finishNotSent(ctx, task, own, started, code, reasonOr(recheck.Reason, "领取后约束已变化"), code, recheck, time.Time{})
	case recheck.Kind == domain.PrepareWait || recheck.Kind == domain.PrepareUnsafe:
		return u.finishNotSent(ctx, task, own, started, codeOr(recheck.ErrorCode, constant.ErrRateLimitWait), reasonOr(recheck.Reason, "领取后不能发送"), "", recheck, recheck.RateLimitUntil)
	}
	remain := own.LeaseExpiresAt.Sub(u.now())
	if remain <= u.Config.ResultReserve {
		return u.finishNotSent(ctx, task, own, started, constant.ErrLeaseExpired, "剩余领取时间不足以发送", "", recheck, time.Time{})
	}
	budget := u.Config.HTTPTimeout
	if gap := remain - u.Config.ResultReserve; gap < budget {
		budget = gap
	}
	buildCtx, cancel := context.WithTimeout(ctx, u.Config.BuildTimeout)
	built, berr := u.Runtime.Build(buildCtx, task, own, prep)
	cancel()
	if berr != nil {
		if ctx.Err() != nil {
			return domain.Step{}, task, ctx.Err()
		}
		return u.finishNotSent(ctx, task, own, started, constant.ErrBuildFailed, "请求构造失败", "", recheck, time.Time{})
	}
	if ctx.Err() != nil {
		return domain.Step{}, task, ctx.Err()
	}
	sendCtx, cancelSend := context.WithTimeout(ctx, budget)
	defer cancelSend()
	if err := u.Store.RecordInvocation(ctx, data_interface.RecordInvocationInput{
		Ownership: own, RequestStartedAt: u.now(), Method: built.Method, TargetPath: built.TargetPath,
		RequestSummary: clip(built.Summary, u.Config.SummaryMax), RequestTruncated: built.Truncated, RequestSize: built.Size,
	}); err != nil {
		if errors.Is(err, data_interface.ErrCommitUncertain) || errors.Is(err, data_interface.ErrUnconfirmed) {
			return domain.Step{Unconfirmed: true}, task, nil
		}
		return domain.Step{}, task, err
	}
	startedAt := time.Now()
	sent, serr := u.Runtime.SendOnce(sendCtx, built)
	httpDur := time.Since(startedAt)
	if ctx.Err() != nil && sent.HTTPStatus == 0 && sent.ErrorCode == "" && sent.BodySummary == "" {
		return domain.Step{}, task, ctx.Err()
	}
	if serr != nil && sent.ErrorCode == "" {
		sent.ErrorCode = constant.ErrConnection
		sent.TransportErr = "transport"
	}
	view, ierr := u.Runtime.Interpret(ctx, task, built, sent)
	if ierr != nil {
		view = data_interface.Interpreted{Outcome: enums.OutcomeUncertain, Certainty: enums.CertaintyUncertain, ErrorCode: constant.ErrResponseRead, Retryable: false}
	}
	in := ResultInput{
		SendState: enums.SendHTTPInvoked, Outcome: view.Outcome, Retryable: view.Retryable,
		ErrorCode: view.ErrorCode, ErrorReason: view.ErrorReason,
		RequestSummary: clip(built.Summary, u.Config.SummaryMax), ResponseSummary: clip(sent.BodySummary, u.Config.SummaryMax),
		RequestTruncated: built.Truncated, ResponseTruncated: sent.Truncated, ProviderRequestID: firstNonEmpty(sent.ProviderRequestID, view.ProviderRequestID),
		Evidence: view.EvidenceScope, HTTPDuration: &httpDur, ConfirmedNotSent: false,
		CoversBusinessKey: view.CoversBusinessKey, CoveredAttemptIDs: view.CoveredAttemptIDs, Conflict: view.Conflict,
		IdempotencyCovers: recheck.IdempotencyCovers, SupplierNotBefore: laterTime(view.SupplierNotBefore, recheck.SupplierNotBefore),
		RoundAttemptCount: started.RoundAttemptCount, RoundNo: own.RoundNo, Prior: data_interface.ToDomainUnresolved(task.Unresolved),
	}
	if sent.HTTPStatus > 0 {
		code := sent.HTTPStatus
		in.HTTPStatus = &code
	}
	if in.Outcome == "" {
		in.Outcome = enums.OutcomeUncertain
	}
	return u.finish(ctx, task, own, in, started.StartedAt)
}

func (u *UseCase) finishNotSent(ctx context.Context, task data_interface.TaskRecord, own domain.Ownership, started data_interface.StartAttemptResult, code, reason, constraint string, recheck data_interface.PrepareResponse, supplier time.Time) (domain.Step, data_interface.TaskRecord, error) {
	in := ResultInput{
		SendState: enums.SendNotSent, Outcome: enums.OutcomeLocalFailure, Retryable: true,
		ErrorCode: code, ErrorReason: reason, ConfirmedNotSent: true, Constraint: constraint,
		IdempotencyCovers: recheck.IdempotencyCovers, SupplierNotBefore: laterTime(supplier, recheck.SupplierNotBefore),
		RoundAttemptCount: started.RoundAttemptCount, RoundNo: own.RoundNo, Prior: data_interface.ToDomainUnresolved(task.Unresolved),
	}
	return u.finish(ctx, task, own, in, started.StartedAt)
}

func (u *UseCase) finish(ctx context.Context, task data_interface.TaskRecord, own domain.Ownership, in ResultInput, started time.Time) (domain.Step, data_interface.TaskRecord, error) {
	policy, err := domain.ParseFrozenPolicy(task.RoundPolicy)
	if err != nil {
		return domain.Step{}, task, err
	}
	numer, err := u.jitter()
	if err != nil {
		return domain.Step{}, task, err
	}
	attemptDur := u.now().Sub(started)
	facts := domain.AttemptFacts{
		Now: u.now(), Deadline: task.RoundDeadlineAt, Policy: policy, RoundAttemptCount: in.RoundAttemptCount,
		AttemptID: own.AttemptID, RoundNo: in.RoundNo, Outcome: in.Outcome, SendState: in.SendState, Retryable: in.Retryable,
		ErrorCode: in.ErrorCode, Prior: in.Prior, CoversBusinessKey: in.CoversBusinessKey, CoveredAttemptIDs: in.CoveredAttemptIDs,
		Conflict: in.Conflict, IdempotencyCovers: in.IdempotencyCovers, ConfirmedNotSent: in.ConfirmedNotSent,
		Constraint: in.Constraint, JitterNumer: numer, SupplierNotBefore: in.SupplierNotBefore, BlockedCheck: u.Config.BlockedCheck,
	}
	plan, err := domain.DecideAfterAttempt(facts)
	if err != nil {
		return domain.Step{}, task, err
	}
	req, reqCut := domain.TruncateUTF8(in.RequestSummary, u.summaryMax())
	resp, respCut := domain.TruncateUTF8(in.ResponseSummary, u.summaryMax())
	reason, reasonCut := domain.TruncateUTF8(in.ErrorReason, u.summaryMax())
	var httpDur *time.Duration
	if in.SendState != enums.SendNotSent {
		httpDur = in.HTTPDuration
	}
	rec, ferr := u.Store.FinishAttemptAndPlan(ctx, data_interface.FinishInput{
		Ownership: own, SendState: in.SendState, Outcome: in.Outcome, ResultCertainty: plan.LastCertainty,
		Retryable: in.Retryable, ErrorCode: plan.ErrorCode, ErrorReason: reason, ErrorTruncated: reasonCut || in.ErrorReason != reason,
		RequestSummary: req, ResponseSummary: resp, RequestTruncated: reqCut || in.RequestTruncated, ResponseTruncated: respCut || in.ResponseTruncated,
		HTTPStatus: in.HTTPStatus, ProviderRequestID: in.ProviderRequestID, EvidenceScope: in.Evidence,
		HTTPDuration: httpDur, AttemptDuration: &attemptDur, Plan: plan,
	})
	if errors.Is(ferr, data_interface.ErrOwnershipLost) {
		lerr := u.Store.AppendLateEvidence(ctx, data_interface.LateEvidenceInput{
			DeliveryID: own.DeliveryID, AttemptID: own.AttemptID, Reason: "迟到结果不推进任务", Evidence: in.Evidence,
		})
		if lerr != nil {
			return domain.Step{Unconfirmed: true}, task, nil
		}
		loaded, rerr := u.Store.LoadTask(ctx, own.DeliveryID)
		if rerr != nil {
			return domain.Step{Unconfirmed: true}, task, nil
		}
		if domain.LateConflict(loaded.Status, in.Outcome) {
			u.LateConflicts.Add(1) // 人工核对告警；结案由 S07 授权核对完成
		}
		return domain.Step{ACK: true}, loaded, nil
	}
	return u.afterWrite(ctx, task, own.AttemptID, plan, rec, ferr)
}

func (u *UseCase) afterWrite(ctx context.Context, before data_interface.TaskRecord, attemptID uint64, plan domain.Plan, rec data_interface.TaskRecord, err error) (domain.Step, data_interface.TaskRecord, error) {
	if errors.Is(err, data_interface.ErrCommitUncertain) || errors.Is(err, data_interface.ErrUnconfirmed) {
		loaded, rerr := u.Store.ReadDisposition(ctx, before.DeliveryID, attemptID)
		if rerr != nil {
			return domain.Step{Unconfirmed: true}, before, nil
		}
		if attemptID != 0 && !attemptDone(loaded, attemptID) {
			return domain.Step{Unconfirmed: true}, loaded, nil
		}
		if attemptID == 0 && loaded.ScheduleVersion == before.ScheduleVersion && loaded.Status == before.Status {
			return domain.Step{Unconfirmed: true}, loaded, nil
		}
		return domain.Step{ACK: true}, loaded, nil
	}
	if err != nil {
		return domain.Step{}, before, err
	}
	return domain.Step{ACK: true}, rec, nil
}

func (u *UseCase) prepare(ctx context.Context, task data_interface.TaskRecord) (data_interface.PrepareResponse, error) {
	return u.Runtime.Prepare(ctx, data_interface.PrepareRequest{Task: task, Now: u.now(), CallerAllowed: u.allow(task.CallerID)})
}

func (u *UseCase) now() time.Time {
	if u.Now != nil {
		return u.Now().UTC()
	}
	return time.Now().UTC()
}

func (u *UseCase) jitter() (uint32, error) {
	if u.Jitter != nil {
		return u.Jitter()
	}
	return randomJitter()
}

func (u *UseCase) waitJitter() (time.Duration, error) {
	if u.WaitJitter != nil {
		return u.WaitJitter()
	}
	var buf [2]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, err
	}
	n := binary.BigEndian.Uint16(buf[:]) % 1001
	return time.Duration(n) * time.Millisecond, nil
}

func (u *UseCase) allow(callerID string) bool {
	if u.Allow == nil {
		return true
	}
	return u.Allow(callerID)
}

func (u *UseCase) summaryMax() int {
	if u.Config.SummaryMax <= 0 {
		return 2048
	}
	return u.Config.SummaryMax
}

func randomJitter() (uint32, error) {
	var buf [2]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, err
	}
	n := binary.BigEndian.Uint16(buf[:]) % 5001
	return 5000 + uint32(n), nil
}

func factsOf(task data_interface.TaskRecord) domain.TaskFacts { return task.Facts() }

func attemptDone(task data_interface.TaskRecord, attemptID uint64) bool {
	for _, item := range task.Attempts {
		if item.AttemptID == attemptID && item.Status == "FINISHED" {
			return true
		}
	}
	return false
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}

func credentialWait(prep data_interface.PrepareResponse, fallback time.Duration) time.Duration {
	if prep.ErrorCode == constant.ErrAuthWait || prep.Kind == domain.PrepareWait && prep.RateLimitUntil.IsZero() {
		return fallback
	}
	return 0
}

func blocking(code string) bool {
	switch code {
	case constant.ErrChannelDisabled, constant.ErrCallerForbidden, constant.ErrAdapterUnavailable, constant.ErrConfigurationUnavail, constant.ErrTargetForbidden:
		return true
	default:
		return false
	}
}

func clip(s string, max int) string {
	out, _ := domain.TruncateUTF8(s, max)
	return out
}

func reasonOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func codeOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func laterTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func domainErr(code, message string) error {
	err, _ := domain.NewError(code, message, "", nil)
	return err
}
