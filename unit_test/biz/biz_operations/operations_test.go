package biz_operations_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"notifier/internal/biz/biz_operations"
	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

const policy = `{"backoff_cap_ms":60000,"backoff_initial_ms":1000,"max_attempts":8,"round_timeout_seconds":86400,"server_wait_cap_seconds":3600}`

var now = time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)

// world 是共享任务表与管理操作表的内存实现，按 mysql.OperationStore 的事务语义串行化。
type world struct {
	data_interface.DeliveryStore
	mu       sync.Mutex
	tasks    map[uint64]*data_interface.TaskRecord
	ops      map[string]data_interface.StoredOperation
	rounds   int
	outboxes int
	loseResp bool
}

func newWorld(tasks ...data_interface.TaskRecord) *world {
	w := &world{tasks: map[uint64]*data_interface.TaskRecord{}, ops: map[string]data_interface.StoredOperation{}}
	for i := range tasks {
		t := tasks[i]
		w.tasks[t.DeliveryID] = &t
	}
	return w
}

func opKey(scope, ty, key string) string { return scope + "|" + ty + "|" + key }

func (w *world) LoadTask(_ context.Context, id uint64) (data_interface.TaskRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	t, ok := w.tasks[id]
	if !ok {
		e, _ := domain.NewError(constant.CodeNotFound, "nf", "", nil)
		return data_interface.TaskRecord{}, e
	}
	return *t, nil
}

func (w *world) ReadOperation(_ context.Context, scope, ty, key string) (data_interface.StoredOperation, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	op, ok := w.ops[opKey(scope, ty, key)]
	return op, ok, nil
}

func derr(code string) error { e, _ := domain.NewError(code, code, "", nil); return e }

func (w *world) begin(m data_interface.OperationMeta) ([]byte, bool, error) {
	if op, ok := w.ops[opKey(m.CallerScope, m.OperationTy, m.Key)]; ok {
		if op.Fingerprint != m.Fingerprint {
			return nil, true, derr(constant.CodeIdempotencyConflict)
		}
		return op.Result, true, nil
	}
	return nil, false, nil
}

func (w *world) finish(m data_interface.OperationMeta, t *data_interface.TaskRecord) ([]byte, error) {
	body, err := m.Build(*t, now)
	if err != nil {
		return nil, err
	}
	w.ops[opKey(m.CallerScope, m.OperationTy, m.Key)] = data_interface.StoredOperation{Fingerprint: m.Fingerprint, Result: body}
	if w.loseResp {
		w.loseResp = false
		return nil, data_interface.ErrCommitUncertain
	}
	return body, nil
}

func (w *world) ReplayTask(_ context.Context, in data_interface.ReplayInput) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if res, done, err := w.begin(in.OperationMeta); done {
		return res, err
	}
	cur := w.tasks[in.DeliveryID]
	if cur.Status != enums.TaskFailed && cur.Status != enums.TaskUnknown || cur.LeaseToken != "" {
		return nil, derr(constant.CodeStateConflict)
	}
	if cur.RowVersion != in.ExpectedRow {
		return nil, derr(constant.CodeVersionConflict)
	}
	next := *cur
	next.RoundNo++
	next.Status, next.RowVersion, next.ScheduleVersion = enums.TaskPending, cur.RowVersion+1, cur.ScheduleVersion+1
	next.MaxAttempts, next.RoundPolicy, next.RoundAttemptCount = in.Policy.MaxAttempts, in.PolicyRaw, 0
	if in.Risk {
		next.RiskOperationID = in.OperationID
	}
	body, err := w.finish(in.OperationMeta, &next)
	if err != nil && !errors.Is(err, data_interface.ErrCommitUncertain) {
		return nil, err
	}
	*cur = next
	w.rounds++
	w.outboxes++
	return body, err
}

func (w *world) ResolveTask(_ context.Context, in data_interface.ResolveInput) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if res, done, err := w.begin(in.OperationMeta); done {
		return res, err
	}
	cur := w.tasks[in.DeliveryID]
	refs := []domain.AttemptRef{}
	for _, a := range cur.Attempts {
		refs = append(refs, domain.AttemptRef{AttemptID: a.AttemptID})
	}
	if code := domain.CheckResolve(cur.Facts(), cur.IdempotencyKey, refs, in.ExpectedRow, in.Resolution, in.Ev); code != "" {
		return nil, derr(code)
	}
	next := *cur
	next.Status, next.RowVersion = enums.TaskStatus(in.Resolution), cur.RowVersion+1
	next.Unresolved = nil
	body, err := w.finish(in.OperationMeta, &next)
	if err != nil && !errors.Is(err, data_interface.ErrCommitUncertain) {
		return nil, err
	}
	*cur = next
	return body, err
}

type checker struct {
	res data_interface.ConstraintResult
}

func (c checker) CheckConstraints(context.Context, data_interface.TaskRecord, time.Time) (data_interface.ConstraintResult, error) {
	return c.res, nil
}

func task(id uint64, status enums.TaskStatus) data_interface.TaskRecord {
	return data_interface.TaskRecord{
		DeliveryID: id, EventID: 10, CallerID: "crm", RouteTargetID: "target", IdempotencyKey: "n1_local_" + string(rune('0'+id)),
		PayloadCanonical: []byte(`{"ok":true}`), RoundPolicy: []byte(policy), Status: status, ResultCertainty: enums.CertaintyNotReceived,
		RoundNo: 1, ScheduleVersion: 3, RowVersion: 18, MaxAttempts: 8, TotalAttemptCount: 8, RoundAttemptCount: 8,
		RoundDeadlineAt: now.Add(-time.Hour),
		Attempts:        []data_interface.AttemptRecord{{AttemptID: 4001, DeliveryID: id, RoundNo: 1}, {AttemptID: 4002, DeliveryID: id, RoundNo: 1}},
	}
}

func unknownTask(id uint64) data_interface.TaskRecord {
	t := task(id, enums.TaskUnknown)
	t.ResultCertainty = enums.CertaintyUncertain
	t.Unresolved = []data_interface.UnresolvedRef{{AttemptID: 4001, RoundNo: 1}, {AttemptID: 4002, RoundNo: 1}}
	return t
}

func usecase(w *world, c data_interface.ConstraintResult) *biz_operations.UseCase {
	return &biz_operations.UseCase{Tasks: w, Ops: w, Constraints: checker{c}, Now: func() time.Time { return now },
		Config: biz_operations.Config{MaxEvidenceAttempts: 256, ConfirmTimeout: time.Second, MaxAllowedAttempts: 32, MaxRoundTimeout: 168 * time.Hour}}
}

var operator = biz_operations.Principal{ActorID: "ops-1", Scopes: []string{"crm"},
	Permissions: []string{constant.PermDeliveriesRetry, constant.PermDeliveriesResolve}}

func riskOperator() biz_operations.Principal {
	p := operator
	p.Permissions = append(append([]string(nil), p.Permissions...), constant.PermDeliveriesRiskRetry)
	return p
}

func retryReq(key string) biz_operations.RetryRequest {
	return biz_operations.RetryRequest{Key: key, ExpectedRow: 18, Reason: "供应商故障已恢复，按原计划重新投递"}
}

func codeOf(err error) string {
	var de *domain.Error
	if errors.As(err, &de) {
		return de.Code
	}
	return ""
}

type body struct {
	OperationID   string `json:"operation_id"`
	OperationType string `json:"operation_type"`
	PolicyNarrow  bool   `json:"policy_narrowed"`
	Task          struct {
		Status          string `json:"status"`
		RoundNo         uint32 `json:"round_no"`
		RowVersion      uint32 `json:"row_version"`
		MaxAttempts     uint32 `json:"max_attempts"`
		UnresolvedCount int    `json:"unresolved_attempt_count"`
		RiskAuthorized  bool   `json:"risk_authorized"`
	} `json:"task"`
}

func decode(t *testing.T, raw []byte) body {
	t.Helper()
	var b body
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if strings.Contains(string(raw), `"ok":true`) {
		t.Fatalf("result must not copy payload: %s", raw)
	}
	return b
}

func TestOPS01ConcurrentSameKeySameParamsCreatesOneRound(t *testing.T) {
	w := newWorld(task(1, enums.TaskFailed))
	uc := usecase(w, data_interface.ConstraintResult{})
	var wg sync.WaitGroup
	results := make([]biz_operations.Result, 8)
	errs := make([]error, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = uc.Retry(context.Background(), operator, 1, retryReq("retry-crm-001"))
		}(i)
	}
	wg.Wait()
	ids := map[string]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		ids[decode(t, results[i].Body).OperationID] = true
	}
	if len(ids) != 1 || w.rounds != 1 || w.outboxes != 1 {
		t.Fatalf("ids=%d rounds=%d outboxes=%d", len(ids), w.rounds, w.outboxes)
	}
	if got := w.tasks[1]; got.RoundNo != 2 || got.Status != enums.TaskPending || got.TotalAttemptCount != 8 {
		t.Fatalf("task after retry: %+v", got)
	}
}

func TestOPS02SameKeyDifferentParamsConflicts(t *testing.T) {
	w := newWorld(task(1, enums.TaskFailed), task(2, enums.TaskFailed))
	uc := usecase(w, data_interface.ConstraintResult{})
	if _, err := uc.Retry(context.Background(), operator, 1, retryReq("k")); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*biz_operations.RetryRequest){
		func(r *biz_operations.RetryRequest) { r.Reason = "另一个原因" },
		func(r *biz_operations.RetryRequest) { r.ExpectedRow = 19 },
		func(r *biz_operations.RetryRequest) { r.RiskAck = true },
	}
	for i, m := range mutations {
		r := retryReq("k")
		m(&r)
		if _, err := uc.Retry(context.Background(), operator, 1, r); codeOf(err) != constant.CodeIdempotencyConflict {
			t.Fatalf("mutation %d: %v", i, err)
		}
	}
	// 同键换任务：同一调用方同类型键已被占用。
	if _, err := uc.Retry(context.Background(), operator, 2, retryReq("k")); codeOf(err) != constant.CodeIdempotencyConflict {
		t.Fatalf("other task: %v", err)
	}
	if w.rounds != 1 || w.tasks[2].RoundNo != 1 {
		t.Fatalf("rounds=%d task2=%d", w.rounds, w.tasks[2].RoundNo)
	}
}

func TestOPS03DifferentKeysSameVersionAtMostOneWins(t *testing.T) {
	w := newWorld(task(1, enums.TaskFailed))
	uc := usecase(w, data_interface.ConstraintResult{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, k := range []string{"a", "b"} {
		wg.Add(1)
		go func(i int, k string) {
			defer wg.Done()
			_, errs[i] = uc.Retry(context.Background(), operator, 1, retryReq(k))
		}(i, k)
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		switch codeOf(err) {
		case "":
			ok++
		case constant.CodeVersionConflict, constant.CodeStateConflict:
		default:
			t.Fatalf("unexpected: %v", err)
		}
	}
	if ok != 1 || w.rounds != 1 {
		t.Fatalf("ok=%d rounds=%d", ok, w.rounds)
	}
}

func TestOPS04SucceededOrActiveTaskRejected(t *testing.T) {
	cases := []data_interface.TaskRecord{task(1, enums.TaskSucceeded), task(2, enums.TaskProcessing), task(3, enums.TaskRetryWait), task(4, enums.TaskBlocked), task(5, enums.TaskPending)}
	leased := task(6, enums.TaskFailed)
	leased.LeaseToken = "lease"
	cases = append(cases, leased)
	w := newWorld(cases...)
	uc := usecase(w, data_interface.ConstraintResult{})
	for _, c := range cases {
		if _, err := uc.Retry(context.Background(), operator, c.DeliveryID, retryReq("k"+string(rune('0'+c.DeliveryID)))); codeOf(err) != constant.CodeStateConflict {
			t.Fatalf("task %d %s: %v", c.DeliveryID, c.Status, err)
		}
	}
	if w.rounds != 0 || len(w.ops) != 0 {
		t.Fatalf("side effects: rounds=%d ops=%d", w.rounds, len(w.ops))
	}
}

func TestOPS05UnknownWithoutSafetyOrRiskRejected(t *testing.T) {
	w := newWorld(unknownTask(1))
	uc := usecase(w, data_interface.ConstraintResult{Covers: false})
	r := retryReq("k1")
	if _, err := uc.Retry(context.Background(), riskOperator(), 1, r); codeOf(err) != constant.CodeUnsafeReplay {
		t.Fatalf("no ack: %v", err)
	}
	r = retryReq("k2")
	r.RiskAck = true
	if _, err := uc.Retry(context.Background(), operator, 1, r); codeOf(err) != constant.CodeUnsafeReplay {
		t.Fatalf("no permission: %v", err)
	}
	if w.tasks[1].Status != enums.TaskUnknown || w.rounds != 0 {
		t.Fatalf("task changed: %+v", w.tasks[1])
	}
	// 渠道阻塞与配置不可用不能借人工操作绕过。
	for code, want := range map[string]string{constant.ErrChannelDisabled: constant.CodeChannelBlocked, constant.ErrConfigurationUnavail: constant.CodeConfigurationUnavailable} {
		uc := usecase(w, data_interface.ConstraintResult{Code: code})
		if _, err := uc.Retry(context.Background(), riskOperator(), 1, r); codeOf(err) != want {
			t.Fatalf("%s: %v", code, err)
		}
	}
}

func TestOPS06RiskRetryKeepsHistoryAndAudits(t *testing.T) {
	w := newWorld(unknownTask(1))
	uc := usecase(w, data_interface.ConstraintResult{})
	r := retryReq("risk-1")
	r.RiskAck = true
	out, err := uc.Retry(context.Background(), riskOperator(), 1, r)
	if err != nil {
		t.Fatal(err)
	}
	b := decode(t, out.Body)
	got := w.tasks[1]
	if !b.Task.RiskAuthorized || got.RiskOperationID != b.OperationID || b.Task.RoundNo != 2 {
		t.Fatalf("risk audit missing: %+v %+v", b, got)
	}
	if got.IdempotencyKey != "n1_local_1" || string(got.PayloadCanonical) != `{"ok":true}` || got.TotalAttemptCount != 8 || len(got.Unresolved) != 2 || len(got.Attempts) != 2 {
		t.Fatalf("original facts lost: %+v", got)
	}
	// 供应商幂等覆盖全部未决时无需风险授权。
	w2 := newWorld(unknownTask(2))
	if _, err := usecase(w2, data_interface.ConstraintResult{Covers: true}).Retry(context.Background(), operator, 2, retryReq("safe")); err != nil || w2.tasks[2].RiskOperationID != "" {
		t.Fatalf("covered retry: %v %+v", err, w2.tasks[2])
	}
}

func resolveReq(key, resolution string, ids ...uint64) biz_operations.ResolveRequest {
	return biz_operations.ResolveRequest{Key: key, ExpectedRow: 18, Resolution: resolution, Reason: "供应商人工确认该业务操作已受理",
		Evidence: domain.Evidence{Type: "provider_confirmation", Reference: "ticket-5678", Summary: "已核对相同账号与业务幂等标识的受理记录", CoveredAttemptIDs: ids}}
}

func TestOPS07ResolveSucceededKeepsAttempts(t *testing.T) {
	w := newWorld(unknownTask(1))
	out, err := usecase(w, data_interface.ConstraintResult{}).Resolve(context.Background(), operator, 1, resolveReq("r1", "SUCCEEDED", 4001, 4002))
	if err != nil {
		t.Fatal(err)
	}
	b := decode(t, out.Body)
	if b.Task.Status != "SUCCEEDED" || b.OperationType != constant.OpResolve || len(w.tasks[1].Attempts) != 2 || w.rounds != 0 || w.outboxes != 0 {
		t.Fatalf("resolve: %+v rounds=%d", b, w.rounds)
	}
	// 业务操作范围：稳定键必须匹配。
	w2 := newWorld(unknownTask(2))
	req := resolveReq("r2", "SUCCEEDED")
	req.Evidence.IdempotencyKey = "n1_local_2"
	if _, err := usecase(w2, data_interface.ConstraintResult{}).Resolve(context.Background(), operator, 2, req); err != nil {
		t.Fatalf("business scope: %v", err)
	}
}

func TestOPS08PartialNotReceivedEvidenceRejected(t *testing.T) {
	w := newWorld(unknownTask(1))
	_, err := usecase(w, data_interface.ConstraintResult{}).Resolve(context.Background(), operator, 1, resolveReq("r1", "FAILED", 4001))
	if codeOf(err) != constant.CodeEvidenceInsufficient || w.tasks[1].Status != enums.TaskUnknown || len(w.ops) != 0 {
		t.Fatalf("partial: %v %+v", err, w.tasks[1].Status)
	}
}

func TestOPS09LostResponseReplayReturnsOriginal(t *testing.T) {
	w := newWorld(task(1, enums.TaskFailed))
	w.loseResp = true
	uc := usecase(w, data_interface.ConstraintResult{})
	first, err := uc.Retry(context.Background(), operator, 1, retryReq("k"))
	if err != nil {
		t.Fatalf("uncertain commit should be confirmed by key: %v", err)
	}
	again, err := uc.Retry(context.Background(), operator, 1, retryReq("k"))
	if err != nil || !again.Replayed || string(again.Body) != string(first.Body) || w.rounds != 1 {
		t.Fatalf("replay: %v replayed=%v rounds=%d", err, again.Replayed, w.rounds)
	}
	// 结果确实不存在时返回 operation_unconfirmed，而非新建操作。
	w2 := newWorld(task(2, enums.TaskFailed))
	uc2 := usecase(w2, data_interface.ConstraintResult{})
	uc2.Ops = uncertain{w2}
	if _, err := uc2.Retry(context.Background(), operator, 2, retryReq("k")); codeOf(err) != constant.CodeOperationUnconfirmed {
		t.Fatalf("unconfirmed: %v", err)
	}
}

type uncertain struct{ *world }

func (u uncertain) ReplayTask(context.Context, data_interface.ReplayInput) ([]byte, error) {
	return nil, data_interface.ErrCommitUncertain
}

func TestOPS10ScopeAndSecretsRejected(t *testing.T) {
	other := task(1, enums.TaskFailed)
	other.CallerID = "billing"
	w := newWorld(other, task(2, enums.TaskFailed))
	uc := usecase(w, data_interface.ConstraintResult{})
	if _, err := uc.Retry(context.Background(), operator, 1, retryReq("k")); codeOf(err) != constant.CodeNotFound {
		t.Fatalf("cross scope: %v", err)
	}
	noPerm := biz_operations.Principal{ActorID: "ro", Scopes: []string{"crm"}}
	if _, err := uc.Retry(context.Background(), noPerm, 2, retryReq("k")); codeOf(err) != constant.CodeForbidden {
		t.Fatalf("no permission: %v", err)
	}
	r := retryReq("k")
	r.Reason = "token=Bearer sk-live-abcdef0123456789"
	if _, err := uc.Retry(context.Background(), operator, 2, r); codeOf(err) != constant.CodeInvalidRequest {
		t.Fatalf("secret reason: %v", err)
	}
	r = retryReq("k")
	r.Reason = strings.Repeat("长", 400)
	if _, err := uc.Retry(context.Background(), operator, 2, r); codeOf(err) != constant.CodeInvalidRequest {
		t.Fatalf("long reason: %v", err)
	}
	w3 := newWorld(unknownTask(3))
	req := resolveReq("r", "SUCCEEDED", 4001, 4002)
	req.Evidence.Summary = "password=hunter2-secret-value"
	if _, err := usecase(w3, data_interface.ConstraintResult{}).Resolve(context.Background(), operator, 3, req); codeOf(err) != constant.CodeEvidenceInvalid {
		t.Fatalf("secret evidence: %v", err)
	}
	if w.rounds != 0 || len(w.ops) != 0 || len(w3.ops) != 0 {
		t.Fatal("side effects on rejection")
	}
}

func TestOPS11SameKeyTwoTasksConcurrentOneWins(t *testing.T) {
	w := newWorld(task(1, enums.TaskFailed), task(2, enums.TaskFailed))
	uc := usecase(w, data_interface.ConstraintResult{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = uc.Retry(context.Background(), operator, uint64(i+1), retryReq("shared"))
		}(i)
	}
	wg.Wait()
	ok, conflict := 0, 0
	for _, err := range errs {
		switch codeOf(err) {
		case "":
			ok++
		case constant.CodeIdempotencyConflict:
			conflict++
		default:
			t.Fatalf("unexpected: %v", err)
		}
	}
	if ok != 1 || conflict != 1 || w.rounds != 1 {
		t.Fatalf("ok=%d conflict=%d rounds=%d", ok, conflict, w.rounds)
	}
	// 并发占键（仓储报告唯一键冲突且原结果属于另一任务）整体回滚并返回幂等冲突。
	w2 := newWorld(task(3, enums.TaskFailed))
	uc2 := usecase(w2, data_interface.ConstraintResult{})
	uc2.Ops = taken{w2}
	if _, err := uc2.Retry(context.Background(), operator, 3, retryReq("x")); codeOf(err) != constant.CodeIdempotencyConflict || w2.tasks[3].RoundNo != 1 {
		t.Fatalf("taken: %v", err)
	}
}

type taken struct{ *world }

func (t taken) ReplayTask(context.Context, data_interface.ReplayInput) ([]byte, error) {
	return nil, data_interface.ErrOperationKeyTaken
}

func TestOPS12EvidenceInvalid(t *testing.T) {
	w := newWorld(unknownTask(1))
	uc := usecase(w, data_interface.ConstraintResult{})
	foreign := resolveReq("a", "SUCCEEDED", 4001, 9999)
	dup := resolveReq("b", "SUCCEEDED", 4001, 4001)
	key := resolveReq("c", "SUCCEEDED")
	key.Evidence.IdempotencyKey = "n1_other"
	for name, req := range map[string]biz_operations.ResolveRequest{"foreign": foreign, "duplicate": dup, "key": key} {
		if _, err := uc.Resolve(context.Background(), operator, 1, req); codeOf(err) != constant.CodeEvidenceInvalid {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if w.tasks[1].Status != enums.TaskUnknown || len(w.ops) != 0 || len(w.tasks[1].Attempts) != 2 {
		t.Fatal("task or ops changed")
	}
}

func TestOPS13NarrowsRoundBudgetOnly(t *testing.T) {
	w := newWorld(task(1, enums.TaskFailed))
	uc := usecase(w, data_interface.ConstraintResult{})
	uc.Config.MaxAllowedAttempts = 3
	uc.Config.MaxRoundTimeout = time.Hour
	out, err := uc.Retry(context.Background(), operator, 1, retryReq("n"))
	if err != nil {
		t.Fatal(err)
	}
	b := decode(t, out.Body)
	got := w.tasks[1]
	p, perr := domain.ParseFrozenPolicy(got.RoundPolicy)
	if perr != nil || !b.PolicyNarrow || got.MaxAttempts != 3 || p.RoundTimeout != time.Hour || p.BackoffInitial != time.Second || got.RouteTargetID != "target" {
		t.Fatalf("narrow: %+v %+v %v", b, p, perr)
	}
	// 当前允许上限无法满足原协议（轮次超时不大于初始退避）则拒绝。
	w2 := newWorld(task(2, enums.TaskFailed))
	uc2 := usecase(w2, data_interface.ConstraintResult{})
	uc2.Config.MaxRoundTimeout = time.Second
	if _, err := uc2.Retry(context.Background(), operator, 2, retryReq("n")); codeOf(err) != constant.CodeConfigurationUnavailable || w2.rounds != 0 {
		t.Fatalf("unsatisfiable: %v", err)
	}
}
