package biz_event_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"notifier/internal/biz/biz_event"
	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

const policyJSON = `{"backoff_cap_ms":60000,"backoff_initial_ms":1000,"max_attempts":8,"round_timeout_seconds":86400,"server_wait_cap_seconds":3600}`

func TestInvalidEventEnvelopeHasPreciseReasons(t *testing.T) {
	cases := []struct {
		body, reason, path string
	}{
		{`invalid`, constant.ReasonInvalidJSON, ""},
		{`{"source_business":"user-service"}`, constant.ReasonMissingField, "source_event_id"},
		{`{"source_business":"user-service","source_event_id":2}`, constant.ReasonInvalidFieldType, "source_event_id"},
		{`{"source_business":"user-service","unexpected":true}`, constant.ReasonUnknownField, "unexpected"},
	}
	for _, tc := range cases {
		repo := &memEvents{}
		uc := newUC(t, repo, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
		_, err := uc.Submit(context.Background(), principal(), []byte(tc.body))
		var de *domain.Error
		if !errors.As(err, &de) || de.Code != constant.CodeInvalidRequest || de.ReasonCode != tc.reason || de.FieldPath != tc.path {
			t.Fatalf("reason %s: %v", tc.reason, err)
		}
		if repo.accepts != 0 {
			t.Fatal("invalid envelope must not create an event")
		}
	}
}

func TestEVT02_ConcurrentIdenticalRequestsOnePlan(t *testing.T) {
	repo := &memEvents{}
	uc := newUC(t, repo, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
	body := eventBody("user-service", "registration-20261109-001", "2026-11-09T09:30:00+08:00", `{"campaign":"autumn","user_id":"user-001"}`)
	start := make(chan struct{})
	var wg sync.WaitGroup
	out := make([]biz_event.Acceptance, 10)
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			out[i], errs[i] = uc.Submit(context.Background(), principal(), body)
		}(i)
	}
	close(start)
	wg.Wait()
	for i := range out {
		if errs[i] != nil || out[i].EventID != out[0].EventID || len(out[i].Deliveries) != 2 {
			t.Fatalf("result %d = %+v %v", i, out[i], errs[i])
		}
		if out[i].Deliveries[0].DeliveryID != out[0].Deliveries[0].DeliveryID || out[i].Deliveries[1].DeliveryID != out[0].Deliveries[1].DeliveryID {
			t.Fatalf("plan diverged: %+v", out[i].Deliveries)
		}
	}
	if repo.accepts != 1 || repo.events != 1 {
		t.Fatalf("accepts=%d events=%d", repo.accepts, repo.events)
	}
}

func TestEVT03_EquivalentCanonicalHitsOriginal(t *testing.T) {
	repo := &memEvents{}
	uc := newUC(t, repo, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
	first, err := uc.Submit(context.Background(), principal(), eventBody("user-service", "evt-1", "2026-11-09T09:30:00+08:00", `{"b":1.0,"a":"\u4e2d"}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := uc.Submit(context.Background(), principal(), []byte(`{"data":{"a":"中","b":1e0},"occurred_at":"2026-11-09T01:30:00Z","schema_version":"1","event_type":"UserRegistered","source_event_id":"evt-1","source_business":"user-service","business_object_type":"user","business_object_id":"user-001"}`))
	if err != nil {
		t.Fatal(err)
	}
	if second.EventID != first.EventID || second.AcceptedAt != first.AcceptedAt || repo.accepts != 1 {
		t.Fatalf("replay = %+v first %+v accepts %d", second, first, repo.accepts)
	}
}

func TestEVT04_MaterialChangeConflicts(t *testing.T) {
	repo := &memEvents{}
	uc := newUC(t, repo, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
	if _, err := uc.Submit(context.Background(), principal(), eventBody("user-service", "evt-1", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`)); err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		eventBody("user-service", "evt-1", "2026-11-09T09:31:00+08:00", `{"user_id":"user-001"}`),
		eventBody("billing", "evt-1", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`),
		eventBody("user-service", "evt-1", "2026-11-09T09:30:00+08:00", `{"user_id":"user-002"}`),
	}
	for _, body := range cases {
		_, err := uc.Submit(context.Background(), principal(), body)
		if codeOf(err) != constant.CodeIdempotencyConflict {
			t.Fatalf("code = %s err %v", codeOf(err), err)
		}
		var de *domain.Error
		if !errors.As(err, &de) || de.ReasonCode != constant.ReasonEventIDConflict {
			t.Fatalf("event conflict reason = %v", err)
		}
	}
	if repo.accepts != 1 || repo.events != 1 {
		t.Fatalf("accepts=%d events=%d", repo.accepts, repo.events)
	}
}

func TestEVT05_ReplayKeepsOriginalRelease(t *testing.T) {
	repo := &memEvents{}
	plan := twoTargetPlan(12)
	uc := newUC(t, repo, &fakePlan{plan: plan}, &fakeRelease{id: 12, hash: hash64()}, &fakeBacklog{}, openLimits())
	body := eventBody("user-service", "evt-v1", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`)
	first, err := uc.Submit(context.Background(), principal(), body)
	if err != nil {
		t.Fatal(err)
	}
	blocked := &fakePlan{err: errCode(constant.CodeNoMatchingRoute, "没有匹配的路由")}
	gone := &fakeRelease{err: data_interface.ErrNoCurrent}
	replayUC := newUC(t, repo, blocked, gone, &fakeBacklog{err: errors.New("backlog down")}, openLimits())
	second, err := replayUC.Submit(context.Background(), principal(), body)
	if err != nil {
		t.Fatal(err)
	}
	if second.RoutingReleaseID != 12 || second.EventID != first.EventID || len(second.Deliveries) != 2 || blocked.calls != 0 || gone.calls != 0 {
		t.Fatalf("replay %+v planner %d release %d", second, blocked.calls, gone.calls)
	}
}

func TestEVT06_MappingFailureLeavesNoEventThenSucceeds(t *testing.T) {
	repo := &memEvents{}
	plan := &fakePlan{err: errCode(constant.CodeMappingInvalid, "映射失败")}
	uc := newUC(t, repo, plan, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
	body := eventBody("user-service", "evt-fix", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`)
	if _, err := uc.Submit(context.Background(), principal(), body); codeOf(err) != constant.CodeMappingInvalid || repo.accepts != 0 || repo.events != 0 {
		t.Fatalf("first = %v accepts %d", err, repo.accepts)
	}
	plan.err = nil
	plan.plan = twoTargetPlan(1)
	out, err := uc.Submit(context.Background(), principal(), body)
	if err != nil || out.RoutingReleaseID != 1 || repo.accepts != 1 || len(out.Deliveries) != 2 {
		t.Fatalf("retry = %+v %v accepts %d", out, err, repo.accepts)
	}
}

func TestEVT08_CommitUncertainRetryReturnsOriginal(t *testing.T) {
	repo := &memEvents{uncertain: true}
	uc := newUC(t, repo, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
	body := eventBody("user-service", "evt-uncertain", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`)
	first, err := uc.Submit(context.Background(), principal(), body)
	if err != nil {
		t.Fatal(err)
	}
	second, err := uc.Submit(context.Background(), principal(), body)
	if err != nil || second.EventID != first.EventID || repo.accepts != 1 || repo.events != 1 {
		t.Fatalf("retry %+v %v accepts %d events %d", second, err, repo.accepts, repo.events)
	}
}

func TestEVT09_BacklogWithinLimitPersistsAndOverLimitPauses(t *testing.T) {
	body := eventBody("user-service", "evt-backlog", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`)
	limits := openLimits()
	limits.MaxPending = 2
	limits.SampleInterval = 0
	okRepo := &memEvents{}
	okUC := newUC(t, okRepo, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{n: 2}, limits)
	out, err := okUC.Submit(context.Background(), principal(), body)
	if err != nil || len(out.Deliveries) != 2 || okRepo.accepts != 1 || out.RoutingReleaseID != 1 {
		t.Fatalf("within limit %+v %v", out, err)
	}
	over := &memEvents{}
	overUC := newUC(t, over, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{n: 3}, limits)
	_, err = overUC.Submit(context.Background(), principal(), body)
	if codeOf(err) != constant.CodeAdmissionPaused || over.accepts != 0 {
		t.Fatalf("over = %v accepts %d", err, over.accepts)
	}
	var countError *domain.Error
	if !errors.As(err, &countError) || countError.ReasonCode != constant.ReasonBacklogCount {
		t.Fatalf("count threshold reason = %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	aged := &memEvents{}
	agedUC := newUC(t, aged, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{n: 0, oldest: &old}, limits)
	_, err = agedUC.Submit(context.Background(), principal(), body)
	if codeOf(err) != constant.CodeAdmissionPaused || aged.accepts != 0 {
		t.Fatalf("aged = %v accepts %d", err, aged.accepts)
	}
	var ageError *domain.Error
	if !errors.As(err, &ageError) || ageError.ReasonCode != constant.ReasonBacklogAge {
		t.Fatalf("age threshold reason = %v", err)
	}
}

func TestEVT10_RejectsForgedCallerTargetAndUnauthorizedSource(t *testing.T) {
	repo := &memEvents{}
	plan := &fakePlan{plan: twoTargetPlan(1)}
	uc := newUC(t, repo, plan, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
	base := eventBody("user-service", "evt-auth", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`)
	for _, body := range []string{
		`{"caller_id":"other","source_business":"user-service","source_event_id":"evt-auth","event_type":"UserRegistered","schema_version":"1","occurred_at":"2026-11-09T09:30:00Z","business_object_type":"user","business_object_id":"user-001","data":{}}`,
		`{"url":"https://evil.example","source_business":"user-service","source_event_id":"evt-auth","event_type":"UserRegistered","schema_version":"1","occurred_at":"2026-11-09T09:30:00Z","business_object_type":"user","business_object_id":"user-001","data":{}}`,
		`{"release_id":"9","source_business":"user-service","source_event_id":"evt-auth","event_type":"UserRegistered","schema_version":"1","occurred_at":"2026-11-09T09:30:00Z","business_object_type":"user","business_object_id":"user-001","data":{}}`,
	} {
		_, err := uc.Submit(context.Background(), principal(), []byte(body))
		if codeOf(err) != constant.CodeInvalidRequest {
			t.Fatalf("forged = %v body %s", err, body)
		}
	}
	_, err := uc.Submit(context.Background(), principal(), eventBody("other-service", "evt-auth", "2026-11-09T09:30:00Z", `{}`))
	if codeOf(err) != constant.CodeForbidden {
		t.Fatalf("unauthorized source = %v", err)
	}
	if _, err := uc.Submit(context.Background(), principal(), base); err != nil {
		t.Fatal(err)
	}
	if repo.accepts != 1 || plan.calls != 1 {
		t.Fatalf("accepts %d plans %d", repo.accepts, plan.calls)
	}
}

func TestEVT11_BoundaryAndOneOverRejects(t *testing.T) {
	payload := []byte(`{"n":"x"}`)
	fixed := fixedTarget("only-target", "ad", "registration")
	size := len(payload) + len(fixed)
	limits := openLimits()
	limits.MaxFanout = 1
	limits.MaxPlanBytes = size
	repo := &memEvents{}
	one := planWithPayload(1, "only-target", payload, fixed)
	uc := newUC(t, repo, &fakePlan{plan: one}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, limits)
	if _, err := uc.Submit(context.Background(), principal(), eventBody("user-service", "edge", "2026-11-09T09:30:00Z", `{"user_id":"user-001"}`)); err != nil || repo.accepts != 1 {
		t.Fatalf("boundary = %v accepts %d", err, repo.accepts)
	}
	overFan := &memEvents{}
	fanUC := newUC(t, overFan, &fakePlan{plan: twoTargetPlan(1)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, limits)
	if _, err := fanUC.Submit(context.Background(), principal(), eventBody("user-service", "fan", "2026-11-09T09:30:00Z", `{"user_id":"user-001"}`)); codeOf(err) != constant.CodeFanoutExceeded || overFan.accepts != 0 || overFan.events != 0 {
		t.Fatalf("fanout = %v accepts %d", err, overFan.accepts)
	}
	overPlan := &memEvents{}
	bigger := append(append([]byte(nil), payload...), ' ')
	planUC := newUC(t, overPlan, &fakePlan{plan: planWithPayload(1, "only-target", bigger, fixed)}, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, limits)
	if _, err := planUC.Submit(context.Background(), principal(), eventBody("user-service", "plan", "2026-11-09T09:30:00Z", `{"user_id":"user-001"}`)); codeOf(err) != constant.CodeLimitExceeded || overPlan.events != 0 {
		t.Fatalf("plan = %v events %d", err, overPlan.events)
	}
}

func TestEVT12_ExistingEventIgnoresUnavailableReleaseAndStaleBacklog(t *testing.T) {
	repo := &memEvents{}
	uc := newUC(t, repo, &fakePlan{plan: twoTargetPlan(4)}, &fakeRelease{id: 4, hash: hash64()}, &fakeBacklog{}, openLimits())
	body := eventBody("user-service", "old", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`)
	first, err := uc.Submit(context.Background(), principal(), body)
	if err != nil {
		t.Fatal(err)
	}
	planner := &fakePlan{err: errors.New("should not calculate")}
	releases := &fakeRelease{err: data_interface.ErrNoCurrent}
	backlog := &fakeBacklog{err: errors.New("sample expired")}
	again := newUC(t, repo, planner, releases, backlog, openLimits())
	got, err := again.Submit(context.Background(), principal(), body)
	if err != nil || got.EventID != first.EventID || got.RoutingReleaseID != 4 || planner.calls != 0 || releases.calls != 0 || backlog.calls != 0 {
		t.Fatalf("replay %+v %v calls plan %d release %d backlog %d", got, err, planner.calls, releases.calls, backlog.calls)
	}
	_, err = again.Submit(context.Background(), principal(), eventBody("user-service", "old", "2026-11-09T10:30:00+08:00", `{"user_id":"user-001"}`))
	if codeOf(err) != constant.CodeIdempotencyConflict || planner.calls != 0 || repo.accepts != 1 {
		t.Fatalf("conflict = %v calls %d accepts %d", err, planner.calls, repo.accepts)
	}
}

func TestEVT14_RecheckAfterValidationFailure(t *testing.T) {
	repo := &memEvents{}
	body := eventBody("user-service", "race", "2026-11-09T09:30:00+08:00", `{"user_id":"user-001"}`)
	plan := twoTargetPlan(1)
	var once sync.Once
	planner := &fakePlan{fn: func(int) (domain.DeliveryPlan, error) {
		once.Do(func() {
			env, fp := mustFact(t, body)
			repo.seed(env, fp, plan, 1)
		})
		return domain.DeliveryPlan{}, errCode(constant.CodeNoMatchingRoute, "没有匹配的路由")
	}}
	uc := newUC(t, repo, planner, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
	out, err := uc.Submit(context.Background(), principal(), body)
	if err != nil || out.RoutingReleaseID != 1 || repo.accepts != 0 {
		t.Fatalf("recheck = %+v %v accepts %d", out, err, repo.accepts)
	}
	repo2 := &memEvents{}
	planner2 := &fakePlan{fn: func(int) (domain.DeliveryPlan, error) {
		env, fp := mustFact(t, eventBody("user-service", "race", "2026-11-09T09:31:00Z", `{"user_id":"other"}`))
		repo2.seed(env, fp, plan, 1)
		return domain.DeliveryPlan{}, errCode(constant.CodeUnsupportedSchema, "不支持的事件类型或版本")
	}}
	uc2 := newUC(t, repo2, planner2, &fakeRelease{id: 1, hash: hash64()}, &fakeBacklog{}, openLimits())
	_, err = uc2.Submit(context.Background(), principal(), body)
	if codeOf(err) != constant.CodeIdempotencyConflict {
		t.Fatalf("conflict recheck = %v", err)
	}
}

func TestEVT01_InitialTaskAndCommandShape(t *testing.T) {
	accepted := time.Date(2026, 11, 9, 1, 30, 1, 0, time.UTC)
	initial, err := domain.NewInitialDelivery(accepted, []byte(policyJSON))
	if err != nil {
		t.Fatal(err)
	}
	if initial.Status != "PENDING" || initial.ResultCertainty != "NOT_RECEIVED" || initial.RoundNo != 1 ||
		initial.ScheduleVersion != 1 || initial.RowVersion != 1 || initial.RoundAttemptCount != 0 || initial.TotalAttemptCount != 0 ||
		initial.MaxAttempts != 8 || !initial.ScheduledAt.Equal(accepted) || !initial.RoundDeadlineAt.Equal(accepted.Add(86400*time.Second)) {
		t.Fatalf("initial = %+v", initial)
	}
	box := domain.NewInitialOutbox(initial.ScheduledAt)
	if box.PublishStatus != "PENDING" || box.MessageType != "immediate" || box.PublishCount != 0 || box.PublishRevision != 1 {
		t.Fatalf("outbox = %+v", box)
	}
	cmd, body, err := domain.NewDeliveryCommand(3001, 1001, 2001, 1, 1)
	if err != nil || cmd.MessageID != "o1_3001" || string(body) == "" {
		t.Fatalf("command %s %s %v", cmd.MessageID, body, err)
	}
	key, err := domain.TaskBusinessKey("local", 2001)
	if err != nil || key != "n1_local_2001" {
		t.Fatalf("key = %s %v", key, err)
	}
}

type memEvents struct {
	mu        sync.Mutex
	ev        *data_interface.AcceptedEvent
	accepts   int
	events    int
	uncertain bool
	nextEvent uint64
	nextTask  uint64
}

func (m *memEvents) FindAcceptedEvent(ctx context.Context, callerID, sourceEventID string) (data_interface.AcceptedEvent, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ev == nil || m.ev.CallerID != callerID || m.ev.SourceEventID != sourceEventID {
		return data_interface.AcceptedEvent{}, false, nil
	}
	return cloneEvent(*m.ev), true, nil
}

func (m *memEvents) AcceptEvent(ctx context.Context, in data_interface.AcceptEventInput) (data_interface.AcceptedEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ev != nil {
		return data_interface.AcceptedEvent{}, data_interface.ErrEventUnique
	}
	m.accepts++
	if m.nextEvent == 0 {
		m.nextEvent = 1000
		m.nextTask = 2000
	}
	m.nextEvent++
	ev := data_interface.AcceptedEvent{
		EventID: m.nextEvent, CallerID: in.CallerID, SourceBusiness: in.SourceBusiness, SourceEventID: in.SourceEventID,
		FingerprintVersion: in.FingerprintVersion, Fingerprint: in.Fingerprint,
		DataCanonical: append([]byte(nil), in.DataCanonical...), RoutingReleaseID: in.ReleaseID,
		AcceptedAt: time.Date(2026, 11, 9, 1, 30, 1, 0, time.UTC),
	}
	targets := append([]domain.TargetPlan(nil), in.Plan.Targets...)
	for i := 0; i < len(targets); i++ {
		for j := i + 1; j < len(targets); j++ {
			if targets[j].RouteTargetID < targets[i].RouteTargetID {
				targets[i], targets[j] = targets[j], targets[i]
			}
		}
	}
	for _, target := range targets {
		m.nextTask++
		ev.Deliveries = append(ev.Deliveries, data_interface.DeliveryView{
			DeliveryID: m.nextTask, RouteTargetID: target.RouteTargetID, ProviderID: target.ProviderID,
			ProviderAccountID: target.ProviderAccountID, Operation: target.Operation,
			PayloadCanonical: append([]byte(nil), target.PayloadCanonical...),
		})
	}
	m.ev = &ev
	m.events++
	if m.uncertain {
		m.uncertain = false
		return data_interface.AcceptedEvent{}, data_interface.ErrCommitUncertain
	}
	return cloneEvent(ev), nil
}

func (m *memEvents) seed(env domain.EventEnvelope, fingerprint string, plan domain.DeliveryPlan, releaseID uint64) {
	_, _ = m.AcceptEvent(context.Background(), data_interface.AcceptEventInput{
		CallerID: env.CallerID, SourceBusiness: env.SourceBusiness, SourceEventID: env.SourceEventID,
		FingerprintVersion: constant.FingerprintVersion, Fingerprint: fingerprint, DataCanonical: env.DataCanonical,
		ReleaseID: releaseID, Plan: plan,
	})
	m.mu.Lock()
	m.accepts = 0
	m.mu.Unlock()
}

type fakePlan struct {
	mu    sync.Mutex
	plan  domain.DeliveryPlan
	err   error
	fn    func(call int) (domain.DeliveryPlan, error)
	calls int
}

func (f *fakePlan) Calculate(ctx context.Context, principal biz_event.Principal, releaseID uint64, env domain.EventEnvelope) (domain.DeliveryPlan, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fn != nil {
		return f.fn(f.calls)
	}
	if f.err != nil {
		return domain.DeliveryPlan{}, f.err
	}
	return f.plan, nil
}

type fakeRelease struct {
	mu    sync.Mutex
	id    uint64
	hash  string
	err   error
	calls int
}

func (f *fakeRelease) LoadCurrentID(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	return f.id, nil
}

func (f *fakeRelease) Load(ctx context.Context, releaseID uint64) (data_interface.ReleaseRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return data_interface.ReleaseRecord{}, f.err
	}
	return data_interface.ReleaseRecord{ReleaseID: releaseID, ContentSHA256: f.hash, ContentCanonical: []byte(`{}`), FingerprintVersion: 1}, nil
}

type fakeBacklog struct {
	mu     sync.Mutex
	n      int64
	oldest *time.Time
	err    error
	calls  int
}

func (f *fakeBacklog) Measure(ctx context.Context) (int64, *time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return 0, nil, f.err
	}
	return f.n, f.oldest, nil
}

func newUC(t *testing.T, repo *memEvents, plan *fakePlan, rel *fakeRelease, backlog *fakeBacklog, limits biz_event.Limits) *biz_event.UseCase {
	t.Helper()
	return biz_event.NewUseCase(repo, backlog, rel, plan, limits, time.Now)
}

func principal() biz_event.Principal {
	return biz_event.Principal{ActorID: "local-dev", CallerID: "caller-1", AllowedSources: []string{"user-service", "billing"}, Scopes: []string{"caller-1"}}
}

func openLimits() biz_event.Limits {
	return biz_event.Limits{MaxBody: 256 << 10, MaxFanout: 50, MaxPlanBytes: 2 << 20, MaxPending: 100000, MaxOldest: time.Hour, SampleInterval: time.Hour, SampleMaxAge: time.Hour, Namespace: "local"}
}

func eventBody(source, id, occurred, data string) []byte {
	return []byte(`{"source_business":"` + source + `","source_event_id":"` + id + `","event_type":"UserRegistered","schema_version":"1","occurred_at":"` + occurred + `","business_object_type":"user","business_object_id":"user-001","data":` + data + `}`)
}

func twoTargetPlan(releaseID uint64) domain.DeliveryPlan {
	return domain.DeliveryPlan{
		ReleaseRef: itoa(releaseID), VersionNo: 1, FormatVersion: 1, RoundPolicy: []byte(policyJSON),
		Targets: []domain.TargetPlan{
			targetPlan("crm-create-user", "crm", "create-user", []byte(`{"user_id":"user-001"}`)),
			targetPlan("ad-registration", "ad", "registration", []byte(`{"user_id":"user-001"}`)),
		},
	}
}

func planWithPayload(releaseID uint64, route string, payload, fixed []byte) domain.DeliveryPlan {
	item := targetPlan(route, "ad", "registration", payload)
	item.FixedTarget = fixed
	return domain.DeliveryPlan{ReleaseRef: itoa(releaseID), VersionNo: 1, RoundPolicy: []byte(policyJSON), Targets: []domain.TargetPlan{item}}
}

func targetPlan(route, provider, op string, payload []byte) domain.TargetPlan {
	return domain.TargetPlan{
		RouteTargetID: route, ProviderID: provider, ProviderAccountID: "demo", Operation: op,
		MappingID: "map-1", MappingVersion: "1", AdapterType: "mock-json", AdapterVersion: "1",
		PayloadCanonical: payload, FixedTarget: fixedTarget(route, provider, op),
	}
}

func fixedTarget(route, provider, op string) []byte {
	raw := []byte(`{"adapter_type":"mock-json","adapter_version":"1","channel_id":"1","credential_ref":"provider/demo","mapping_id":"map-1","mapping_version":"1","operation":"` + op + `","provider_account_id":"demo","provider_id":"` + provider + `","route_target_id":"` + route + `","semantic_config":{}}`)
	value, err := canonicaljson.ParseObject(raw)
	if err != nil {
		panic(err)
	}
	return canonicaljson.AppendCanonical(nil, value)
}

func mustFact(t *testing.T, body []byte) (domain.EventEnvelope, string) {
	t.Helper()
	env := domain.EventEnvelope{CallerID: "caller-1", SourceBusiness: "user-service", SourceEventID: "race", EventType: "UserRegistered", SchemaVersion: "1", BusinessObjectType: "user", BusinessObjectID: "user-001", FingerprintVersion: 1}
	value, err := canonicaljson.ParseObject(body)
	if err != nil {
		t.Fatal(err)
	}
	fields, _ := value.Object()
	for _, field := range fields {
		text, _ := field.Value.Text()
		switch field.Key {
		case "source_business":
			env.SourceBusiness = text
		case "source_event_id":
			env.SourceEventID = text
		case "occurred_at":
			env.OccurredAt, err = domain.ParseRFC3339("occurred_at", text)
		case "data":
			env.DataCanonical = canonicaljson.AppendCanonical(nil, field.Value)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	_, fp, err := domain.FingerprintEvent(env)
	if err != nil {
		t.Fatal(err)
	}
	return env, fp
}

func cloneEvent(ev data_interface.AcceptedEvent) data_interface.AcceptedEvent {
	ev.DataCanonical = append([]byte(nil), ev.DataCanonical...)
	ev.Deliveries = append([]data_interface.DeliveryView(nil), ev.Deliveries...)
	for i := range ev.Deliveries {
		ev.Deliveries[i].PayloadCanonical = append([]byte(nil), ev.Deliveries[i].PayloadCanonical...)
	}
	return ev
}

func hash64() string {
	return "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

func itoa(n uint64) string {
	text, _ := domain.FormatInternalID(n)
	return text
}

func codeOf(err error) string {
	var de *domain.Error
	if errors.As(err, &de) && de != nil {
		return de.Code
	}
	return ""
}

func errCode(code, message string) error {
	err, _ := domain.NewError(code, message, "", nil)
	return err
}
