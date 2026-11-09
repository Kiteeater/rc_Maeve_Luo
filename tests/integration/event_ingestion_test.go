//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"notifier/internal/biz/biz_event"
	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/data/mysql"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

func TestEVT01_OneEventTwoTasksOutboxHistoryOneTransaction(t *testing.T) {
	db := s02DB(t)
	releaseID, hash := insertRelease(t, db, 1, []byte(`{"v":1}`))
	store := mysql.NewEventStore(db, mysql.NewExecutor(db, 2*time.Second))
	out, err := store.AcceptEvent(context.Background(), acceptInput(t, releaseID, hash, "evt-01", twoTargets(releaseID)))
	if err != nil {
		t.Fatal(err)
	}
	if out.EventID == 0 || len(out.Deliveries) != 2 {
		t.Fatalf("saved %+v", out)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM business_events`)
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM delivery_tasks`)
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM outbox_messages WHERE publish_status = 'PENDING' AND message_type = 'immediate' AND message_id LIKE 'o1\\_%'`)
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM delivery_history WHERE phase = 'TASK_CREATED' AND history_seq = 1`)
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM delivery_history WHERE phase = 'SCHEDULE_CREATED' AND history_seq = 2`)
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM delivery_tasks WHERE status = 'PENDING' AND round_no = 1 AND schedule_version = 1 AND row_version = 1 AND round_attempt_count = 0 AND total_attempt_count = 0 AND result_certainty = 'NOT_RECEIVED'`)
	var key string
	if err := db.QueryRow(`SELECT idempotency_key FROM delivery_tasks WHERE route_target_id = 'ad-registration'`).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if len(key) < len("n1_local_") || key[:len("n1_local_")] != "n1_local_" {
		t.Fatalf("idempotency key = %s", key)
	}
}

func TestEVT02_ConcurrentIdenticalRequestsOnePlan(t *testing.T) {
	db := s02DB(t)
	releaseID, hash := insertRelease(t, db, 1, []byte(`{"v":1}`))
	uc := s02UC(t, db, &fixedPlan{id: releaseID, hash: hash, plan: twoTargets(releaseID)}, openEventLimits())
	body := evtJSON("user-service", "evt-02", "2026-11-09T09:30:00+08:00", `{"user_id":"u-1"}`)
	var wg sync.WaitGroup
	out := make([]biz_event.Acceptance, 10)
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out[i], errs[i] = uc.Submit(context.Background(), evtPrincipal(), body)
		}(i)
	}
	wg.Wait()
	for i := range out {
		if errs[i] != nil || out[i].EventID != out[0].EventID || len(out[i].Deliveries) != 2 {
			t.Fatalf("i %d %+v %v", i, out[i], errs[i])
		}
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM business_events`)
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM delivery_tasks`)
}

func TestEVT07_ConcurrentReleaseSwitchDoesNotMixPlans(t *testing.T) {
	db := s02DB(t)
	v1, hash1 := insertRelease(t, db, 1, []byte(`{"v":1}`))
	v2, hash2 := insertRelease(t, db, 2, []byte(`{"v":2}`))
	if _, err := db.Exec(`UPDATE routing_current SET release_id = ? WHERE singleton = 1`, v1); err != nil {
		t.Fatal(err)
	}
	plan := &switchingPlan{a: v1, b: v2, planA: oneTarget(v1, "target-a"), planB: oneTarget(v2, "target-b")}
	releases := mysql.NewReleaseStore(db, mysql.NewExecutor(db, 2*time.Second))
	store := mysql.NewEventStore(db, mysql.NewExecutor(db, 2*time.Second))
	uc := biz_event.NewUseCase(store, store, releases, plan, openEventLimits(), time.Now)
	stop := make(chan struct{})
	go func() {
		ids := []uint64{v1, v2}
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				_, _ = db.Exec(`UPDATE routing_current SET release_id = ? WHERE singleton = 1`, ids[i%2])
			}
		}
	}()
	var wg sync.WaitGroup
	errs := make([]error, 8)
	got := make([]biz_event.Acceptance, 8)
	body := evtJSON("user-service", "evt-07", "2026-11-09T09:30:00Z", `{"user_id":"u-7"}`)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = uc.Submit(context.Background(), evtPrincipal(), body)
		}(i)
	}
	wg.Wait()
	close(stop)
	for i := range got {
		if errs[i] != nil || got[i].EventID != got[0].EventID || got[i].RoutingReleaseID != got[0].RoutingReleaseID {
			t.Fatalf("i %d %+v %v hash %s %s", i, got[i], errs[i], hash1, hash2)
		}
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM business_events`)
	assertSQLCount(t, db, 1, `SELECT COUNT(DISTINCT route_target_id) FROM delivery_tasks`)
	assertSQLCount(t, db, 1, `SELECT COUNT(DISTINCT routing_release_id) FROM business_events`)
	var routes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM delivery_tasks WHERE route_target_id IN ('target-a','target-b')`).Scan(&routes); err != nil || routes != 1 {
		t.Fatalf("mixed routes = %d %v", routes, err)
	}
}

func TestEVT08_RetryAfterAcceptedDoesNotIncreaseCounts(t *testing.T) {
	db := s02DB(t)
	releaseID, hash := insertRelease(t, db, 1, []byte(`{"v":1}`))
	uc := s02UC(t, db, &fixedPlan{id: releaseID, hash: hash, plan: twoTargets(releaseID)}, openEventLimits())
	body := evtJSON("user-service", "evt-08", "2026-11-09T09:30:00+08:00", `{"user_id":"u-8"}`)
	first, err := uc.Submit(context.Background(), evtPrincipal(), body)
	if err != nil {
		t.Fatal(err)
	}
	second, err := uc.Submit(context.Background(), evtPrincipal(), body)
	if err != nil || second.EventID != first.EventID || len(second.Deliveries) != len(first.Deliveries) {
		t.Fatalf("retry %+v %v", second, err)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM business_events`)
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM delivery_tasks`)
	assertSQLCount(t, db, 4, `SELECT COUNT(*) FROM delivery_history`)
}

func TestEVT11_OverLimitLeavesNoRows(t *testing.T) {
	db := s02DB(t)
	releaseID, hash := insertRelease(t, db, 1, []byte(`{"v":1}`))
	store := mysql.NewEventStore(db, mysql.NewExecutor(db, 2*time.Second))
	plan := oneTarget(releaseID, "only-target")
	size := len(plan.Targets[0].PayloadCanonical) + len(plan.Targets[0].FixedTarget)
	in := acceptInput(t, releaseID, hash, "evt-11-ok", plan)
	in.MaxPlanBytes = size
	in.MaxFanout = 1
	if _, err := store.AcceptEvent(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM business_events`)
	clearEvents(t, db)
	in.SourceEventID = "evt-11-plan"
	in.MaxPlanBytes = size - 1
	if _, err := store.AcceptEvent(context.Background(), in); err == nil {
		t.Fatal("oversize plan accepted")
	}
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM business_events`)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM delivery_tasks`)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM outbox_messages`)
	in.SourceEventID = "evt-11-fan"
	in.MaxPlanBytes = size * 4
	in.MaxFanout = 1
	in.Plan = twoTargets(releaseID)
	if _, err := store.AcceptEvent(context.Background(), in); err == nil {
		t.Fatal("fanout over accepted")
	}
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM business_events`)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM delivery_history`)
}

func TestEVT13_ConcurrentCanonicalFactsStayStable(t *testing.T) {
	db := s02DB(t)
	releaseID, hash := insertRelease(t, db, 1, []byte(`{"v":1}`))
	uc := s02UC(t, db, &fixedPlan{id: releaseID, hash: hash, plan: twoTargets(releaseID), echoData: true}, openEventLimits())
	bodies := [][]byte{
		evtJSON("user-service", "evt-13", "2026-11-09T09:30:00+08:00", `{"name":"\u0061","n":9007199254740993}`),
		[]byte(`{"business_object_id":"user-001","business_object_type":"user","data":{"n":9007199254740993.0,"name":"a"},"event_type":"UserRegistered","occurred_at":"2026-11-09T01:30:00Z","schema_version":"1","source_business":"user-service","source_event_id":"evt-13"}`),
	}
	var wg sync.WaitGroup
	errs := make([]error, len(bodies)*5)
	for n := 0; n < len(errs); n++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = uc.Submit(context.Background(), evtPrincipal(), bodies[i%2])
		}(n)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("i %d %v", i, err)
		}
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM business_events`)
	var storedFP string
	var canonical []byte
	if err := db.QueryRow(`SELECT fingerprint, data_canonical FROM business_events`).Scan(&storedFP, &canonical); err != nil {
		t.Fatal(err)
	}
	env := domain.EventEnvelope{
		CallerID: "caller-1", SourceBusiness: "user-service", SourceEventID: "evt-13", EventType: "UserRegistered", SchemaVersion: "1",
		OccurredAt: time.Date(2026, 11, 9, 1, 30, 0, 0, time.UTC), BusinessObjectType: "user", BusinessObjectID: "user-001",
		DataCanonical: canonical, FingerprintVersion: 1,
	}
	_, fp, err := domain.FingerprintEvent(env)
	if err != nil || fp != storedFP {
		t.Fatalf("stored fingerprint %s recomputed %s %v", storedFP, fp, err)
	}
	var payload []byte
	if err := db.QueryRow(`SELECT payload_canonical FROM delivery_tasks LIMIT 1`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if string(payload) != string(canonical) {
		t.Fatalf("payload %s canonical %s", payload, canonical)
	}
}

func TestEVT14_RecheckUniqueKeyAfterConcurrentAccept(t *testing.T) {
	db := s02DB(t)
	releaseID, hash := insertRelease(t, db, 1, []byte(`{"v":1}`))
	inner := mysql.NewEventStore(db, mysql.NewExecutor(db, 2*time.Second))
	gate := &hideInitialFind{inner: inner}
	plan := twoTargets(releaseID)
	var calls int32
	planner := planFunc(func(id uint64) (domain.DeliveryPlan, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			for atomic.LoadInt32(&calls) < 2 {
				time.Sleep(time.Millisecond)
			}
			return plan, nil
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			_, ok, err := inner.FindAcceptedEvent(context.Background(), "caller-1", "evt-14")
			if err != nil {
				return domain.DeliveryPlan{}, err
			}
			if ok {
				break
			}
			time.Sleep(time.Millisecond)
		}
		err, _ := domain.NewError(constant.CodeNoMatchingRoute, "没有匹配的路由", "", nil)
		return domain.DeliveryPlan{}, err
	})
	uc := biz_event.NewUseCase(gate, staticBacklog{}, &fixedRelease{id: releaseID, hash: hash}, planner, openEventLimits(), time.Now)
	body := evtJSON("user-service", "evt-14", "2026-11-09T09:30:00Z", `{"user_id":"u-14"}`)
	var wg sync.WaitGroup
	out := make([]biz_event.Acceptance, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out[i], errs[i] = uc.Submit(context.Background(), evtPrincipal(), body)
		}(i)
	}
	wg.Wait()
	for i := range out {
		if errs[i] != nil || out[i].EventID == 0 || out[i].EventID != out[0].EventID {
			t.Fatalf("i %d %+v %v", i, out[i], errs[i])
		}
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM business_events`)
	_ = hash
}

func TestC01_SameCallerTwoSourcesSameEventIDConflicts(t *testing.T) {
	db := s02DB(t)
	releaseID, hash := insertRelease(t, db, 1, []byte(`{"v":1}`))
	uc := s02UC(t, db, &fixedPlan{id: releaseID, hash: hash, plan: oneTarget(releaseID, "only-target")}, openEventLimits())
	if _, err := uc.Submit(context.Background(), evtPrincipal(), evtJSON("user-service", "shared-id", "2026-11-09T09:30:00Z", `{"user_id":"a"}`)); err != nil {
		t.Fatal(err)
	}
	_, err := uc.Submit(context.Background(), evtPrincipal(), evtJSON("billing", "shared-id", "2026-11-09T09:30:00Z", `{"user_id":"b"}`))
	var de *domain.Error
	if !errors.As(err, &de) || de.Code != constant.CodeIdempotencyConflict {
		t.Fatalf("conflict = %v", err)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM business_events`)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM delivery_tasks`)
}

func s02DB(t *testing.T) *sql.DB {
	t.Helper()
	const name = "notifier_s02_evt"
	s02Once.Do(func() {
		admin := adminDB(t)
		if _, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name); err != nil {
			s02Err = err
			return
		}
		if _, err := admin.ExecContext(context.Background(), "CREATE DATABASE "+name); err != nil {
			s02Err = err
			return
		}
		runGoose(t, repoRoot(t), gooseDSN(name), "up")
	})
	if s02Err != nil {
		t.Fatal(s02Err)
	}
	db := dbNamed(t, name)
	clearEvents(t, db)
	if _, err := db.Exec(`DELETE FROM routing_releases`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE routing_current SET release_id = NULL WHERE singleton = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM channels`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO channels (channel_id, provider_id, provider_account_id, credential_ref, adapter_type, adapter_version, operation, row_version, enabled)
		VALUES (1, 'ad', 'demo', 'provider/demo', 'mock-json', '1', 'registration', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	return db
}

var (
	s02Once sync.Once
	s02Err  error
)

func clearEvents(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		`DELETE FROM delivery_history`,
		`DELETE FROM outbox_messages`,
		`DELETE FROM delivery_tasks`,
		`DELETE FROM business_events`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
}

func insertRelease(t *testing.T, db *sql.DB, version int, body []byte) (uint64, string) {
	t.Helper()
	sum := canonicaljson.Fingerprint(body)
	res, err := db.Exec(`
		INSERT INTO routing_releases (version_no, content, status, content_canonical, content_sha256, fingerprint_version, published_at)
		VALUES (?, CAST(? AS JSON), 'published', ?, ?, 1, UTC_TIMESTAMP(6))`, version, string(body), body, sum)
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE routing_current SET release_id = ? WHERE singleton = 1`, id); err != nil {
		t.Fatal(err)
	}
	return uint64(id), sum
}

func s02UC(t *testing.T, db *sql.DB, planner biz_event.Planner, limits biz_event.Limits) *biz_event.UseCase {
	t.Helper()
	exec := mysql.NewExecutor(db, 2*time.Second)
	store := mysql.NewEventStore(db, exec)
	releases := mysql.NewReleaseStore(db, exec)
	return biz_event.NewUseCase(store, store, releases, planner, limits, time.Now)
}

func openEventLimits() biz_event.Limits {
	return biz_event.Limits{MaxBody: 1 << 20, MaxFanout: 50, MaxPlanBytes: 2 << 20, MaxPending: 100000, MaxOldest: time.Hour, SampleInterval: 0, SampleMaxAge: 5 * time.Second, Namespace: "local"}
}

func evtPrincipal() biz_event.Principal {
	return biz_event.Principal{ActorID: "local-dev", CallerID: "caller-1", AllowedSources: []string{"user-service", "billing"}, Scopes: []string{"caller-1"}}
}

func evtJSON(source, id, occurred, data string) []byte {
	return []byte(`{"source_business":"` + source + `","source_event_id":"` + id + `","event_type":"UserRegistered","schema_version":"1","occurred_at":"` + occurred + `","business_object_type":"user","business_object_id":"user-001","data":` + data + `}`)
}

func acceptInput(t *testing.T, releaseID uint64, hash, sourceEventID string, plan domain.DeliveryPlan) data_interface.AcceptEventInput {
	t.Helper()
	body := evtJSON("user-service", sourceEventID, "2026-11-09T09:30:00Z", `{"user_id":"u-1"}`)
	env, fp := factOf(t, body)
	return data_interface.AcceptEventInput{
		CallerID: env.CallerID, AllowedSources: []string{"user-service", "billing"}, SourceBusiness: env.SourceBusiness,
		SourceEventID: env.SourceEventID, EventType: env.EventType, SchemaVersion: env.SchemaVersion, OccurredAt: env.OccurredAt,
		BusinessObjectType: env.BusinessObjectType, BusinessObjectID: env.BusinessObjectID, DataCanonical: env.DataCanonical,
		FingerprintVersion: 1, Fingerprint: fp, ReleaseID: releaseID, ExpectedReleaseHash: hash, Plan: plan,
		IdempotencyNamespace: "local", MaxFanout: 50, MaxPlanBytes: 2 << 20,
	}
}

func factOf(t *testing.T, body []byte) (domain.EventEnvelope, string) {
	t.Helper()
	value, err := canonicaljson.ParseObject(body)
	if err != nil {
		t.Fatal(err)
	}
	env := domain.EventEnvelope{CallerID: "caller-1", EventType: "UserRegistered", SchemaVersion: "1", BusinessObjectType: "user", BusinessObjectID: "user-001", FingerprintVersion: 1}
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

func twoTargets(releaseID uint64) domain.DeliveryPlan {
	return domain.DeliveryPlan{ReleaseRef: u64(releaseID), RoundPolicy: []byte(policyText), Targets: []domain.TargetPlan{
		taskTarget("ad-registration", "ad", "registration"),
		taskTarget("crm-create-user", "crm", "create-user"),
	}}
}

func oneTarget(releaseID uint64, route string) domain.DeliveryPlan {
	return domain.DeliveryPlan{ReleaseRef: u64(releaseID), RoundPolicy: []byte(policyText), Targets: []domain.TargetPlan{taskTarget(route, "ad", "registration")}}
}

const policyText = `{"backoff_cap_ms":60000,"backoff_initial_ms":1000,"max_attempts":8,"round_timeout_seconds":86400,"server_wait_cap_seconds":3600}`

func taskTarget(route, provider, op string) domain.TargetPlan {
	fixed := []byte(`{"adapter_type":"mock-json","adapter_version":"1","channel_id":"1","credential_ref":"provider/demo","mapping_id":"map-1","mapping_version":"1","operation":"` + op + `","provider_account_id":"demo","provider_id":"` + provider + `","route_target_id":"` + route + `","semantic_config":{}}`)
	value, err := canonicaljson.ParseObject(fixed)
	if err != nil {
		panic(err)
	}
	return domain.TargetPlan{
		RouteTargetID: route, ProviderID: provider, ProviderAccountID: "demo", Operation: op,
		MappingID: "map-1", MappingVersion: "1", AdapterType: "mock-json", AdapterVersion: "1",
		PayloadCanonical: []byte(`{"user_id":"u-1"}`), FixedTarget: canonicaljson.AppendCanonical(nil, value),
	}
}

func assertSQLCount(t *testing.T, db *sql.DB, want int, query string) {
	t.Helper()
	var got int
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", query, got, want)
	}
}

func u64(n uint64) string {
	text, _ := domain.FormatInternalID(n)
	return text
}

type fixedPlan struct {
	id       uint64
	hash     string
	plan     domain.DeliveryPlan
	echoData bool
}

func (p *fixedPlan) Calculate(ctx context.Context, principal biz_event.Principal, releaseID uint64, env domain.EventEnvelope) (domain.DeliveryPlan, error) {
	plan := p.plan
	plan.ReleaseRef = u64(releaseID)
	if p.echoData {
		for i := range plan.Targets {
			plan.Targets[i].PayloadCanonical = append([]byte(nil), env.DataCanonical...)
		}
	}
	return plan, nil
}

type switchingPlan struct {
	a, b  uint64
	planA domain.DeliveryPlan
	planB domain.DeliveryPlan
}

func (p *switchingPlan) Calculate(ctx context.Context, principal biz_event.Principal, releaseID uint64, env domain.EventEnvelope) (domain.DeliveryPlan, error) {
	if releaseID == p.a {
		return p.planA, nil
	}
	if releaseID == p.b {
		return p.planB, nil
	}
	err, _ := domain.NewError(constant.CodeDependencyUnavailable, "当前发布不可读取", "", nil)
	return domain.DeliveryPlan{}, err
}

type planFunc func(releaseID uint64) (domain.DeliveryPlan, error)

func (f planFunc) Calculate(ctx context.Context, principal biz_event.Principal, releaseID uint64, env domain.EventEnvelope) (domain.DeliveryPlan, error) {
	return f(releaseID)
}

type fixedRelease struct {
	id   uint64
	hash string
}

func (f *fixedRelease) LoadCurrentID(ctx context.Context) (uint64, error) { return f.id, nil }

func (f *fixedRelease) Load(ctx context.Context, releaseID uint64) (data_interface.ReleaseRecord, error) {
	return data_interface.ReleaseRecord{ReleaseID: releaseID, ContentSHA256: f.hash, ContentCanonical: []byte(`{"v":1}`), FingerprintVersion: 1}, nil
}

type staticBacklog struct{}

func (staticBacklog) Measure(ctx context.Context) (int64, *time.Time, error) { return 0, nil, nil }

type hideInitialFind struct {
	inner data_interface.EventRepository
	mu    sync.Mutex
	n     int
}

func (h *hideInitialFind) FindAcceptedEvent(ctx context.Context, callerID, sourceEventID string) (data_interface.AcceptedEvent, bool, error) {
	h.mu.Lock()
	h.n++
	n := h.n
	h.mu.Unlock()
	if n <= 2 {
		return data_interface.AcceptedEvent{}, false, nil
	}
	return h.inner.FindAcceptedEvent(ctx, callerID, sourceEventID)
}

func (h *hideInitialFind) AcceptEvent(ctx context.Context, in data_interface.AcceptEventInput) (data_interface.AcceptedEvent, error) {
	return h.inner.AcceptEvent(ctx, in)
}
