package biz_tracking_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"notifier/internal/biz/biz_tracking"
	"notifier/internal/constant"
	"notifier/internal/data/mysql"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
	"notifier/internal/service"
)

var now = time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)

const policy = `{"backoff_cap_ms":60000,"backoff_initial_ms":1000,"max_attempts":8,"round_timeout_seconds":86400,"server_wait_cap_seconds":3600}`

func keys(active string) domain.CursorKeys {
	return domain.CursorKeys{Active: active, Keys: map[string][]byte{
		"k1": []byte(strings.Repeat("a", 32)), "k2": []byte(strings.Repeat("b", 32)),
	}}
}

type store struct {
	data_interface.TrackingStore
	rows      []data_interface.DeliveryRow
	attempts  []data_interface.AttemptRow
	history   []data_interface.HistoryRow
	overview  data_interface.OverviewCounts
	overviews int
	lastQuery data_interface.TrackingQuery
}

func inScope(scopes []string, c string) bool {
	for _, s := range scopes {
		if s == c {
			return true
		}
	}
	return false
}

// QueryDeliveries 模拟 seek：(accepted_at, delivery_id) 倒序，并按范围与时间窗过滤。
func (s *store) QueryDeliveries(_ context.Context, q data_interface.TrackingQuery) ([]data_interface.DeliveryRow, error) {
	s.lastQuery = q
	var out []data_interface.DeliveryRow
	for _, r := range s.rows {
		if !inScope(q.Scopes, r.Task.CallerID) {
			continue
		}
		if q.Filter.ProviderID != "" && r.Task.ProviderID != q.Filter.ProviderID {
			continue
		}
		if !q.Filter.Unbounded && (r.AcceptedAt.Before(q.Filter.From) || !r.AcceptedAt.Before(q.Filter.To)) {
			continue
		}
		if len(q.After) == 2 {
			at, id := q.After[0], uint64(q.After[1])
			if !(r.AcceptedAt.UnixNano() < at || (r.AcceptedAt.UnixNano() == at && r.Task.DeliveryID < id)) {
				continue
			}
		}
		out = append(out, r)
		if len(out) == q.Limit {
			break
		}
	}
	return out, nil
}

func (s *store) LoadDelivery(_ context.Context, scopes []string, id uint64) (data_interface.DeliveryRow, error) {
	for _, r := range s.rows {
		if r.Task.DeliveryID == id && inScope(scopes, r.Task.CallerID) {
			return r, nil
		}
	}
	e, _ := domain.NewError(constant.CodeNotFound, "nf", "", nil)
	return data_interface.DeliveryRow{}, e
}

func (s *store) LoadEventView(_ context.Context, scopes []string, id uint64, limit int) (data_interface.EventRow, []data_interface.DeliveryRow, error) {
	var rows []data_interface.DeliveryRow
	var statuses []enums.TaskStatus
	for _, r := range s.rows {
		if r.Task.EventID == id && inScope(scopes, r.Task.CallerID) {
			statuses = append(statuses, r.Task.Status)
			if len(rows) < limit {
				rows = append(rows, r)
			}
		}
	}
	return data_interface.EventRow{EventID: id, CallerID: "crm", DeliveryCount: len(statuses), StatusCounts: domain.StatusCounts(statuses)}, rows, nil
}

func (s *store) ListAttempts(_ context.Context, _ []string, id uint64, q data_interface.TrackingQuery) ([]data_interface.AttemptRow, error) {
	var out []data_interface.AttemptRow
	for _, a := range s.attempts {
		if len(q.After) == 2 && !(int64(a.RoundNo) > q.After[0] || (int64(a.RoundNo) == q.After[0] && int64(a.AttemptNo) > q.After[1])) {
			continue
		}
		if q.Filter.RoundNo != 0 && a.RoundNo != q.Filter.RoundNo {
			continue
		}
		out = append(out, a)
		if len(out) == q.Limit {
			break
		}
	}
	return out, nil
}

func (s *store) ListHistory(_ context.Context, _ []string, id uint64, q data_interface.TrackingQuery) ([]data_interface.HistoryRow, error) {
	var out []data_interface.HistoryRow
	for _, h := range s.history {
		if len(q.After) == 1 && int64(h.Seq) <= q.After[0] {
			continue
		}
		out = append(out, h)
		if len(out) == q.Limit {
			break
		}
	}
	return out, nil
}

func (s *store) Overview(context.Context, data_interface.OverviewQuery) (data_interface.OverviewCounts, error) {
	s.overviews++
	return s.overview, nil
}

func row(id uint64, caller, provider string, status enums.TaskStatus, accepted time.Time) data_interface.DeliveryRow {
	sched := now.Add(10 * time.Second)
	return data_interface.DeliveryRow{
		Task: data_interface.TaskRecord{DeliveryID: id, EventID: 1001, CallerID: caller, ProviderID: provider, Status: status,
			ResultCertainty: enums.CertaintyNotReceived, RoundNo: 1, RowVersion: 3, ScheduleVersion: 2, MaxAttempts: 8,
			RoundPolicy: []byte(policy), RoundDeadlineAt: now.Add(24 * time.Hour), ScheduledAt: &sched, NextRetryAt: &sched,
			PayloadCanonical: []byte(`{"secret_payload":true}`), LeaseToken: "lease-secret"},
		SourceBusiness: "order", SourceEventID: "evt-" + caller, BusinessObjectType: "order", BusinessObjectID: "O-1",
		AcceptedAt: accepted, CreatedAt: accepted, UpdatedAt: accepted,
	}
}

func query(s *store) *biz_tracking.Query {
	k := keys("k1")
	return &biz_tracking.Query{Store: s, Keys: func() (domain.CursorKeys, error) { return k, nil }, Now: func() time.Time { return now },
		Config: biz_tracking.Config{Limits: domain.TrackingLimits{DefaultPageSize: 20, MaxPageSize: 100, DefaultWindow: 168 * time.Hour,
			MaxWindow: 744 * time.Hour, CursorTTL: 24 * time.Hour}, QueryTimeout: time.Second, OverviewParallelism: 2}}
}

var caller = biz_tracking.Principal{CallerID: "crm", Scopes: []string{"crm"}, Permissions: []string{constant.PermDataRead}}

func codeOf(err error) string {
	var de *domain.Error
	if errors.As(err, &de) {
		return de.Code
	}
	return ""
}

func TestDiagnosticCategoriesAreVisibleWithoutChangingStoredCodes(t *testing.T) {
	cases := []struct {
		code     string
		category string
	}{
		{constant.ErrHTTPTimeout, "transport"},
		{constant.ErrRateLimiterDown, "dependency"},
		{constant.ErrRateLimitWait, "quota"},
		{constant.ErrCredentialRevoked, "credential"},
		{"LEGACY_UNREGISTERED_CODE", "unknown"},
	}
	for _, tc := range cases {
		attempt := biz_tracking.NewAttemptView(data_interface.AttemptRow{AttemptRecord: data_interface.AttemptRecord{ErrorCode: tc.code}})
		delivery := biz_tracking.NewDeliveryView(data_interface.DeliveryRow{Task: data_interface.TaskRecord{LastErrorCode: tc.code}})
		if attempt.ErrorCategory == nil || *attempt.ErrorCategory != tc.category || attempt.ErrorCode == nil || *attempt.ErrorCode != tc.code {
			t.Fatalf("attempt classification = %+v", attempt)
		}
		if delivery.LastErrorCategory == nil || *delivery.LastErrorCategory != tc.category || delivery.LastErrorCode == nil || *delivery.LastErrorCode != tc.code {
			t.Fatalf("delivery classification = %+v", delivery)
		}
	}
	if biz_tracking.NewAttemptView(data_interface.AttemptRow{}).ErrorCategory != nil || biz_tracking.NewDeliveryView(data_interface.DeliveryRow{}).LastErrorCategory != nil {
		t.Fatal("no error must keep a null category")
	}
}

func TestTRK01ScopedExactFilters(t *testing.T) {
	s := &store{rows: []data_interface.DeliveryRow{
		row(3, "crm", "sms", enums.TaskRetryWait, now.Add(-time.Hour)),
		row(2, "billing", "sms", enums.TaskRetryWait, now.Add(-time.Hour)),
		row(1, "crm", "mail", enums.TaskSucceeded, now.Add(-time.Hour)),
	}}
	q := query(s)
	page, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"provider_id": "sms", "source_business": "order", "business_object_type": "order", "business_object_id": "O-1", "status": "RETRY_WAIT"})
	if err != nil || len(page.Items) != 1 || page.Items[0].DeliveryID != "3" {
		t.Fatalf("filter: %v %+v", err, page.Items)
	}
	if len(s.lastQuery.Scopes) != 1 || s.lastQuery.Scopes[0] != "crm" || s.lastQuery.Filter.BusinessObjectID != "O-1" {
		t.Fatalf("scope not pushed to store: %+v", s.lastQuery)
	}
	if _, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"caller_id": "billing"}); codeOf(err) != constant.CodeForbidden {
		t.Fatalf("cross caller: %v", err)
	}
	if _, err := q.Delivery(context.Background(), caller, 2); codeOf(err) != constant.CodeNotFound {
		t.Fatalf("cross detail: %v", err)
	}
	if _, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"sort": "status"}); codeOf(err) != constant.CodeInvalidRequest {
		t.Fatalf("sort must be rejected: %v", err)
	}
	sqlText, args, err := mysql.BuildDeliveriesQuery(data_interface.TrackingQuery{Scopes: []string{"crm"}, Filter: domain.QueryFilter{ProviderID: "sms", BusinessObjectID: "O-1", Unbounded: true}, Limit: 21})
	if err != nil || !strings.Contains(sqlText, "e.caller_id IN (?)") || !strings.Contains(sqlText, "e.business_object_id = ?") || args[0] != "crm" {
		t.Fatalf("sql: %v %s %v", err, sqlText, args)
	}
	if _, _, err := mysql.BuildDeliveriesQuery(data_interface.TrackingQuery{Limit: 21}); err == nil {
		t.Fatal("empty scope must not mean all callers")
	}
}

func TestTRK02StablePagingAndCursorBinding(t *testing.T) {
	same := now.Add(-time.Hour)
	s := &store{}
	for id := uint64(5); id >= 1; id-- {
		s.rows = append(s.rows, row(id, "crm", "sms", enums.TaskPending, same))
	}
	q := query(s)
	seen := []string{}
	params := biz_tracking.Params{"page_size": "2"}
	for i := 0; i < 5; i++ {
		page, err := q.Deliveries(context.Background(), caller, params)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range page.Items {
			seen = append(seen, it.DeliveryID)
		}
		if page.NextCursor == nil {
			break
		}
		params = biz_tracking.Params{"page_size": "2", "cursor": *page.NextCursor}
	}
	if strings.Join(seen, ",") != "5,4,3,2,1" {
		t.Fatalf("unstable paging: %v", seen)
	}
	first, _ := q.Deliveries(context.Background(), caller, biz_tracking.Params{"page_size": "2"})
	c := *first.NextCursor
	tampered := c[:len(c)-2] + "xx"
	for name, p := range map[string]biz_tracking.Params{
		"tampered":       {"page_size": "2", "cursor": tampered},
		"filter changed": {"page_size": "2", "cursor": c, "provider_id": "mail"},
		"size changed":   {"page_size": "3", "cursor": c},
	} {
		if _, err := q.Deliveries(context.Background(), caller, p); codeOf(err) != constant.CodeInvalidCursor {
			t.Fatalf("%s: %v", name, err)
		}
	}
	ops := biz_tracking.Principal{CallerID: "ops", Scopes: []string{"crm", "billing"}, Permissions: []string{constant.PermOpsRead}}
	if _, err := q.Deliveries(context.Background(), ops, biz_tracking.Params{"page_size": "2", "cursor": c}); codeOf(err) != constant.CodeInvalidCursor {
		t.Fatalf("cross scope: %v", err)
	}
}

func TestTRK03AggregateAndDetailSemantics(t *testing.T) {
	ok := row(1, "crm", "sms", enums.TaskSucceeded, now)
	wait := row(2, "crm", "sms", enums.TaskRetryWait, now)
	unknown := row(3, "crm", "mail", enums.TaskUnknown, now)
	unknown.Task.ResultCertainty = enums.CertaintyUncertain
	unknown.Task.Unresolved = []data_interface.UnresolvedRef{{AttemptID: 4001, RoundNo: 1}}
	s := &store{rows: []data_interface.DeliveryRow{ok, wait, unknown}}
	ev, err := query(s).Event(context.Background(), caller, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if len(ev.StatusCounts) != 7 || ev.StatusCounts["SUCCEEDED"] != 1 || ev.StatusCounts["RETRY_WAIT"] != 1 || ev.StatusCounts["UNKNOWN"] != 1 || ev.StatusCounts["FAILED"] != 0 {
		t.Fatalf("counts: %+v", ev.StatusCounts)
	}
	var w, u biz_tracking.DeliveryView
	for _, d := range ev.Deliveries {
		switch d.DeliveryID {
		case "2":
			w = d
		case "3":
			u = d
		}
	}
	if w.NextRetryAt == nil || w.Policy == nil || w.Policy.MaxAttempts != 8 || w.NeedsManualAction {
		t.Fatalf("retry view: %+v", w)
	}
	if u.NextRetryAt != nil || !u.NeedsManualAction || len(u.UnresolvedAttempts) != 1 {
		t.Fatalf("unknown view: %+v", u)
	}
	raw, _ := json.Marshal(u)
	if strings.Contains(string(raw), "lease-secret") || strings.Contains(string(raw), "secret_payload") {
		t.Fatalf("leaks internals: %s", raw)
	}
}

func attempt(id uint64, round, no uint32, outcome enums.Outcome) data_interface.AttemptRow {
	code := 504
	d := 30 * time.Second
	start := now.Add(time.Duration(id) * time.Minute)
	return data_interface.AttemptRow{AttemptRecord: data_interface.AttemptRecord{AttemptID: id, DeliveryID: 1, RoundNo: round, AttemptNo: no,
		Status: "FINISHED", SendState: enums.SendState("HTTP_INVOKED"), Outcome: outcome, HTTPStatus: &code, StartedAt: start,
		FinishedAt: &start, HTTPDuration: &d, AttemptDuration: &d}}
}

func TestTRK04AttemptsAndHistoryAcrossRounds(t *testing.T) {
	s := &store{
		attempts: []data_interface.AttemptRow{attempt(1, 1, 1, enums.OutcomeUncertain), attempt(2, 1, 2, enums.Outcome("RETRYABLE_REJECT")), attempt(3, 2, 1, enums.Outcome("RECEIVED"))},
		history: []data_interface.HistoryRow{
			{HistoryRecord: data_interface.HistoryRecord{Seq: 1, Phase: "TASK_CREATED"}}, {HistoryRecord: data_interface.HistoryRecord{Seq: 2, Phase: "ATTEMPT_FINISHED", AttemptID: 1}},
			{HistoryRecord: data_interface.HistoryRecord{Seq: 3, Phase: "MANUAL_RETRY"}, OperationID: "op1", Actor: "ops-1"}, {HistoryRecord: data_interface.HistoryRecord{Seq: 4, Phase: "ATTEMPT_FINISHED", AttemptID: 3}},
		},
	}
	q := query(s)
	all := []string{}
	params := biz_tracking.Params{"page_size": "2"}
	for {
		p, err := q.Attempts(context.Background(), caller, 1, params)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range p.Items {
			all = append(all, a.AttemptID+":"+*a.Outcome)
		}
		if p.NextCursor == nil {
			break
		}
		params = biz_tracking.Params{"page_size": "2", "cursor": *p.NextCursor}
	}
	if strings.Join(all, ",") != "1:UNCERTAIN,2:RETRYABLE_REJECT,3:RECEIVED" {
		t.Fatalf("attempts: %v", all)
	}
	r2, _ := q.Attempts(context.Background(), caller, 1, biz_tracking.Params{"round_no": "2"})
	if len(r2.Items) != 1 || r2.Items[0].RoundNo != 2 {
		t.Fatalf("round filter: %+v", r2.Items)
	}
	h, err := q.History(context.Background(), caller, 1, biz_tracking.Params{})
	if err != nil || len(h.Items) != 4 || h.Items[2].Phase != "MANUAL_RETRY" || *h.Items[2].OperationID != "op1" {
		t.Fatalf("history: %v %+v", err, h.Items)
	}
	// 尝试游标绑定父任务，不能跨任务复用。
	p, _ := q.Attempts(context.Background(), caller, 1, biz_tracking.Params{"page_size": "1"})
	if _, err := q.Attempts(context.Background(), caller, 2, biz_tracking.Params{"page_size": "1", "cursor": *p.NextCursor}); codeOf(err) != constant.CodeInvalidCursor {
		t.Fatalf("cross parent cursor: %v", err)
	}
}

func TestTRK05NullsKeepRealMeaning(t *testing.T) {
	notSent := data_interface.AttemptRow{AttemptRecord: data_interface.AttemptRecord{AttemptID: 1, DeliveryID: 1, RoundNo: 1, AttemptNo: 1,
		Status: "FINISHED", SendState: enums.SendState("NOT_SENT"), Outcome: enums.Outcome("LOCAL_FAILURE"), StartedAt: now}}
	crashed := data_interface.AttemptRow{AttemptRecord: data_interface.AttemptRecord{AttemptID: 2, DeliveryID: 1, RoundNo: 1, AttemptNo: 2,
		Status: "FINISHED", SendState: enums.SendState("UNCERTAIN"), Outcome: enums.OutcomeUncertain, StartedAt: now, TimingIncomplete: true}}
	readFail := attempt(3, 1, 3, enums.OutcomeUncertain)
	f := false
	readFail.BodyTruncated = &f
	readFail.ErrorCode = "RESPONSE_READ_FAILED"
	open := data_interface.AttemptRow{AttemptRecord: data_interface.AttemptRecord{AttemptID: 4, DeliveryID: 1, RoundNo: 1, AttemptNo: 4, Status: "PROCESSING", StartedAt: now}}
	v1, v2, v3, v4 := biz_tracking.NewAttemptView(notSent), biz_tracking.NewAttemptView(crashed), biz_tracking.NewAttemptView(readFail), biz_tracking.NewAttemptView(open)
	if v1.HTTPStatus != nil || v1.HTTPDurationMs != nil || v1.ResponseSummary != nil {
		t.Fatalf("not sent: %+v", v1)
	}
	if !v2.TimingIncomplete || v2.AttemptDurationMs != nil || v2.HTTPStatus != nil {
		t.Fatalf("crash recovery: %+v", v2)
	}
	if v3.ResponseSummary != nil || v3.ErrorCode == nil || *v3.ErrorCode != "RESPONSE_READ_FAILED" || v3.BodyTruncated == nil {
		t.Fatalf("read failure: %+v", v3)
	}
	if v4.FinishedAt != nil || v4.Outcome != nil || v4.Retryable != nil {
		t.Fatalf("open attempt: %+v", v4)
	}
	raw, _ := json.Marshal(v1)
	if !strings.Contains(string(raw), `"http_status":null`) {
		t.Fatalf("null must be explicit: %s", raw)
	}
}

func TestTRK06TimelineFromMySQLOnly(t *testing.T) {
	// 查询只依赖仓储（MySQL）事实：无日志/trace 输入，重建 Query 实例（模拟重启）后仍能还原恢复过程。
	s := &store{history: []data_interface.HistoryRow{
		{HistoryRecord: data_interface.HistoryRecord{Seq: 1, Phase: "TASK_CREATED"}},
		{HistoryRecord: data_interface.HistoryRecord{Seq: 2, Phase: "LEASE_EXPIRED", Reason: "worker crashed"}},
		{HistoryRecord: data_interface.HistoryRecord{Seq: 3, Phase: "DISPATCH_REPAIRED"}},
	}}
	for i := 0; i < 2; i++ {
		h, err := query(s).History(context.Background(), caller, 1, biz_tracking.Params{})
		if err != nil || len(h.Items) != 3 || h.Items[1].Phase != "LEASE_EXPIRED" || h.Items[2].HistorySeq != 3 {
			t.Fatalf("timeline: %v %+v", err, h.Items)
		}
	}
}

type cache struct {
	fail bool
	data map[string][]byte
}

func (c *cache) Get(_ context.Context, k string) ([]byte, bool, error) {
	if c.fail {
		return nil, false, errors.New("redis down")
	}
	v, ok := c.data[k]
	return v, ok, nil
}

func (c *cache) Set(_ context.Context, k string, v []byte, _ time.Duration) error {
	if c.fail {
		return errors.New("redis down")
	}
	c.data[k] = v
	return nil
}

type broker struct{ err error }

func (b broker) DLQDepth(context.Context) (int64, error) { return 7, b.err }

func TestTRK07CacheFallbackAndUnavailableNotZero(t *testing.T) {
	n := int64(2)
	s := &store{overview: data_interface.OverviewCounts{GeneratedAt: now, PendingPublish: &n, StatusCounts: map[string]int64{"PENDING": 2}}}
	q := query(s)
	q.Cache, q.Config.OverviewCacheTTL = &cache{fail: true}, 5*time.Second
	v, err := q.Overview(context.Background(), caller, biz_tracking.Params{})
	if err != nil || s.overviews != 1 {
		t.Fatalf("fallback: %v %d", err, s.overviews)
	}
	if m := v.Metrics["http_p95_ms"]; m.Available || m.Value != nil {
		t.Fatalf("unavailable must be null: %+v", m)
	}
	if m := v.Metrics["pending_publish"]; !m.Available || m.Value != int64(2) {
		t.Fatalf("pending: %+v", m)
	}
	if sc := v.Metrics["status_counts"].Value.(map[string]int64); len(sc) != 7 {
		t.Fatalf("status counts: %+v", sc)
	}
	c := &cache{data: map[string][]byte{}}
	q2 := query(s)
	q2.Cache, q2.Config.OverviewCacheTTL = c, 5*time.Second
	_, _ = q2.Overview(context.Background(), caller, biz_tracking.Params{})
	_, _ = q2.Overview(context.Background(), caller, biz_tracking.Params{})
	if s.overviews != 2 {
		t.Fatalf("cache should serve second read, store calls=%d", s.overviews)
	}
	ops := biz_tracking.Principal{CallerID: "ops", Scopes: []string{"crm", "billing"}, Permissions: []string{constant.PermOpsRead}}
	q2.Broker = broker{err: errors.New("broker down")}
	v, _ = q2.Overview(context.Background(), ops, biz_tracking.Params{})
	if m := v.Metrics["dlq_depth"]; m.Available || m.Value != nil {
		t.Fatalf("broker unavailable shown as value: %+v", m)
	}
}

func finished(d time.Duration) *time.Time { t := now.Add(-d); return &t }

func TestTRK08ActiveOrUnknownNeverCleaned(t *testing.T) {
	old := 60 * 24 * time.Hour
	cutoff := now.Add(-30 * 24 * time.Hour)
	for _, st := range []enums.TaskStatus{enums.TaskBlocked, enums.TaskUnknown, enums.TaskPending, enums.TaskRetryWait, enums.TaskProcessing} {
		tasks := []domain.RetentionTask{{Status: enums.TaskSucceeded, RoundFinishedAt: finished(old)}, {Status: st, RoundFinishedAt: finished(old)}}
		if got := domain.CheckRetention(tasks, *finished(old), cutoff, 10, 5000); got != domain.RetentionSkip {
			t.Fatalf("%s: %s", st, got)
		}
	}
	unresolved := []domain.RetentionTask{{Status: enums.TaskFailed, RoundFinishedAt: finished(old), Unresolved: 1}}
	if domain.CheckRetention(unresolved, *finished(old), cutoff, 10, 5000) != domain.RetentionSkip {
		t.Fatal("unresolved evidence must be kept")
	}
	ok := []domain.RetentionTask{{Status: enums.TaskSucceeded, RoundFinishedAt: finished(old)}, {Status: enums.TaskFailed, RoundFinishedAt: finished(old)}}
	if domain.CheckRetention(ok, *finished(old), cutoff, 10, 5000) != domain.RetentionDelete {
		t.Fatal("resolved event should be eligible")
	}
}

func TestTRK09RetryAfterDiscoverySkips(t *testing.T) {
	old := 60 * 24 * time.Hour
	cutoff := now.Add(-30 * 24 * time.Hour)
	discovered := *finished(old)
	// 人工重投：任务回到 PENDING，结案时间清空。
	retried := []domain.RetentionTask{{Status: enums.TaskPending}}
	if domain.CheckRetention(retried, discovered, cutoff, 10, 5000) != domain.RetentionSkip {
		t.Fatal("retried task must not be cleaned")
	}
	// 重投后又结案：最后结案时间变化，重检失败。
	reclosed := []domain.RetentionTask{{Status: enums.TaskSucceeded, RoundFinishedAt: finished(time.Hour)}}
	if domain.CheckRetention(reclosed, discovered, cutoff, 10, 5000) != domain.RetentionSkip {
		t.Fatal("changed resolution must skip")
	}
}

func TestTRK10SharedConfigKept(t *testing.T) {
	if domain.ReleaseCleanable(false, 1) || domain.ReleaseCleanable(true, 0) || !domain.ReleaseCleanable(false, 0) {
		t.Fatal("release cleanability")
	}
}

func TestTRK11ReadinessDegradesWithoutKillingLiveness(t *testing.T) {
	base := domain.ReadinessFacts{MySQL: true, Migrations: true, Release: true, Identities: true, MQ: false, Backlog: 10, BacklogThreshold: 100, BacklogKnown: true}
	r := domain.EvaluateReadiness(base)
	if !r.Ready || r.Admission != "open" || r.Components["publisher"] != domain.ComponentDegraded || r.Components["api"] != domain.ComponentReady {
		t.Fatalf("mq down within backlog: %+v", r)
	}
	base.Backlog = 101
	r = domain.EvaluateReadiness(base)
	if r.Ready || r.Admission != "paused" || r.ReasonCode != "admission_backlog" {
		t.Fatalf("over backlog: %+v", r)
	}
	h := service.NewHealthService(nil).WithReport(func(context.Context) domain.ReadinessReport { return r })
	rec := httptest.NewRecorder()
	h.Readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"admission":"paused"`) || strings.Contains(rec.Body.String(), "dsn") {
		t.Fatalf("readyz: %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.Healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz must stay alive: %d", rec.Code)
	}
	if domain.EvaluateReadiness(domain.ReadinessFacts{MySQL: true, Migrations: true, Release: false, Identities: true, MQ: true, BacklogKnown: true, BacklogThreshold: 1}).Ready {
		t.Fatal("missing release must block readiness")
	}
}

func TestTRK12TruncationAndLateEvidence(t *testing.T) {
	a := attempt(1, 1, 1, enums.OutcomeUncertain)
	a.ResponseSummary, a.ResponseTruncated, a.RequestTruncated = strings.Repeat("x", 2048), true, true
	tr := true
	a.BodyTruncated = &tr
	v := biz_tracking.NewAttemptView(a)
	if !v.ResponseTruncated || !v.RequestTruncated || v.BodyTruncated == nil || !*v.BodyTruncated || len(*v.ResponseSummary) != 2048 {
		t.Fatalf("truncation: %+v", v)
	}
	late := data_interface.HistoryRow{HistoryRecord: data_interface.HistoryRecord{Seq: 9, Phase: "LATE_EVIDENCE", AttemptID: 1,
		OccurredAt: now.Add(-time.Hour), Evidence: []byte(`{"outcome":"RECEIVED"}`)}, RecordedAt: now}
	hv := biz_tracking.NewHistoryView(late)
	if hv.HistorySeq != 9 || *hv.OccurredAt == hv.RecordedAt || string(hv.Evidence) != `{"outcome":"RECEIVED"}` {
		t.Fatalf("late evidence: %+v", hv)
	}
	bad := biz_tracking.NewHistoryView(data_interface.HistoryRow{HistoryRecord: data_interface.HistoryRecord{Seq: 1, Evidence: []byte("not json")}})
	if bad.Evidence != nil {
		t.Fatal("invalid evidence must not be emitted raw")
	}
	// 迟到证据不覆盖原尝试：原尝试视图保持 UNCERTAIN。
	if *v.Outcome != "UNCERTAIN" {
		t.Fatal("attempt overwritten")
	}
}

func TestTRK13FixedWindowKeyRotationExpiry(t *testing.T) {
	s := &store{}
	for id := uint64(3); id >= 1; id-- {
		s.rows = append(s.rows, row(id, "crm", "sms", enums.TaskPending, now.Add(-time.Duration(4-id)*time.Hour)))
	}
	q := query(s)
	first, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"page_size": "1"})
	if err != nil || first.Range == nil {
		t.Fatalf("first: %v", err)
	}
	fixed := *first.Range
	// 时间前进且密钥轮换（k2 生效，k1 保留）。
	later := now.Add(2 * time.Hour)
	q.Now = func() time.Time { return later }
	k := keys("k2")
	q.Keys = func() (domain.CursorKeys, error) { return k, nil }
	second, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"page_size": "1", "cursor": *first.NextCursor})
	if err != nil || !second.Range.From.Equal(fixed.From) || !second.Range.To.Equal(fixed.To) {
		t.Fatalf("fixed window: %v %+v", err, second.Range)
	}
	if !s.lastQuery.Filter.To.Equal(fixed.To) {
		t.Fatal("store must use cursor window")
	}
	other := now.Add(-time.Hour).Format(time.RFC3339)
	if _, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"page_size": "1", "cursor": *first.NextCursor, "accepted_from": other, "accepted_to": later.Format(time.RFC3339)}); codeOf(err) != constant.CodeInvalidCursor {
		t.Fatalf("explicit different window: %v", err)
	}
	q.Now = func() time.Time { return now.Add(25 * time.Hour) }
	if _, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"page_size": "1", "cursor": *first.NextCursor}); codeOf(err) != constant.CodeInvalidCursor {
		t.Fatalf("expired: %v", err)
	}
	// 移除 k1 后旧游标失效。
	k = domain.CursorKeys{Active: "k2", Keys: map[string][]byte{"k2": []byte(strings.Repeat("b", 32))}}
	q.Now = func() time.Time { return later }
	if _, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"page_size": "1", "cursor": *first.NextCursor}); codeOf(err) != constant.CodeInvalidCursor {
		t.Fatalf("retired key: %v", err)
	}
	// 显式窗口超过上限返回 400，不截断。
	if _, err := q.Deliveries(context.Background(), caller, biz_tracking.Params{"accepted_from": now.Add(-800 * time.Hour).Format(time.RFC3339), "accepted_to": now.Format(time.RFC3339)}); codeOf(err) != constant.CodeInvalidRequest {
		t.Fatalf("window too large: %v", err)
	}
}

func TestTRK14ProviderAndStatusOnSameTask(t *testing.T) {
	sqlText, args, err := mysql.BuildEventsQuery(data_interface.TrackingQuery{Scopes: []string{"crm"},
		Filter: domain.QueryFilter{ProviderID: "sms", Statuses: []string{"FAILED"}, Unbounded: true}, Limit: 21})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(sqlText, "EXISTS") != 1 || !strings.Contains(sqlText, "t.event_id = e.event_id AND t.provider_id = ? AND t.status IN (?)") {
		t.Fatalf("must be one correlated subquery: %s", sqlText)
	}
	if len(args) != 4 || args[1] != "sms" || args[2] != "FAILED" {
		t.Fatalf("args: %v", args)
	}
	if strings.Contains(sqlText, "JOIN delivery_tasks") {
		t.Fatal("event list must not join tasks (would trim aggregates)")
	}
}

func TestTRK15CallerOverviewHidesGlobal(t *testing.T) {
	s := &store{overview: data_interface.OverviewCounts{GeneratedAt: now}}
	c := &cache{data: map[string][]byte{}}
	q := query(s)
	q.Cache, q.Config.OverviewCacheTTL, q.Broker = c, 5*time.Second, broker{}
	ops := biz_tracking.Principal{CallerID: "ops", Scopes: []string{"crm", "billing"}, Permissions: []string{constant.PermOpsRead}}
	g, err := q.Overview(context.Background(), ops, biz_tracking.Params{})
	if err != nil || g.Scope != "global" || g.Metrics["dlq_depth"].Value != int64(7) {
		t.Fatalf("global: %v %+v", err, g.Metrics["dlq_depth"])
	}
	v, err := q.Overview(context.Background(), caller, biz_tracking.Params{})
	if err != nil || v.Scope != "callers" || v.Metrics["dlq_depth"].Available || v.Metrics["dlq_depth"].Value != nil {
		t.Fatalf("caller overview leaked global: %v %+v", err, v.Metrics["dlq_depth"])
	}
	if s.overviews != 2 || len(c.data) != 2 {
		t.Fatalf("cache must be keyed by scope: calls=%d keys=%d", s.overviews, len(c.data))
	}
}

type retention struct {
	cands []data_interface.RetentionCandidate
	dec   domain.RetentionDecision
}

func (r *retention) ListRetentionCandidates(context.Context, time.Time, int) ([]data_interface.RetentionCandidate, error) {
	return r.cands, nil
}

func (r *retention) DeleteResolvedEvent(context.Context, data_interface.RetentionCandidate, time.Time, int) (domain.RetentionDecision, error) {
	return r.dec, nil
}

func TestTRK16OversizedEventKeptWhole(t *testing.T) {
	old := 60 * 24 * time.Hour
	tasks := []domain.RetentionTask{{Status: enums.TaskSucceeded, RoundFinishedAt: finished(old)}}
	if domain.CheckRetention(tasks, *finished(old), now.Add(-30*24*time.Hour), 5001, 5000) != domain.RetentionTooLarge {
		t.Fatal("oversized event must be kept")
	}
	r := &retention{cands: []data_interface.RetentionCandidate{{EventID: 1}}, dec: domain.RetentionTooLarge}
	c := &biz_tracking.Cleaner{Store: r, Counters: &biz_tracking.RetentionCounters{}, Config: biz_tracking.RetentionConfig{ResolvedAfter: 720 * time.Hour, BatchEvents: 10, MaxRowsPerEvent: 5000}}
	if err := c.ScanOnce(context.Background()); err != nil || c.Counters.TooLarge.Load() != 1 || c.Counters.Deleted.Load() != 0 {
		t.Fatalf("cleaner: %v too_large=%d", err, c.Counters.TooLarge.Load())
	}
}
