// Package biz_tracking 实现 S08 查询、概览与协调清理用例。查询不触发重发或供应商调用。
package biz_tracking

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// Principal 是查询身份。普通调用方仅自身；ops:read 可在明确 caller_scopes 内跨调用方读取。
type Principal struct {
	CallerID    string
	Scopes      []string
	Permissions []string
}

func (p Principal) has(perm string) bool {
	for _, x := range p.Permissions {
		if x == perm {
			return true
		}
	}
	return false
}

// Global 表示具备运维范围，可见全局 Broker 指标。
func (p Principal) Global() bool { return p.has(constant.PermOpsRead) }

// Config 是 S08 §9 参数。
type Config struct {
	Limits              domain.TrackingLimits
	QueryTimeout        time.Duration
	OverviewCacheTTL    time.Duration
	OverviewParallelism int
	BlockedAfter        time.Duration
	UnknownAfter        time.Duration
}

// Query 是 S08 查询用例。
type Query struct {
	Store  data_interface.TrackingStore
	Keys   func() (domain.CursorKeys, error)
	Cache  data_interface.OverviewCache
	Broker data_interface.BrokerMetrics
	Config Config
	Now    func() time.Time

	once sync.Once
	sem  chan struct{}
}

// Params 是已解析的查询参数（原始字符串，由 biz 规范化）。
type Params map[string]string

// Page 是统一分页响应。
type Page[T any] struct {
	Items      []T               `json:"items"`
	NextCursor *string           `json:"next_cursor"`
	Range      *domain.TimeRange `json:"effective_time_range,omitempty"`
}

func errOf(code, msg, path string) error {
	e, _ := domain.NewError(code, msg, path, nil)
	return e
}

func (q *Query) now() time.Time {
	if q.Now != nil {
		return q.Now().UTC()
	}
	return time.Now().UTC()
}

// scopes 计算本次查询的明确范围，拒绝空范围（TRK-01、§10）。
func scopes(p Principal, callerID string) ([]string, error) {
	allowed := []string{p.CallerID}
	if p.has(constant.PermOpsRead) {
		allowed = p.Scopes
	}
	if callerID != "" {
		for _, s := range allowed {
			if s == callerID {
				return []string{callerID}, nil
			}
		}
		return nil, errOf(constant.CodeForbidden, "无权读取该调用方", "caller_id")
	}
	out := []string{}
	for _, s := range allowed {
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, errOf(constant.CodeForbidden, "查询范围为空", "")
	}
	return out, nil
}

var listParams = map[domain.ListKind]map[string]bool{
	domain.ListEvents:     {"caller_id": true, "source_business": true, "source_event_id": true, "event_id": true, "business_object_type": true, "business_object_id": true, "provider_id": true, "status": true, "accepted_from": true, "accepted_to": true, "page_size": true, "cursor": true},
	domain.ListDeliveries: {"caller_id": true, "source_business": true, "source_event_id": true, "event_id": true, "business_object_type": true, "business_object_id": true, "provider_id": true, "status": true, "accepted_from": true, "accepted_to": true, "page_size": true, "cursor": true},
	domain.ListAttempts:   {"round_no": true, "page_size": true, "cursor": true},
	domain.ListHistory:    {"page_size": true, "cursor": true},
}

// prepared 是规范化后的列表请求。
type prepared struct {
	scopes []string
	filter domain.QueryFilter
	after  []int64
	rng    *domain.TimeRange
	scope  string
	fsha   string
	kind   domain.ListKind
}

func (q *Query) prepare(p Principal, kind domain.ListKind, in Params, parent uint64) (prepared, error) {
	for k := range in {
		if !listParams[kind][k] {
			return prepared{}, errOf(constant.CodeInvalidRequest, "不支持的查询参数", k)
		}
	}
	for k, v := range in {
		if len(v) > 191 && k != "cursor" {
			return prepared{}, errOf(constant.CodeInvalidRequest, "参数过长", k)
		}
	}
	var f domain.QueryFilter
	var err error
	if f.PageSize, err = domain.ResolvePageSize(kind, in["page_size"], q.Config.Limits); err != nil {
		return prepared{}, err
	}
	sc := p.Scopes
	if kind == domain.ListEvents || kind == domain.ListDeliveries {
		if sc, err = scopes(p, in["caller_id"]); err != nil {
			return prepared{}, err
		}
		f.CallerID, f.SourceBusiness, f.SourceEventID = in["caller_id"], in["source_business"], in["source_event_id"]
		f.BusinessObjectType, f.BusinessObjectID, f.ProviderID = in["business_object_type"], in["business_object_id"], in["provider_id"]
		if raw := in["event_id"]; raw != "" {
			if f.EventID, err = domain.ParseInternalID("event_id", raw); err != nil {
				return prepared{}, err
			}
		}
		if f.Statuses, err = domain.ParseStatuses(in["status"]); err != nil {
			return prepared{}, err
		}
	} else if sc, err = scopes(p, ""); err != nil {
		return prepared{}, err
	}
	if raw := in["round_no"]; raw != "" {
		n, perr := strconv.ParseUint(raw, 10, 32)
		if perr != nil || n == 0 {
			return prepared{}, errOf(constant.CodeInvalidRequest, "round_no 不合法", "round_no")
		}
		f.RoundNo = uint32(n)
	}
	out := prepared{scopes: sc, filter: f, kind: kind, scope: domain.ScopeHash(sc), fsha: domain.FilterHash(kind, f)}
	timed := kind == domain.ListEvents || kind == domain.ListDeliveries
	if !timed {
		out.fsha = domain.FilterHash(kind, withParent(f, parent))
	}
	if c := in["cursor"]; c != "" {
		keys, kerr := q.keys()
		if kerr != nil {
			return prepared{}, kerr
		}
		cp, verr := domain.VerifyCursor(keys, c, q.now(), out.scope, out.fsha, kind)
		if verr != nil {
			return prepared{}, verr
		}
		if timed {
			// 带游标时省略的时间参数沿用原边界；显式不同则 invalid_cursor（TRK-13）。
			if in["accepted_from"] != "" || in["accepted_to"] != "" {
				rng, unb, rerr := domain.ResolveWindow(in["accepted_from"], in["accepted_to"], false, q.now(), q.Config.Limits)
				if rerr != nil || unb || cp.Range == nil || !rng.From.Equal(cp.Range.From) || !rng.To.Equal(cp.Range.To) {
					return prepared{}, errOf(constant.CodeInvalidCursor, "游标与时间参数不一致", "cursor")
				}
			}
			out.rng = cp.Range
		}
		out.after = cp.LastSort
	} else if timed {
		unique := (f.EventID != 0 || f.SourceEventID != "") && len(sc) == 1
		rng, unb, rerr := domain.ResolveWindow(in["accepted_from"], in["accepted_to"], unique, q.now(), q.Config.Limits)
		if rerr != nil {
			return prepared{}, rerr
		}
		if !unb {
			out.rng = &rng
		}
	}
	if out.rng != nil {
		out.filter.From, out.filter.To = out.rng.From, out.rng.To
	} else {
		out.filter.Unbounded = timed
	}
	return out, nil
}

func (q *Query) keys() (domain.CursorKeys, error) {
	if q.Keys == nil {
		return domain.CursorKeys{}, errOf(constant.CodeDependencyUnavailable, "游标密钥未配置", "")
	}
	k, err := q.Keys()
	if err != nil {
		return domain.CursorKeys{}, errOf(constant.CodeDependencyUnavailable, "游标密钥不可用", "")
	}
	return k, nil
}

func (q *Query) next(pr prepared, last []int64) (*string, error) {
	keys, err := q.keys()
	if err != nil {
		return nil, err
	}
	now := q.now()
	ttl := q.Config.Limits.CursorTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	c, err := domain.SignCursor(keys, domain.CursorPayload{
		ScopeHash: pr.scope, FilterSHA: pr.fsha, Order: domain.OrderOf(pr.kind), LastSort: last, Range: pr.rng,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(ttl).Unix(),
	})
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (q *Query) tq(pr prepared) data_interface.TrackingQuery {
	return data_interface.TrackingQuery{Scopes: pr.scopes, Filter: pr.filter, After: pr.after, Limit: pr.filter.PageSize + 1}
}

func (q *Query) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	t := q.Config.QueryTimeout
	if t <= 0 {
		t = 2 * time.Second
	}
	return context.WithTimeout(ctx, t)
}

// mapTimeout 把查询超时转换为 query_timeout，不返回半页。
func mapTimeout(ctx context.Context, err error) error {
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		return errOf(constant.CodeQueryTimeout, "查询超时", "")
	}
	return err
}

// Deliveries 实现 GET /v1/deliveries。
func (q *Query) Deliveries(ctx context.Context, p Principal, in Params) (Page[DeliveryView], error) {
	pr, err := q.prepare(p, domain.ListDeliveries, in, 0)
	if err != nil {
		return Page[DeliveryView]{}, err
	}
	cctx, cancel := q.ctx(ctx)
	defer cancel()
	rows, err := q.Store.QueryDeliveries(cctx, q.tq(pr))
	if err != nil {
		return Page[DeliveryView]{}, mapTimeout(cctx, err)
	}
	out := Page[DeliveryView]{Items: []DeliveryView{}, Range: pr.rng}
	for i, r := range rows {
		if i == pr.filter.PageSize {
			last := rows[i-1]
			if out.NextCursor, err = q.next(pr, []int64{last.AcceptedAt.UnixNano(), int64(last.Task.DeliveryID)}); err != nil {
				return Page[DeliveryView]{}, err
			}
			break
		}
		out.Items = append(out.Items, NewDeliveryView(r))
	}
	return out, nil
}

// Events 实现 GET /v1/events。
func (q *Query) Events(ctx context.Context, p Principal, in Params) (Page[EventView], error) {
	pr, err := q.prepare(p, domain.ListEvents, in, 0)
	if err != nil {
		return Page[EventView]{}, err
	}
	cctx, cancel := q.ctx(ctx)
	defer cancel()
	rows, err := q.Store.QueryEvents(cctx, q.tq(pr))
	if err != nil {
		return Page[EventView]{}, mapTimeout(cctx, err)
	}
	out := Page[EventView]{Items: []EventView{}, Range: pr.rng}
	for i, r := range rows {
		if i == pr.filter.PageSize {
			last := rows[i-1]
			if out.NextCursor, err = q.next(pr, []int64{last.AcceptedAt.UnixNano(), int64(last.EventID)}); err != nil {
				return Page[EventView]{}, err
			}
			break
		}
		out.Items = append(out.Items, NewEventView(r, nil))
	}
	return out, nil
}

// EventDetailLimit 是事件详情最多返回的任务摘要数。
const EventDetailLimit = 50

// Event 实现 GET /v1/events/{id}：聚合完整，任务摘要最多 50 个，超过返回 deliveries_next_cursor。
func (q *Query) Event(ctx context.Context, p Principal, id uint64) (EventView, error) {
	sc, err := scopes(p, "")
	if err != nil {
		return EventView{}, err
	}
	cctx, cancel := q.ctx(ctx)
	defer cancel()
	ev, rows, err := q.Store.LoadEventView(cctx, sc, id, EventDetailLimit+1)
	if err != nil {
		return EventView{}, mapTimeout(cctx, err)
	}
	view := NewEventView(ev, rows)
	if len(rows) > EventDetailLimit {
		view.Deliveries = view.Deliveries[:EventDetailLimit]
		last := rows[EventDetailLimit-1]
		f := domain.QueryFilter{CallerID: ev.CallerID, EventID: id, PageSize: q.Config.Limits.DefaultPageSize}
		pr := prepared{scopes: []string{ev.CallerID}, filter: f, kind: domain.ListDeliveries}
		pr.scope, pr.fsha = domain.ScopeHash(pr.scopes), domain.FilterHash(domain.ListDeliveries, f)
		if view.DeliveriesNextCursor, err = q.next(pr, []int64{last.AcceptedAt.UnixNano(), int64(last.Task.DeliveryID)}); err != nil {
			return EventView{}, err
		}
	}
	return view, nil
}

// Delivery 实现 GET /v1/deliveries/{id}。
func (q *Query) Delivery(ctx context.Context, p Principal, id uint64) (DeliveryView, error) {
	sc, err := scopes(p, "")
	if err != nil {
		return DeliveryView{}, err
	}
	cctx, cancel := q.ctx(ctx)
	defer cancel()
	row, err := q.Store.LoadDelivery(cctx, sc, id)
	if err != nil {
		return DeliveryView{}, mapTimeout(cctx, err)
	}
	return NewDeliveryView(row), nil
}

// Attempts 实现 GET /v1/deliveries/{id}/attempts，按 (round_no, attempt_no) 升序。
func (q *Query) Attempts(ctx context.Context, p Principal, id uint64, in Params) (Page[AttemptView], error) {
	pr, err := q.prepare(p, domain.ListAttempts, in, id)
	if err != nil {
		return Page[AttemptView]{}, err
	}
	cctx, cancel := q.ctx(ctx)
	defer cancel()
	rows, err := q.Store.ListAttempts(cctx, pr.scopes, id, q.tq(pr))
	if err != nil {
		return Page[AttemptView]{}, mapTimeout(cctx, err)
	}
	out := Page[AttemptView]{Items: []AttemptView{}}
	for i, r := range rows {
		if i == pr.filter.PageSize {
			last := rows[i-1]
			if out.NextCursor, err = q.next(pr, []int64{int64(last.RoundNo), int64(last.AttemptNo)}); err != nil {
				return Page[AttemptView]{}, err
			}
			break
		}
		out.Items = append(out.Items, NewAttemptView(r))
	}
	return out, nil
}

// History 实现 GET /v1/deliveries/{id}/history，按 history_seq 升序。
func (q *Query) History(ctx context.Context, p Principal, id uint64, in Params) (Page[HistoryView], error) {
	pr, err := q.prepare(p, domain.ListHistory, in, id)
	if err != nil {
		return Page[HistoryView]{}, err
	}
	cctx, cancel := q.ctx(ctx)
	defer cancel()
	rows, err := q.Store.ListHistory(cctx, pr.scopes, id, q.tq(pr))
	if err != nil {
		return Page[HistoryView]{}, mapTimeout(cctx, err)
	}
	out := Page[HistoryView]{Items: []HistoryView{}}
	for i, r := range rows {
		if i == pr.filter.PageSize {
			if out.NextCursor, err = q.next(pr, []int64{int64(rows[i-1].Seq)}); err != nil {
				return Page[HistoryView]{}, err
			}
			break
		}
		out.Items = append(out.Items, NewHistoryView(r))
	}
	return out, nil
}

// withParent 让尝试/历史游标绑定父任务，不能跨任务复用。
func withParent(f domain.QueryFilter, id uint64) domain.QueryFilter {
	f.EventID = id
	return f
}

// Metric 是概览单项：不可用时 value=null。
type Metric struct {
	Value     any    `json:"value"`
	Available bool   `json:"available"`
	SampledAt string `json:"sampled_at,omitempty"`
}

// OverviewView 是 GET /v1/overview 响应。
type OverviewView struct {
	GeneratedAt string            `json:"generated_at"`
	Scope       string            `json:"scope"`
	WindowFrom  string            `json:"window_from"`
	WindowTo    string            `json:"window_to"`
	Stale       bool              `json:"stale"`
	Metrics     map[string]Metric `json:"metrics"`
}

// Overview 实现 TRK-O01：按范围 hash 与窗口缓存；缓存故障受控回源；Broker 指标仅全局运维可用。
func (q *Query) Overview(ctx context.Context, p Principal, in Params) (OverviewView, error) {
	for k := range in {
		if k != "window" && k != "caller_id" {
			return OverviewView{}, errOf(constant.CodeInvalidRequest, "不支持的查询参数", k)
		}
	}
	sc, err := scopes(p, in["caller_id"])
	if err != nil {
		return OverviewView{}, err
	}
	window := 15 * time.Minute
	if raw := in["window"]; raw != "" {
		d, perr := time.ParseDuration(raw)
		if perr != nil || d < time.Minute || d > 24*time.Hour {
			return OverviewView{}, errOf(constant.CodeInvalidRequest, "window 超出范围", "window")
		}
		window = d
	}
	global := p.Global() && in["caller_id"] == ""
	key := "overview:" + domain.ScopeHash(sc) + ":" + strconv.FormatBool(global) + ":" + window.String()
	if q.Cache != nil && q.Config.OverviewCacheTTL > 0 {
		if raw, ok, cerr := q.Cache.Get(ctx, key); cerr == nil && ok {
			var v OverviewView
			if json.Unmarshal(raw, &v) == nil {
				return v, nil
			}
		}
	}
	q.once.Do(func() {
		n := q.Config.OverviewParallelism
		if n <= 0 {
			n = 2
		}
		q.sem = make(chan struct{}, n)
	})
	select {
	case q.sem <- struct{}{}:
		defer func() { <-q.sem }()
	default:
		return OverviewView{}, errOf(constant.CodeDependencyUnavailable, "概览统计繁忙", "")
	}
	cctx, cancel := q.ctx(ctx)
	defer cancel()
	to := q.now()
	counts, err := q.Store.Overview(cctx, data_interface.OverviewQuery{Scopes: sc, From: to.Add(-window), To: to, BlockedAfter: q.Config.BlockedAfter, UnknownAfter: q.Config.UnknownAfter})
	if err != nil {
		return OverviewView{}, mapTimeout(cctx, err)
	}
	v := buildOverview(counts, sc, global, to.Add(-window), to)
	if global && q.Broker != nil {
		if depth, berr := q.Broker.DLQDepth(cctx); berr == nil {
			v.Metrics["dlq_depth"] = Metric{Value: depth, Available: true, SampledAt: v.GeneratedAt}
		}
	}
	if q.Cache != nil && q.Config.OverviewCacheTTL > 0 {
		if raw, merr := json.Marshal(v); merr == nil {
			_ = q.Cache.Set(ctx, key, raw, q.Config.OverviewCacheTTL)
		}
	}
	return v, nil
}

func buildOverview(c data_interface.OverviewCounts, sc []string, global bool, from, to time.Time) OverviewView {
	at := c.GeneratedAt
	if at.IsZero() {
		at = to
	}
	ts := at.UTC().Format(time.RFC3339Nano)
	m := map[string]Metric{}
	put := func(name string, v any, ok bool) {
		if !ok {
			m[name] = Metric{Value: nil, Available: false}
			return
		}
		m[name] = Metric{Value: v, Available: true, SampledAt: ts}
	}
	i64 := func(name string, v *int64) {
		if v == nil {
			put(name, nil, false)
		} else {
			put(name, *v, true)
		}
	}
	f64 := func(name string, v *float64) {
		if v == nil {
			put(name, nil, false)
		} else {
			put(name, *v, true)
		}
	}
	dur := func(name string, v *time.Duration) {
		if v == nil {
			put(name, nil, false)
		} else {
			put(name, v.Milliseconds(), true)
		}
	}
	i64("pending_publish", c.PendingPublish)
	dur("oldest_publish_age_ms", c.OldestPublishAge)
	i64("due_not_executed", c.DueNotExecuted)
	i64("expired_leases", c.ExpiredLeases)
	i64("long_blocked", c.LongBlocked)
	i64("long_unknown", c.LongUnknown)
	f64("task_receive_rate", c.TaskReceiveRate)
	f64("attempt_receive_rate", c.AttemptRecvRate)
	dur("http_p50_ms", c.HTTPp50)
	dur("http_p95_ms", c.HTTPp95)
	i64("timing_incomplete", c.TimingIncomplete)
	i64("compensation_failures", c.CompensationFails)
	if c.StatusCounts != nil {
		full := map[string]int64{}
		for _, s := range domain.AllTaskStatuses {
			full[string(s)] = c.StatusCounts[string(s)]
		}
		put("status_counts", full, true)
	} else {
		put("status_counts", nil, false)
	}
	// 全局 Broker 指标：默认不可用，仅全局运维且读取成功时覆盖。
	put("dlq_depth", nil, false)
	scope := "callers"
	if global {
		scope = "global"
	}
	_ = sc
	return OverviewView{GeneratedAt: ts, Scope: scope, WindowFrom: from.UTC().Format(time.RFC3339Nano), WindowTo: to.UTC().Format(time.RFC3339Nano), Metrics: m}
}
