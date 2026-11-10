// Package biz_event 受理已经发生的业务事实，并在一个事务里保存固定计划。
// 不发布 MQ，不调用供应商。发布由 S04 完成，任务状态机由 S03 完成。
package biz_event

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// Principal 是已认证的调用方。请求体里的 caller_id 不进入这里。
type Principal struct {
	ActorID        string
	CallerID       string
	AllowedSources []string
	Scopes         []string
}

// Delivery 是受理响应里的固定计划摘要。
type Delivery struct {
	DeliveryID        uint64
	RouteTargetID     string
	ProviderID        string
	ProviderAccountID string
	Operation         string
}

// Acceptance 是首次受理或同内容重复提交的结果。
type Acceptance struct {
	EventID          uint64
	SourceBusiness   string
	SourceEventID    string
	AcceptedAt       time.Time
	RoutingReleaseID uint64
	Deliveries       []Delivery
}

// Limits 是启动时注入的受理上限。
type Limits struct {
	MaxBody        int
	MaxFanout      int
	MaxPlanBytes   int
	MaxPending     int64
	MaxOldest      time.Duration
	SampleInterval time.Duration
	SampleMaxAge   time.Duration
	Namespace      string
}

// Planner 按已经选定的发布计算计划。实现来自 S01。
type Planner interface {
	Calculate(ctx context.Context, principal Principal, releaseID uint64, env domain.EventEnvelope) (domain.DeliveryPlan, error)
}

// ReleaseReader 读取当前指针和不可变发布摘要。
type ReleaseReader interface {
	LoadCurrentID(ctx context.Context) (uint64, error)
	Load(ctx context.Context, releaseID uint64) (data_interface.ReleaseRecord, error)
}

// UseCase 编排幂等、准入、计划计算和原子保存。
type UseCase struct {
	events   data_interface.EventRepository
	backlog  data_interface.BacklogSource
	releases ReleaseReader
	planner  Planner
	limits   Limits
	now      func() time.Time

	mu     sync.Mutex
	sample backlogSample
}

type backlogSample struct {
	at      time.Time
	pending int64
	oldest  *time.Time
	ok      bool
}

func NewUseCase(events data_interface.EventRepository, backlog data_interface.BacklogSource, releases ReleaseReader, planner Planner, limits Limits, now func() time.Time) *UseCase {
	if now == nil {
		now = time.Now
	}
	return &UseCase{events: events, backlog: backlog, releases: releases, planner: planner, limits: limits, now: now}
}

// Submit 处理一次事件提交。已有事件先按原指纹返回，不重新展开计划。
func (u *UseCase) Submit(ctx context.Context, principal Principal, raw []byte) (Acceptance, error) {
	if len(raw) > u.limits.MaxBody {
		return Acceptance{}, fail(constant.CodePayloadTooLarge, "请求体超过上限", "")
	}
	env, err := parseEnvelope(principal, raw)
	if err != nil {
		return Acceptance{}, err
	}
	_, fingerprint, err := domain.FingerprintEvent(env)
	if err != nil {
		return Acceptance{}, err
	}
	env.FingerprintVersion = constant.FingerprintVersion
	existing, ok, err := u.events.FindAcceptedEvent(ctx, env.CallerID, env.SourceEventID)
	if err != nil {
		return Acceptance{}, infra(err)
	}
	if ok {
		return acceptanceOf(existing, fingerprint)
	}
	if err := u.admit(ctx); err != nil {
		return u.preferExisting(ctx, env, fingerprint, err)
	}
	releaseID, hash, err := u.currentRelease(ctx)
	if err != nil {
		return u.preferExisting(ctx, env, fingerprint, err)
	}
	plan, err := u.planner.Calculate(ctx, principal, releaseID, env)
	if err != nil {
		return u.preferExisting(ctx, env, fingerprint, translatePlan(err))
	}
	if err := checkPlan(plan, releaseID, u.limits); err != nil {
		return u.preferExisting(ctx, env, fingerprint, err)
	}
	saved, err := u.events.AcceptEvent(ctx, data_interface.AcceptEventInput{
		CallerID: env.CallerID, AllowedSources: principal.AllowedSources,
		SourceBusiness: env.SourceBusiness, SourceEventID: env.SourceEventID,
		EventType: env.EventType, SchemaVersion: env.SchemaVersion, OccurredAt: env.OccurredAt,
		BusinessObjectType: env.BusinessObjectType, BusinessObjectID: env.BusinessObjectID,
		DataCanonical: env.DataCanonical, FingerprintVersion: constant.FingerprintVersion, Fingerprint: fingerprint,
		ReleaseID: releaseID, ExpectedReleaseHash: hash, Plan: plan,
		IdempotencyNamespace: u.limits.Namespace, MaxFanout: u.limits.MaxFanout, MaxPlanBytes: u.limits.MaxPlanBytes,
	})
	if err == nil {
		return acceptanceOf(saved, fingerprint)
	}
	if errors.Is(err, data_interface.ErrEventUnique) || errors.Is(err, data_interface.ErrCommitUncertain) {
		return u.recover(ctx, env, fingerprint)
	}
	if errors.Is(err, data_interface.ErrInconsistent) {
		return Acceptance{}, fail(constant.CodeInternal, "受理数据不一致", "")
	}
	return u.preferExisting(ctx, env, fingerprint, infra(err))
}

func (u *UseCase) admit(ctx context.Context) error {
	pending, oldest, err := u.backlogSample(ctx)
	if err != nil {
		return fail(constant.ReasonBacklogUnavailable, "积压统计不可用", "")
	}
	if pending > u.limits.MaxPending {
		return fail(constant.ReasonBacklogCount, "积压超过受理阈值", "")
	}
	if oldest != nil && u.now().Sub(*oldest) > u.limits.MaxOldest {
		return fail(constant.ReasonBacklogAge, "积压时间超过受理阈值", "")
	}
	return nil
}

func (u *UseCase) backlogSample(ctx context.Context) (int64, *time.Time, error) {
	now := u.now()
	u.mu.Lock()
	cached := u.sample
	u.mu.Unlock()
	if cached.ok && now.Sub(cached.at) < u.limits.SampleInterval {
		return cached.pending, cached.oldest, nil
	}
	pending, oldest, err := u.backlog.Measure(ctx)
	if err != nil {
		if cached.ok && now.Sub(cached.at) <= u.limits.SampleMaxAge {
			return cached.pending, cached.oldest, nil
		}
		return 0, nil, err
	}
	if oldest != nil {
		copyAt := oldest.UTC()
		oldest = &copyAt
	}
	u.mu.Lock()
	u.sample = backlogSample{at: now, pending: pending, oldest: oldest, ok: true}
	u.mu.Unlock()
	return pending, oldest, nil
}

func (u *UseCase) currentRelease(ctx context.Context) (uint64, string, error) {
	id, err := u.releases.LoadCurrentID(ctx)
	if errors.Is(err, data_interface.ErrNoCurrent) || errors.Is(err, data_interface.ErrReleaseNotFound) {
		return 0, "", fail(constant.ReasonEventRelease, "当前发布不可读取", "")
	}
	if err != nil {
		return 0, "", translatePlan(err)
	}
	rec, err := u.releases.Load(ctx, id)
	if errors.Is(err, data_interface.ErrReleaseNotFound) || errors.Is(err, data_interface.ErrNoCurrent) {
		return 0, "", fail(constant.ReasonEventRelease, "当前发布不可读取", "")
	}
	if err != nil {
		return 0, "", translatePlan(err)
	}
	if rec.ContentSHA256 == "" || rec.ReleaseID != id {
		return 0, "", fail(constant.ReasonEventRelease, "当前发布不可读取", "")
	}
	return id, rec.ContentSHA256, nil
}

func (u *UseCase) preferExisting(ctx context.Context, env domain.EventEnvelope, fingerprint string, fallback error) (Acceptance, error) {
	existing, ok, err := u.events.FindAcceptedEvent(ctx, env.CallerID, env.SourceEventID)
	if err != nil {
		return Acceptance{}, infra(err)
	}
	if !ok {
		return Acceptance{}, fallback
	}
	return acceptanceOf(existing, fingerprint)
}

func (u *UseCase) recover(ctx context.Context, env domain.EventEnvelope, fingerprint string) (Acceptance, error) {
	existing, ok, err := u.events.FindAcceptedEvent(ctx, env.CallerID, env.SourceEventID)
	if err != nil || !ok {
		return Acceptance{}, fail(constant.CodeAcceptanceUnconfirmed, "受理结果尚未确认，请沿用原来源事件 ID 重试", "source_event_id")
	}
	return acceptanceOf(existing, fingerprint)
}

func acceptanceOf(ev data_interface.AcceptedEvent, fingerprint string) (Acceptance, error) {
	same, err := domain.SameFingerprint(ev.FingerprintVersion, ev.Fingerprint, fingerprint)
	if err != nil {
		return Acceptance{}, err
	}
	if !same {
		return Acceptance{}, fail(constant.ReasonEventIDConflict, "相同来源事件的内容不一致", "source_event_id")
	}
	if len(ev.Deliveries) == 0 {
		return Acceptance{}, fail(constant.CodeInternal, "受理数据不一致", "")
	}
	out := Acceptance{
		EventID: ev.EventID, SourceBusiness: ev.SourceBusiness, SourceEventID: ev.SourceEventID,
		AcceptedAt: ev.AcceptedAt, RoutingReleaseID: ev.RoutingReleaseID,
		Deliveries: make([]Delivery, len(ev.Deliveries)),
	}
	for i, item := range ev.Deliveries {
		out.Deliveries[i] = Delivery{
			DeliveryID: item.DeliveryID, RouteTargetID: item.RouteTargetID,
			ProviderID: item.ProviderID, ProviderAccountID: item.ProviderAccountID, Operation: item.Operation,
		}
	}
	slices.SortFunc(out.Deliveries, func(a, b Delivery) int {
		if a.RouteTargetID < b.RouteTargetID {
			return -1
		}
		if a.RouteTargetID > b.RouteTargetID {
			return 1
		}
		return 0
	})
	return out, nil
}

func checkPlan(plan domain.DeliveryPlan, releaseID uint64, limits Limits) error {
	ref, err := domain.FormatInternalID(releaseID)
	if err != nil || plan.ReleaseRef != ref {
		return fail(constant.ReasonEventRelease, "当前发布不可读取", "")
	}
	if len(plan.Targets) == 0 {
		return fail(constant.CodeNoMatchingRoute, "没有匹配的路由", "")
	}
	if len(plan.Targets) > limits.MaxFanout {
		return fail(constant.CodeFanoutExceeded, "扇出超过上限", "/targets")
	}
	var n int64
	seen := map[string]struct{}{}
	for _, target := range plan.Targets {
		if _, ok := seen[target.RouteTargetID]; ok {
			return fail(constant.ReasonDuplicateTarget, "路由目标重复", target.RouteTargetID)
		}
		seen[target.RouteTargetID] = struct{}{}
		if err := domain.CheckTarget(target); err != nil {
			return err
		}
		n += int64(len(target.PayloadCanonical)) + int64(len(target.FixedTarget))
	}
	if n > int64(limits.MaxPlanBytes) {
		return fail(constant.ReasonPlanTooLarge, "固定计划超过上限", "")
	}
	if _, err := domain.NewInitialDelivery(time.Now().UTC(), plan.RoundPolicy); err != nil {
		return err
	}
	return nil
}

func parseEnvelope(principal Principal, raw []byte) (domain.EventEnvelope, error) {
	if err := domain.ValidateCallerID(principal.CallerID); err != nil {
		return domain.EventEnvelope{}, fail(constant.ReasonCallerScopeDenied, "调用方不在授权范围", "caller_id")
	}
	if !slices.Contains(principal.Scopes, principal.CallerID) {
		return domain.EventEnvelope{}, fail(constant.ReasonCallerScopeDenied, "调用方不在授权范围", "caller_id")
	}
	value, err := canonicaljson.ParseObject(raw)
	if err != nil {
		return domain.EventEnvelope{}, fail(constant.ReasonInvalidJSON, "JSON 不是完整对象", "")
	}
	fields, _ := value.Object()
	allowed := map[string]struct{}{
		"source_business": {}, "source_event_id": {}, "event_type": {}, "schema_version": {},
		"occurred_at": {}, "business_object_type": {}, "business_object_id": {}, "data": {},
	}
	present := map[string]canonicaljson.Value{}
	for _, field := range fields {
		if _, ok := allowed[field.Key]; !ok {
			return domain.EventEnvelope{}, fail(constant.ReasonUnknownField, "请求包含未知字段", field.Key)
		}
		present[field.Key] = field.Value
	}
	source, err := resolveSource(principal.AllowedSources, present["source_business"], has(present, "source_business"))
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	sourceEvent, err := requiredText(present, "source_event_id")
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	if err := domain.ValidateExternalID("source_event_id", sourceEvent); err != nil {
		return domain.EventEnvelope{}, fail(constant.ReasonInvalidFieldValue, "来源事件标识不合法", "source_event_id")
	}
	eventType, err := requiredText(present, "event_type")
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	if err := domain.ValidateEventType(eventType); err != nil {
		return domain.EventEnvelope{}, fail(constant.ReasonInvalidFieldValue, "事件类型不合法", "event_type")
	}
	schemaVersion, err := requiredText(present, "schema_version")
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	if err := domain.ValidateSchemaVersion(schemaVersion); err != nil {
		return domain.EventEnvelope{}, fail(constant.ReasonInvalidFieldValue, "数据版本不合法", "schema_version")
	}
	occurredText, err := requiredText(present, "occurred_at")
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	occurred, err := domain.ParseRFC3339("occurred_at", occurredText)
	if err != nil {
		return domain.EventEnvelope{}, fail(constant.ReasonInvalidFieldValue, "发生时间不合法", "occurred_at")
	}
	objectType, err := requiredText(present, "business_object_type")
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	if err := domain.ValidateBusinessObjectType(objectType); err != nil {
		return domain.EventEnvelope{}, fail(constant.ReasonInvalidFieldValue, "业务对象类型不合法", "business_object_type")
	}
	objectID, err := requiredText(present, "business_object_id")
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	if err := domain.ValidateExternalID("business_object_id", objectID); err != nil {
		return domain.EventEnvelope{}, fail(constant.ReasonInvalidFieldValue, "业务对象标识不合法", "business_object_id")
	}
	data, ok := present["data"]
	if !ok || data.Kind() == canonicaljson.KindNull || data.Kind() != canonicaljson.KindObject {
		return domain.EventEnvelope{}, fail(constant.ReasonInvalidFieldType, "事件数据必须是 JSON 对象", "data")
	}
	return domain.EventEnvelope{
		CallerID: principal.CallerID, SourceBusiness: source, SourceEventID: sourceEvent,
		EventType: eventType, SchemaVersion: schemaVersion, OccurredAt: occurred.UTC(),
		BusinessObjectType: objectType, BusinessObjectID: objectID,
		DataCanonical:      canonicaljson.AppendCanonical(nil, data),
		FingerprintVersion: constant.FingerprintVersion,
	}, nil
}

func resolveSource(allowed []string, value canonicaljson.Value, present bool) (string, error) {
	if !present {
		if len(allowed) != 1 {
			return "", fail(constant.ReasonSourceAmbiguous, "来源不明确", "source_business")
		}
		if err := domain.ValidateSourceBusiness(allowed[0]); err != nil {
			return "", fail(constant.ReasonSourceDenied, "来源不在授权范围", "source_business")
		}
		return allowed[0], nil
	}
	if value.Kind() == canonicaljson.KindNull {
		return "", fail(constant.ReasonSourceAmbiguous, "来源不明确", "source_business")
	}
	text, ok := value.Text()
	if !ok || domain.ValidateSourceBusiness(text) != nil {
		return "", fail(constant.ReasonInvalidFieldValue, "来源不合法", "source_business")
	}
	if !slices.Contains(allowed, text) {
		return "", fail(constant.ReasonSourceDenied, "来源不在授权范围", "source_business")
	}
	return text, nil
}

func requiredText(present map[string]canonicaljson.Value, key string) (string, error) {
	value, ok := present[key]
	if !ok || value.Kind() == canonicaljson.KindNull {
		return "", fail(constant.ReasonMissingField, "缺少必填字段", key)
	}
	text, ok := value.Text()
	if !ok {
		return "", fail(constant.ReasonInvalidFieldType, "字段类型不正确", key)
	}
	return text, nil
}

func has(present map[string]canonicaljson.Value, key string) bool {
	_, ok := present[key]
	return ok
}

func translatePlan(err error) error {
	var de *domain.Error
	if !errors.As(err, &de) || de == nil {
		return fail(constant.ReasonEventRelease, "当前发布不可读取", "")
	}
	switch de.Code {
	case constant.CodeUnsupportedSchema:
		if len(de.Details) > 0 {
			return withCode(de, constant.CodeEventDataInvalid)
		}
		return de
	case constant.CodeLimitExceeded:
		if de.FieldPath == "/targets" {
			return withCode(de, constant.CodeFanoutExceeded)
		}
		return de
	case constant.CodeConfigurationUnavailable:
		return fail(constant.ReasonEventRelease, "当前发布不可读取", "")
	case constant.CodeInternal:
		return fail(constant.ReasonEventRelease, "当前发布不可读取", "")
	default:
		return de
	}
}

func withCode(de *domain.Error, code string) error {
	next, err := domain.NewError(code, de.Message, de.FieldPath, de.Details)
	if err != nil {
		return fail(code, de.Message, de.FieldPath)
	}
	return next
}

func infra(err error) error {
	if err == nil {
		return nil
	}
	var de *domain.Error
	if errors.As(err, &de) {
		return err
	}
	return fail(constant.ReasonEventStore, "数据库不可用", "")
}

func fail(code, message, path string) error {
	err, _ := domain.NewError(code, message, path, nil)
	return err
}
