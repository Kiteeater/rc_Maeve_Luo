package biz_tracking

import (
	"encoding/json"
	"strconv"
	"time"

	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

func id(v uint64) string { return strconv.FormatUint(v, 10) }

func idPtr(v *uint64) *string {
	if v == nil {
		return nil
	}
	s := id(*v)
	return &s
}

func ts(t *time.Time) *string {
	if t == nil || t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

func tsv(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func str(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// PolicyView 是任务冻结策略。
type PolicyView struct {
	MaxAttempts          uint32 `json:"max_attempts"`
	RoundTimeoutSeconds  int64  `json:"round_timeout_seconds"`
	BackoffInitialMs     int64  `json:"backoff_initial_ms"`
	BackoffCapMs         int64  `json:"backoff_cap_ms"`
	ServerWaitCapSeconds int64  `json:"server_wait_cap_seconds"`
}

// DeliveryView 是任务详情/摘要（TRK-D01）。不返回 lease_token、凭证句柄或业务载荷。
type DeliveryView struct {
	DeliveryID          string      `json:"delivery_id"`
	EventID             string      `json:"event_id"`
	CallerID            string      `json:"caller_id"`
	SourceBusiness      string      `json:"source_business"`
	SourceEventID       string      `json:"source_event_id"`
	BusinessObjectType  string      `json:"business_object_type"`
	BusinessObjectID    string      `json:"business_object_id"`
	ProviderID          string      `json:"provider_id"`
	ProviderAccountID   string      `json:"provider_account_id"`
	RouteTargetID       string      `json:"route_target_id"`
	ChannelID           string      `json:"channel_id"`
	Operation           string      `json:"operation"`
	RoutingReleaseID    string      `json:"routing_release_id"`
	MappingID           string      `json:"mapping_id"`
	MappingVersion      string      `json:"mapping_version"`
	AdapterType         string      `json:"adapter_type"`
	AdapterVersion      string      `json:"adapter_version"`
	Status              string      `json:"status"`
	ResultCertainty     string      `json:"result_certainty"`
	LastResultCertainty *string     `json:"last_result_certainty"`
	RoundNo             uint32      `json:"round_no"`
	RoundAttemptCount   uint32      `json:"round_attempt_count"`
	TotalAttemptCount   uint32      `json:"total_attempt_count"`
	MaxAttempts         uint32      `json:"max_attempts"`
	Policy              *PolicyView `json:"policy"`
	RowVersion          uint32      `json:"row_version"`
	ScheduleVersion     uint32      `json:"schedule_version"`
	LastAttemptID       *string     `json:"last_attempt_id"`
	CurrentAttemptID    *string     `json:"current_attempt_id"`
	ScheduledAt         *string     `json:"scheduled_at"`
	NextRetryAt         *string     `json:"next_retry_at"`
	NextCheckAt         *string     `json:"next_check_at"`
	RoundDeadlineAt     string      `json:"round_deadline_at"`
	RoundFinishedAt     *string     `json:"round_finished_at"`
	LastErrorCode       *string     `json:"last_error_code"`
	LastErrorReason     *string     `json:"last_error_reason"`
	LastErrorTruncated  bool        `json:"last_error_truncated"`
	BlockedReason       *string     `json:"blocked_reason"`
	BlockedSince        *string     `json:"blocked_since"`
	UnresolvedAttempts  []string    `json:"unresolved_attempt_ids"`
	NeedsManualAction   bool        `json:"needs_manual_action"`
	CreatedAt           *string     `json:"created_at"`
	UpdatedAt           *string     `json:"updated_at"`
}

// NewDeliveryView 映射任务视图：BLOCKED/UNKNOWN 的 next_retry_at 为空；UNKNOWN 明确需人工核对。
func NewDeliveryView(r data_interface.DeliveryRow) DeliveryView {
	t := r.Task
	v := DeliveryView{
		DeliveryID: id(t.DeliveryID), EventID: id(t.EventID), CallerID: t.CallerID,
		SourceBusiness: r.SourceBusiness, SourceEventID: r.SourceEventID,
		BusinessObjectType: r.BusinessObjectType, BusinessObjectID: r.BusinessObjectID,
		ProviderID: t.ProviderID, ProviderAccountID: t.ProviderAccountID, RouteTargetID: t.RouteTargetID,
		ChannelID: id(t.ChannelID), Operation: t.Operation, RoutingReleaseID: id(t.ReleaseID),
		MappingID: t.MappingID, MappingVersion: t.MappingVersion, AdapterType: t.AdapterType, AdapterVersion: t.AdapterVersion,
		Status: string(t.Status), ResultCertainty: string(t.ResultCertainty), LastResultCertainty: str(string(t.LastCertainty)),
		RoundNo: t.RoundNo, RoundAttemptCount: t.RoundAttemptCount, TotalAttemptCount: t.TotalAttemptCount, MaxAttempts: t.MaxAttempts,
		RowVersion: t.RowVersion, ScheduleVersion: t.ScheduleVersion,
		LastAttemptID: idPtr(t.LastAttemptID), CurrentAttemptID: idPtr(t.CurrentAttemptID),
		ScheduledAt: ts(t.ScheduledAt), NextRetryAt: ts(t.NextRetryAt), NextCheckAt: ts(t.NextCheckAt),
		RoundDeadlineAt: tsv(t.RoundDeadlineAt), RoundFinishedAt: ts(t.RoundFinishedAt),
		LastErrorCode: str(t.LastErrorCode), LastErrorReason: str(t.LastErrorReason), LastErrorTruncated: t.LastErrorTruncated,
		BlockedReason: str(t.BlockedReason), BlockedSince: ts(t.BlockedSince), UnresolvedAttempts: []string{},
		NeedsManualAction: domain.NeedsManualAction(t.Status, r.Escalated),
		CreatedAt:         ts(&r.CreatedAt), UpdatedAt: ts(&r.UpdatedAt),
	}
	if t.Status == enums.TaskBlocked || t.Status == enums.TaskUnknown {
		v.NextRetryAt = nil
	}
	for _, u := range t.Unresolved {
		v.UnresolvedAttempts = append(v.UnresolvedAttempts, id(u.AttemptID))
	}
	if p, err := domain.ParseFrozenPolicy(t.RoundPolicy); err == nil {
		v.Policy = &PolicyView{MaxAttempts: p.MaxAttempts, RoundTimeoutSeconds: int64(p.RoundTimeout / time.Second),
			BackoffInitialMs: p.BackoffInitial.Milliseconds(), BackoffCapMs: p.BackoffCap.Milliseconds(), ServerWaitCapSeconds: int64(p.ServerWaitCap / time.Second)}
	}
	return v
}

// EventView 是事件详情/摘要。status_counts 固定 7 个状态。
type EventView struct {
	EventID              string         `json:"event_id"`
	CallerID             string         `json:"caller_id"`
	SourceBusiness       string         `json:"source_business"`
	SourceEventID        string         `json:"source_event_id"`
	EventType            string         `json:"event_type"`
	SchemaVersion        string         `json:"schema_version"`
	OccurredAt           string         `json:"occurred_at"`
	AcceptedAt           string         `json:"accepted_at"`
	BusinessObjectType   string         `json:"business_object_type"`
	BusinessObjectID     string         `json:"business_object_id"`
	RoutingReleaseID     string         `json:"routing_release_id"`
	DeliveryCount        int            `json:"delivery_count"`
	StatusCounts         map[string]int `json:"status_counts"`
	Deliveries           []DeliveryView `json:"deliveries,omitempty"`
	DeliveriesNextCursor *string        `json:"deliveries_next_cursor,omitempty"`
}

func NewEventView(e data_interface.EventRow, rows []data_interface.DeliveryRow) EventView {
	counts := domain.StatusCounts(nil)
	for k, n := range e.StatusCounts {
		counts[k] = n
	}
	v := EventView{
		EventID: id(e.EventID), CallerID: e.CallerID, SourceBusiness: e.SourceBusiness, SourceEventID: e.SourceEventID,
		EventType: e.EventType, SchemaVersion: e.SchemaVersion, OccurredAt: tsv(e.OccurredAt), AcceptedAt: tsv(e.AcceptedAt),
		BusinessObjectType: e.BusinessObjectType, BusinessObjectID: e.BusinessObjectID, RoutingReleaseID: id(e.RoutingReleaseID),
		DeliveryCount: e.DeliveryCount, StatusCounts: counts,
	}
	if rows != nil {
		v.Deliveries = make([]DeliveryView, 0, len(rows))
		for _, r := range rows {
			v.Deliveries = append(v.Deliveries, NewDeliveryView(r))
		}
	}
	return v
}

// AttemptView 是逐次尝试（TRK-D02）。空值保留真实含义：无响应无状态码、未发送无 HTTP 耗时。
type AttemptView struct {
	AttemptID          string          `json:"attempt_id"`
	DeliveryID         string          `json:"delivery_id"`
	RoundNo            uint32          `json:"round_no"`
	AttemptNo          uint32          `json:"attempt_no"`
	OutboxID           *string         `json:"outbox_id"`
	TraceID            *string         `json:"trace_id"`
	WorkerID           *string         `json:"worker_id"`
	DeploymentVersion  *string         `json:"deployment_version"`
	Status             string          `json:"status"`
	SendState          *string         `json:"send_state"`
	Method             *string         `json:"method"`
	TargetPath         *string         `json:"target_path"`
	RequestSizeBytes   *int64          `json:"request_size_bytes"`
	RequestSummary     *string         `json:"request_summary"`
	RequestTruncated   bool            `json:"request_summary_truncated"`
	HTTPStatus         *int            `json:"http_status"`
	ProviderCode       *string         `json:"provider_code"`
	ProviderRequestID  *string         `json:"provider_request_id"`
	ResponseSizeBytes  *int64          `json:"response_size_bytes"`
	ResponseSummary    *string         `json:"response_summary"`
	ResponseTruncated  bool            `json:"response_summary_truncated"`
	BodyTruncated      *bool           `json:"response_body_truncated"`
	Outcome            *string         `json:"outcome"`
	ResultCertainty    *string         `json:"result_certainty"`
	Retryable          *bool           `json:"retryable"`
	ErrorCode          *string         `json:"error_code"`
	ErrorReason        *string         `json:"error_reason"`
	ErrorTruncated     bool            `json:"error_summary_truncated"`
	ProviderWaitHint   *string         `json:"provider_wait_hint"`
	EvidenceScope      json.RawMessage `json:"evidence_scope"`
	StartedAt          string          `json:"started_at"`
	RequestStartedAt   *string         `json:"request_started_at"`
	ResponseReceivedAt *string         `json:"response_received_at"`
	FinishedAt         *string         `json:"finished_at"`
	HTTPDurationMs     *int64          `json:"http_duration_ms"`
	AttemptDurationMs  *int64          `json:"attempt_duration_ms"`
	TimingIncomplete   bool            `json:"timing_incomplete"`
}

func ms(d *time.Duration) *int64 {
	if d == nil {
		return nil
	}
	v := d.Milliseconds()
	return &v
}

func NewAttemptView(a data_interface.AttemptRow) AttemptView {
	v := AttemptView{
		AttemptID: id(a.AttemptID), DeliveryID: id(a.DeliveryID), RoundNo: a.RoundNo, AttemptNo: a.AttemptNo,
		TraceID: str(a.TraceID), WorkerID: str(a.WorkerID), DeploymentVersion: str(a.DeploymentVersion), Status: a.Status,
		SendState: str(string(a.SendState)), Method: str(a.Method), TargetPath: str(a.TargetPath), RequestSizeBytes: a.RequestSize,
		RequestSummary: str(a.RequestSummary), RequestTruncated: a.RequestTruncated, HTTPStatus: a.HTTPStatus,
		ProviderCode: str(a.ProviderCode), ProviderRequestID: str(a.ProviderRequestID), ResponseSizeBytes: a.ResponseSize,
		ResponseSummary: str(a.ResponseSummary), ResponseTruncated: a.ResponseTruncated, BodyTruncated: a.BodyTruncated,
		Outcome: str(string(a.Outcome)), ResultCertainty: str(string(a.ResultCertainty)),
		ErrorCode: str(a.ErrorCode), ErrorReason: str(a.ErrorReason), ErrorTruncated: a.ErrorTruncated,
		ProviderWaitHint: str(a.ProviderWaitHint), StartedAt: tsv(a.StartedAt), RequestStartedAt: ts(a.RequestStartedAt),
		ResponseReceivedAt: ts(a.ResponseReceivedAt), FinishedAt: ts(a.FinishedAt),
		HTTPDurationMs: ms(a.HTTPDuration), AttemptDurationMs: ms(a.AttemptDuration), TimingIncomplete: a.TimingIncomplete,
	}
	if a.OutboxID != 0 {
		v.OutboxID = idPtr(&a.OutboxID)
	}
	if a.Outcome != "" {
		r := a.Retryable
		v.Retryable = &r
	}
	if len(a.EvidenceScope) > 0 && json.Valid(a.EvidenceScope) {
		v.EvidenceScope = a.EvidenceScope
	}
	return v
}

// HistoryView 是时间线记录（TRK-D03）。不返回内部锁 token。
type HistoryView struct {
	HistorySeq      uint32          `json:"history_seq"`
	Phase           string          `json:"phase"`
	FromStatus      *string         `json:"from_status"`
	ToStatus        *string         `json:"to_status"`
	Reason          *string         `json:"reason"`
	OccurredAt      *string         `json:"occurred_at"`
	RecordedAt      string          `json:"recorded_at"`
	RoundNo         uint32          `json:"round_no,omitempty"`
	ScheduleVersion uint32          `json:"schedule_version,omitempty"`
	AttemptID       *string         `json:"attempt_id"`
	OutboxID        *string         `json:"outbox_id"`
	OperationID     *string         `json:"operation_id"`
	Actor           *string         `json:"actor"`
	Evidence        json.RawMessage `json:"evidence"`
}

func NewHistoryView(h data_interface.HistoryRow) HistoryView {
	v := HistoryView{
		HistorySeq: h.Seq, Phase: string(h.Phase), FromStatus: str(string(h.From)), ToStatus: str(string(h.To)),
		Reason: str(h.Reason), OccurredAt: ts(&h.OccurredAt), RecordedAt: tsv(h.RecordedAt),
		RoundNo: h.RoundNo, ScheduleVersion: h.ScheduleVersion, OperationID: str(h.OperationID), Actor: str(h.Actor),
	}
	if h.AttemptID != 0 {
		v.AttemptID = idPtr(&h.AttemptID)
	}
	if h.OutboxID != 0 {
		v.OutboxID = idPtr(&h.OutboxID)
	}
	if len(h.Evidence) > 0 && json.Valid(h.Evidence) {
		v.Evidence = h.Evidence
	}
	return v
}
