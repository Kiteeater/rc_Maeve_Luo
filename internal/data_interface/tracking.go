package data_interface

import (
	"context"
	"time"

	"notifier/internal/domain"
)

// TrackingQuery 是进入仓储的列表查询。Scopes 必须非空，空范围不解释为全部调用方。
type TrackingQuery struct {
	Scopes []string
	Filter domain.QueryFilter
	// After 为上一页最后排序键：事件/任务为 (accepted_at 纳秒, id)，尝试为 (round_no, attempt_no)，历史为 (history_seq)。
	After []int64
	Limit int // 已含 +1
}

// EventRow 是事件详情/列表所需的事件事实（不含数据快照）。
type EventRow struct {
	EventID            uint64
	CallerID           string
	SourceBusiness     string
	SourceEventID      string
	EventType          string
	SchemaVersion      string
	OccurredAt         time.Time
	AcceptedAt         time.Time
	BusinessObjectType string
	BusinessObjectID   string
	RoutingReleaseID   uint64
	DeliveryCount      int
	StatusCounts       map[string]int
}

// DeliveryRow 是任务列表/详情行：任务快照加所属事件关联。
type DeliveryRow struct {
	Task               TaskRecord
	SourceBusiness     string
	SourceEventID      string
	BusinessObjectType string
	BusinessObjectID   string
	AcceptedAt         time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Escalated          bool
}

// AttemptRow 是逐次尝试的完整追踪事实；指针为空表示真实缺失。
type AttemptRow struct {
	AttemptRecord
	Method             string
	TargetPath         string
	RequestSize        *int64
	ProviderCode       string
	ResponseSize       *int64
	ProviderWaitHint   string
	ResponseReceivedAt *time.Time
	BodyTruncated      *bool // 响应体读取截断；nil 表示无响应体读取
}

// HistoryRow 是时间线记录。
type HistoryRow struct {
	HistoryRecord
	OperationID string
	Actor       string
	RecordedAt  time.Time
}

// OverviewCounts 是有界统计结果；nil 指针表示该项不可用。
type OverviewCounts struct {
	GeneratedAt       time.Time
	PendingPublish    *int64
	OldestPublishAge  *time.Duration
	DueNotExecuted    *int64
	ExpiredLeases     *int64
	StatusCounts      map[string]int64
	LongBlocked       *int64
	LongUnknown       *int64
	TaskReceiveRate   *float64
	AttemptRecvRate   *float64
	HTTPp50, HTTPp95  *time.Duration
	TimingIncomplete  *int64
	CompensationFails *int64
}

// OverviewQuery 是概览统计输入。
type OverviewQuery struct {
	Scopes                     []string
	From, To                   time.Time
	BlockedAfter, UnknownAfter time.Duration
}

// TrackingStore 是 S08 只读仓储端口。不存在与越权都返回 not_found；超时返回 query_timeout。
type TrackingStore interface {
	QueryEvents(ctx context.Context, q TrackingQuery) ([]EventRow, error)
	QueryDeliveries(ctx context.Context, q TrackingQuery) ([]DeliveryRow, error)
	// LoadEventView 在一个一致性读取中返回事件聚合与本页任务摘要。
	LoadEventView(ctx context.Context, scopes []string, eventID uint64, limit int) (EventRow, []DeliveryRow, error)
	LoadDelivery(ctx context.Context, scopes []string, deliveryID uint64) (DeliveryRow, error)
	ListAttempts(ctx context.Context, scopes []string, deliveryID uint64, q TrackingQuery) ([]AttemptRow, error)
	ListHistory(ctx context.Context, scopes []string, deliveryID uint64, q TrackingQuery) ([]HistoryRow, error)
	Overview(ctx context.Context, q OverviewQuery) (OverviewCounts, error)
}

// OverviewCache 是概览快照缓存；不可用时由 biz 受控回源。
type OverviewCache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// BrokerMetrics 读取全局 Broker 指标（DLQ 等）；不可读返回错误。
type BrokerMetrics interface {
	DLQDepth(ctx context.Context) (int64, error)
}

// RetentionCandidate 是清理候选事件。
type RetentionCandidate struct {
	EventID      uint64
	LastResolved time.Time
}

// RetentionStore 是 S08 协调清理端口。
type RetentionStore interface {
	ListRetentionCandidates(ctx context.Context, cutoff time.Time, limit int) ([]RetentionCandidate, error)
	// DeleteResolvedEvent 在单事件事务内锁事件与全部任务并复核；超出行数整体回滚并返回 TOO_LARGE。
	DeleteResolvedEvent(ctx context.Context, c RetentionCandidate, cutoff time.Time, maxRows int) (domain.RetentionDecision, error)
}
