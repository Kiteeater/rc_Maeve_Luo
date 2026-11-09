package data_interface

import (
	"context"
	"errors"
	"time"

	"notifier/internal/domain"
)

var (
	// ErrEventUnique 表示 (caller_id, source_event_id) 唯一约束冲突。调用方必须结束原事务后再读。
	ErrEventUnique = errors.New("data_interface: event unique conflict")
	// ErrInconsistent 表示任务或 Outbox 唯一约束冲突，不能当成事件幂等成功。
	ErrInconsistent = errors.New("data_interface: acceptance inconsistent")
	// ErrCommitUncertain 表示提交时连接中断，不能判断受理是否落地。
	ErrCommitUncertain = errors.New("data_interface: commit uncertain")
)

// DeliveryView 是已落库的固定计划摘要。不含随时间变化的投递状态。
type DeliveryView struct {
	DeliveryID        uint64
	RouteTargetID     string
	ProviderID        string
	ProviderAccountID string
	Operation         string
	PayloadCanonical  []byte
}

// AcceptedEvent 是按幂等键读到的原受理。
type AcceptedEvent struct {
	EventID            uint64
	CallerID           string
	SourceBusiness     string
	SourceEventID      string
	FingerprintVersion int
	Fingerprint        string
	DataCanonical      []byte
	RoutingReleaseID   uint64
	AcceptedAt         time.Time
	Deliveries         []DeliveryView
}

// AcceptEventInput 是已经完成校验的受理参数。仓储仍检查引用、唯一键和大小。
type AcceptEventInput struct {
	CallerID             string
	AllowedSources       []string
	SourceBusiness       string
	SourceEventID        string
	EventType            string
	SchemaVersion        string
	OccurredAt           time.Time
	BusinessObjectType   string
	BusinessObjectID     string
	DataCanonical        []byte
	FingerprintVersion   int
	Fingerprint          string
	ReleaseID            uint64
	ExpectedReleaseHash  string
	Plan                 domain.DeliveryPlan
	IdempotencyNamespace string
	MaxFanout            int
	MaxPlanBytes         int
}

// EventRepository 保存并读取已受理事件。读取走主库新事务。
type EventRepository interface {
	FindAcceptedEvent(ctx context.Context, callerID, sourceEventID string) (AcceptedEvent, bool, error)
	AcceptEvent(ctx context.Context, in AcceptEventInput) (AcceptedEvent, error)
}

// BacklogSource 统计尚未完成发布的有效 Outbox。Broker 停机不在此判断；发布由 S04 完成。
type BacklogSource interface {
	Measure(ctx context.Context) (pending int64, oldest *time.Time, err error)
}
