package data_interface

import (
	"context"
	"errors"
	"time"

	"notifier/internal/domain"
)

// ErrOutboxNotFound 表示指令引用的 Outbox 不存在。来源无法确认时不能直接 ACK。
var ErrOutboxNotFound = errors.New("data_interface: outbox not found")

// ClaimPublishInput 是一批发布领取。窗口外的行只推迟 next_publish_at，不分配发布尝试。
type ClaimPublishInput struct {
	Limit       int
	Lease       time.Duration
	MaxDelay    time.Duration
	ClockSafety time.Duration
}

// ClaimedPublish 是已提交的一次发布尝试。send 必须在领取事务之外进行。
type ClaimedPublish struct {
	Command      domain.DeliveryCommand
	Body         []byte
	Token        string
	Revision     uint32
	PublishCount uint32
	ScheduledAt  time.Time
	LeaseExpires time.Time
}

// RecordPublishInput 是一次 send 的结果。Outcome 取 PUBLISHED / FAILED / CANCELLED。
type RecordPublishInput struct {
	OutboxID      uint64
	Token         string
	Revision      uint32
	PublishCount  uint32
	Outcome       string
	ErrorSummary  string
	NextPublishAt time.Time
}

// PublishRecord 是结果事务的落点。Late 为真时只追加了迟到证据，状态未被覆盖。
type PublishRecord struct {
	Applied bool
	Late    bool
	Status  string
}

// RescheduleInput 把同一条 Outbox 重新放回待发布，不新建 message_id，不增加业务尝试。
type RescheduleInput struct {
	Command     domain.DeliveryCommand
	NotBefore   time.Time
	Reason      string
	MaxDelay    time.Duration
	ClockSafety time.Duration
}

// RescheduleResult.Disposition 取 domain.RescheduleStale / Busy / Due / OK。
type RescheduleResult struct {
	Disposition   string
	Revision      uint32
	NextPublishAt time.Time
}

// CommandBinding 是 Outbox 行及其任务上的关联标识，用于拒绝错绑消息。
type CommandBinding struct {
	OutboxID        uint64
	MessageID       string
	EventID         uint64
	DeliveryID      uint64
	RoundNo         uint32
	ScheduleVersion uint32
}

// OutboxStore 是发布领取、结果记录和同消息再发布。实现不得在持锁时调用 Broker。
type OutboxStore interface {
	ClaimPublishBatch(ctx context.Context, in ClaimPublishInput) ([]ClaimedPublish, error)
	RecordPublishResult(ctx context.Context, in RecordPublishInput) (PublishRecord, error)
	RescheduleSameCommand(ctx context.Context, in RescheduleInput) (RescheduleResult, error)
	LoadBinding(ctx context.Context, outboxID uint64) (CommandBinding, error)
}

// CommandSender 发送一条不可变指令。messageID 作为应用消息键；deliverAt 为零值即时可见。
type CommandSender interface {
	SendDeliveryCommand(ctx context.Context, topic, messageID string, body []byte, deliverAt time.Time) error
}

// ReceivedMessage 是一条原始消息。Receipt 只由入口短期持有，用于 ACK 或续期。
type ReceivedMessage struct {
	Body    []byte
	Receipt string
}

// CommandConsumer 是有界拉取、ACK 和不可见期限调整，不隐藏业务重试。
type CommandConsumer interface {
	Receive(ctx context.Context, max int, invisible time.Duration) ([]ReceivedMessage, error)
	Ack(ctx context.Context, receipt string) error
	ChangeInvisible(ctx context.Context, receipt string, invisible time.Duration) error
	// Release 放弃一个未 ACK 的 receipt，消息在不可见期限后由 Broker 重投，超限进入 DLQ。
	Release(receipt string)
}
