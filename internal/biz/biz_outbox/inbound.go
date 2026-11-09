package biz_outbox

import (
	"context"
	"errors"
	"time"

	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// ErrExecuteNow 表示早到判断时计划已到期，交由当前指令执行路径；本次不 ACK。
var ErrExecuteNow = errors.New("biz_outbox: schedule already due")

// Rescheduler 实现 S03 的 ScheduleOriginal：早到指令把同一 Outbox 安排到计划时间后再发布。
type Rescheduler struct {
	Store       data_interface.OutboxStore
	MaxDelay    time.Duration
	ClockSafety time.Duration
}

// ScheduleOriginal 返回 nil 表示已持久化再发布或消息已过时，可以 ACK；其他情况不 ACK。
func (r *Rescheduler) ScheduleOriginal(ctx context.Context, cmd domain.DeliveryCommand, notBefore time.Time) error {
	res, err := r.Store.RescheduleSameCommand(ctx, data_interface.RescheduleInput{
		Command: cmd, NotBefore: notBefore, Reason: domain.RescheduleEarly,
		MaxDelay: r.MaxDelay, ClockSafety: r.ClockSafety,
	})
	if err != nil {
		return err
	}
	switch res.Disposition {
	case domain.RescheduleOK, domain.RescheduleStale:
		return nil
	case domain.RescheduleDue:
		return ErrExecuteNow
	default:
		return errors.New("biz_outbox: task has an active owner")
	}
}

// Decoder 校验消费到的原始消息：协议版本、字段和与 Outbox 行的关联。
// 不合法的消息不执行、不 ACK，交 Broker 有限重投后进入 DLQ。
type Decoder struct {
	Store    data_interface.OutboxStore
	Counters *Counters
}

func (d *Decoder) Decode(ctx context.Context, body []byte) (domain.DeliveryCommand, error) {
	cmd, err := domain.ParseCommand(body)
	if err != nil {
		d.fail()
		return domain.DeliveryCommand{}, err
	}
	b, err := d.Store.LoadBinding(ctx, cmd.OutboxID)
	if err != nil {
		d.fail()
		return domain.DeliveryCommand{}, err
	}
	if err := domain.CorrelateCommand(cmd, b.MessageID, b.OutboxID, b.EventID, b.DeliveryID, b.RoundNo, b.ScheduleVersion); err != nil {
		d.fail()
		return domain.DeliveryCommand{}, err
	}
	return cmd, nil
}

func (d *Decoder) fail() {
	if d.Counters != nil {
		d.Counters.ConsumeErrors.Add(1)
	}
}
