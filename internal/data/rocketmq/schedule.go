package rocketmq

import (
	"context"
	"errors"
	"time"

	"notifier/internal/domain"
)

// PendingSchedule 不向 Broker 再发布。原指令的到期再发布由 S04 完成。
type PendingSchedule struct{}

func NewPendingSchedule() *PendingSchedule { return &PendingSchedule{} }

func (PendingSchedule) ScheduleOriginal(context.Context, domain.DeliveryCommand, time.Time) error {
	return errors.New("rocketmq: original republish is completed by S04")
}
