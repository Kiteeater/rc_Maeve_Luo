// Package biz_outbox 编排 Outbox 可靠发布、早到指令再发布和消费端指令校验（S04）。
package biz_outbox

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// Config 是 S04 第 8 节的发布参数，由 Wire 从启动配置注入。
type Config struct {
	BatchSize      int
	Parallelism    int
	Lease          time.Duration
	SendTimeout    time.Duration
	BackoffInitial time.Duration
	BackoffCap     time.Duration
	MaxDelay       time.Duration
	ClockSafety    time.Duration
	TopicImmediate string
	TopicDelayed   string
	// AlertAfter 是同一消息连续发布失败多少次后计入告警。
	AlertAfter uint32
}

// Counters 是进程内发布与消费观察值，不作为事实来源。
type Counters struct {
	Published     atomic.Int64
	PublishFailed atomic.Int64
	PublishAlerts atomic.Int64
	RecordFailed  atomic.Int64
	LateResults   atomic.Int64
	ConsumeErrors atomic.Int64
}

// Publisher 领取到期 Outbox，在事务外 send，再记录结果。
type Publisher struct {
	Store    data_interface.OutboxStore
	Sender   data_interface.CommandSender
	Config   Config
	Now      func() time.Time
	Jitter   func() float64
	Counters *Counters
	Log      *slog.Logger
}

// PublishOnce 处理一批。返回本批领取数，供循环决定是否立即继续。
func (p *Publisher) PublishOnce(ctx context.Context) (int, error) {
	claimed, err := p.Store.ClaimPublishBatch(ctx, data_interface.ClaimPublishInput{
		Limit: p.Config.BatchSize, Lease: p.Config.Lease,
		MaxDelay: p.Config.MaxDelay, ClockSafety: p.Config.ClockSafety,
	})
	if err != nil {
		return 0, err
	}
	par := p.Config.Parallelism
	if par < 1 {
		par = 1
	}
	sem := make(chan struct{}, par)
	var wg sync.WaitGroup
	for _, c := range claimed {
		sem <- struct{}{}
		wg.Add(1)
		go func(c data_interface.ClaimedPublish) {
			defer wg.Done()
			defer func() { <-sem }()
			p.publishOne(ctx, c)
		}(c)
	}
	wg.Wait()
	return len(claimed), nil
}

func (p *Publisher) publishOne(ctx context.Context, c data_interface.ClaimedPublish) {
	now := p.now()
	action, deliverAt, _, err := domain.DeliveryWindow(now, c.ScheduledAt, p.Config.MaxDelay, p.Config.ClockSafety)
	topic := p.Config.TopicImmediate
	if err == nil && action == domain.WindowDelayed {
		topic = p.Config.TopicDelayed
	}
	var sendErr error
	switch {
	case err != nil:
		sendErr = err
	case action == domain.WindowDefer:
		// 领取后计划被推远：不提前发送，按退避留在 FAILED，进入窗口后按原键再发。
		sendErr = errors.New("outbox: schedule outside broker window")
	default:
		sendCtx, cancel := context.WithTimeout(ctx, p.Config.SendTimeout)
		sendErr = p.Sender.SendDeliveryCommand(sendCtx, topic, c.Command.MessageID, c.Body, deliverAt)
		cancel()
	}
	in := data_interface.RecordPublishInput{
		OutboxID: c.Command.OutboxID, Token: c.Token, Revision: c.Revision, PublishCount: c.PublishCount,
		Outcome: domain.PublishPublished,
	}
	if sendErr != nil {
		in.Outcome = domain.PublishFailed
		in.ErrorSummary = "broker send failed or unconfirmed"
		in.NextPublishAt = now.Add(domain.PublishBackoff(c.PublishCount, p.Config.BackoffInitial, p.Config.BackoffCap, p.jitter()))
		p.count(func(c *Counters) { c.PublishFailed.Add(1) })
		if p.Config.AlertAfter > 0 && c.PublishCount >= p.Config.AlertAfter {
			p.count(func(c *Counters) { c.PublishAlerts.Add(1) })
		}
	}
	rec, err := p.Store.RecordPublishResult(ctx, in)
	if err != nil {
		// 保留 PUBLISHING，领取过期后按原 message_id 恢复；不补写虚假 FAILED。
		p.count(func(c *Counters) { c.RecordFailed.Add(1) })
		p.logger().Warn("outbox publish result not recorded", "outbox_id", c.Command.OutboxID)
		return
	}
	if rec.Late {
		p.count(func(c *Counters) { c.LateResults.Add(1) })
		return
	}
	if rec.Applied && in.Outcome == domain.PublishPublished {
		p.count(func(c *Counters) { c.Published.Add(1) })
	}
}

func (p *Publisher) now() time.Time {
	if p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}

func (p *Publisher) jitter() float64 {
	if p.Jitter != nil {
		return p.Jitter()
	}
	return 0.5 + rand.Float64()/2
}

func (p *Publisher) count(fn func(*Counters)) {
	if p.Counters != nil {
		fn(p.Counters)
	}
}

func (p *Publisher) logger() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}
