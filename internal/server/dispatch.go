package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// Decoder 校验原始消息并与 Outbox 关联核对（biz_outbox.Decoder）。
type Decoder interface {
	Decode(ctx context.Context, body []byte) (domain.DeliveryCommand, error)
}

// MQSource 把 RocketMQ 消费包装成 worker 的 Source。
// 非法格式、未知协议或关联不符的消息不执行、不 ACK，交 Broker 有限重投后进入 DLQ。
type MQSource struct {
	Consumer  data_interface.CommandConsumer
	Decoder   Decoder
	Invisible time.Duration
	Log       *slog.Logger
}

func (s *MQSource) Receive(ctx context.Context, limit int) ([]Inbound, error) {
	msgs, err := s.Consumer.Receive(ctx, limit, s.Invisible)
	if err != nil {
		return nil, err
	}
	out := make([]Inbound, 0, len(msgs))
	for _, m := range msgs {
		cmd, err := s.Decoder.Decode(ctx, m.Body)
		if err != nil {
			s.Consumer.Release(m.Receipt)
			if s.Log != nil {
				s.Log.Warn("delivery command rejected", "reason", "invalid or uncorrelated command")
			}
			continue
		}
		out = append(out, Inbound{Command: cmd, Receipt: m.Receipt})
	}
	return out, nil
}

func (s *MQSource) Ack(ctx context.Context, receipt string) error {
	return s.Consumer.Ack(ctx, receipt)
}

// PublishRunner 是一批 Outbox 发布（biz_outbox.Publisher）。
type PublishRunner interface {
	PublishOnce(ctx context.Context) (int, error)
}

// Publisher 是 Outbox 发布后台角色。停止时不再领取新批次，在途批次按 ctx 有界结束；
// 未确定的发布不删除，领取到期后可恢复。
type Publisher struct {
	Runner   PublishRunner
	Interval time.Duration
	Batch    int
	Log      *slog.Logger
}

func (p *Publisher) Name() string { return "outbox-publisher" }

func (p *Publisher) Run(ctx context.Context) error {
	if p == nil || p.Runner == nil || p.Interval <= 0 {
		return errors.New("server: outbox publisher is incomplete")
	}
	for {
		n, err := p.Runner.PublishOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && p.Log != nil {
			p.Log.Info("outbox claim failed")
		}
		if err == nil && p.Batch > 0 && n >= p.Batch {
			continue // 有积压时继续有界批处理
		}
		t := time.NewTimer(p.Interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}
