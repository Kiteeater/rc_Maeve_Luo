package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"

	"notifier/internal/conf"
	"notifier/internal/data_interface"
)

// SendDeliveryCommand 以应用 message_id 作为消息键发送。Broker 物理 ID 只作诊断，不参与去重。
// 不确定的确认与失败一样返回错误，由发布器按原 message_id 退避重发。
func (c *Client) SendDeliveryCommand(ctx context.Context, topic, messageID string, body []byte, deliverAt time.Time) error {
	if c == nil || c.producer == nil {
		return errors.New("rocketmq: producer not ready")
	}
	msg := &rmq.Message{Topic: topic, Body: body}
	msg.SetKeys(messageID)
	if !deliverAt.IsZero() {
		msg.SetDelayTimestamp(deliverAt)
	}
	if _, err := c.producer.Send(ctx, msg); err != nil {
		return errors.New("rocketmq: send failed or unconfirmed")
	}
	return nil
}

// Consumer 是即时与延迟两个 Topic 的 SimpleConsumer 包装。receipt 映射到 MessageView，ACK 后释放。
type Consumer struct {
	consumer rmq.SimpleConsumer
	mu       sync.Mutex
	views    map[string]heldView
	seq      atomic.Uint64
}

type heldView struct {
	v         *rmq.MessageView
	at        time.Time
	invisible time.Duration
}

// OpenConsumer 订阅即时与延迟 Topic。应用启动只验证，不创建或重置 Topic/DLQ。
func OpenConsumer(cfg conf.RocketMQ, _ *Client) (*Consumer, func(), error) {
	sc, err := rmq.NewSimpleConsumer(&rmq.Config{Endpoint: cfg.Endpoint, ConsumerGroup: cfg.Group},
		rmq.WithSimpleAwaitDuration(15*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			cfg.TopicImmediate: rmq.SUB_ALL,
			cfg.TopicDelayed:   rmq.SUB_ALL,
		}),
	)
	if err != nil {
		return nil, nil, errors.New("rocketmq: ROCKETMQ_GROUP: open consumer failed")
	}
	if err := sc.Start(); err != nil {
		return nil, nil, errors.New("rocketmq: ROCKETMQ_GROUP: start consumer failed")
	}
	c := &Consumer{consumer: sc, views: map[string]heldView{}}
	return c, func() { _ = sc.GracefulStop() }, nil
}

// Receive 有界拉取。MESSAGE_NOT_FOUND 视为空结果，其他错误原样暴露（脱敏）。
func (c *Consumer) Receive(ctx context.Context, max int, invisible time.Duration) ([]data_interface.ReceivedMessage, error) {
	views, err := c.consumer.Receive(ctx, int32(max), invisible)
	if err != nil {
		if status, ok := rmq.AsErrRpcStatus(err); ok && status.GetCode() == 40401 {
			return nil, nil
		}
		return nil, fmt.Errorf("rocketmq: receive failed: %s", rpcSummary(err))
	}
	out := make([]data_interface.ReceivedMessage, 0, len(views))
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, h := range c.views {
		// 超过不可见期限的 receipt 已失效，Broker 会重投新副本。
		if now.Sub(h.at) > h.invisible {
			delete(c.views, k)
		}
	}
	for _, v := range views {
		receipt := strconv.FormatUint(c.seq.Add(1), 10)
		c.views[receipt] = heldView{v: v, at: now, invisible: invisible}
		out = append(out, data_interface.ReceivedMessage{Body: append([]byte(nil), v.GetBody()...), Receipt: receipt})
	}
	return out, nil
}

func (c *Consumer) take(receipt string, remove bool) (*rmq.MessageView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.views[receipt]
	if !ok {
		return nil, errors.New("rocketmq: unknown receipt")
	}
	if remove {
		delete(c.views, receipt)
	}
	return h.v, nil
}

// Ack 返回真实错误；失败时保留 receipt 映射直到不可见期限后 Broker 重投。
func (c *Consumer) Ack(ctx context.Context, receipt string) error {
	v, err := c.take(receipt, false)
	if err != nil {
		return err
	}
	if err := c.consumer.Ack(ctx, v); err != nil {
		return errors.New("rocketmq: ack failed")
	}
	_, _ = c.take(receipt, true)
	return nil
}

// Release 丢弃未 ACK 的 receipt（不 ACK，等待 Broker 重投或进入 DLQ）。
func (c *Consumer) Release(receipt string) { _, _ = c.take(receipt, true) }

// ChangeInvisible 有界续期，由入口按需调用。
func (c *Consumer) ChangeInvisible(_ context.Context, receipt string, invisible time.Duration) error {
	v, err := c.take(receipt, false)
	if err != nil {
		return err
	}
	if err := c.consumer.ChangeInvisibleDuration(v, invisible); err != nil {
		return errors.New("rocketmq: change invisible failed")
	}
	return nil
}
