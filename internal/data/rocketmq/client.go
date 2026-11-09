// Package rocketmq 构造 gRPC Proxy 客户端。
// 长期消费、发布重试和 ACK 由 S04 完成。本包只负责连通、健康检查和一次收发，供启动与批次 0 验证。
package rocketmq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"

	"notifier/internal/conf"
)

func init() {
	// 客户端 init 会把日志写到用户主目录。这里改到临时目录，且不把正文打到标准输出。
	_ = os.Setenv("mq.consoleAppender.enabled", "false")
	_ = os.Setenv("rocketmq.client.logRoot", os.TempDir())
	rmq.ResetLogger()
}

// Client 持有已启动的生产者。本地 Proxy 不启用 ACL，凭证保持为空，Sign 不会附带密钥。
type Client struct {
	endpoint string
	producer rmq.Producer
}

// Open 连接 Proxy。*sql.DB 只用于把拨号排在 MySQL 之后。
func Open(cfg conf.RocketMQ, _ *sql.DB) (*Client, func(), error) {
	producer, err := rmq.NewProducer(&rmq.Config{
		Endpoint: cfg.Endpoint,
	}, rmq.WithTopics(cfg.TopicImmediate, cfg.TopicDelayed))
	if err != nil {
		return nil, nil, errors.New("rocketmq: ROCKETMQ_ENDPOINT: open failed")
	}
	if err := producer.Start(); err != nil {
		_ = producer.GracefulStop()
		return nil, nil, errors.New("rocketmq: ROCKETMQ_ENDPOINT: connect failed")
	}
	client := &Client{endpoint: cfg.Endpoint, producer: producer}
	return client, func() { _ = producer.GracefulStop() }, nil
}

// Healthy 确认 Proxy 端口仍可连接。错误不包含端点文本。
func (c *Client) Healthy(ctx context.Context) error {
	if c == nil || c.producer == nil || c.endpoint == "" {
		return errors.New("rocketmq: ROCKETMQ_ENDPOINT: not ready")
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", c.endpoint)
	if err != nil {
		return errors.New("rocketmq: ROCKETMQ_ENDPOINT: not ready")
	}
	_ = conn.Close()
	return nil
}

// Publish 发送一条即时或延迟消息。deliverAt 为零值时立即可见。由 S04 的 Outbox 发布器调用。
func (c *Client) Publish(ctx context.Context, topic string, body []byte, deliverAt time.Time) error {
	msg := &rmq.Message{Topic: topic, Body: body}
	if !deliverAt.IsZero() {
		msg.SetDelayTimestamp(deliverAt)
	}
	_, err := c.producer.Send(ctx, msg)
	if err != nil {
		return errors.New("rocketmq: publish failed")
	}
	return nil
}

// Pull 用独立消费组拉取至多 max 条消息并确认。MESSAGE_NOT_FOUND 视为空结果。
// 这是连通性验证用的短生命周期消费者，不是 S04 的长期循环。
func Pull(ctx context.Context, endpoint, group, topic string, invisible time.Duration, max int32) ([][]byte, error) {
	// 5.3.1 Proxy 默认长轮询窗口大约是 5～20 秒。短于下限会返回 40018 ILLEGAL_POLLING_TIME。
	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
	},
		rmq.WithSimpleAwaitDuration(15*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			topic: rmq.SUB_ALL,
		}),
	)
	if err != nil {
		return nil, errors.New("rocketmq: ROCKETMQ_ENDPOINT: open failed")
	}
	if err := consumer.Start(); err != nil {
		return nil, errors.New("rocketmq: ROCKETMQ_ENDPOINT: connect failed")
	}
	defer func() { _ = consumer.GracefulStop() }()

	views, err := consumer.Receive(ctx, max, invisible)
	if err != nil {
		if status, ok := rmq.AsErrRpcStatus(err); ok && status.GetCode() == 40401 {
			return nil, nil
		}
		return nil, fmt.Errorf("rocketmq: receive failed: %s", rpcSummary(err))
	}
	out := make([][]byte, 0, len(views))
	for _, view := range views {
		body := append([]byte(nil), view.GetBody()...)
		if ackErr := consumer.Ack(ctx, view); ackErr != nil {
			return nil, errors.New("rocketmq: ack failed")
		}
		out = append(out, body)
	}
	return out, nil
}

// rpcSummary 只保留状态码和简短原因，避免把接入端点或凭证写进错误。
func rpcSummary(err error) string {
	if status, ok := rmq.AsErrRpcStatus(err); ok {
		return fmt.Sprintf("code %d", status.GetCode())
	}
	return "rpc error"
}
