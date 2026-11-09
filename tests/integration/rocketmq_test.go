//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"notifier/internal/conf"
	"notifier/internal/data/rocketmq"
)

func TestBatch0_RocketMQImmediateAndDelay(t *testing.T) {
	root := repoRoot(t)
	cmd := exec.Command("sh", "scripts/init-topics.sh")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init topics: %v\n%s", err, out)
	}

	endpoint := mysqlEnv("ROCKETMQ_ENDPOINT", "127.0.0.1:8081")
	immediate := mysqlEnv("ROCKETMQ_TOPIC_IMMEDIATE", "notifier-delivery")
	delayed := mysqlEnv("ROCKETMQ_TOPIC_DELAYED", "notifier-delivery-delay")
	client, cleanup, err := rocketmq.Open(conf.RocketMQ{
		Endpoint:       endpoint,
		TopicImmediate: immediate,
		TopicDelayed:   delayed,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := client.Healthy(ctx); err != nil {
		t.Fatal(err)
	}

	immBody := []byte("s00-immediate-" + fmt.Sprint(time.Now().UnixNano()))
	immGroup := "s00imm" + fmt.Sprint(time.Now().UnixNano())
	if _, err := rocketmq.Pull(ctx, endpoint, immGroup, immediate, 20*time.Second, 8); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(ctx, immediate, immBody, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if !waitBody(ctx, endpoint, immGroup, immediate, immBody) {
		t.Fatal("immediate message was not received")
	}

	delayBody := []byte("s00-delay-" + fmt.Sprint(time.Now().UnixNano()))
	delayGroup := "s00delay" + fmt.Sprint(time.Now().UnixNano())
	if _, err := rocketmq.Pull(ctx, endpoint, delayGroup, delayed, 20*time.Second, 8); err != nil {
		t.Fatal(err)
	}
	deliverAt := time.Now().Add(8 * time.Second)
	if err := client.Publish(ctx, delayed, delayBody, deliverAt); err != nil {
		t.Fatal(err)
	}
	// 长轮询可能跨过投递时刻才返回。返回时仍早于投递时刻却已看到消息，才算提前可见。
	early, err := rocketmq.Pull(ctx, endpoint, delayGroup, delayed, 20*time.Second, 8)
	if err != nil {
		t.Fatal(err)
	}
	if containsBody(early, delayBody) {
		if time.Now().Before(deliverAt) {
			t.Fatal("delayed message was visible before its delivery time")
		}
		return
	}
	if !waitBody(ctx, endpoint, delayGroup, delayed, delayBody) {
		t.Fatal("delayed message was not received")
	}
}

func waitBody(ctx context.Context, endpoint, group, topic string, want []byte) bool {
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		got, err := rocketmq.Pull(ctx, endpoint, group, topic, 20*time.Second, 16)
		if err == nil && containsBody(got, want) {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func containsBody(batch [][]byte, want []byte) bool {
	for _, item := range batch {
		if string(item) == string(want) {
			return true
		}
	}
	return false
}

func init() {
	_ = os.Setenv("mq.consoleAppender.enabled", "false")
}
