//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"notifier/internal/biz/biz_outbox"
	"notifier/internal/conf"
	"notifier/internal/data/mysql"
	"notifier/internal/data/rocketmq"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

type recSender struct {
	mu   sync.Mutex
	ids  []string
	fail int
}

func (r *recSender) SendDeliveryCommand(_ context.Context, _ string, id string, _ []byte, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
	if r.fail > 0 {
		r.fail--
		return errors.New("confirmation lost")
	}
	return nil
}

func s04Publisher(db *sql.DB, snd data_interface.CommandSender) (*biz_outbox.Publisher, *mysql.OutboxStore) {
	store := mysql.NewOutboxStore(db, mysql.NewExecutor(db, 2*time.Second))
	return &biz_outbox.Publisher{Store: store, Sender: snd, Jitter: func() float64 { return 1 }, Counters: &biz_outbox.Counters{},
		Config: biz_outbox.Config{BatchSize: 100, Parallelism: 4, Lease: 15 * time.Second, SendTimeout: 3 * time.Second,
			BackoffInitial: 100 * time.Millisecond, BackoffCap: time.Second, MaxDelay: 24 * time.Hour, ClockSafety: time.Second,
			TopicImmediate: "notifier-delivery", TopicDelayed: "notifier-delivery-delay"}}, store
}

func outboxRow(t *testing.T, db *sql.DB, deliveryID uint64) (status string, count uint32) {
	t.Helper()
	if err := db.QueryRow(`SELECT publish_status, publish_count FROM outbox_messages WHERE delivery_id = ?`, deliveryID).Scan(&status, &count); err != nil {
		t.Fatal(err)
	}
	return
}

func TestMQ01_AcceptedOutboxPublishedByFreshPublisher(t *testing.T) {
	db := s02DB(t)
	task, _ := s03Task(t, db, "mq-01")
	snd := &recSender{}
	p, _ := s04Publisher(db, snd)
	if _, err := p.PublishOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st, n := outboxRow(t, db, task.DeliveryID); st != domain.PublishPublished || n != 1 {
		t.Fatalf("%s %d", st, n)
	}
	assertSQLCount(t, db, 1, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_history WHERE delivery_id = %d AND phase = 'PUBLISH_SUCCEEDED'`, task.DeliveryID))
}

func TestMQ02_LostConfirmResendsSameKeyWithoutNewOutbox(t *testing.T) {
	db := s02DB(t)
	task, _ := s03Task(t, db, "mq-02")
	snd := &recSender{fail: 1}
	p, _ := s04Publisher(db, snd)
	_, _ = p.PublishOnce(context.Background())
	if _, err := db.Exec(`UPDATE outbox_messages SET next_publish_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE delivery_id = ?`, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
	_, _ = p.PublishOnce(context.Background())
	if len(snd.ids) < 2 || snd.ids[0] != snd.ids[len(snd.ids)-1] {
		t.Fatalf("ids %v", snd.ids)
	}
	assertSQLCount(t, db, 1, fmt.Sprintf(`SELECT COUNT(*) FROM outbox_messages WHERE delivery_id = %d`, task.DeliveryID))
}

func TestMQ03_ExpiredClaimRecoverable(t *testing.T) {
	db := s02DB(t)
	task, _ := s03Task(t, db, "mq-03")
	_, store := s04Publisher(db, &recSender{})
	in := data_interface.ClaimPublishInput{Limit: 100, Lease: 15 * time.Second, MaxDelay: 24 * time.Hour, ClockSafety: time.Second}
	if _, err := store.ClaimPublishBatch(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	snd := &recSender{}
	p, _ := s04Publisher(db, snd)
	_, _ = p.PublishOnce(context.Background())
	if st, _ := outboxRow(t, db, task.DeliveryID); st != domain.PublishPublishing {
		t.Fatal("valid claim taken over")
	}
	if _, err := db.Exec(`UPDATE outbox_messages SET lease_expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE delivery_id = ?`, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
	_, _ = p.PublishOnce(context.Background())
	if st, n := outboxRow(t, db, task.DeliveryID); st != domain.PublishPublished || n != 2 {
		t.Fatalf("%s %d", st, n)
	}
}

func TestMQ12_RecordFailureKeepsPublishingAndAttemptsUnchanged(t *testing.T) {
	db := s02DB(t)
	task, _ := s03Task(t, db, "mq-12")
	_, store := s04Publisher(db, &recSender{})
	claimed, err := store.ClaimPublishBatch(context.Background(), data_interface.ClaimPublishInput{Limit: 100, Lease: 15 * time.Second, MaxDelay: 24 * time.Hour, ClockSafety: time.Second})
	if err != nil || len(claimed) == 0 {
		t.Fatal(err)
	}
	// send 成功后进程崩溃，结果未记录
	assertSQLCount(t, db, 0, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id = %d`, task.DeliveryID))
	if st, _ := outboxRow(t, db, task.DeliveryID); st != domain.PublishPublishing {
		t.Fatal(st)
	}
}

// 真实 Broker：以原消息键发送即时与延迟指令（需要本地 compose 与 scripts/init-topics.sh）。
func TestMQ06_RealBrokerImmediateAndDelayed(t *testing.T) {
	if raceEnabled {
		t.Skip("skipped under -race: data race inside github.com/apache/rocketmq-clients/golang v5.1.4 " +
			"(producerSettings.applySettingsCommand vs protobuf marshal of settings telemetry), not in notifier code")
	}
	endpoint := os.Getenv("ROCKETMQ_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:8081"
	}
	cfg := conf.RocketMQ{Endpoint: endpoint, TopicImmediate: "notifier-delivery", TopicDelayed: "notifier-delivery-delay", Group: "notifier-worker"}
	client, closeFn, err := rocketmq.Open(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	cmd, body, _ := domain.NewDeliveryCommand(77, 1, 2, 1, 1)
	if err := client.SendDeliveryCommand(context.Background(), cfg.TopicImmediate, cmd.MessageID, body, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := client.SendDeliveryCommand(context.Background(), cfg.TopicDelayed, cmd.MessageID, body, time.Now().Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
}
