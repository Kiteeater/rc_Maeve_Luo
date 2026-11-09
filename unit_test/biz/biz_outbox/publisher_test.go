package biz_outbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"notifier/internal/biz/biz_outbox"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/server"
)

var t0 = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

func newPub(s *memStore, snd *memSender, c *clock) (*biz_outbox.Publisher, *biz_outbox.Counters) {
	cnt := &biz_outbox.Counters{}
	return &biz_outbox.Publisher{
		Store: s, Sender: snd, Now: c.Now, Jitter: func() float64 { return 1 }, Counters: cnt,
		Config: biz_outbox.Config{
			BatchSize: 10, Parallelism: 4, Lease: 15 * time.Second, SendTimeout: time.Second,
			BackoffInitial: time.Second, BackoffCap: time.Minute, MaxDelay: 24 * time.Hour, ClockSafety: time.Second,
			TopicImmediate: "imm", TopicDelayed: "dly", AlertAfter: 3,
		},
	}, cnt
}

func TestMQ01_PendingOutboxPublishedAfterRestart(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	s.add(1, t0) // 受理事务已提交，进程随即退出
	snd := &memSender{}
	p, _ := newPub(s, snd, c) // 重启后的新发布器
	if n, err := p.PublishOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	got := snd.all()
	if len(got) != 1 || got[0].messageID != "o1_1" || got[0].topic != "imm" {
		t.Fatalf("sent %+v", got)
	}
	if r := s.get(1); r.status != domain.PublishPublished {
		t.Fatalf("status %s", r.status)
	}
}

func TestMQ02_ConfirmationLostResendsSameMessageID(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	s.add(1, t0)
	snd := &memSender{failNext: 1, acceptErr: true}
	p, _ := newPub(s, snd, c)
	_, _ = p.PublishOnce(context.Background())
	if r := s.get(1); r.status != domain.PublishFailed || r.count != 1 {
		t.Fatalf("after lost confirm %+v", r)
	}
	c.Add(2 * time.Second)
	_, _ = p.PublishOnce(context.Background())
	got := snd.all()
	if len(got) != 2 || got[0].messageID != got[1].messageID || len(s.rows) != 1 {
		t.Fatalf("sent %+v rows %d", got, len(s.rows))
	}
	if r := s.get(1); r.status != domain.PublishPublished || r.count != 2 {
		t.Fatalf("final %+v", r)
	}
}

func TestMQ03_ConcurrentPublishersAndExpiredClaim(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	for i := uint64(1); i <= 5; i++ {
		s.add(i, t0)
	}
	snd := &memSender{}
	p1, _ := newPub(s, snd, c)
	p2, _ := newPub(s, snd, c)
	var wg sync.WaitGroup
	for _, p := range []*biz_outbox.Publisher{p1, p2} {
		wg.Add(1)
		go func(p *biz_outbox.Publisher) { defer wg.Done(); _, _ = p.PublishOnce(context.Background()) }(p)
	}
	wg.Wait()
	if len(snd.all()) != 5 {
		t.Fatalf("each outbox must be sent once by its current claimant: %d", len(snd.all()))
	}
	// 领取后退出：不记录结果
	s.add(9, t0)
	claimed, _ := s.ClaimPublishBatch(context.Background(), data_interface.ClaimPublishInput{Limit: 1, Lease: 15 * time.Second, MaxDelay: 24 * time.Hour, ClockSafety: time.Second})
	if len(claimed) != 1 {
		t.Fatal("claim")
	}
	if n, _ := p2.PublishOnce(context.Background()); n != 0 {
		t.Fatal("valid claim must not be taken over")
	}
	c.Add(16 * time.Second)
	if n, _ := p2.PublishOnce(context.Background()); n != 1 || s.get(9).status != domain.PublishPublished {
		t.Fatalf("expired claim must be recoverable: n=%d %+v", n, s.get(9))
	}
}

func TestMQ04_LatePublisherResultDoesNotOverride(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	s.add(1, t0)
	in := data_interface.ClaimPublishInput{Limit: 1, Lease: 15 * time.Second, MaxDelay: 24 * time.Hour, ClockSafety: time.Second}
	old, _ := s.ClaimPublishBatch(context.Background(), in)
	c.Add(16 * time.Second)
	cur, _ := s.ClaimPublishBatch(context.Background(), in)
	rec, err := s.RecordPublishResult(context.Background(), data_interface.RecordPublishInput{OutboxID: 1, Token: old[0].Token,
		Revision: old[0].Revision, PublishCount: old[0].PublishCount, Outcome: domain.PublishFailed, NextPublishAt: c.Now().Add(time.Minute)})
	if err != nil || !rec.Late {
		t.Fatalf("old result must be late: %+v %v", rec, err)
	}
	r := s.get(1)
	if r.status != domain.PublishPublishing || r.token != cur[0].Token || r.history[len(r.history)-1] != "LATE_RESPONSE:FAILED" {
		t.Fatalf("current claim overwritten: %+v", r)
	}
}

func TestMQ05_BrokerDownCountsPublishesNotAttempts(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	s.add(1, t0)
	snd := &memSender{down: true}
	p, cnt := newPub(s, snd, c)
	for i := 0; i < 4; i++ {
		_, _ = p.PublishOnce(context.Background())
		c.Add(2 * time.Minute)
	}
	r := s.get(1)
	if r.count != 4 || r.status != domain.PublishFailed || s.bizAttempts != 0 {
		t.Fatalf("row %+v attempts %d", r, s.bizAttempts)
	}
	if cnt.PublishFailed.Load() != 4 || cnt.PublishAlerts.Load() < 1 {
		t.Fatalf("alerts %d failed %d", cnt.PublishAlerts.Load(), cnt.PublishFailed.Load())
	}
}

func TestMQ06_DelayedEarlyAndOutOfRangeNeverEarly(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	s.add(1, t0.Add(time.Hour))    // 窗口内延迟
	s.add(2, t0.Add(48*time.Hour)) // 超出 Broker 范围
	snd := &memSender{}
	p, _ := newPub(s, snd, c)
	_, _ = p.PublishOnce(context.Background())
	got := snd.all()
	if len(got) != 1 || got[0].topic != "dly" || !got[0].deliverAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("delayed %+v", got)
	}
	if r := s.get(2); r.count != 0 || r.status != domain.PublishPending {
		t.Fatalf("out of range must stay pending: %+v", r)
	}
	// 过早到达：同一 Outbox 安排再发布，不执行
	rs := &biz_outbox.Rescheduler{Store: s, MaxDelay: 24 * time.Hour, ClockSafety: time.Second}
	if err := rs.ScheduleOriginal(context.Background(), s.get(1).cmd, t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if r := s.get(1); r.status != domain.PublishPending || !r.next.Equal(t0) {
		t.Fatalf("early reschedule %+v", r)
	}
	for _, g := range snd.all() {
		if g.topic == "dly" && g.deliverAt.Before(t0.Add(time.Hour)) {
			t.Fatal("never deliver before schedule")
		}
	}
}

// MQ-07：结果已持久化后 ACK 失败，消息重投；入口再次交给 S03，S03 读取已有处置后 ACK，不再发送。
type ackFailConsumer struct {
	mu      sync.Mutex
	body    []byte
	acks    int
	failAck int
	queue   int
	release int
}

func (a *ackFailConsumer) Receive(ctx context.Context, max int, _ time.Duration) ([]data_interface.ReceivedMessage, error) {
	a.mu.Lock()
	if a.queue > 0 {
		a.queue--
		a.mu.Unlock()
		return []data_interface.ReceivedMessage{{Body: a.body, Receipt: "r"}}, nil
	}
	a.mu.Unlock()
	<-ctx.Done()
	return nil, nil
}
func (a *ackFailConsumer) Ack(context.Context, string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks++
	if a.failAck > 0 {
		a.failAck--
		a.queue++ // Broker 重投
		return errors.New("ack failed")
	}
	return nil
}
func (a *ackFailConsumer) ChangeInvisible(context.Context, string, time.Duration) error { return nil }
func (a *ackFailConsumer) Release(string)                                               { a.mu.Lock(); a.release++; a.mu.Unlock() }

type persistedRunner struct {
	mu        sync.Mutex
	sends     int
	persisted bool
	calls     int
}

func (r *persistedRunner) ExecuteInstruction(context.Context, domain.DeliveryCommand) (domain.Step, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if !r.persisted {
		r.sends++
		r.persisted = true
	}
	return domain.Step{ACK: true}, nil
}

func runWorker(t *testing.T, src server.Source, run server.Runner, d time.Duration) {
	t.Helper()
	w := &server.Worker{Parallel: 1, Batch: 1, ReceiveTimeout: 50 * time.Millisecond, AckTimeout: time.Second, Drain: time.Second, Runner: run, Source: src}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMQ07_AckFailureRedeliveryDoesNotResend(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	r := s.add(1, t0)
	_, body, _ := domain.NewDeliveryCommand(r.cmd.OutboxID, r.cmd.EventID, r.cmd.DeliveryID, 1, 1)
	cons := &ackFailConsumer{body: body, queue: 1, failAck: 1}
	run := &persistedRunner{}
	src := &server.MQSource{Consumer: cons, Decoder: &biz_outbox.Decoder{Store: s}, Invisible: time.Minute}
	runWorker(t, src, run, 600*time.Millisecond)
	if run.calls != 2 || run.sends != 1 || cons.acks != 2 {
		t.Fatalf("calls %d sends %d acks %d", run.calls, run.sends, cons.acks)
	}
}

func TestMQ08_BadCorrelationOrUnknownVersionNotExecuted(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	s.add(1, t0)
	cnt := &biz_outbox.Counters{}
	dec := &biz_outbox.Decoder{Store: s, Counters: cnt}
	_, wrong, _ := domain.NewDeliveryCommand(1, 1001, 9999, 1, 1) // delivery_id 与 Outbox 不符
	unknown := []byte(`{"protocol_version":2,"outbox_id":"1","message_id":"o1_1","event_id":"1001","delivery_id":"2001","round_no":1,"schedule_version":1}`)
	for _, body := range [][]byte{wrong, unknown, []byte(`{"protocol_version":1}`)} {
		if _, err := dec.Decode(context.Background(), body); err == nil {
			t.Fatalf("must reject %s", body)
		}
	}
	if cnt.ConsumeErrors.Load() != 3 {
		t.Fatalf("errors %d", cnt.ConsumeErrors.Load())
	}
	cons := &ackFailConsumer{body: wrong, queue: 1}
	run := &persistedRunner{}
	runWorker(t, &server.MQSource{Consumer: cons, Decoder: dec, Invisible: time.Minute}, run, 300*time.Millisecond)
	if run.calls != 0 || cons.acks != 0 || cons.release != 1 {
		t.Fatalf("calls %d acks %d release %d", run.calls, cons.acks, cons.release)
	}
}

func TestMQ09_RepeatedRepairKeepsMessageAndOutbox(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	r := s.add(1, t0.Add(time.Hour))
	for i := 0; i < 3; i++ {
		res, err := s.RescheduleSameCommand(context.Background(), data_interface.RescheduleInput{Command: r.cmd, NotBefore: t0.Add(time.Hour),
			Reason: domain.RescheduleRepair, MaxDelay: 24 * time.Hour, ClockSafety: time.Second})
		if err != nil || res.Disposition != domain.RescheduleOK {
			t.Fatalf("%+v %v", res, err)
		}
	}
	got := s.get(1)
	if len(s.rows) != 1 || got.cmd.MessageID != "o1_1" || got.rev != 4 || len(got.history) != 3 {
		t.Fatalf("%+v", got)
	}
}

func TestMQ10_RescheduleWinsOverOldPublisherSuccess(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	r := s.add(1, t0)
	in := data_interface.ClaimPublishInput{Limit: 1, Lease: 15 * time.Second, MaxDelay: 24 * time.Hour, ClockSafety: time.Second}
	old, _ := s.ClaimPublishBatch(context.Background(), in)
	s.mu.Lock()
	s.rows[1].scheduled = t0.Add(time.Minute) // 任务计划被推后，消费者安排原 Outbox 再发布
	s.mu.Unlock()
	res, err := s.RescheduleSameCommand(context.Background(), data_interface.RescheduleInput{Command: r.cmd, NotBefore: t0.Add(time.Minute),
		Reason: domain.RescheduleEarly, MaxDelay: 24 * time.Hour, ClockSafety: time.Second})
	if err != nil || res.Disposition != domain.RescheduleOK {
		t.Fatalf("%+v %v", res, err)
	}
	rec, _ := s.RecordPublishResult(context.Background(), data_interface.RecordPublishInput{OutboxID: 1, Token: old[0].Token,
		Revision: old[0].Revision, PublishCount: old[0].PublishCount, Outcome: domain.PublishPublished})
	got := s.get(1)
	if !rec.Late || got.status != domain.PublishPending || got.rev != res.Revision || !got.next.Equal(res.NextPublishAt) {
		t.Fatalf("rec %+v row %+v", rec, got)
	}
}

func TestMQ11_OutOfRangeNotClaimedUntilWindow(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	s.add(1, t0.Add(30*time.Hour))
	snd := &memSender{}
	p, _ := newPub(s, snd, c)
	_, _ = p.PublishOnce(context.Background())
	r := s.get(1)
	wantNext := t0.Add(30 * time.Hour).Add(-24 * time.Hour).Add(time.Second)
	if r.count != 0 || len(snd.all()) != 0 || !r.next.Equal(wantNext) {
		t.Fatalf("%+v", r)
	}
	c.Add(5 * time.Hour) // 仍未进入窗口
	_, _ = p.PublishOnce(context.Background())
	if s.get(1).count != 0 {
		t.Fatal("must not claim before window")
	}
	c.Add(time.Hour + 2*time.Second)
	_, _ = p.PublishOnce(context.Background())
	got := snd.all()
	if len(got) != 1 || got[0].messageID != "o1_1" || got[0].topic != "dly" || s.get(1).count != 1 {
		t.Fatalf("%+v %+v", got, s.get(1))
	}
}

func TestMQ12_SendOkRecordFailsRecoverAfterLease(t *testing.T) {
	c := &clock{now: t0}
	s := newStore(c)
	s.add(1, t0)
	s.failRecord = 1
	snd := &memSender{}
	p, cnt := newPub(s, snd, c)
	_, _ = p.PublishOnce(context.Background())
	if r := s.get(1); r.status != domain.PublishPublishing || cnt.RecordFailed.Load() != 1 {
		t.Fatalf("must stay PUBLISHING, not fake FAILED: %+v", r)
	}
	c.Add(16 * time.Second)
	_, _ = p.PublishOnce(context.Background())
	got := snd.all()
	if r := s.get(1); r.status != domain.PublishPublished || r.count != 2 || len(got) != 2 || got[1].messageID != "o1_1" || s.bizAttempts != 0 {
		t.Fatalf("%+v %+v", r, got)
	}
}
