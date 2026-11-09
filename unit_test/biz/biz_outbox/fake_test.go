package biz_outbox_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// clock 是受控时钟。
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Add(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

type row struct {
	cmd          domain.DeliveryCommand
	scheduled    time.Time
	status       string
	count, rev   uint32
	token        string
	leaseExpires time.Time
	next         time.Time
	taskStatus   string
	taskRound    uint32
	taskSchedule uint32
	history      []string
}

// memStore 按 S04 规则模拟 MySQL OutboxStore：同样使用 domain 的窗口、领取和所有权判断。
type memStore struct {
	mu          sync.Mutex
	clock       *clock
	rows        map[uint64]*row
	tokenSeq    int
	failRecord  int // 接下来多少次 RecordPublishResult 失败（模拟结果事务失败）
	bizAttempts int // 业务尝试数，发布路径不得改变
}

func newStore(c *clock) *memStore { return &memStore{clock: c, rows: map[uint64]*row{}} }

func (s *memStore) add(outboxID uint64, scheduled time.Time) *row {
	cmd, _, err := domain.NewDeliveryCommand(outboxID, 1000+outboxID, 2000+outboxID, 1, 1)
	if err != nil {
		panic(err)
	}
	r := &row{cmd: cmd, scheduled: scheduled, status: domain.PublishPending, rev: 1, next: s.clock.Now(),
		taskStatus: "PENDING", taskRound: 1, taskSchedule: 1}
	s.mu.Lock()
	s.rows[outboxID] = r
	s.mu.Unlock()
	return r
}

func (s *memStore) get(id uint64) row { s.mu.Lock(); defer s.mu.Unlock(); return *s.rows[id] }

func (s *memStore) ClaimPublishBatch(_ context.Context, in data_interface.ClaimPublishInput) ([]data_interface.ClaimedPublish, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	var out []data_interface.ClaimedPublish
	for _, r := range s.rows {
		if len(out) >= in.Limit {
			break
		}
		if domain.ClaimSelectable(r.status, r.next, r.leaseExpires, now) == "" {
			continue
		}
		action, _, next, err := domain.DeliveryWindow(now, r.scheduled, in.MaxDelay, in.ClockSafety)
		if err != nil {
			return nil, err
		}
		if action == domain.WindowDefer {
			r.status, r.next, r.token = domain.PublishPending, next, ""
			continue
		}
		s.tokenSeq++
		r.status, r.count, r.rev = domain.PublishPublishing, r.count+1, r.rev+1
		r.token, r.leaseExpires = "tok"+strconv.Itoa(s.tokenSeq), now.Add(in.Lease)
		_, body, _ := domain.NewDeliveryCommand(r.cmd.OutboxID, r.cmd.EventID, r.cmd.DeliveryID, r.cmd.RoundNo, r.cmd.ScheduleVersion)
		out = append(out, data_interface.ClaimedPublish{Command: r.cmd, Body: body, Token: r.token, Revision: r.rev,
			PublishCount: r.count, ScheduledAt: r.scheduled, LeaseExpires: r.leaseExpires})
	}
	return out, nil
}

func (s *memStore) RecordPublishResult(_ context.Context, in data_interface.RecordPublishInput) (data_interface.PublishRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failRecord > 0 {
		s.failRecord--
		return data_interface.PublishRecord{}, errors.New("record failed")
	}
	r, ok := s.rows[in.OutboxID]
	if !ok {
		return data_interface.PublishRecord{}, data_interface.ErrOutboxNotFound
	}
	view := domain.PublishView{Status: r.status, Token: r.token, Revision: r.rev, Count: r.count, LeaseExpires: r.leaseExpires, NextPublishAt: r.next}
	eff, err := domain.ApplyPublishResult(view, s.clock.Now(), in.Token, in.Revision, in.PublishCount, in.Outcome, in.NextPublishAt)
	if err != nil {
		return data_interface.PublishRecord{}, err
	}
	if eff.Late {
		r.history = append(r.history, "LATE_RESPONSE:"+in.Outcome)
		return data_interface.PublishRecord{Late: true, Status: r.status}, nil
	}
	r.status, r.next, r.token, r.leaseExpires = eff.Status, eff.NextPublishAt, "", time.Time{}
	if in.Outcome == domain.PublishPublished {
		r.history = append(r.history, "PUBLISH_SUCCEEDED")
	} else {
		r.history = append(r.history, "PUBLISH_FAILED")
	}
	return data_interface.PublishRecord{Applied: true, Status: r.status}, nil
}

func (s *memStore) RescheduleSameCommand(_ context.Context, in data_interface.RescheduleInput) (data_interface.RescheduleResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[in.Command.OutboxID]
	if !ok {
		return data_interface.RescheduleResult{}, data_interface.ErrOutboxNotFound
	}
	now := s.clock.Now()
	notBefore := in.NotBefore
	if notBefore.Before(r.scheduled) {
		notBefore = r.scheduled
	}
	kind := domain.RescheduleKind(r.taskStatus, r.taskRound, r.taskSchedule, r.cmd.RoundNo, r.cmd.ScheduleVersion, notBefore, now, "", nil)
	if kind != domain.RescheduleOK {
		return data_interface.RescheduleResult{Disposition: kind}, nil
	}
	next := now
	if action, _, deferred, _ := domain.DeliveryWindow(now, notBefore, in.MaxDelay, in.ClockSafety); action == domain.WindowDefer {
		next = deferred
	}
	r.status, r.next, r.rev, r.token, r.leaseExpires = domain.PublishPending, next, r.rev+1, "", time.Time{}
	r.history = append(r.history, "WAIT_PLANNED:"+in.Reason)
	return data_interface.RescheduleResult{Disposition: kind, Revision: r.rev, NextPublishAt: next}, nil
}

func (s *memStore) LoadBinding(_ context.Context, id uint64) (data_interface.CommandBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return data_interface.CommandBinding{}, data_interface.ErrOutboxNotFound
	}
	return data_interface.CommandBinding{OutboxID: id, MessageID: r.cmd.MessageID, EventID: r.cmd.EventID,
		DeliveryID: r.cmd.DeliveryID, RoundNo: r.cmd.RoundNo, ScheduleVersion: r.cmd.ScheduleVersion}, nil
}

type sent struct {
	topic, messageID string
	deliverAt        time.Time
}

// memSender 记录 Broker 实际收到的副本。failNext 次发送返回错误（可模拟“已接收但确认丢失”）。
type memSender struct {
	mu        sync.Mutex
	got       []sent
	failNext  int
	acceptErr bool // 失败时 Broker 仍然收到（确认丢失）
	down      bool
}

func (m *memSender) SendDeliveryCommand(_ context.Context, topic, id string, _ []byte, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return errors.New("broker down")
	}
	if m.failNext > 0 {
		m.failNext--
		if m.acceptErr {
			m.got = append(m.got, sent{topic, id, at})
		}
		return errors.New("confirmation lost")
	}
	m.got = append(m.got, sent{topic, id, at})
	return nil
}

func (m *memSender) all() []sent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sent(nil), m.got...)
}
