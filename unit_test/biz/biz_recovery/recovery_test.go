package biz_recovery_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"notifier/internal/biz/biz_recovery"
	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

var now = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func tp(t time.Time) *time.Time { return &t }

// memStore 模拟 RecoverTask 的锁内重检与修复键语义。
type memStore struct {
	mu        sync.Mutex
	tasks     map[uint64]*data_interface.TaskRecord
	outbox    map[uint64]*domain.OutboxFacts
	keys      map[string]bool
	inserts   int
	failOnce  map[uint64]error
	lostAck   map[uint64]bool // 提交成功但确认丢失
	history   []string
	recovered int
	dlq       []data_interface.DLQRecord
	beforeTx  func(id uint64)
}

func newStore() *memStore {
	return &memStore{tasks: map[uint64]*data_interface.TaskRecord{}, outbox: map[uint64]*domain.OutboxFacts{},
		keys: map[string]bool{}, failOnce: map[uint64]error{}, lostAck: map[uint64]bool{}}
}

func (m *memStore) ListRecoveryCandidates(_ context.Context, kind domain.RecoveryKind, limit int) ([]data_interface.RecoveryCandidate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []data_interface.RecoveryCandidate
	for id, t := range m.tasks {
		match := false
		switch kind {
		case domain.RecoverLease:
			match = t.Status == enums.TaskProcessing && t.LeaseExpiresAt != nil && !now.Before(*t.LeaseExpiresAt)
		case domain.RecoverBlocked:
			match = t.Status == enums.TaskBlocked && t.NextCheckAt != nil && !now.Before(*t.NextCheckAt)
		case domain.RecoverDue:
			match = (t.Status == enums.TaskPending || t.Status == enums.TaskRetryWait) && t.ScheduledAt != nil && !now.Before(*t.ScheduledAt)
		case domain.RecoverDeadline:
			match = t.Status != enums.TaskProcessing && !now.Before(t.RoundDeadlineAt) && t.RoundFinishedAt == nil
		}
		if match && len(out) < limit {
			out = append(out, data_interface.RecoveryCandidate{DeliveryID: id, RowVersion: t.RowVersion})
		}
	}
	return out, nil
}

func (m *memStore) LoadRecoverySnapshot(_ context.Context, id uint64) (data_interface.RecoverySnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return data_interface.RecoverySnapshot{}, errors.New("not found")
	}
	var ob domain.OutboxFacts
	if o := m.outbox[id]; o != nil {
		ob = *o
	}
	return data_interface.RecoverySnapshot{Task: *t, Outbox: ob, Now: now}, nil
}

func (m *memStore) RecoverTask(_ context.Context, in data_interface.RecoverInput) (data_interface.RecoveryOutcome, error) {
	if m.beforeTx != nil {
		m.beforeTx(in.DeliveryID)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failOnce[in.DeliveryID]; err != nil {
		delete(m.failOnce, in.DeliveryID)
		return "", err
	}
	if m.keys[in.RepairKey] {
		return data_interface.RecoveryAlreadyApplied, nil
	}
	t := m.tasks[in.DeliveryID]
	if t.RowVersion != in.ExpectedRowVersion || t.ScheduleVersion != in.ScheduleVersion {
		return data_interface.RecoveryStale, nil
	}
	switch in.Action {
	case domain.RecoveryRepair:
		o := m.outbox[in.DeliveryID]
		if in.Repair == domain.RepairInsert {
			if o != nil && o.Exists {
				return data_interface.RecoveryAlreadyApplied, nil
			}
			m.outbox[in.DeliveryID] = &domain.OutboxFacts{Exists: true, Status: "PENDING", CreatedAt: now}
			m.inserts++
		} else {
			if o.RepairCount != in.RepairCount {
				return data_interface.RecoveryStale, nil
			}
			o.Status = "PENDING"
			o.RepairCount++
			o.NextRepairAt = tp(now.Add(in.Cooldown))
		}
	default:
		if in.Plan == nil {
			t.NextCheckAt = in.NextCheckAt
			t.RowVersion++
			break
		}
		p := in.Plan
		t.Status, t.ScheduledAt, t.NextRetryAt, t.NextCheckAt = p.Status, p.ScheduledAt, p.NextRetryAt, p.NextCheckAt
		t.ResumeStatus = p.ResumeStatus
		t.ResumeNotBefore = p.ResumeNotBefore
		t.RoundFinishedAt = p.RoundFinishedAt
		t.CurrentAttemptID, t.LeaseToken, t.LeaseExpiresAt = nil, "", nil
		t.RowVersion++
		if p.BumpSchedule {
			t.ScheduleVersion++
			if p.Status == enums.TaskPending || p.Status == enums.TaskRetryWait {
				m.outbox[in.DeliveryID] = &domain.OutboxFacts{Exists: true, Status: "PENDING", CreatedAt: now}
				m.inserts++
			} else {
				delete(m.outbox, in.DeliveryID)
			}
		}
		m.recovered++
	}
	m.keys[in.RepairKey] = true
	m.history = append(m.history, string(in.Action))
	if m.lostAck[in.DeliveryID] {
		delete(m.lostAck, in.DeliveryID)
		return "", errors.New("commit uncertain")
	}
	return data_interface.RecoveryApplied, nil
}

func (m *memStore) RecordDLQ(_ context.Context, in data_interface.DLQRecord) (data_interface.RecoveryOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlq = append(m.dlq, in)
	return data_interface.RecoveryApplied, nil
}

type checker struct {
	code   string
	covers bool
}

func (c *checker) CheckConstraints(context.Context, data_interface.TaskRecord, time.Time) (data_interface.ConstraintResult, error) {
	return data_interface.ConstraintResult{Code: c.code, Covers: c.covers}, nil
}

const policyJSON = `{"backoff_cap_ms":60000,"backoff_initial_ms":1000,"max_attempts":5,"round_timeout_seconds":3600,"server_wait_cap_seconds":3600}`

func waitingTask(id uint64) *data_interface.TaskRecord {
	return &data_interface.TaskRecord{
		DeliveryID: id, Status: enums.TaskRetryWait, RoundNo: 1, ScheduleVersion: 2, RowVersion: 5, RoundAttemptCount: 1,
		MaxAttempts: 5, RoundDeadlineAt: now.Add(time.Hour), ScheduledAt: tp(now.Add(-10 * time.Minute)), RoundPolicy: []byte(policyJSON),
	}
}

func recoverer(s *memStore, c *checker) *biz_recovery.Recoverer {
	return &biz_recovery.Recoverer{Store: s, Constraints: c, Counters: &biz_recovery.Counters{}, Config: biz_recovery.Config{
		BatchSize: 10, Parallelism: 4, Grace: 5 * time.Minute, RepairCooldown: 5 * time.Minute, MaxRepairs: 3,
		BlockedCheck: time.Minute, DLQAutoRepairs: 3,
	}}
}

func TestRCV01_ConcurrentScannersInsertOneOutbox(t *testing.T) {
	s := newStore()
	s.tasks[1] = waitingTask(1)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = recoverer(s, &checker{}).Recover(context.Background(), 1) }()
	}
	wg.Wait()
	if s.inserts != 1 || len(s.history) != 1 {
		t.Fatalf("inserts=%d history=%v", s.inserts, s.history)
	}
}

func TestRCV06_WorkerAdvancedFirstIsStale(t *testing.T) {
	s := newStore()
	task := waitingTask(1)
	task.Status = enums.TaskProcessing
	task.ScheduledAt = nil
	id := uint64(9)
	task.CurrentAttemptID, task.LeaseToken, task.LeaseExpiresAt = &id, "tok", tp(now.Add(-time.Second))
	s.tasks[1] = task
	// worker 在扫描器读取后提交结果：行版本推进。
	s.beforeTx = func(uint64) { s.mu.Lock(); s.tasks[1].RowVersion++; s.mu.Unlock() }
	r := recoverer(s, &checker{covers: true})
	out, err := r.Recover(context.Background(), 1)
	if err != nil || out != data_interface.RecoveryStale || s.recovered != 0 || r.Counters.Stale.Load() != 1 {
		t.Fatalf("out=%s err=%v recovered=%d", out, err, s.recovered)
	}
}

func TestRCV10_FailedCandidateRetriedOthersContinue(t *testing.T) {
	s := newStore()
	s.tasks[1], s.tasks[2] = waitingTask(1), waitingTask(2)
	s.failOnce[1] = errors.New("lock wait timeout")
	r := recoverer(s, &checker{})
	res, _ := r.ScanOnce(context.Background())
	if res.Failed != 1 || res.Applied != 1 {
		t.Fatalf("%+v", res)
	}
	res, _ = r.ScanOnce(context.Background())
	if s.inserts != 2 {
		t.Fatalf("failed candidate must be repaired on the next scan: inserts=%d %+v", s.inserts, res)
	}
}

func TestRCV12_ResumeAfterChannelRestoredOnce(t *testing.T) {
	blockedTask := func() *data_interface.TaskRecord {
		task := waitingTask(1)
		task.Status, task.ScheduledAt = enums.TaskBlocked, nil
		task.ResumeStatus, task.ResumeNotBefore, task.NextCheckAt = string(enums.TaskRetryWait), tp(now.Add(3*time.Minute)), tp(now.Add(-time.Second))
		task.Unresolved = []data_interface.UnresolvedRef{{AttemptID: 9, RoundNo: 1}}
		return task
	}
	s := newStore()
	s.tasks[1] = blockedTask()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = recoverer(s, &checker{covers: true}).Recover(context.Background(), 1) }()
	}
	wg.Wait()
	got := s.tasks[1]
	if s.inserts != 1 || s.recovered != 1 || got.Status != enums.TaskRetryWait || !got.ScheduledAt.Equal(now.Add(3*time.Minute)) || got.ScheduleVersion != 3 {
		t.Fatalf("inserts=%d recovered=%d task=%+v", s.inserts, s.recovered, got)
	}
	// 仍阻塞：只推迟检查时间，不展示为发送时间。
	s2 := newStore()
	s2.tasks[1] = blockedTask()
	_, _ = recoverer(s2, &checker{code: constant.ErrChannelDisabled}).Recover(context.Background(), 1)
	if s2.tasks[1].Status != enums.TaskBlocked || s2.tasks[1].ScheduledAt != nil || !s2.tasks[1].NextCheckAt.After(now) {
		t.Fatalf("%+v", s2.tasks[1])
	}
}

func TestRCV15_LostCommitAckAdvancesOnce(t *testing.T) {
	s := newStore()
	task := waitingTask(1)
	s.tasks[1] = task
	s.outbox[1] = &domain.OutboxFacts{Exists: true, Status: "PUBLISHED", CreatedAt: now.Add(-time.Hour), LastPublishedAt: tp(now.Add(-30 * time.Minute))}
	s.lostAck[1] = true
	r := recoverer(s, &checker{})
	if _, err := r.Recover(context.Background(), 1); err == nil {
		t.Fatal("uncertain commit must surface")
	}
	// 扫描器重启后重新发现候选：冷却生效，不再推进。
	if out, err := recoverer(s, &checker{}).Recover(context.Background(), 1); err != nil || out != "" {
		t.Fatalf("out=%s err=%v", out, err)
	}
	if s.outbox[1].RepairCount != 1 || len(s.history) != 1 {
		t.Fatalf("repair_count=%d history=%v", s.outbox[1].RepairCount, s.history)
	}
}

func TestRCV16_RepairLimitEscalatesKeepsPlan(t *testing.T) {
	s := newStore()
	s.tasks[1] = waitingTask(1)
	s.outbox[1] = &domain.OutboxFacts{Exists: true, Status: "PUBLISHED", CreatedAt: now.Add(-time.Hour), RepairCount: 3}
	r := recoverer(s, &checker{})
	if _, err := r.Recover(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if r.Counters.ManualRequired.Load() != 1 || len(s.history) != 0 || s.tasks[1].Status != enums.TaskRetryWait {
		t.Fatalf("must alert and keep plan: %v", s.history)
	}
}

func TestRCV09_DLQSucceededRecordOnly(t *testing.T) {
	s := newStore()
	task := waitingTask(1)
	task.Status = enums.TaskSucceeded
	s.tasks[1] = task
	r := recoverer(s, &checker{})
	a, err := r.HandleDLQ(context.Background(), "o1_5", domain.DeliveryCommand{DeliveryID: 1, RoundNo: 1, ScheduleVersion: 2}, true)
	if err != nil || a != domain.DLQRecordOnly || len(s.history) != 0 || len(s.dlq) != 1 {
		t.Fatalf("a=%s err=%v history=%v dlq=%v", a, err, s.history, s.dlq)
	}
}

func TestRCV11_ExpiredBlockedNoOutbox(t *testing.T) {
	s := newStore()
	task := waitingTask(1)
	task.Status, task.ScheduledAt = enums.TaskProcessing, nil
	id := uint64(9)
	task.CurrentAttemptID, task.LeaseToken, task.LeaseExpiresAt = &id, "tok", tp(now.Add(-time.Second))
	s.tasks[1] = task
	if _, err := recoverer(s, &checker{code: constant.ErrChannelDisabled, covers: true}).Recover(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	got := s.tasks[1]
	if got.Status != enums.TaskBlocked || got.ResumeStatus != string(enums.TaskRetryWait) || got.CurrentAttemptID != nil || s.inserts != 0 {
		t.Fatalf("%+v inserts=%d", got, s.inserts)
	}
}
