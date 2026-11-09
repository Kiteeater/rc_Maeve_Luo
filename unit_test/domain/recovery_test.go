package domain_test

import (
	"testing"
	"time"

	"notifier/internal/constant"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

var rnow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func tp(t time.Time) *time.Time { return &t }

func waiting() domain.TaskFacts {
	return domain.TaskFacts{
		DeliveryID: 1, Status: enums.TaskRetryWait, RoundNo: 1, ScheduleVersion: 2, RowVersion: 5,
		RoundAttemptCount: 1, MaxAttempts: 5, RoundDeadlineAt: rnow.Add(time.Hour), ScheduledAt: tp(rnow.Add(-10 * time.Minute)),
	}
}

func decide(task domain.TaskFacts, ob domain.OutboxFacts) domain.RecoveryDecision {
	return domain.DecideRecovery(domain.RecoveryFacts{Task: task, Outbox: ob, Now: rnow, Grace: 5 * time.Minute, MaxRepairs: 3})
}

func TestRCV01_MissingOutboxRepairsByInsert(t *testing.T) {
	d := decide(waiting(), domain.OutboxFacts{})
	if d.Action != domain.RecoveryRepair || d.Repair != domain.RepairInsert {
		t.Fatalf("%+v", d)
	}
}

func TestRCV02_PublishedWithinGraceIsNoop(t *testing.T) {
	ob := domain.OutboxFacts{Exists: true, Status: "PUBLISHED", CreatedAt: rnow.Add(-20 * time.Minute), LastPublishedAt: tp(rnow.Add(-2 * time.Minute))}
	if d := decide(waiting(), ob); d.Action != domain.RecoveryNoop {
		t.Fatalf("within grace must not republish: %+v", d)
	}
	// 发布中/待发布由 S04 发布器处理，不另造消息。
	for _, st := range []string{"PENDING", "PUBLISHING", "FAILED"} {
		if d := decide(waiting(), domain.OutboxFacts{Exists: true, Status: st, CreatedAt: rnow.Add(-time.Hour)}); d.Action != domain.RecoveryNoop {
			t.Fatalf("%s: %+v", st, d)
		}
	}
}

func TestRCV03_RepublishCooldownAndLimitPersisted(t *testing.T) {
	ob := domain.OutboxFacts{Exists: true, Status: "PUBLISHED", CreatedAt: rnow.Add(-time.Hour), LastPublishedAt: tp(rnow.Add(-30 * time.Minute))}
	if d := decide(waiting(), ob); d.Action != domain.RecoveryRepair || d.Repair != domain.RepairRepublish {
		t.Fatalf("stalled published → republish: %+v", d)
	}
	ob.RepairCount, ob.NextRepairAt = 1, tp(rnow.Add(time.Minute))
	if d := decide(waiting(), ob); d.Action != domain.RecoveryNoop {
		t.Fatalf("cooldown (persisted next_repair_at) must hold: %+v", d)
	}
	ob.NextRepairAt, ob.RepairCount = tp(rnow.Add(-time.Minute)), 3
	if d := decide(waiting(), ob); d.Action != domain.RecoveryEscalate || d.Code != constant.ErrRecoveryManualRequired {
		t.Fatalf("limit → escalate: %+v", d)
	}
}

func processing() domain.TaskFacts {
	t := waiting()
	t.Status = enums.TaskProcessing
	t.ScheduledAt = nil
	t.CurrentAttemptID = u64(9)
	t.LeaseToken = "tok"
	t.LeaseExpiresAt = tp(rnow.Add(-time.Second))
	return t
}

func rpolicy() domain.FrozenPolicy {
	return domain.FrozenPolicy{MaxAttempts: 5, RoundTimeout: time.Hour, BackoffInitial: time.Second, BackoffCap: time.Minute, ServerWaitCap: time.Hour}
}

func TestRCV04_ExpiredLeaseConservativelyUncertain(t *testing.T) {
	task := processing()
	if d := decide(task, domain.OutboxFacts{}); d.Action != domain.RecoveryExpire {
		t.Fatalf("expired lease → EXPIRE: %+v", d)
	}
	plan, err := domain.ExpirePlan(task, domain.ExpireFacts{Now: rnow, Policy: rpolicy(), AttemptID: 9, Covers: false, JitterNumer: 5000})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != enums.TaskUnknown || plan.LastCertainty != enums.CertaintyUncertain || len(plan.Unresolved) != 1 || plan.Unresolved[0].AttemptID != 9 {
		t.Fatalf("no safety basis → UNKNOWN with attempt unresolved: %+v", plan)
	}
	// 领取仍有效：已有处理者，不处置。
	task.LeaseExpiresAt = tp(rnow.Add(time.Minute))
	if d := decide(task, domain.OutboxFacts{}); d.Action != domain.RecoveryNoop {
		t.Fatalf("valid lease: %+v", d)
	}
}

func TestRCV05_CommittedResultNotReprocessed(t *testing.T) {
	// 结果已提交：任务已推进到 RETRY_WAIT，当前 Outbox 正在发布 → 不新建尝试或重试计划。
	task := waiting()
	task.ScheduledAt = tp(rnow.Add(time.Minute))
	if d := decide(task, domain.OutboxFacts{Exists: true, Status: "PENDING"}); d.Action != domain.RecoveryNoop {
		t.Fatalf("%+v", d)
	}
	// 执行中但尝试已结束而无配套处置：一致性告警，不盲目发送。
	p := processing()
	d := domain.DecideRecovery(domain.RecoveryFacts{Task: p, AttemptFinished: true, Now: rnow})
	if d.Action != domain.RecoveryEscalate || d.Code != constant.ErrRecoveryInconsistent {
		t.Fatalf("%+v", d)
	}
}

func blocked() domain.TaskFacts {
	t := waiting()
	t.Status = enums.TaskBlocked
	t.ScheduledAt = nil
	t.BlockedReason = "CHANNEL_DISABLED"
	t.ResumeStatus = string(enums.TaskRetryWait)
	t.ResumeNotBefore = tp(rnow.Add(2 * time.Minute))
	t.NextCheckAt = tp(rnow.Add(-time.Second))
	return t
}

func TestRCV07_BlockedResumeOrCloseAtDeadline(t *testing.T) {
	b := blocked()
	if d := decide(b, domain.OutboxFacts{}); d.Action != domain.RecoveryRecheck {
		t.Fatalf("%+v", d)
	}
	plan, err := domain.ResumePlan(b, rnow, true)
	if err != nil || plan.Status != enums.TaskRetryWait || !plan.ScheduledAt.Equal(rnow.Add(2*time.Minute)) || !plan.BumpSchedule {
		t.Fatalf("resume keeps semantic plan and earliest time: %+v %v", plan, err)
	}
	b.RoundDeadlineAt = rnow
	if d := decide(b, domain.OutboxFacts{}); d.Action != domain.RecoveryClose {
		t.Fatalf("deadline → close: %+v", d)
	}
	cp := domain.CloseRoundPlan(b, rnow)
	if cp.Status != enums.TaskFailed || cp.RoundFinishedAt == nil || cp.ScheduledAt != nil || cp.NextCheckAt != nil {
		t.Fatalf("no unresolved → FAILED, plans cleared: %+v", cp)
	}
	b.Unresolved = []domain.UnresolvedRef{{AttemptID: 3, RoundNo: 1}}
	if cp := domain.CloseRoundPlan(b, rnow); cp.Status != enums.TaskUnknown {
		t.Fatalf("unresolved → UNKNOWN: %+v", cp)
	}
}

func TestRCV08_LateEvidenceConflictAlerts(t *testing.T) {
	if !domain.LateConflict(enums.TaskFailed, enums.OutcomeReceived) || !domain.LateConflict(enums.TaskSucceeded, enums.OutcomePermanentReject) {
		t.Fatal("conflicting late evidence must alert")
	}
	if domain.LateConflict(enums.TaskSucceeded, enums.OutcomeReceived) {
		t.Fatal("consistent late evidence must not alert")
	}
}

func TestRCV09_DLQForFinishedTaskRecordOnly(t *testing.T) {
	task := waiting()
	task.Status = enums.TaskSucceeded
	cmd := domain.DeliveryCommand{DeliveryID: 1, RoundNo: 1, ScheduleVersion: 2}
	if a := domain.DisposeDLQ(true, cmd, task, 0, 3); a != domain.DLQRecordOnly {
		t.Fatalf("%s", a)
	}
	if a := domain.DisposeDLQ(true, domain.DeliveryCommand{RoundNo: 1, ScheduleVersion: 1}, waiting(), 0, 3); a != domain.DLQRecordOnly {
		t.Fatalf("old version: %s", a)
	}
	if a := domain.DisposeDLQ(true, cmd, waiting(), 0, 3); a != domain.DLQRepair {
		t.Fatalf("current plan: %s", a)
	}
	if a := domain.DisposeDLQ(true, cmd, waiting(), 3, 3); a != domain.DLQEscalate {
		t.Fatalf("limit: %s", a)
	}
	if a := domain.DisposeDLQ(false, cmd, waiting(), 0, 3); a != domain.DLQInvalid {
		t.Fatalf("invalid: %s", a)
	}
}

func TestRCV11_ExpiredSafeButChannelDisabledBlocks(t *testing.T) {
	task := processing()
	task.RiskOperationID = "op"
	plan, err := domain.ExpirePlan(task, domain.ExpireFacts{Now: rnow, Policy: rpolicy(), AttemptID: 9, Covers: true,
		Constraint: constant.ErrChannelDisabled, JitterNumer: 5000, BlockedCheck: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != enums.TaskBlocked || plan.ResumeStatus != string(enums.TaskRetryWait) || plan.ResumeNotBefore == nil ||
		plan.NextCheckAt == nil || plan.ScheduledAt != nil || !plan.BumpSchedule || len(plan.Unresolved) != 1 || plan.ErrorCode != constant.ErrChannelDisabled {
		t.Fatalf("%+v", plan)
	}
	if !plan.NextCheckAt.Equal(rnow.Add(time.Minute)) {
		t.Fatalf("check time: %v", plan.NextCheckAt)
	}
}

func TestRCV13_ExpiredDisabledWithoutSafetyIsUnknown(t *testing.T) {
	task := processing()
	task.RiskOperationID = "op" // 已被原尝试消耗的授权不作为依据
	task.RiskAttemptID = u64(9)
	plan, _ := domain.ExpirePlan(task, domain.ExpireFacts{Now: rnow, Policy: rpolicy(), AttemptID: 9, Covers: false, Constraint: constant.ErrChannelDisabled})
	if plan.Status != enums.TaskUnknown {
		t.Fatalf("%+v", plan)
	}
	// 预算耗尽也直接 UNKNOWN。
	task.RoundAttemptCount = 5
	plan, _ = domain.ExpirePlan(task, domain.ExpireFacts{Now: rnow, Policy: rpolicy(), AttemptID: 9, Covers: true, Constraint: constant.ErrChannelDisabled, JitterNumer: 5000})
	if plan.Status != enums.TaskUnknown {
		t.Fatalf("budget gone: %+v", plan)
	}
}

func TestRCV14_BlockedWindowLostOrDeadline(t *testing.T) {
	b := blocked()
	b.Unresolved = []domain.UnresolvedRef{{AttemptID: 9, RoundNo: 1}}
	plan, err := domain.ResumePlan(b, rnow, false)
	if err != nil || plan.Status != enums.TaskUnknown || plan.ErrorCode != constant.ErrUnsafeResend || plan.ScheduledAt != nil {
		t.Fatalf("window lost → UNKNOWN: %+v", plan)
	}
	if next := domain.NextBlockedCheck(b, rnow, 2*time.Hour); !next.Before(b.RoundDeadlineAt) {
		t.Fatalf("next check must stay before deadline: %v", next)
	}
}

func TestRCV17_LeaseCandidatesScannedFirst(t *testing.T) {
	if domain.RecoveryKinds[0] != domain.RecoverLease || len(domain.RecoveryKinds) != 4 {
		t.Fatalf("%v", domain.RecoveryKinds)
	}
}
