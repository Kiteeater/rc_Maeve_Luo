package domain_test

import (
	"testing"
	"time"

	"notifier/internal/constant"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

func TestDLV04_UncertainThenRejectExhaustsBudget(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	policy := policy(2, 5000, 5000, 86400)
	first, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now, Deadline: now.Add(24 * time.Hour), Policy: policy, RoundAttemptCount: 1,
		AttemptID: 11, RoundNo: 1, Outcome: enums.OutcomeUncertain, SendState: enums.SendHTTPInvoked,
		IdempotencyCovers: true, JitterNumer: 10000, BlockedCheck: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Kind != enums.ActionRetry || len(first.Unresolved) != 1 || first.Unresolved[0].AttemptID != 11 {
		t.Fatalf("first plan = %+v", first)
	}
	second, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now.Add(time.Minute), Deadline: now.Add(24 * time.Hour), Policy: policy, RoundAttemptCount: 2,
		AttemptID: 12, RoundNo: 1, Outcome: enums.OutcomePermanentReject, SendState: enums.SendHTTPInvoked,
		Prior: first.Unresolved, JitterNumer: 10000, BlockedCheck: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != enums.TaskUnknown || second.Kind != enums.ActionUnknown {
		t.Fatalf("status = %s %s", second.Status, second.Kind)
	}
	if len(second.Unresolved) != 1 || second.Unresolved[0].AttemptID != 11 || second.LastCertainty != enums.CertaintyNotReceived {
		t.Fatalf("evidence = %+v last %s", second.Unresolved, second.LastCertainty)
	}
	if second.ScheduledAt != nil || second.NextRetryAt != nil || second.NextCheckAt != nil || second.RoundFinishedAt == nil {
		t.Fatal("terminal plan kept a send schedule")
	}
}

func TestDLV10_DeadlineDoesNotCloseValidLease(t *testing.T) {
	now := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	deadline := now.Add(-time.Second)
	exp := now.Add(20 * time.Second)
	attempt := uint64(4)
	task := domain.TaskFacts{
		DeliveryID: 1, EventID: 1, OutboxID: 8, Status: enums.TaskProcessing, RoundNo: 1, ScheduleVersion: 1,
		RoundDeadlineAt: deadline, CurrentAttemptID: &attempt, LeaseToken: "tok", LeaseExpiresAt: &exp,
	}
	if !domain.DeadlineKeepsInFlight(task, now) {
		t.Fatal("valid lease must survive the round deadline")
	}
	cmd := command(task)
	kind, err := domain.InspectInstruction(cmd, task, now)
	if err != nil || kind != domain.InspectActiveOwner {
		t.Fatalf("inspect = %s %v", kind, err)
	}
}

func TestDLV12_RiskAuthorizationIsSingleUse(t *testing.T) {
	now := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	policy := policy(8, 5000, 5000, 86400)
	opened, err := domain.OpenRiskRound(domain.TaskFacts{
		Status: enums.TaskUnknown, ResultCertainty: enums.CertaintyUncertain, RoundNo: 1, ScheduleVersion: 3, RowVersion: 4,
		TotalAttemptCount: 2, Unresolved: []domain.UnresolvedRef{{AttemptID: 3, RoundNo: 1}},
	}, now, "op-risk", policy)
	if err != nil {
		t.Fatal(err)
	}
	if opened.RoundNo != 2 || opened.RoundAttemptCount != 0 || opened.TotalAttemptCount != 2 || !domain.RiskAuthAvailable(opened) {
		t.Fatalf("opened = %+v", opened)
	}
	ready, ok, err := domain.DecideBeforeAttempt(domain.BeforeFacts{
		Now: now, Deadline: opened.RoundDeadlineAt, Status: opened.Status, RoundAttemptCount: 0, MaxAttempts: opened.MaxAttempts,
		HasUnresolved: true, RiskAuthAvailable: true, Prepare: domain.PrepareReady,
	})
	if err != nil || !ok || ready.Kind != "" {
		t.Fatalf("first authorized send blocked: %+v %v %v", ready, ok, err)
	}
	opened.RoundAttemptCount = 1
	opened.RiskAttemptID = u64(21)
	after, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now, Deadline: opened.RoundDeadlineAt, Policy: policy, RoundAttemptCount: 1, AttemptID: 21, RoundNo: 2,
		Outcome: enums.OutcomeUncertain, SendState: enums.SendHTTPInvoked, Prior: opened.Unresolved, JitterNumer: 8000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != enums.TaskUnknown || after.Kind != enums.ActionUnknown {
		t.Fatalf("after = %s", after.Status)
	}
	again, ok, err := domain.DecideBeforeAttempt(domain.BeforeFacts{
		Now: now, Deadline: opened.RoundDeadlineAt, Status: enums.TaskPending, RoundAttemptCount: 1, MaxAttempts: 8,
		HasUnresolved: true, Prepare: domain.PrepareReady,
	})
	if err != nil || ok || again.Status != enums.TaskUnknown {
		t.Fatalf("second auto send = %+v ok %v err %v", again, ok, err)
	}
}

func TestDLV15_BlockDoesNotHideBudgetOrUnsafe(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	policy := policy(1, 5000, 5000, 86400)
	exhausted, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now, Deadline: now.Add(time.Hour), Policy: policy, RoundAttemptCount: 1, AttemptID: 1, RoundNo: 1,
		Outcome: enums.OutcomeLocalFailure, SendState: enums.SendNotSent, Retryable: true, ConfirmedNotSent: true,
		Constraint: constant.ErrChannelDisabled, ErrorCode: constant.ErrChannelDisabled, JitterNumer: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exhausted.Status != enums.TaskFailed || exhausted.Kind == enums.ActionBlock {
		t.Fatalf("exhausted = %s %s", exhausted.Status, exhausted.Kind)
	}
	late, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now, Deadline: now.Add(time.Second), Policy: policyOf(8), RoundAttemptCount: 1, AttemptID: 2, RoundNo: 1,
		Outcome: enums.OutcomeLocalFailure, SendState: enums.SendNotSent, Retryable: true, ConfirmedNotSent: true,
		Constraint: constant.ErrChannelDisabled, JitterNumer: 10000, BlockedCheck: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if late.Status != enums.TaskFailed {
		t.Fatalf("deadline = %s", late.Status)
	}
	unsafe, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now, Deadline: now.Add(24 * time.Hour), Policy: policyOf(8), RoundAttemptCount: 1, AttemptID: 4, RoundNo: 2,
		Outcome: enums.OutcomeLocalFailure, SendState: enums.SendNotSent, Retryable: true, ConfirmedNotSent: true,
		Constraint: constant.ErrCallerForbidden, Prior: []domain.UnresolvedRef{{AttemptID: 3, RoundNo: 1}}, JitterNumer: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.Status != enums.TaskUnknown || unsafe.Kind == enums.ActionBlock {
		t.Fatalf("unsafe = %s %s", unsafe.Status, unsafe.Kind)
	}
}

func TestDLV21_BackoffReusesJitterAndIgnoresOverflow(t *testing.T) {
	half, err := domain.LocalBackoff(3, 8*time.Second, time.Minute, 5000)
	if err != nil || half != 16*time.Second {
		t.Fatalf("half = %s %v", half, err)
	}
	full, err := domain.LocalBackoff(3, 8*time.Second, time.Minute, 10000)
	if err != nil || full != 32*time.Second {
		t.Fatalf("full = %s %v", full, err)
	}
	capped, err := domain.LocalBackoff(32, 60*time.Second, time.Hour, 10000)
	if err != nil || capped != time.Hour {
		t.Fatalf("cap = %s %v", capped, err)
	}
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	frozen := policy(4, 8000, 60000, 86400)
	plan, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now, Deadline: now.Add(24 * time.Hour), Policy: frozen, RoundAttemptCount: 3, AttemptID: 1, RoundNo: 1,
		Outcome: enums.OutcomeRetryableReject, SendState: enums.SendHTTPInvoked, Retryable: true, JitterNumer: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	again, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now, Deadline: now.Add(24 * time.Hour), Policy: frozen, RoundAttemptCount: 3, AttemptID: 1, RoundNo: 1,
		Outcome: enums.OutcomeRetryableReject, SendState: enums.SendHTTPInvoked, Retryable: true, JitterNumer: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ScheduledAt == nil || again.ScheduledAt == nil || !plan.ScheduledAt.Equal(*again.ScheduledAt) || plan.Delay != 16*time.Second {
		t.Fatalf("plan = %v %s", plan.ScheduledAt, plan.Delay)
	}
	changed := policy(1, 1000, 1000, 3600)
	if changed.MaxAttempts == frozen.MaxAttempts {
		t.Fatal("fixture policies must differ")
	}
	if plan.Status == enums.TaskFailed {
		t.Fatal("frozen max_attempts 4 must still schedule a retry")
	}
}

func TestC04_PriorUnresolvedSurvivesRiskRoundReject(t *testing.T) {
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	opened, err := domain.OpenRiskRound(domain.TaskFacts{
		Status: enums.TaskUnknown, RoundNo: 1, TotalAttemptCount: 3, ScheduleVersion: 2,
		Unresolved: []domain.UnresolvedRef{{AttemptID: 9, RoundNo: 1}}, ResultCertainty: enums.CertaintyUncertain,
	}, now, "op-1", policy(1, 5000, 5000, 86400))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := domain.DecideAfterAttempt(domain.AttemptFacts{
		Now: now, Deadline: opened.RoundDeadlineAt, Policy: policy(1, 5000, 5000, 86400), RoundAttemptCount: 1,
		AttemptID: 10, RoundNo: opened.RoundNo, Outcome: enums.OutcomePermanentReject, SendState: enums.SendHTTPInvoked,
		Prior: opened.Unresolved, JitterNumer: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != enums.TaskUnknown || len(plan.Unresolved) != 1 || plan.Unresolved[0].AttemptID != 9 {
		t.Fatalf("plan = %+v", plan)
	}
}

func policy(max, initial, cap, timeout int) domain.FrozenPolicy {
	raw := []byte(`{"backoff_cap_ms":` + itoa(cap) + `,"backoff_initial_ms":` + itoa(initial) + `,"max_attempts":` + itoa(max) + `,"round_timeout_seconds":` + itoa(timeout) + `,"server_wait_cap_seconds":3600}`)
	parsed, err := domain.ParseFrozenPolicy(raw)
	if err != nil {
		panic(err)
	}
	return parsed
}

func policyOf(max int) domain.FrozenPolicy { return policy(max, 5000, 5000, 86400) }

func command(task domain.TaskFacts) domain.DeliveryCommand {
	messageID, err := domain.OutboxMessageID(task.OutboxID)
	if err != nil {
		panic(err)
	}
	return domain.DeliveryCommand{
		ProtocolVersion: 1, OutboxID: task.OutboxID, MessageID: messageID, EventID: task.EventID,
		DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
	}
}

func u64(v uint64) *uint64 { return &v }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
