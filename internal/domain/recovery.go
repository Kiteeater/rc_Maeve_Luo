package domain

import (
	"time"

	"notifier/internal/constant"
	"notifier/internal/enums"
)

// RecoveryKind 是候选类别；各类别独立分页与限额（RCV-C01、RCV-17）。
type RecoveryKind string

const (
	RecoverLease    RecoveryKind = "LEASE"    // PROCESSING 领取过期：lease_expires_at
	RecoverBlocked  RecoveryKind = "BLOCKED"  // BLOCKED 待检查：next_check_at
	RecoverDeadline RecoveryKind = "DEADLINE" // 无有效领取且本轮已截止：round_deadline_at
	RecoverDue      RecoveryKind = "DUE"      // 到期 PENDING / RETRY_WAIT：scheduled_at
)

// RecoveryKinds 是轮询顺序：过期领取最先，避免被阻塞检查或到期积压饿死。
var RecoveryKinds = []RecoveryKind{RecoverLease, RecoverDeadline, RecoverBlocked, RecoverDue}

// RecoveryAction 是 InspectRecovery 的结论。
type RecoveryAction string

const (
	RecoveryNoop     RecoveryAction = "NOOP"
	RecoveryRepair   RecoveryAction = "REPAIR_DISPATCH"
	RecoveryExpire   RecoveryAction = "EXPIRE_ATTEMPT"
	RecoveryRecheck  RecoveryAction = "RECHECK_BLOCK"
	RecoveryClose    RecoveryAction = "CLOSE_ROUND"
	RecoveryEscalate RecoveryAction = "ESCALATE"
)

// 补建方式。
const (
	RepairInsert    = "INSERT"    // 当前版本 Outbox 缺失：唯一插入
	RepairRepublish = "REPUBLISH" // 已发布但宽限/冷却后无进展：同一 message_id 限频补发
)

// OutboxFacts 是当前调度元组对应的 Outbox 视图。
type OutboxFacts struct {
	Exists          bool
	OutboxID        uint64
	Status          string
	CreatedAt       time.Time
	LastPublishedAt *time.Time
	RepairCount     uint32
	NextRepairAt    *time.Time
}

// RecoveryFacts 是一次检查的输入。Now 为数据库 UTC 时间。
type RecoveryFacts struct {
	Task            TaskFacts
	AttemptFinished bool // 当前尝试已结束（结果事务已提交）
	Outbox          OutboxFacts
	Now             time.Time
	Grace           time.Duration
	MaxRepairs      uint32
}

// RecoveryDecision 是检查结论。
type RecoveryDecision struct {
	Action RecoveryAction
	Repair string
	Code   string
	Reason string
}

// DecideRecovery（S06 InspectRecovery 端口）只做判断，不写入；进入 RecoverTask 后仓储仍在锁内复核（RCV-P01、RCV-L01）。
func DecideRecovery(f RecoveryFacts) RecoveryDecision {
	t := f.Task
	now := f.Now.UTC()
	switch t.Status {
	case enums.TaskSucceeded, enums.TaskFailed, enums.TaskUnknown:
		return RecoveryDecision{Action: RecoveryNoop, Reason: "任务已结案"}
	case enums.TaskProcessing:
		if leaseValid(t, now) {
			return RecoveryDecision{Action: RecoveryNoop, Reason: "已有有效处理者"}
		}
		if t.CurrentAttemptID == nil || f.AttemptFinished {
			return RecoveryDecision{Action: RecoveryEscalate, Code: constant.ErrRecoveryInconsistent, Reason: "执行中任务与尝试状态不一致"}
		}
		return RecoveryDecision{Action: RecoveryExpire, Reason: "领取已过期"}
	case enums.TaskBlocked:
		if !now.Before(t.RoundDeadlineAt) {
			return RecoveryDecision{Action: RecoveryClose, Reason: "阻塞持续到本轮截止"}
		}
		if t.NextCheckAt == nil || !now.Before(*t.NextCheckAt) {
			return RecoveryDecision{Action: RecoveryRecheck, Reason: "阻塞到检查时间"}
		}
		return RecoveryDecision{Action: RecoveryNoop, Reason: "未到检查时间"}
	case enums.TaskPending, enums.TaskRetryWait:
	default:
		return RecoveryDecision{Action: RecoveryNoop}
	}
	if t.CurrentAttemptID != nil {
		return RecoveryDecision{Action: RecoveryEscalate, Code: constant.ErrRecoveryInconsistent, Reason: "等待任务仍有当前尝试"}
	}
	if !now.Before(t.RoundDeadlineAt) {
		return RecoveryDecision{Action: RecoveryClose, Reason: "等待持续到本轮截止"}
	}
	if t.ScheduledAt == nil || now.Before(*t.ScheduledAt) {
		return RecoveryDecision{Action: RecoveryNoop, Reason: "未到计划时间"}
	}
	o := f.Outbox
	if !o.Exists || o.Status == "CANCELLED" {
		return RecoveryDecision{Action: RecoveryRepair, Repair: RepairInsert, Reason: "当前指令缺失"}
	}
	if o.Status != "PUBLISHED" {
		// PENDING / PUBLISHING / FAILED 由 S04 发布器按领取过期和退避恢复，不另造消息。
		return RecoveryDecision{Action: RecoveryNoop, Reason: "指令仍在发布流程"}
	}
	base := o.CreatedAt
	if o.LastPublishedAt != nil {
		base = *o.LastPublishedAt
	}
	if t.ScheduledAt.After(base) {
		base = *t.ScheduledAt
	}
	if now.Before(base.Add(f.Grace)) {
		return RecoveryDecision{Action: RecoveryNoop, Reason: "正常排队宽限内"}
	}
	if o.NextRepairAt != nil && now.Before(*o.NextRepairAt) {
		return RecoveryDecision{Action: RecoveryNoop, Reason: "补发冷却中"}
	}
	if f.MaxRepairs > 0 && o.RepairCount >= f.MaxRepairs {
		return RecoveryDecision{Action: RecoveryEscalate, Code: constant.ErrRecoveryManualRequired, Reason: "自动补发达到上限"}
	}
	return RecoveryDecision{Action: RecoveryRepair, Repair: RepairRepublish, Reason: "宽限后仍无进展"}
}

// ExpireFacts 是过期尝试处置输入。Covers 为事务外无副作用读取的供应商幂等覆盖；
// Constraint 为当前运行约束（空表示允许）。已被原尝试消耗的人工授权不作为依据（RCV-13）。
type ExpireFacts struct {
	Now          time.Time
	Policy       FrozenPolicy
	AttemptID    uint64
	Covers       bool
	Constraint   string
	JitterNumer  uint32
	BlockedCheck time.Duration
}

// ExpirePlan 计算 RCV-L02 的处置：原尝试按 UNCERTAIN / LEASE_EXPIRED 结束；
// 无安全依据或预算耗尽 → UNKNOWN；可安全重发且约束允许 → RETRY_WAIT；仍有可恢复约束 → BLOCKED(RETRY_WAIT)。
func ExpirePlan(task TaskFacts, f ExpireFacts) (Plan, error) {
	plan, err := DecideAfterAttempt(AttemptFacts{
		Now: f.Now, Deadline: task.RoundDeadlineAt, Policy: f.Policy, RoundAttemptCount: task.RoundAttemptCount,
		AttemptID: f.AttemptID, RoundNo: task.RoundNo, Outcome: enums.OutcomeUncertain, SendState: enums.SendUncertain,
		Retryable: true, ErrorCode: constant.ErrLeaseExpired, Prior: task.Unresolved, IdempotencyCovers: f.Covers,
		JitterNumer: f.JitterNumer, BlockedCheck: f.BlockedCheck,
	})
	if err != nil {
		return Plan{}, err
	}
	if plan.Status != enums.TaskRetryWait || !blockingConstraint(f.Constraint) {
		return plan, nil
	}
	now := f.Now.UTC().Truncate(time.Microsecond)
	check := checkInstant(now, task.RoundDeadlineAt.UTC(), f.BlockedCheck)
	next := *plan.ScheduledAt
	plan.Kind = enums.ActionBlock
	plan.Status = enums.TaskBlocked
	plan.Reason = "领取过期后运行约束阻塞"
	plan.ErrorCode = f.Constraint
	plan.ResumeStatus = string(enums.TaskRetryWait)
	plan.ResumeNotBefore = &next
	plan.NextCheckAt = &check
	plan.ScheduledAt = nil
	plan.NextRetryAt = nil
	return plan, nil
}

// CloseRoundPlan 是本轮截止时的确定性结案：无未决 FAILED，有未决 UNKNOWN（RCV-B02）。
func CloseRoundPlan(task TaskFacts, now time.Time) Plan {
	plan := closePlan(len(task.Unresolved) > 0, now, Plan{Reason: "本轮截止仍未完成", ErrorCode: constant.ErrRoundDeadline})
	plan.Unresolved = append([]UnresolvedRef{}, task.Unresolved...)
	return plan
}

// NextBlockedCheck 是阻塞条件未解除时的下一检查时间，不晚于本轮截止。
func NextBlockedCheck(task TaskFacts, now time.Time, interval time.Duration) time.Time {
	return checkInstant(now.UTC(), task.RoundDeadlineAt.UTC(), interval)
}

// IsBlockingConstraint 暴露可恢复运行约束判断。
func IsBlockingConstraint(code string) bool { return blockingConstraint(code) }

// DLQAction 是死信处置结论（RCV-D01）。
type DLQAction string

const (
	DLQRecordOnly DLQAction = "RECORD_ONLY" // 已完成或旧版本：只记录处置
	DLQRepair     DLQAction = "REPAIR"      // 当前有效计划：按同一 Outbox 协议恢复
	DLQInvalid    DLQAction = "INVALID"     // 不合法或关联缺失：记录原因，保留死信依据
	DLQEscalate   DLQAction = "ESCALATE"    // 反复死信或达到自动修复上限
)

// DisposeDLQ 判断死信处置。cmdOK 表示消息解析并与 Outbox 关联成功。
func DisposeDLQ(cmdOK bool, cmd DeliveryCommand, task TaskFacts, outboxRepairs, autoLimit uint32) DLQAction {
	if !cmdOK {
		return DLQInvalid
	}
	switch task.Status {
	case enums.TaskSucceeded, enums.TaskFailed, enums.TaskUnknown:
		return DLQRecordOnly
	}
	if cmd.RoundNo != task.RoundNo || cmd.ScheduleVersion != task.ScheduleVersion {
		return DLQRecordOnly
	}
	if task.Status != enums.TaskPending && task.Status != enums.TaskRetryWait {
		return DLQRecordOnly
	}
	if outboxRepairs >= autoLimit {
		return DLQEscalate
	}
	return DLQRepair
}

// LateConflict 判断迟到证据是否与当前结论冲突，需要人工核对告警（RCV-08）。
func LateConflict(current enums.TaskStatus, late enums.Outcome) bool {
	switch current {
	case enums.TaskSucceeded:
		return late == enums.OutcomePermanentReject || late == enums.OutcomeRetryableReject
	case enums.TaskFailed:
		return late == enums.OutcomeReceived
	case enums.TaskUnknown, enums.TaskPending, enums.TaskRetryWait, enums.TaskBlocked, enums.TaskProcessing:
		// 新轮次或待处理中收到旧尝试的接收证据，可能导致重复执行。
		return late == enums.OutcomeReceived
	}
	return false
}
