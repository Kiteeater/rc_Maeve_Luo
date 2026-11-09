package domain

import (
	"time"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/enums"
)

// 任务状态、预算和确定性规则。worker、恢复和人工处置都调用这里，不各自解释超时。

const (
	InspectStale       = "STALE"
	InspectActiveOwner = "ACTIVE_OWNER"
	InspectEarly       = "EARLY"
	InspectPrepare     = "PREPARE"
	InspectRecovery    = "RECOVERY"

	PrepareReady  = "READY"
	PrepareWait   = "WAIT"
	PrepareBlock  = "BLOCK"
	PrepareUnsafe = "UNSAFE"

	jitterMin = 5000
	jitterMax = 10000
)

// FrozenPolicy 是受理或人工重投时冻结的轮次策略。执行不得改用启动默认值。
type FrozenPolicy struct {
	MaxAttempts    uint32
	RoundTimeout   time.Duration
	BackoffInitial time.Duration
	BackoffCap     time.Duration
	ServerWaitCap  time.Duration
}

// UnresolvedRef 是仍未消除的一次尝试。只按尝试标识覆盖，不按供应商名称消除。
type UnresolvedRef struct {
	AttemptID uint64
	RoundNo   uint32
}

// TaskFacts 是决策和指令核对用的任务视图。
type TaskFacts struct {
	DeliveryID        uint64
	EventID           uint64
	OutboxID          uint64
	Status            enums.TaskStatus
	ResultCertainty   enums.ResultCertainty
	RoundNo           uint32
	ScheduleVersion   uint32
	RowVersion        uint32
	RoundAttemptCount uint32
	TotalAttemptCount uint32
	MaxAttempts       uint32
	RoundDeadlineAt   time.Time
	ScheduledAt       *time.Time
	NextRetryAt       *time.Time
	NextCheckAt       *time.Time
	RoundFinishedAt   *time.Time
	CurrentAttemptID  *uint64
	LeaseToken        string
	LeaseExpiresAt    *time.Time
	BlockedReason     string
	ResumeStatus      string
	ResumeNotBefore   *time.Time
	BlockedSince      *time.Time
	RiskOperationID   string
	RiskAttemptID     *uint64
	Unresolved        []UnresolvedRef
}

// TaskTimes 是状态与时间字段的不变量。
type TaskTimes struct {
	Status           enums.TaskStatus
	ScheduledAt      *time.Time
	NextRetryAt      *time.Time
	NextCheckAt      *time.Time
	RoundFinishedAt  *time.Time
	CurrentAttemptID *uint64
	LeaseToken       string
	LeaseExpiresAt   *time.Time
	BlockedReason    string
}

// Step 是入口可以执行的 ACK 结论。Unconfirmed 时不 ACK。
type Step struct {
	ACK         bool
	Unconfirmed bool
}

// Plan 是一次持久化处置。时间在进入事务前算好，事务重试不得另抽抖动。
type Plan struct {
	Kind             enums.NextAction
	Status           enums.TaskStatus
	Reason           string
	ErrorCode        string
	ResultCertainty  enums.ResultCertainty
	LastCertainty    enums.ResultCertainty
	ClearLastError   bool
	ScheduledAt      *time.Time
	NextRetryAt      *time.Time
	NextCheckAt      *time.Time
	ResumeStatus     string
	ResumeNotBefore  *time.Time
	RoundFinishedAt  *time.Time
	BumpSchedule     bool
	Unresolved       []UnresolvedRef
	JitterNumer      uint32
	Delay            time.Duration
	KeepBlockedSince bool
}

// AttemptFacts 是一次已发生的尝试事实。调用方先保存发送事实，再交给本函数。
type AttemptFacts struct {
	Now               time.Time
	Deadline          time.Time
	Policy            FrozenPolicy
	RoundAttemptCount uint32
	AttemptID         uint64
	RoundNo           uint32
	Outcome           enums.Outcome
	SendState         enums.SendState
	Retryable         bool
	ErrorCode         string
	Prior             []UnresolvedRef
	CoversBusinessKey bool
	CoveredAttemptIDs []uint64
	Conflict          bool
	IdempotencyCovers bool
	ConfirmedNotSent  bool
	Constraint        string
	JitterNumer       uint32
	SupplierNotBefore time.Time
	BlockedCheck      time.Duration
}

// BeforeFacts 是创建尝试前的检查输入。
type BeforeFacts struct {
	Now               time.Time
	Deadline          time.Time
	Status            enums.TaskStatus
	RoundAttemptCount uint32
	MaxAttempts       uint32
	HasUnresolved     bool
	IdempotencyCovers bool
	RiskAuthAvailable bool
	Prepare           string
	Reason            string
	ErrorCode         string
	RateLimitUntil    time.Time
	SupplierNotBefore time.Time
	CredentialWait    time.Duration
	CredentialJitter  time.Duration
	BlockedCheck      time.Duration
	ScheduledAt       time.Time
}

// ParseFrozenPolicy 读取本轮冻结策略。缺字段或越界都拒绝，不用启动默认值填补。
func ParseFrozenPolicy(raw []byte) (FrozenPolicy, error) {
	value, err := canonicaljson.Parse(raw)
	if err != nil || value.Kind() != canonicaljson.KindObject {
		return FrozenPolicy{}, codeErr(constant.CodeMappingInvalid, "轮次策略不合法", "round_policy")
	}
	attempts, timeout, err := roundBudget(raw)
	if err != nil {
		return FrozenPolicy{}, err
	}
	initial, err := policyMillis(value, "backoff_initial_ms", 100, 60000)
	if err != nil {
		return FrozenPolicy{}, err
	}
	capMs, err := policyMillis(value, "backoff_cap_ms", initial, 3600000)
	if err != nil {
		return FrozenPolicy{}, err
	}
	if capMs < initial {
		return FrozenPolicy{}, codeErr(constant.CodeMappingInvalid, "退避上限小于起始值", "backoff_cap_ms")
	}
	wait, ok := value.Member("server_wait_cap_seconds")
	seconds, okN := wait.Int64()
	if !ok || !okN || seconds < 1 || seconds > 3600 {
		return FrozenPolicy{}, codeErr(constant.CodeMappingInvalid, "供应商等待上限不合法", "server_wait_cap_seconds")
	}
	return FrozenPolicy{
		MaxAttempts:    attempts,
		RoundTimeout:   time.Duration(timeout) * time.Second,
		BackoffInitial: time.Duration(initial) * time.Millisecond,
		BackoffCap:     time.Duration(capMs) * time.Millisecond,
		ServerWaitCap:  time.Duration(seconds) * time.Second,
	}, nil
}

func policyMillis(value canonicaljson.Value, name string, min, max int64) (int64, error) {
	member, ok := value.Member(name)
	n, okN := member.Int64()
	if !ok || !okN || n < min || n > max {
		return 0, codeErr(constant.CodeMappingInvalid, "轮次策略不合法", name)
	}
	return n, nil
}

// LocalBackoff 计算第 n 次已计入尝试后的本地退避。n 从 1 起。抖动分子是万分比，5000～10000。
func LocalBackoff(n uint32, initial, cap time.Duration, jitterNumer uint32) (time.Duration, error) {
	if n == 0 || initial <= 0 || cap < initial {
		return 0, codeErr(constant.CodeInternal, "退避参数不合法", "backoff")
	}
	if jitterNumer < jitterMin || jitterNumer > jitterMax {
		return 0, codeErr(constant.CodeInternal, "退避抖动不合法", "jitter")
	}
	base := initial
	for i := uint32(1); i < n; i++ {
		if base >= cap || base > cap/2 {
			base = cap
			break
		}
		base *= 2
	}
	if base > cap {
		base = cap
	}
	// base * numer / 10000，拆开乘，避免纳秒乘积溢出。
	delay := base/jitterMax*time.Duration(jitterNumer) + (base%jitterMax)*time.Duration(jitterNumer)/jitterMax
	if delay < 0 {
		return 0, codeErr(constant.CodeInternal, "退避时长溢出", "backoff")
	}
	return delay, nil
}

// ValidateDeliveryCommand 核对 S04 协议版本 1。不读取载荷。
func ValidateDeliveryCommand(cmd DeliveryCommand) error {
	if cmd.ProtocolVersion != 1 || cmd.OutboxID == 0 || cmd.EventID == 0 || cmd.DeliveryID == 0 || cmd.RoundNo == 0 || cmd.ScheduleVersion == 0 {
		return codeErr(constant.CodeInvalidRequest, "投递指令不合法", "protocol_version")
	}
	messageID, err := OutboxMessageID(cmd.OutboxID)
	if err != nil || messageID != cmd.MessageID {
		return codeErr(constant.CodeInvalidRequest, "投递指令不合法", "message_id")
	}
	return nil
}

// InspectInstruction 只读判断指令处置。过期领取返回 RECOVERY，调用方不 ACK，交给 S06。
func InspectInstruction(cmd DeliveryCommand, task TaskFacts, now time.Time) (string, error) {
	if err := ValidateDeliveryCommand(cmd); err != nil {
		return "", err
	}
	if cmd.DeliveryID != task.DeliveryID || cmd.EventID != task.EventID {
		return "", codeErr(constant.CodeInvalidRequest, "指令与任务不一致", "delivery_id")
	}
	now = now.UTC()
	if cmd.RoundNo != task.RoundNo || cmd.ScheduleVersion != task.ScheduleVersion || cmd.OutboxID != task.OutboxID {
		return InspectStale, nil
	}
	switch task.Status {
	case enums.TaskSucceeded, enums.TaskFailed, enums.TaskUnknown, enums.TaskBlocked:
		return InspectStale, nil
	case enums.TaskProcessing:
		if leaseValid(task, now) {
			return InspectActiveOwner, nil
		}
		return InspectRecovery, nil
	case enums.TaskPending, enums.TaskRetryWait:
		if task.ScheduledAt != nil && now.Before(task.ScheduledAt.UTC()) {
			return InspectEarly, nil
		}
		return InspectPrepare, nil
	default:
		return "", codeErr(constant.CodeStateConflict, "任务状态不可执行", "status")
	}
}

func leaseValid(task TaskFacts, now time.Time) bool {
	return task.CurrentAttemptID != nil && task.LeaseToken != "" && task.LeaseExpiresAt != nil && now.Before(task.LeaseExpiresAt.UTC())
}

// DeadlineKeepsInFlight 为真时，本轮截止不得结案仍有有效领取的请求。
func DeadlineKeepsInFlight(task TaskFacts, now time.Time) bool {
	return !now.UTC().Before(task.RoundDeadlineAt.UTC()) && leaseValid(task, now.UTC())
}

// RiskAuthAvailable 表示本轮首次业务尝试还可以使用尚未关联的人工授权。
func RiskAuthAvailable(task TaskFacts) bool {
	return task.RiskOperationID != "" && task.RiskAttemptID == nil && task.RoundAttemptCount == 0
}

// DecideBeforeAttempt 在创建尝试前选择等待、阻塞、结案或继续领取。ready 为真时不写等待计划。
func DecideBeforeAttempt(f BeforeFacts) (Plan, bool, error) {
	now := f.Now.UTC().Truncate(time.Microsecond)
	deadline := f.Deadline.UTC().Truncate(time.Microsecond)
	if f.MaxAttempts == 0 || deadline.IsZero() {
		return Plan{}, false, codeErr(constant.CodeInternal, "轮次预算不合法", "max_attempts")
	}
	exhausted := f.RoundAttemptCount >= f.MaxAttempts || !now.Before(deadline)
	if exhausted {
		return closePlan(f.HasUnresolved, now, exhaustedReason(f)), false, nil
	}
	safe := !f.HasUnresolved || f.IdempotencyCovers || f.RiskAuthAvailable
	switch f.Prepare {
	case PrepareUnsafe:
		return closePlan(true, now, Plan{Reason: reasonOr(f.Reason, "未决结果不能安全重发"), ErrorCode: codeOr(f.ErrorCode, constant.ErrUnsafeResend)}), false, nil
	case PrepareWait:
		if !safe {
			return closePlan(true, now, Plan{Reason: reasonOr(f.Reason, "未决结果不能安全重发"), ErrorCode: codeOr(f.ErrorCode, constant.ErrUnsafeResend)}), false, nil
		}
		next := waitInstant(now, f)
		if !next.Before(deadline) {
			return closePlan(f.HasUnresolved, now, Plan{Reason: "下一次允许时间不早于本轮截止", ErrorCode: constant.ErrRoundDeadline}), false, nil
		}
		plan := Plan{
			Kind: enums.ActionWait, Status: enums.TaskRetryWait, Reason: reasonOr(f.Reason, "执行前等待"),
			ErrorCode: f.ErrorCode, BumpSchedule: true, ResultCertainty: certaintyIf(f.HasUnresolved),
		}
		plan.ScheduledAt = &next
		plan.NextRetryAt = &next
		return plan, false, nil
	case PrepareBlock:
		nextCheck := checkInstant(now, deadline, f.BlockedCheck)
		resume := string(f.Status)
		if resume != string(enums.TaskPending) && resume != string(enums.TaskRetryWait) {
			return Plan{}, false, codeErr(constant.CodeStateConflict, "当前状态不能阻塞", "status")
		}
		origin := f.ScheduledAt.UTC().Truncate(time.Microsecond)
		plan := Plan{
			Kind: enums.ActionBlock, Status: enums.TaskBlocked, Reason: reasonOr(f.Reason, "当前约束不允许执行"),
			ErrorCode: codeOr(f.ErrorCode, constant.ErrChannelDisabled), BumpSchedule: true,
			ResultCertainty: certaintyIf(f.HasUnresolved), ResumeStatus: resume, KeepBlockedSince: false,
		}
		plan.NextCheckAt = &nextCheck
		plan.ResumeNotBefore = &origin
		return plan, false, nil
	case PrepareReady:
		if !safe {
			return closePlan(true, now, Plan{Reason: reasonOr(f.Reason, "未决结果不能安全重发"), ErrorCode: codeOr(f.ErrorCode, constant.ErrUnsafeResend)}), false, nil
		}
		return Plan{}, true, nil
	default:
		return Plan{}, false, codeErr(constant.CodeInternal, "准备结果不合法", "prepare")
	}
}

func exhaustedReason(f BeforeFacts) Plan {
	if f.RoundAttemptCount >= f.MaxAttempts {
		return Plan{Reason: "本轮尝试次数已用完", ErrorCode: constant.ErrAttemptLimit}
	}
	return Plan{Reason: "本轮截止时间已到", ErrorCode: constant.ErrRoundDeadline}
}

func closePlan(unresolved bool, now time.Time, base Plan) Plan {
	finished := now.UTC().Truncate(time.Microsecond)
	base.BumpSchedule = true
	base.RoundFinishedAt = &finished
	base.ClearLastError = false
	if unresolved {
		base.Kind = enums.ActionUnknown
		base.Status = enums.TaskUnknown
		base.ResultCertainty = enums.CertaintyUncertain
		base.LastCertainty = enums.CertaintyUncertain
		return base
	}
	base.Kind = enums.ActionFail
	base.Status = enums.TaskFailed
	base.ResultCertainty = enums.CertaintyNotReceived
	base.LastCertainty = enums.CertaintyNotReceived
	return base
}

func certaintyIf(unresolved bool) enums.ResultCertainty {
	if unresolved {
		return enums.CertaintyUncertain
	}
	return enums.CertaintyNotReceived
}

func waitInstant(now time.Time, f BeforeFacts) time.Time {
	next := now
	if !f.RateLimitUntil.IsZero() {
		next = later(next, f.RateLimitUntil)
	}
	if f.CredentialWait > 0 {
		next = later(next, now.Add(f.CredentialWait+f.CredentialJitter))
	}
	if !f.SupplierNotBefore.IsZero() {
		next = later(next, f.SupplierNotBefore)
	}
	if next.Equal(now) {
		next = now.Add(time.Millisecond)
	}
	return next.UTC().Truncate(time.Microsecond)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func checkInstant(now, deadline time.Time, interval time.Duration) time.Time {
	if interval <= 0 {
		interval = time.Second
	}
	check := now.Add(interval)
	if !deadline.IsZero() && !check.Before(deadline) {
		check = deadline.Add(-time.Microsecond)
	}
	if !check.After(now) {
		check = now
	}
	return check.UTC().Truncate(time.Microsecond)
}

// DecideAfterAttempt 按固定顺序给出下一动作。运行阻塞不能盖过预算耗尽或不安全重发。
func DecideAfterAttempt(f AttemptFacts) (Plan, error) {
	now := f.Now.UTC().Truncate(time.Microsecond)
	deadline := f.Deadline.UTC().Truncate(time.Microsecond)
	if f.Policy.MaxAttempts == 0 {
		f.Policy.MaxAttempts = 1
	}
	last := certaintyOf(f)
	unresolved := mergeUnresolved(f, last)
	taskCert := enums.CertaintyNotReceived
	if len(unresolved) > 0 {
		taskCert = enums.CertaintyUncertain
	} else if last == enums.CertaintyReceived {
		taskCert = enums.CertaintyReceived
	}
	base := Plan{
		ResultCertainty: taskCert, LastCertainty: last, Unresolved: unresolved, JitterNumer: f.JitterNumer,
	}
	if f.Outcome == enums.OutcomeReceived {
		if f.Conflict || len(unresolved) > 0 {
			base.Reason = "接收证据不能消除未决或存在冲突"
			base.ErrorCode = constant.ErrUnsafeResend
			return finishTerminal(base, true, now), nil
		}
		base.Kind = enums.ActionSucceed
		base.Status = enums.TaskSucceeded
		base.Reason = "供应商已确认接收"
		base.ClearLastError = true
		base.BumpSchedule = true
		finished := now
		base.RoundFinishedAt = &finished
		base.ResultCertainty = enums.CertaintyReceived
		base.LastCertainty = enums.CertaintyReceived
		base.Unresolved = []UnresolvedRef{}
		return base, nil
	}
	continuable := canContinue(f, len(unresolved) > 0)
	if !continuable {
		base.Reason = reasonOr(f.ErrorCode, "不能继续自动发送")
		base.ErrorCode = codeOr(f.ErrorCode, constant.ErrUnsafeResend)
		return finishTerminal(base, len(unresolved) > 0, now), nil
	}
	delay, err := LocalBackoff(f.RoundAttemptCount, f.Policy.BackoffInitial, f.Policy.BackoffCap, f.JitterNumer)
	if err != nil {
		return Plan{}, err
	}
	next := now.Add(delay)
	if !f.SupplierNotBefore.IsZero() && f.SupplierNotBefore.After(next) {
		next = f.SupplierNotBefore.UTC()
	}
	next = next.Truncate(time.Microsecond)
	budgetGone := f.RoundAttemptCount >= f.Policy.MaxAttempts || !now.Before(deadline) || !next.Before(deadline)
	if budgetGone {
		if f.RoundAttemptCount >= f.Policy.MaxAttempts {
			base.ErrorCode = constant.ErrAttemptLimit
			base.Reason = "本轮尝试次数已用完"
		} else {
			base.ErrorCode = constant.ErrRoundDeadline
			base.Reason = "下一次允许时间不早于本轮截止"
		}
		return finishTerminal(base, len(unresolved) > 0, now), nil
	}
	base.Delay = delay
	if f.ConfirmedNotSent && blockingConstraint(f.Constraint) {
		check := checkInstant(now, deadline, f.BlockedCheck)
		base.Kind = enums.ActionBlock
		base.Status = enums.TaskBlocked
		base.Reason = reasonOr(f.ErrorCode, "尝试结束后阻塞")
		base.ErrorCode = codeOr(f.ErrorCode, f.Constraint)
		base.BumpSchedule = true
		base.ResumeStatus = string(enums.TaskRetryWait)
		base.NextCheckAt = &check
		base.ResumeNotBefore = &next
		return base, nil
	}
	base.Kind = enums.ActionRetry
	base.Status = enums.TaskRetryWait
	base.Reason = "已安排下一次发送"
	base.ErrorCode = f.ErrorCode
	base.BumpSchedule = true
	base.ScheduledAt = &next
	base.NextRetryAt = &next
	return base, nil
}

func finishTerminal(base Plan, unresolved bool, now time.Time) Plan {
	finished := now
	base.BumpSchedule = true
	base.RoundFinishedAt = &finished
	base.ScheduledAt = nil
	base.NextRetryAt = nil
	base.NextCheckAt = nil
	if unresolved || base.ResultCertainty == enums.CertaintyUncertain {
		base.Kind = enums.ActionUnknown
		base.Status = enums.TaskUnknown
		base.ResultCertainty = enums.CertaintyUncertain
		return base
	}
	base.Kind = enums.ActionFail
	base.Status = enums.TaskFailed
	base.ResultCertainty = enums.CertaintyNotReceived
	return base
}

func canContinue(f AttemptFacts, unresolved bool) bool {
	if unresolved && !f.IdempotencyCovers {
		return false
	}
	switch f.Outcome {
	case enums.OutcomeRetryableReject:
		return true
	case enums.OutcomeLocalFailure:
		return f.Retryable
	case enums.OutcomeUncertain:
		return f.IdempotencyCovers
	default:
		return false
	}
}

func certaintyOf(f AttemptFacts) enums.ResultCertainty {
	switch f.Outcome {
	case enums.OutcomeReceived:
		return enums.CertaintyReceived
	case enums.OutcomePermanentReject, enums.OutcomeRetryableReject:
		return enums.CertaintyNotReceived
	case enums.OutcomeLocalFailure:
		if f.ConfirmedNotSent || f.SendState == enums.SendNotSent {
			return enums.CertaintyNotReceived
		}
		return enums.CertaintyUncertain
	default:
		return enums.CertaintyUncertain
	}
}

func mergeUnresolved(f AttemptFacts, last enums.ResultCertainty) []UnresolvedRef {
	out := append([]UnresolvedRef(nil), f.Prior...)
	if last == enums.CertaintyUncertain {
		out = addUnresolved(out, UnresolvedRef{AttemptID: f.AttemptID, RoundNo: f.RoundNo})
	}
	if f.Outcome == enums.OutcomeReceived && !f.Conflict {
		if f.CoversBusinessKey {
			return nil
		}
		if len(f.CoveredAttemptIDs) > 0 {
			return dropCovered(out, f.CoveredAttemptIDs)
		}
	}
	return out
}

func addUnresolved(items []UnresolvedRef, item UnresolvedRef) []UnresolvedRef {
	for _, have := range items {
		if have.AttemptID == item.AttemptID {
			return items
		}
	}
	return append(items, item)
}

func dropCovered(items []UnresolvedRef, ids []uint64) []UnresolvedRef {
	out := make([]UnresolvedRef, 0, len(items))
	for _, item := range items {
		covered := false
		for _, id := range ids {
			if item.AttemptID == id {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, item)
		}
	}
	return out
}

func blockingConstraint(code string) bool {
	switch code {
	case constant.ErrChannelDisabled, constant.ErrCallerForbidden, constant.ErrAdapterUnavailable, constant.ErrConfigurationUnavail, constant.ErrTargetForbidden:
		return true
	default:
		return false
	}
}

// ResumePlan 在阻塞解除且检查通过时恢复到保存的类别。不增加次数或轮次。
func ResumePlan(task TaskFacts, now time.Time, allowed bool) (Plan, error) {
	if task.Status != enums.TaskBlocked {
		return Plan{}, codeErr(constant.CodeStateConflict, "任务不在阻塞", "status")
	}
	now = now.UTC().Truncate(time.Microsecond)
	deadline := task.RoundDeadlineAt.UTC()
	unresolved := len(task.Unresolved) > 0
	// allowed 由调用方在重新检查幂等覆盖或未使用的人工授权后传入。
	if !allowed || task.RoundAttemptCount >= task.MaxAttempts || !now.Before(deadline) {
		plan := closePlan(unresolved, now, Plan{Reason: "阻塞解除后不能继续发送", ErrorCode: constant.ErrRoundDeadline})
		if task.RoundAttemptCount >= task.MaxAttempts {
			plan.ErrorCode = constant.ErrAttemptLimit
			plan.Reason = "本轮尝试次数已用完"
		}
		if unresolved && !allowed {
			plan.ErrorCode = constant.ErrUnsafeResend
			plan.Reason = "未决结果不能安全重发"
		}
		return plan, nil
	}
	earliest := now
	if task.ResumeNotBefore != nil && task.ResumeNotBefore.After(earliest) {
		earliest = task.ResumeNotBefore.UTC().Truncate(time.Microsecond)
	}
	if !earliest.Before(deadline) {
		return closePlan(unresolved, now, Plan{Reason: "恢复时间不早于本轮截止", ErrorCode: constant.ErrRoundDeadline}), nil
	}
	status := enums.TaskStatus(task.ResumeStatus)
	if status != enums.TaskPending && status != enums.TaskRetryWait {
		return Plan{}, codeErr(constant.CodeStateConflict, "阻塞恢复类别不合法", "resume_status")
	}
	plan := Plan{
		Kind: enums.ActionWait, Status: status, Reason: "阻塞条件已解除", BumpSchedule: true,
		ResultCertainty: task.ResultCertainty, ScheduledAt: &earliest,
	}
	if status == enums.TaskRetryWait {
		plan.NextRetryAt = &earliest
	}
	plan.Unresolved = append([]UnresolvedRef(nil), task.Unresolved...)
	return plan, nil
}

// OpenRiskRound 描述 S07 授权后的新轮次字段。持久化由 S07 完成；这里只固定计数和未决证据。
func OpenRiskRound(task TaskFacts, now time.Time, operationID string, policy FrozenPolicy) (TaskFacts, error) {
	if task.Status != enums.TaskFailed && task.Status != enums.TaskUnknown {
		return TaskFacts{}, codeErr(constant.CodeStateConflict, "当前状态不能重投", "status")
	}
	if operationID == "" {
		return TaskFacts{}, codeErr(constant.CodeInvalidRequest, "缺少风险授权", "operation_id")
	}
	if policy.MaxAttempts == 0 || policy.RoundTimeout <= 0 {
		return TaskFacts{}, codeErr(constant.CodeMappingInvalid, "轮次策略不合法", "round_policy")
	}
	now = now.UTC().Truncate(time.Microsecond)
	deadline := now.Add(policy.RoundTimeout)
	task.RoundNo++
	task.RoundAttemptCount = 0
	task.Status = enums.TaskPending
	task.ScheduleVersion++
	task.RowVersion++
	task.MaxAttempts = policy.MaxAttempts
	task.ScheduledAt = &now
	task.RoundDeadlineAt = deadline
	task.NextRetryAt = nil
	task.NextCheckAt = nil
	task.RoundFinishedAt = nil
	task.CurrentAttemptID = nil
	task.LeaseToken = ""
	task.LeaseExpiresAt = nil
	task.BlockedReason = ""
	task.ResumeStatus = ""
	task.ResumeNotBefore = nil
	task.BlockedSince = nil
	task.RiskOperationID = operationID
	task.RiskAttemptID = nil
	if len(task.Unresolved) > 0 {
		task.ResultCertainty = enums.CertaintyUncertain
	} else {
		task.ResultCertainty = enums.CertaintyNotReceived
	}
	return task, nil
}

// CheckTaskTimes 核对状态与时间字段。违反时返回稳定错误，调用方应回滚。
func CheckTaskTimes(t TaskTimes) error {
	switch t.Status {
	case enums.TaskPending:
		if t.ScheduledAt == nil || t.NextRetryAt != nil || t.NextCheckAt != nil || t.RoundFinishedAt != nil || leased(t) {
			return codeErr(constant.CodeStateConflict, "待执行时间字段不合法", "scheduled_at")
		}
	case enums.TaskRetryWait:
		if t.ScheduledAt == nil || t.NextRetryAt == nil || !t.ScheduledAt.Equal(*t.NextRetryAt) || t.NextCheckAt != nil || t.RoundFinishedAt != nil || leased(t) {
			return codeErr(constant.CodeStateConflict, "等待时间字段不合法", "next_retry_at")
		}
	case enums.TaskProcessing:
		if t.ScheduledAt != nil || t.NextRetryAt != nil || t.NextCheckAt != nil || t.RoundFinishedAt != nil || !leased(t) {
			return codeErr(constant.CodeStateConflict, "执行中字段不合法", "lease_token")
		}
	case enums.TaskBlocked:
		if t.BlockedReason == "" || t.NextCheckAt == nil || t.ScheduledAt != nil || t.NextRetryAt != nil || t.RoundFinishedAt != nil || leased(t) {
			return codeErr(constant.CodeStateConflict, "阻塞字段不合法", "next_check_at")
		}
	case enums.TaskSucceeded, enums.TaskFailed, enums.TaskUnknown:
		if t.ScheduledAt != nil || t.NextRetryAt != nil || t.NextCheckAt != nil || t.RoundFinishedAt == nil || leased(t) || t.BlockedReason != "" {
			return codeErr(constant.CodeStateConflict, "结案字段不合法", "round_finished_at")
		}
	default:
		return codeErr(constant.CodeStateConflict, "未知任务状态", "status")
	}
	return nil
}

func leased(t TaskTimes) bool {
	return t.CurrentAttemptID != nil || t.LeaseToken != "" || t.LeaseExpiresAt != nil
}

// InvocationIsNotReceipt 说明 HTTP_INVOKED 只表示准备进入调用，不能当成供应商已接收。
func InvocationIsNotReceipt(state enums.SendState) bool {
	return state == enums.SendHTTPInvoked || state == enums.SendUncertain
}

func reasonOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func codeOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
