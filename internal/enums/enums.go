// Package enums 存放跨模块共享的业务枚举。状态推进规则由 S03 完成。
package enums

// Outcome 是单次尝试的协议结论。
type Outcome string

const (
	OutcomeReceived        Outcome = "RECEIVED"
	OutcomeRetryableReject Outcome = "RETRYABLE_REJECT"
	OutcomePermanentReject Outcome = "PERMANENT_REJECT"
	OutcomeLocalFailure    Outcome = "LOCAL_FAILURE"
	OutcomeUncertain       Outcome = "UNCERTAIN"
)

// ResultCertainty 是任务级或尝试级的接收确定性。
// 初始任务没有尝试时为 NOT_RECEIVED，只表示当前没有已知接收或未决发送。
type ResultCertainty string

const (
	CertaintyReceived    ResultCertainty = "RECEIVED"
	CertaintyNotReceived ResultCertainty = "NOT_RECEIVED"
	CertaintyUncertain   ResultCertainty = "UNCERTAIN"
)

// SendState 区分明确未发送、已进入 HTTP 调用和无法判断。
// 进入 HTTP 调用不证明供应商已经收到。迁移草案中的 SENT 已校准为 HTTP_INVOKED。
type SendState string

const (
	SendNotSent     SendState = "NOT_SENT"
	SendHTTPInvoked SendState = "HTTP_INVOKED"
	SendUncertain   SendState = "UNCERTAIN"
)

// TaskStatus 是投递任务状态。允许的迁移由 S03 定义。
type TaskStatus string

const (
	TaskPending    TaskStatus = "PENDING"
	TaskProcessing TaskStatus = "PROCESSING"
	TaskRetryWait  TaskStatus = "RETRY_WAIT"
	TaskBlocked    TaskStatus = "BLOCKED"
	TaskSucceeded  TaskStatus = "SUCCEEDED"
	TaskFailed     TaskStatus = "FAILED"
	TaskUnknown    TaskStatus = "UNKNOWN"
)

// NextAction 是领域统一决策的动作名。决策过程由 S03 完成。
type NextAction string

const (
	ActionSucceed NextAction = "SUCCEED"
	ActionFail    NextAction = "FAIL"
	ActionUnknown NextAction = "UNKNOWN"
	ActionRetry   NextAction = "RETRY"
	ActionWait    NextAction = "WAIT"
	ActionBlock   NextAction = "BLOCK"
)

// HistoryPhase 是时间线阶段全集。追加两类历史时必须使用不同动作值。
type HistoryPhase string

const (
	PhaseTaskCreated      HistoryPhase = "TASK_CREATED"
	PhaseScheduleCreated  HistoryPhase = "SCHEDULE_CREATED"
	PhasePublishSucceeded HistoryPhase = "PUBLISH_SUCCEEDED"
	PhasePublishFailed    HistoryPhase = "PUBLISH_FAILED"
	PhaseAttemptStarted   HistoryPhase = "ATTEMPT_STARTED"
	PhaseAttemptFinished  HistoryPhase = "ATTEMPT_FINISHED"
	PhaseRetryPlanned     HistoryPhase = "RETRY_PLANNED"
	PhaseWaitPlanned      HistoryPhase = "WAIT_PLANNED"
	PhaseBlocked          HistoryPhase = "BLOCKED"
	PhaseUnblocked        HistoryPhase = "UNBLOCKED"
	PhaseLeaseExpired     HistoryPhase = "LEASE_EXPIRED"
	PhaseDispatchRepaired HistoryPhase = "DISPATCH_REPAIRED"
	PhaseDLQDisposed      HistoryPhase = "DLQ_DISPOSED"
	PhaseManualRetry      HistoryPhase = "MANUAL_RETRY"
	PhaseManualResolve    HistoryPhase = "MANUAL_RESOLVE"
	PhaseLateResponse     HistoryPhase = "LATE_RESPONSE"
)

// HistoryPhases 是固定阶段全集，供迁移容量和校验对照。
var HistoryPhases = []HistoryPhase{
	PhaseTaskCreated,
	PhaseScheduleCreated,
	PhasePublishSucceeded,
	PhasePublishFailed,
	PhaseAttemptStarted,
	PhaseAttemptFinished,
	PhaseRetryPlanned,
	PhaseWaitPlanned,
	PhaseBlocked,
	PhaseUnblocked,
	PhaseLeaseExpired,
	PhaseDispatchRepaired,
	PhaseDLQDisposed,
	PhaseManualRetry,
	PhaseManualResolve,
	PhaseLateResponse,
}
