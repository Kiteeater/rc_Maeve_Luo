package data_interface

import (
	"context"
	"errors"
	"time"

	"notifier/internal/domain"
	"notifier/internal/enums"
)

var (
	// ErrOwnershipLost 表示领取已过期或被替换。调用方不得覆盖任务，可追加迟到证据。
	ErrOwnershipLost = errors.New("data_interface: ownership_lost")
	// ErrPreconditionChanged 表示通道版本或发布引用已变化。不得沿用过期的 READY。
	ErrPreconditionChanged = errors.New("data_interface: precondition_changed")
	// ErrUnconfirmed 表示按稳定键仍不能确认事务已提交。
	ErrUnconfirmed = errors.New("data_interface: disposition unconfirmed")
)

// UnresolvedRef 与领域未决引用对应，供仓储读写。
type UnresolvedRef struct {
	AttemptID uint64
	RoundNo   uint32
}

// AttemptRecord 是一次尝试的持久化事实。
type AttemptRecord struct {
	AttemptID         uint64
	DeliveryID        uint64
	RoundNo           uint32
	AttemptNo         uint32
	OutboxID          uint64
	TraceID           string
	WorkerID          string
	DeploymentVersion string
	Status            string
	SendState         enums.SendState
	Outcome           enums.Outcome
	ResultCertainty   enums.ResultCertainty
	ErrorCode         string
	ErrorReason       string
	ErrorTruncated    bool
	RequestSummary    string
	ResponseSummary   string
	RequestTruncated  bool
	ResponseTruncated bool
	HTTPStatus        *int
	ProviderRequestID string
	Retryable         bool
	EvidenceScope     []byte
	StartedAt         time.Time
	RequestStartedAt  *time.Time
	FinishedAt        *time.Time
	HTTPDuration      *time.Duration
	AttemptDuration   *time.Duration
	TimingIncomplete  bool
}

// HistoryRecord 是一条任务历史。
type HistoryRecord struct {
	Seq             uint32
	WriteID         string
	Phase           enums.HistoryPhase
	From            enums.TaskStatus
	To              enums.TaskStatus
	Reason          string
	RoundNo         uint32
	ScheduleVersion uint32
	AttemptID       uint64
	OutboxID        uint64
	Evidence        []byte
	OccurredAt      time.Time
}

// TaskRecord 是执行所需的任务快照。后台读取走主库。
type TaskRecord struct {
	DeliveryID         uint64
	EventID            uint64
	CallerID           string
	RouteTargetID      string
	ChannelID          uint64
	ChannelVersion     uint32
	ReleaseID          uint64
	ProviderID         string
	ProviderAccountID  string
	Operation          string
	AdapterType        string
	AdapterVersion     string
	MappingID          string
	MappingVersion     string
	FixedTarget        []byte
	PayloadCanonical   []byte
	RoundPolicy        []byte
	IdempotencyKey     string
	Status             enums.TaskStatus
	ResultCertainty    enums.ResultCertainty
	LastCertainty      enums.ResultCertainty
	RoundNo            uint32
	ScheduleVersion    uint32
	RowVersion         uint32
	HistorySeq         uint32
	RoundAttemptCount  uint32
	TotalAttemptCount  uint32
	MaxAttempts        uint32
	RoundDeadlineAt    time.Time
	ScheduledAt        *time.Time
	NextRetryAt        *time.Time
	NextCheckAt        *time.Time
	CurrentAttemptID   *uint64
	LastAttemptID      *uint64
	LeaseToken         string
	LeaseExpiresAt     *time.Time
	BlockedReason      string
	BlockedSince       *time.Time
	ResumeStatus       string
	ResumeNotBefore    *time.Time
	RoundFinishedAt    *time.Time
	LastErrorCode      string
	LastErrorReason    string
	LastErrorTruncated bool
	RiskOperationID    string
	RiskAttemptID      *uint64
	OutboxID           uint64
	Unresolved         []UnresolvedRef
	Attempts           []AttemptRecord
	History            []HistoryRecord
}

// StartAttemptInput 是领取参数。仓储在事务内用数据库时间复核。
type StartAttemptInput struct {
	DeliveryID             uint64
	EventID                uint64
	RoundNo                uint32
	ScheduleVersion        uint32
	ExpectedRowVersion     uint32
	OutboxID               uint64
	WorkerID               string
	DeploymentVersion      string
	TraceID                string
	ExpectedChannelVersion uint32
	ExpectedReleaseID      uint64
	LeaseTimeout           time.Duration
	SafetyNotAfter         time.Time
	UseRiskAuthorization   bool
}

// StartAttemptResult 是已提交的领取。提交不确定时不得发送。
type StartAttemptResult struct {
	Ownership         domain.Ownership
	AttemptNo         uint32
	StartedAt         time.Time
	RowVersion        uint32
	RoundAttemptCount uint32
	TotalAttemptCount uint32
	ChannelVersion    uint32
}

// RecordInvocationInput 只证明准备进入 HTTP，不证明供应商已收到。
type RecordInvocationInput struct {
	Ownership        domain.Ownership
	RequestStartedAt time.Time
	Method           string
	TargetPath       string
	RequestSummary   string
	RequestTruncated bool
	RequestSize      int
}

// FinishInput 携带已经算好的计划。仓储不重新抽取抖动。
type FinishInput struct {
	Ownership         domain.Ownership
	SendState         enums.SendState
	Outcome           enums.Outcome
	ResultCertainty   enums.ResultCertainty
	Retryable         bool
	ErrorCode         string
	ErrorReason       string
	ErrorTruncated    bool
	RequestSummary    string
	ResponseSummary   string
	RequestTruncated  bool
	ResponseTruncated bool
	HTTPStatus        *int
	ProviderRequestID string
	EvidenceScope     []byte
	HTTPDuration      *time.Duration
	AttemptDuration   *time.Duration
	TimingIncomplete  bool
	Plan              domain.Plan
	BlockedCheck      time.Duration
}

// WaitOrBlockInput 在创建尝试前写等待、阻塞或结案。不增加次数。
type WaitOrBlockInput struct {
	DeliveryID         uint64
	EventID            uint64
	RoundNo            uint32
	ScheduleVersion    uint32
	ExpectedRowVersion uint32
	OutboxID           uint64
	Plan               domain.Plan
}

// ResumeInput 是阻塞解除时的条件更新。扫描循环由 S06 完成。
type ResumeInput struct {
	DeliveryID         uint64
	ExpectedRowVersion uint32
	ScheduleVersion    uint32
	RoundNo            uint32
	Plan               domain.Plan
}

// LateEvidenceInput 追加迟到证据，不改任务状态和原尝试结果。
type LateEvidenceInput struct {
	DeliveryID uint64
	AttemptID  uint64
	Reason     string
	Evidence   []byte
}

// DeliveryStore 是 S03 的完整事务端口。
type DeliveryStore interface {
	LoadTask(ctx context.Context, deliveryID uint64) (TaskRecord, error)
	StartAttempt(ctx context.Context, in StartAttemptInput) (StartAttemptResult, error)
	RecordInvocation(ctx context.Context, in RecordInvocationInput) error
	FinishAttemptAndPlan(ctx context.Context, in FinishInput) (TaskRecord, error)
	WaitOrBlock(ctx context.Context, in WaitOrBlockInput) (TaskRecord, error)
	ResumeBlocked(ctx context.Context, in ResumeInput) (TaskRecord, error)
	AppendLateEvidence(ctx context.Context, in LateEvidenceInput) error
	// ReadDisposition 用新事务读主库。不能在提交不确定时沿用旧快照。
	ReadDisposition(ctx context.Context, deliveryID, attemptID uint64) (TaskRecord, error)
}

// PrepareRequest 是 S05 准备端口的输入。真实凭证和限流由 S05 完成。
type PrepareRequest struct {
	Task          TaskRecord
	Now           time.Time
	CallerAllowed bool
}

// PrepareResponse 是一次准备结论。READY 的句柄不得写入日志。
type PrepareResponse struct {
	Kind              string
	ChannelVersion    uint32
	ReleaseID         uint64
	CredentialHandle  string
	SafeUntil         time.Time
	IdempotencyCovers bool
	WindowStart       time.Time
	RateLimitUntil    time.Time
	SupplierNotBefore time.Time
	Reason            string
	ErrorCode         string
}

// BuiltRequest 是一次构造结果。不含凭证原文。
type BuiltRequest struct {
	Method     string
	TargetPath string
	Summary    string
	Truncated  bool
	Size       int
	Body       []byte
}

// SendResponse 是单次传输的原始事实。禁止隐式重试。
type SendResponse struct {
	HTTPStatus        int
	BodySummary       string
	Truncated         bool
	ProviderRequestID string
	ErrorCode         string
	TransportErr      string
}

// Interpreted 是 S05 对单次响应的协议解释。
type Interpreted struct {
	Outcome           enums.Outcome
	Certainty         enums.ResultCertainty
	Retryable         bool
	ErrorCode         string
	ErrorReason       string
	ProviderRequestID string
	CoversBusinessKey bool
	CoveredAttemptIDs []uint64
	Conflict          bool
	SupplierNotBefore time.Time
	EvidenceScope     []byte
}

// ProviderRuntime 是 S05 端口。本包的 mock 不访问网络；真实受控 HTTP 由 S05 完成。
type ProviderRuntime interface {
	Prepare(ctx context.Context, in PrepareRequest) (PrepareResponse, error)
	Build(ctx context.Context, task TaskRecord, own domain.Ownership, prep PrepareResponse) (BuiltRequest, error)
	SendOnce(ctx context.Context, built BuiltRequest) (SendResponse, error)
	Interpret(ctx context.Context, task TaskRecord, built BuiltRequest, sent SendResponse) (Interpreted, error)
}

// ScheduleOriginal 安排同一条指令到期再发布。真实 MQ 由 S04 完成。
type ScheduleOriginal interface {
	ScheduleOriginal(ctx context.Context, cmd domain.DeliveryCommand, notBefore time.Time) error
}
