package domain

import (
	"time"

	"notifier/internal/enums"
)

// EventEnvelope 是受理前的事件事实。DataCanonical 是精确 JSON 字节，由 S02 组成指纹。
type EventEnvelope struct {
	CallerID           string
	SourceBusiness     string
	SourceEventID      string
	EventType          string
	SchemaVersion      string
	OccurredAt         time.Time
	BusinessObjectType string
	BusinessObjectID   string
	DataCanonical      []byte
	FingerprintVersion int
}

// TargetPlan 是一个路由目标的固定语义。FixedTarget 与 PayloadCanonical 的内部结构由 S01 定义。
type TargetPlan struct {
	RouteTargetID     string
	ProviderID        string
	ProviderAccountID string
	Operation         string
	MappingID         string
	MappingVersion    string
	AdapterType       string
	AdapterVersion    string
	PayloadCanonical  []byte
	FixedTarget       []byte
}

// DeliveryPlan 是一次发布计算出的固定计划。轮次策略 JSON 的结构由 S01 定义。
type DeliveryPlan struct {
	ReleaseRef  string
	Targets     []TargetPlan
	RoundPolicy []byte
}

// Ownership 是一次领取的所有权。校验规则由 S03 完成。
type Ownership struct {
	DeliveryID      uint64
	RoundNo         uint32
	ScheduleVersion uint32
	AttemptID       uint64
	LeaseToken      string
	LeaseExpiresAt  time.Time
}

// TaskSnapshot 是任务恢复与执行所需的持久化视图。状态推进由 S03 完成。
type TaskSnapshot struct {
	DeliveryID         uint64
	EventID            uint64
	RouteTargetID      string
	CallerID           string
	Status             enums.TaskStatus
	ResultCertainty    enums.ResultCertainty
	RoundNo            uint32
	ScheduleVersion    uint32
	RowVersion         uint32
	HistorySeq         uint32
	Ownership          *Ownership
	RoundPolicy        []byte
	PayloadCanonical   []byte
	ResumeStatus       string
	ResumeNotBefore    *time.Time
	BlockedSince       *time.Time
	UnresolvedEvidence []byte
}

// AttemptResult 是一次发送事实。是否推进任务由 S03 判定。
type AttemptResult struct {
	SendState         enums.SendState
	Outcome           enums.Outcome
	ResultCertainty   enums.ResultCertainty
	EvidenceScope     []byte
	StartedAt         time.Time
	Duration          time.Duration
	RequestSummary    string
	ResponseSummary   string
	ErrorSummary      string
	RequestTruncated  bool
	ResponseTruncated bool
	ErrorTruncated    bool
	TimingIncomplete  bool
}

// Decision 是统一决策的输出。Kind 取 SUCCEED/FAIL/UNKNOWN/RETRY/WAIT/BLOCK。决策过程由 S03 完成。
type Decision struct {
	Kind           enums.NextAction
	Reason         string
	At             time.Time
	ResumeCategory string
}

// OperationResult 是管理或发布操作的幂等结果。快照内容由对应模块填充。
type OperationResult struct {
	OperationID   string
	OperationType string
	CallerScope   string
	Task          *TaskSnapshot
	ReleaseRef    string
}
