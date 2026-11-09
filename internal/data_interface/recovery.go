package data_interface

import (
	"context"
	"time"

	"notifier/internal/domain"
)

// RecoveryCandidate 只是提示，不表示获得处置权。
type RecoveryCandidate struct {
	DeliveryID uint64
	RowVersion uint32
}

// RecoverySnapshot 是一次检查读取的当前任务、尝试与当前 Outbox，以及数据库时间。
type RecoverySnapshot struct {
	Task            TaskRecord
	AttemptFinished bool
	Outbox          domain.OutboxFacts
	Now             time.Time
}

// RecoveryOutcome 是 RecoverTask 结果。
type RecoveryOutcome string

const (
	RecoveryApplied        RecoveryOutcome = "APPLIED"
	RecoveryStale          RecoveryOutcome = "STALE"
	RecoveryAlreadyApplied RecoveryOutcome = "ALREADY_APPLIED"
)

// RecoverInput 携带所见版本、动作和稳定修复键。仓储在锁内重检全部条件。
type RecoverInput struct {
	DeliveryID         uint64
	RoundNo            uint32
	ScheduleVersion    uint32
	ExpectedRowVersion uint32
	Action             domain.RecoveryAction
	AttemptID          uint64        // EXPIRE_ATTEMPT 的当前尝试
	Plan               *domain.Plan  // EXPIRE / CLOSE / RECHECK 解除时的处置
	NextCheckAt        *time.Time    // RECHECK 条件未解除时仅推迟检查
	Repair             string        // REPAIR_DISPATCH 方式
	RepairCount        uint32        // 所见 repair_count
	Cooldown           time.Duration // 补发冷却
	MaxRepairs         uint32
	RepairKey          string
	Actor              string
	Reason             string
}

// DLQRecord 是死信处置记录。
type DLQRecord struct {
	DeliveryID uint64
	MessageID  string
	Action     domain.DLQAction
	Reason     string
	Actor      string
}

// RecoveryStore 是 S06 仓储端口。
type RecoveryStore interface {
	ListRecoveryCandidates(ctx context.Context, kind domain.RecoveryKind, limit int) ([]RecoveryCandidate, error)
	LoadRecoverySnapshot(ctx context.Context, deliveryID uint64) (RecoverySnapshot, error)
	RecoverTask(ctx context.Context, in RecoverInput) (RecoveryOutcome, error)
	RecordDLQ(ctx context.Context, in DLQRecord) (RecoveryOutcome, error)
}

// ConstraintResult 是无副作用的约束与重发安全检查结果：不取凭证、不扣限流令牌、不发请求。
type ConstraintResult struct {
	Code   string // 空表示当前约束允许
	Covers bool   // 供应商幂等覆盖全部未决结果（考虑本次 HTTP 预算）
}

// ConstraintChecker 由 S05 提供给 S06 使用。
type ConstraintChecker interface {
	CheckConstraints(ctx context.Context, task TaskRecord, now time.Time) (ConstraintResult, error)
}
