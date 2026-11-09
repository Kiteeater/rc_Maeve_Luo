package data_interface

import (
	"context"
	"errors"
	"time"

	"notifier/internal/domain"
)

// ErrOperationKeyTaken 表示同一 (caller_scope, operation_type, key) 已被并发事务占用；当前事务已整体回滚。
var ErrOperationKeyTaken = errors.New("data_interface: operation key taken")

// StoredOperation 是已成功提交的管理操作。
type StoredOperation struct {
	Fingerprint string
	Result      []byte
}

// ResultBuilder 由 biz 提供，在事务内用最终任务快照生成操作结果（不复制业务载荷）。
type ResultBuilder func(task TaskRecord, completedAt time.Time) ([]byte, error)

// OperationMeta 是幂等记录与审计公共字段。
type OperationMeta struct {
	OperationID string
	OperationTy string
	Key         string
	CallerScope string
	Fingerprint string
	Actor       string
	Reason      string
	Evidence    []byte // 脱敏证据/风险确认摘要
	DeliveryID  uint64
	ExpectedRow uint32
	Build       ResultBuilder
}

// ReplayInput 是 ReplayTask 输入（OPS-R03）。
type ReplayInput struct {
	OperationMeta
	Policy    domain.FrozenPolicy
	PolicyRaw []byte
	Risk      bool
}

// ResolveInput 是 ResolveTask 输入（OPS-V02）。
type ResolveInput struct {
	OperationMeta
	Resolution  string
	Ev          domain.Evidence
	MaxEvidence int
}

// OperationStore 是 S07 仓储端口。
type OperationStore interface {
	ReadOperation(ctx context.Context, scope, opType, key string) (StoredOperation, bool, error)
	ReplayTask(ctx context.Context, in ReplayInput) ([]byte, error)
	ResolveTask(ctx context.Context, in ResolveInput) ([]byte, error)
}
