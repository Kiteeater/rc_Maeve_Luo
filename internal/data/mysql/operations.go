package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// OperationStore 实现 S07 人工重投与核对结案。操作幂等记录、任务变化、Outbox 与历史同事务。
type OperationStore struct {
	db   *sql.DB
	exec *Executor
}

func NewOperationStore(db *sql.DB, exec *Executor) *OperationStore {
	return &OperationStore{db: db, exec: exec}
}

func (s *OperationStore) ReadOperation(ctx context.Context, scope, opType, key string) (data_interface.StoredOperation, bool, error) {
	var op data_interface.StoredOperation
	err := s.db.QueryRowContext(ctx, `SELECT request_fingerprint, result FROM management_events
		WHERE caller_scope = ? AND operation_type = ? AND idempotency_key = ?`, scope, opType, key).Scan(&op.Fingerprint, &op.Result)
	if errors.Is(err, sql.ErrNoRows) {
		return op, false, nil
	}
	return op, err == nil, err
}

// existingOp 在事务内先读唯一键：同键同参数返回原结果，不因版本已变而冲突（OPS-09）。
func existingOp(ctx context.Context, tx *sql.Tx, m data_interface.OperationMeta) ([]byte, bool, error) {
	var fp string
	var result []byte
	err := tx.QueryRowContext(ctx, `SELECT request_fingerprint, result FROM management_events
		WHERE caller_scope = ? AND operation_type = ? AND idempotency_key = ? FOR UPDATE`, m.CallerScope, m.OperationTy, m.Key).Scan(&fp, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if fp != m.Fingerprint {
		return nil, false, domainErr(constant.ReasonOperationKey, "相同操作键的参数不一致")
	}
	return result, true, nil
}

func (s *OperationStore) ReplayTask(ctx context.Context, in data_interface.ReplayInput) ([]byte, error) {
	var out []byte
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		res, err := replayTx(ctx, tx, in)
		out = res
		return err
	})
	return out, mapOp(err)
}

func replayTx(ctx context.Context, tx *sql.Tx, in data_interface.ReplayInput) ([]byte, error) {
	if res, ok, err := existingOp(ctx, tx, in.OperationMeta); err != nil || ok {
		return res, err
	}
	now, err := txNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	task, err := lockTask(ctx, tx, in.DeliveryID)
	if err != nil {
		return nil, err
	}
	if task.Status != enums.TaskFailed && task.Status != enums.TaskUnknown {
		return nil, domainErr(constant.ReasonTaskNotRetryable, "任务当前不能重投")
	}
	if task.CurrentAttemptID != nil || task.LeaseToken != "" {
		return nil, domainErr(constant.ReasonTaskLeaseActive, "任务仍有当前尝试或领取者")
	}
	if task.RowVersion != in.ExpectedRow {
		return nil, domainErr(constant.ReasonTaskVersion, "任务版本已变化")
	}
	deadline := now.Add(in.Policy.RoundTimeout)
	var risk any
	if in.Risk {
		risk = in.OperationID
	}
	// 新轮次：次数清零仅限本轮；保留累计次数、最近尝试、最近结论与未决证据；清空阻塞/恢复/领取/结案字段。
	res, err := tx.ExecContext(ctx, `
		UPDATE delivery_tasks SET status = 'PENDING', round_no = round_no + 1, round_attempt_count = 0,
			max_attempts = ?, round_policy = ?, round_deadline_at = ?, scheduled_at = ?,
			next_retry_at = NULL, next_check_at = NULL, current_attempt_id = NULL, lease_token = NULL, lease_expires_at = NULL,
			blocked_reason = NULL, blocked_since = NULL, resume_status = NULL, resume_not_before = NULL, round_finished_at = NULL,
			risk_operation_id = ?, risk_authorized_attempt_id = NULL,
			schedule_version = schedule_version + 1, row_version = row_version + 1
		WHERE delivery_id = ? AND row_version = ? AND status = ?`,
		in.Policy.MaxAttempts, string(in.PolicyRaw), utcWall(deadline), utcWall(now), risk,
		task.DeliveryID, task.RowVersion, string(task.Status))
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, domainErr(constant.ReasonTaskVersion, "任务版本已变化")
	}
	fresh, err := loadTask(ctx, tx, task.DeliveryID)
	if err != nil {
		return nil, err
	}
	outboxID, err := insertOutbox(ctx, tx, fresh, domain.Plan{ScheduledAt: &now}, now)
	if err != nil {
		return nil, err
	}
	if err := insertOpHistory(ctx, tx, fresh, enums.PhaseManualRetry, task.Status, enums.TaskPending, in.Reason, 0, in.OperationMeta, in.Evidence, now); err != nil {
		return nil, err
	}
	if err := insertHistory(ctx, tx, fresh, enums.PhaseScheduleCreated, task.Status, enums.TaskPending, "人工重投新轮次", fresh.ScheduleVersion, 0, outboxID, nil, now); err != nil {
		return nil, err
	}
	if fresh, err = loadTask(ctx, tx, task.DeliveryID); err != nil {
		return nil, err
	}
	return finishOp(ctx, tx, in.OperationMeta, fresh, now)
}

func (s *OperationStore) ResolveTask(ctx context.Context, in data_interface.ResolveInput) ([]byte, error) {
	var out []byte
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		res, err := resolveTx(ctx, tx, in)
		out = res
		return err
	})
	return out, mapOp(err)
}

func resolveTx(ctx context.Context, tx *sql.Tx, in data_interface.ResolveInput) ([]byte, error) {
	if res, ok, err := existingOp(ctx, tx, in.OperationMeta); err != nil || ok {
		return res, err
	}
	now, err := txNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	task, err := lockTask(ctx, tx, in.DeliveryID)
	if err != nil {
		return nil, err
	}
	attempts, err := loadAttempts(ctx, tx, task.DeliveryID)
	if err != nil {
		return nil, err
	}
	refs := make([]domain.AttemptRef, len(attempts))
	for i, a := range attempts {
		refs[i] = domain.AttemptRef{AttemptID: a.AttemptID}
	}
	// 锁定后重新计算待覆盖集合。
	if code := domain.CheckResolve(task.Facts(), task.IdempotencyKey, refs, in.ExpectedRow, in.Resolution, in.Ev); code != "" {
		return nil, domainErr(code, "核对条件不满足")
	}
	cert := enums.CertaintyNotReceived
	if in.Resolution == string(enums.TaskSucceeded) {
		cert = enums.CertaintyReceived
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE delivery_tasks SET status = ?, result_certainty = ?, unresolved_evidence = JSON_ARRAY(),
			scheduled_at = NULL, next_retry_at = NULL, next_check_at = NULL, round_finished_at = ?,
			schedule_version = schedule_version + 1, row_version = row_version + 1
		WHERE delivery_id = ? AND row_version = ? AND status = 'UNKNOWN' AND current_attempt_id IS NULL`,
		in.Resolution, string(cert), utcWall(now), task.DeliveryID, task.RowVersion)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, domainErr(constant.ReasonTaskVersion, "任务版本已变化")
	}
	fresh, err := loadTask(ctx, tx, task.DeliveryID)
	if err != nil {
		return nil, err
	}
	if err := insertOpHistory(ctx, tx, fresh, enums.PhaseManualResolve, enums.TaskUnknown, enums.TaskStatus(in.Resolution), in.Reason, 0, in.OperationMeta, in.Evidence, now); err != nil {
		return nil, err
	}
	if fresh, err = loadTask(ctx, tx, task.DeliveryID); err != nil {
		return nil, err
	}
	return finishOp(ctx, tx, in.OperationMeta, fresh, now)
}

// finishOp 生成结果并插入操作幂等记录；唯一键被并发事务占用时整体回滚（OPS-11）。
func finishOp(ctx context.Context, tx *sql.Tx, m data_interface.OperationMeta, task data_interface.TaskRecord, now time.Time) ([]byte, error) {
	result, err := m.Build(task, now)
	if err != nil {
		return nil, err
	}
	var evidence any
	if len(m.Evidence) > 0 {
		evidence = string(m.Evidence)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO management_events (
			operation_type, operation_id, idempotency_key, caller_scope, request_fingerprint, fingerprint_version,
			expected_version, expected_row_version, actor, reason, evidence, target_type, target_id, result, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,'delivery',?,?,?)`,
		m.OperationTy, m.OperationID, m.Key, m.CallerScope, m.Fingerprint, constant.OperationFingerprintV1,
		m.ExpectedRow, m.ExpectedRow, m.Actor, clipReason(m.Reason), evidence, strconv.FormatUint(m.DeliveryID, 10), string(result), utcWall(now))
	if isUnique(err) {
		return nil, data_interface.ErrOperationKeyTaken
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func insertOpHistory(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, phase enums.HistoryPhase, from, to enums.TaskStatus, reason string, attemptID uint64, m data_interface.OperationMeta, evidence []byte, now time.Time) error {
	seq, err := NextHistorySeq(ctx, tx, task.DeliveryID)
	if err != nil {
		return err
	}
	writeID, err := domain.WriteID(task.DeliveryID, phase, attemptID, m.OperationID, 0, 0)
	if err != nil {
		return err
	}
	var raw any
	if len(evidence) > 0 {
		raw = string(evidence)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO delivery_history (
			delivery_id, history_seq, write_id, phase, from_status, to_status, reason,
			round_no, schedule_version, attempt_id, outbox_id, evidence, occurred_at, actor)
		VALUES (?,?,?,?,?,?,?,?,?,?,NULL,?,?,?)`,
		task.DeliveryID, seq, writeID, string(phase), string(from), string(to), clipReason(reason),
		task.RoundNo, task.ScheduleVersion, nullU64(attemptID), raw, utcWall(now), m.Actor)
	return err
}

func mapOp(err error) error {
	var uncertain *CommitUncertainError
	if errors.As(err, &uncertain) {
		return data_interface.ErrCommitUncertain
	}
	return err
}
