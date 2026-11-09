package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// RecoveryStore 实现 S06 仓储端口。所有处置在任务锁内重检，修复键保证同一修复只推进一次。
type RecoveryStore struct {
	db   *sql.DB
	exec *Executor
}

func NewRecoveryStore(db *sql.DB, exec *Executor) *RecoveryStore {
	return &RecoveryStore{db: db, exec: exec}
}

// errAlready / errStale 只在事务内部传递结论，并让 RunTx 回滚未完成写入。
var (
	errAlready = errors.New("recovery: already applied")
	errStale   = errors.New("recovery: stale")
)

var candidateSQL = map[domain.RecoveryKind]string{
	domain.RecoverLease: `SELECT delivery_id, row_version FROM delivery_tasks
		WHERE status = 'PROCESSING' AND lease_expires_at <= UTC_TIMESTAMP(6)
		ORDER BY lease_expires_at, delivery_id LIMIT ?`,
	domain.RecoverBlocked: `SELECT delivery_id, row_version FROM delivery_tasks
		WHERE status = 'BLOCKED' AND next_check_at <= UTC_TIMESTAMP(6)
		ORDER BY next_check_at, delivery_id LIMIT ?`,
	domain.RecoverDeadline: `SELECT delivery_id, row_version FROM delivery_tasks
		WHERE status IN ('PENDING','RETRY_WAIT','BLOCKED') AND round_deadline_at <= UTC_TIMESTAMP(6)
		ORDER BY round_deadline_at, delivery_id LIMIT ?`,
	domain.RecoverDue: `SELECT delivery_id, row_version FROM delivery_tasks
		WHERE status IN ('PENDING','RETRY_WAIT') AND scheduled_at <= UTC_TIMESTAMP(6)
		ORDER BY scheduled_at, delivery_id LIMIT ?`,
}

// ListRecoveryCandidates 每轮从当前最早未处置候选开始，不使用持久游标（RCV-17）。
func (s *RecoveryStore) ListRecoveryCandidates(ctx context.Context, kind domain.RecoveryKind, limit int) ([]data_interface.RecoveryCandidate, error) {
	q, ok := candidateSQL[kind]
	if !ok {
		return nil, errors.New("mysql: unknown recovery kind")
	}
	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []data_interface.RecoveryCandidate
	for rows.Next() {
		var c data_interface.RecoveryCandidate
		if err := rows.Scan(&c.DeliveryID, &c.RowVersion); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// LoadRecoverySnapshot 在只读事务中读取任务、当前尝试状态、当前 Outbox 与数据库时间。
func (s *RecoveryStore) LoadRecoverySnapshot(ctx context.Context, deliveryID uint64) (data_interface.RecoverySnapshot, error) {
	var snap data_interface.RecoverySnapshot
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return snap, err
	}
	defer func() { _ = tx.Rollback() }()
	if snap.Now, err = txNow(ctx, tx); err != nil {
		return snap, err
	}
	if snap.Task, err = loadTask(ctx, tx, deliveryID); err != nil {
		return snap, err
	}
	if id := snap.Task.CurrentAttemptID; id != nil {
		if snap.AttemptFinished, err = attemptFinished(ctx, tx, *id); err != nil {
			return snap, err
		}
	}
	snap.Outbox, err = loadOutboxFacts(ctx, tx, snap.Task, false)
	return snap, err
}

func loadOutboxFacts(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, lock bool) (domain.OutboxFacts, error) {
	q := `SELECT outbox_id, publish_status, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'),
		DATE_FORMAT(last_published_at, '%Y-%m-%d %H:%i:%s.%f'), repair_count, DATE_FORMAT(next_repair_at, '%Y-%m-%d %H:%i:%s.%f')
		FROM outbox_messages WHERE delivery_id = ? AND round_no = ? AND schedule_version = ?`
	if lock {
		q += ` FOR UPDATE`
	}
	var f domain.OutboxFacts
	var created string
	var published, nextRepair sql.NullString
	err := tx.QueryRowContext(ctx, q, task.DeliveryID, task.RoundNo, task.ScheduleVersion).
		Scan(&f.OutboxID, &f.Status, &created, &published, &f.RepairCount, &nextRepair)
	if errors.Is(err, sql.ErrNoRows) {
		return f, nil
	}
	if err != nil {
		return f, err
	}
	f.Exists = true
	if f.CreatedAt, err = parseUTCWall(created); err != nil {
		return f, err
	}
	if published.Valid {
		t, err := parseUTCWall(published.String)
		if err != nil {
			return f, err
		}
		f.LastPublishedAt = &t
	}
	if nextRepair.Valid {
		t, err := parseUTCWall(nextRepair.String)
		if err != nil {
			return f, err
		}
		f.NextRepairAt = &t
	}
	return f, nil
}

// RecoverTask 在一个事务内锁定任务并重检；尝试结束、所有权清空、版本推进、Outbox 与历史同事务提交。
func (s *RecoveryStore) RecoverTask(ctx context.Context, in data_interface.RecoverInput) (data_interface.RecoveryOutcome, error) {
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return recoverTx(ctx, tx, in)
	})
	switch {
	case errors.Is(err, errAlready):
		return data_interface.RecoveryAlreadyApplied, nil
	case errors.Is(err, errStale), errors.Is(err, data_interface.ErrOwnershipLost):
		return data_interface.RecoveryStale, nil
	case err != nil:
		return "", err
	}
	return data_interface.RecoveryApplied, nil
}

func recoverTx(ctx context.Context, tx *sql.Tx, in data_interface.RecoverInput) error {
	now, err := txNow(ctx, tx)
	if err != nil {
		return err
	}
	task, err := lockTask(ctx, tx, in.DeliveryID)
	if err != nil {
		return err
	}
	if in.RepairKey != "" {
		if done, err := keyExists(ctx, tx, task.DeliveryID, in.RepairKey); err != nil {
			return err
		} else if done {
			return errAlready
		}
	}
	if task.RoundNo != in.RoundNo || task.ScheduleVersion != in.ScheduleVersion || task.RowVersion != in.ExpectedRowVersion {
		return errStale
	}
	switch in.Action {
	case domain.RecoveryExpire:
		return expireTx(ctx, tx, task, in, now)
	case domain.RecoveryClose:
		return closeTx(ctx, tx, task, in, now)
	case domain.RecoveryRecheck:
		return recheckTx(ctx, tx, task, in, now)
	case domain.RecoveryRepair:
		return repairTx(ctx, tx, task, in, now)
	}
	return errors.New("mysql: unsupported recovery action")
}

func expireTx(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, in data_interface.RecoverInput, now time.Time) error {
	if task.Status != enums.TaskProcessing || task.CurrentAttemptID == nil || *task.CurrentAttemptID != in.AttemptID ||
		task.LeaseExpiresAt == nil || now.Before(*task.LeaseExpiresAt) || in.Plan == nil {
		return errStale
	}
	// 结果事务实际已提交则保留已有结果（RCV-05）。
	if finished, err := attemptFinished(ctx, tx, in.AttemptID); err != nil {
		return err
	} else if finished {
		return errStale
	}
	if err := domain.CheckTaskTimes(timesFromPlan(*in.Plan)); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE delivery_attempts SET status = 'FINISHED', send_state = 'UNCERTAIN', outcome = 'UNCERTAIN',
			result_certainty = 'UNCERTAIN', error_code = 'LEASE_EXPIRED', error_reason = ?, retryable = TRUE,
			finished_at = ?, duration_ms = NULL, attempt_duration_ms = NULL, timing_incomplete = TRUE
		WHERE attempt_id = ? AND delivery_id = ? AND status = 'PROCESSING'`,
		clipReason("领取过期，结果不确定"), utcWall(now), in.AttemptID, task.DeliveryID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errStale
	}
	own := domain.Ownership{DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
		AttemptID: in.AttemptID, LeaseToken: task.LeaseToken}
	return applyRecovery(ctx, tx, task, *in.Plan, &own, enums.PhaseLeaseExpired, in, now)
}

func closeTx(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, in data_interface.RecoverInput, now time.Time) error {
	if task.CurrentAttemptID != nil || task.LeaseToken != "" || now.Before(task.RoundDeadlineAt) || in.Plan == nil {
		return errStale // 仍有有效在途领取时不能强制结案
	}
	switch task.Status {
	case enums.TaskPending, enums.TaskRetryWait, enums.TaskBlocked:
	default:
		return errStale
	}
	return applyRecovery(ctx, tx, task, *in.Plan, nil, enums.PhaseRoundClosed, in, now)
}

func recheckTx(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, in data_interface.RecoverInput, now time.Time) error {
	if task.Status != enums.TaskBlocked {
		return errStale
	}
	if in.Plan == nil {
		if in.NextCheckAt == nil {
			return errStale
		}
		res, err := tx.ExecContext(ctx, `UPDATE delivery_tasks SET next_check_at = ?, row_version = row_version + 1
			WHERE delivery_id = ? AND status = 'BLOCKED' AND row_version = ?`, utcWall(*in.NextCheckAt), task.DeliveryID, task.RowVersion)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return errStale
		}
		return nil
	}
	phase := enums.PhaseUnblocked
	if in.Plan.Status == enums.TaskFailed || in.Plan.Status == enums.TaskUnknown {
		phase = enums.PhaseRoundClosed
	}
	return applyRecovery(ctx, tx, task, *in.Plan, nil, phase, in, now)
}

// applyRecovery 应用计划、写带修复键的历史、作废旧指令并在需要时建立新版本 Outbox。
func applyRecovery(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, plan domain.Plan, own *domain.Ownership, phase enums.HistoryPhase, in data_interface.RecoverInput, now time.Time) error {
	if err := domain.CheckTaskTimes(timesFromPlan(plan)); err != nil {
		return err
	}
	if err := applyTaskPlan(ctx, tx, task, plan, own, now); err != nil {
		return err
	}
	from := task.Status
	oldOutbox := task.OutboxID
	task.ScheduleVersion = nextVersion(task.ScheduleVersion, plan.BumpSchedule)
	task.Status = plan.Status
	var attemptID uint64
	if own != nil {
		attemptID = own.AttemptID
	}
	if err := insertKeyedHistory(ctx, tx, task, phase, from, plan.Status, plan.Reason, attemptID, oldOutbox, in.RepairKey, in.Actor, now); err != nil {
		return err
	}
	if extra := planPhase(plan); extra != "" && extra != phase {
		if err := insertHistory(ctx, tx, task, extra, from, plan.Status, plan.Reason, task.ScheduleVersion, attemptID, oldOutbox, nil, now); err != nil {
			return err
		}
	}
	if plan.BumpSchedule {
		if err := cancelOutbox(ctx, tx, oldOutbox, now); err != nil {
			return err
		}
		if needsOutbox(plan.Status) {
			if _, err := insertOutbox(ctx, tx, task, plan, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func repairTx(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, in data_interface.RecoverInput, now time.Time) error {
	if (task.Status != enums.TaskPending && task.Status != enums.TaskRetryWait) || task.CurrentAttemptID != nil ||
		task.ScheduledAt == nil || now.Before(*task.ScheduledAt) || !now.Before(task.RoundDeadlineAt) {
		return errStale
	}
	ob, err := loadOutboxFacts(ctx, tx, task, true)
	if err != nil {
		return err
	}
	switch in.Repair {
	case domain.RepairInsert:
		switch {
		case !ob.Exists:
			id, err := insertOutbox(ctx, tx, task, domain.Plan{ScheduledAt: task.ScheduledAt}, now)
			if isUnique(err) {
				return errAlready // 并发插入命中唯一键：已有记录
			}
			if err != nil {
				return err
			}
			ob.OutboxID = id
		case ob.Status == "CANCELLED":
			// 当前版本指令被误作废：恢复其发布资格，不另造消息标识。
			if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET publish_status = 'PENDING', next_publish_at = ?,
				lease_token = NULL, lease_expires_at = NULL, publish_revision = publish_revision + 1 WHERE outbox_id = ?`,
				utcWall(now), ob.OutboxID); err != nil {
				return err
			}
		default:
			return errAlready
		}
	case domain.RepairRepublish:
		if !ob.Exists || ob.Status != "PUBLISHED" || ob.RepairCount != in.RepairCount ||
			(ob.NextRepairAt != nil && now.Before(*ob.NextRepairAt)) || (in.MaxRepairs > 0 && ob.RepairCount >= in.MaxRepairs) {
			return errStale
		}
		// 同一 Outbox 与 message_id 重新进入发布；冷却与计数持久化，重启不清零（RCV-03）。
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET publish_status = 'PENDING', next_publish_at = ?,
			lease_token = NULL, lease_expires_at = NULL, publish_revision = publish_revision + 1,
			repair_count = repair_count + 1, last_repair_at = ?, next_repair_at = ?
			WHERE outbox_id = ? AND repair_count = ?`,
			utcWall(now), utcWall(now), utcWall(now.Add(in.Cooldown)), ob.OutboxID, in.RepairCount); err != nil {
			return err
		}
	default:
		return errors.New("mysql: unknown repair kind")
	}
	return insertKeyedHistory(ctx, tx, task, enums.PhaseDispatchRepaired, task.Status, task.Status, in.Reason, 0, ob.OutboxID, in.RepairKey, in.Actor, now)
}

// RecordDLQ 记录死信处置；同一死信同一结论只记录一次。
func (s *RecoveryStore) RecordDLQ(ctx context.Context, in data_interface.DLQRecord) (data_interface.RecoveryOutcome, error) {
	key := "dlq:" + in.MessageID + ":" + string(in.Action)
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now, err := txNow(ctx, tx)
		if err != nil {
			return err
		}
		task, err := lockTask(ctx, tx, in.DeliveryID)
		if err != nil {
			return err
		}
		if done, err := keyExists(ctx, tx, task.DeliveryID, key); err != nil {
			return err
		} else if done {
			return errAlready
		}
		return insertKeyedHistory(ctx, tx, task, enums.PhaseDLQDisposed, task.Status, task.Status, in.Reason, 0, task.OutboxID, key, in.Actor, now)
	})
	if errors.Is(err, errAlready) {
		return data_interface.RecoveryAlreadyApplied, nil
	}
	if err != nil {
		return "", err
	}
	return data_interface.RecoveryApplied, nil
}

func keyWriteID(deliveryID uint64, key string) (string, error) {
	return domain.WriteID(deliveryID, enums.PhaseDispatchRepaired, 0, "key:"+key, 0, 0)
}

func keyExists(ctx context.Context, tx *sql.Tx, deliveryID uint64, key string) (bool, error) {
	id, err := keyWriteID(deliveryID, key)
	if err != nil {
		return false, err
	}
	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM delivery_history WHERE write_id = ?`, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// insertKeyedHistory 以修复键派生稳定 write_id；同一修复重放命中唯一键而不重复。
func insertKeyedHistory(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, phase enums.HistoryPhase, from, to enums.TaskStatus, reason string, attemptID, outboxID uint64, key, actor string, now time.Time) error {
	if key == "" {
		return insertHistory(ctx, tx, task, phase, from, to, reason, task.ScheduleVersion, attemptID, outboxID, nil, now)
	}
	seq, err := NextHistorySeq(ctx, tx, task.DeliveryID)
	if err != nil {
		return err
	}
	writeID, err := keyWriteID(task.DeliveryID, key)
	if err != nil {
		return err
	}
	if actor == "" {
		actor = "recovery"
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO delivery_history (
			delivery_id, history_seq, write_id, phase, from_status, to_status, reason,
			round_no, schedule_version, attempt_id, outbox_id, evidence, occurred_at, actor)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,?,?)`,
		task.DeliveryID, seq, writeID, string(phase), string(from), string(to), clipReason(reason),
		task.RoundNo, task.ScheduleVersion, nullU64(attemptID), nullU64(outboxID), utcWall(now), actor)
	if isUnique(err) {
		return errAlready
	}
	return err
}

// lateKey 由原 attempt_id、原因与已脱敏证据的 hash 组成，不使用当前时间（RCV-E01）。
func lateKey(in data_interface.LateEvidenceInput) string {
	sum := sha256.Sum256(append([]byte(strconv.FormatUint(in.AttemptID, 10)+"\n"+in.Reason+"\n"), in.Evidence...))
	return "late:" + hex.EncodeToString(sum[:])
}
