package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// DeliveryStore 实现领取、调用记录、结果计划和执行前等待。网络调用不得放进这些事务。
type DeliveryStore struct {
	db   *sql.DB
	exec *Executor
}

func NewDeliveryStore(db *sql.DB, exec *Executor) *DeliveryStore {
	return &DeliveryStore{db: db, exec: exec}
}

func (s *DeliveryStore) LoadTask(ctx context.Context, deliveryID uint64) (data_interface.TaskRecord, error) {
	var out data_interface.TaskRecord
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rec, err := loadTask(ctx, tx, deliveryID)
		out = rec
		return err
	})
	return out, mapDelivery(err)
}

func (s *DeliveryStore) ReadDisposition(ctx context.Context, deliveryID, attemptID uint64) (data_interface.TaskRecord, error) {
	rec, err := s.LoadTask(ctx, deliveryID)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	if attemptID != 0 {
		for _, item := range rec.Attempts {
			if item.AttemptID == attemptID {
				return rec, nil
			}
		}
		return data_interface.TaskRecord{}, data_interface.ErrUnconfirmed
	}
	return rec, nil
}

func (s *DeliveryStore) StartAttempt(ctx context.Context, in data_interface.StartAttemptInput) (data_interface.StartAttemptResult, error) {
	var out data_interface.StartAttemptResult
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rec, err := startTx(ctx, tx, in)
		out = rec
		return err
	})
	return out, mapDelivery(err)
}

func (s *DeliveryStore) RecordInvocation(ctx context.Context, in data_interface.RecordInvocationInput) error {
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return invocationTx(ctx, tx, in)
	})
	return mapDelivery(err)
}

func (s *DeliveryStore) FinishAttemptAndPlan(ctx context.Context, in data_interface.FinishInput) (data_interface.TaskRecord, error) {
	var out data_interface.TaskRecord
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rec, err := finishTx(ctx, tx, in)
		out = rec
		return err
	})
	return out, mapDelivery(err)
}

func (s *DeliveryStore) WaitOrBlock(ctx context.Context, in data_interface.WaitOrBlockInput) (data_interface.TaskRecord, error) {
	var out data_interface.TaskRecord
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rec, err := waitTx(ctx, tx, in)
		out = rec
		return err
	})
	return out, mapDelivery(err)
}

func (s *DeliveryStore) ResumeBlocked(ctx context.Context, in data_interface.ResumeInput) (data_interface.TaskRecord, error) {
	var out data_interface.TaskRecord
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rec, err := resumeTx(ctx, tx, in)
		out = rec
		return err
	})
	return out, mapDelivery(err)
}

func (s *DeliveryStore) AppendLateEvidence(ctx context.Context, in data_interface.LateEvidenceInput) error {
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return lateTx(ctx, tx, in)
	})
	return mapDelivery(err)
}

func startTx(ctx context.Context, tx *sql.Tx, in data_interface.StartAttemptInput) (data_interface.StartAttemptResult, error) {
	now, err := txNow(ctx, tx)
	if err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	task, err := lockTask(ctx, tx, in.DeliveryID)
	if err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	if task.EventID != in.EventID || task.RoundNo != in.RoundNo || task.ScheduleVersion != in.ScheduleVersion || task.RowVersion != in.ExpectedRowVersion || task.OutboxID != in.OutboxID {
		return data_interface.StartAttemptResult{}, data_interface.ErrOwnershipLost
	}
	if task.Status != enums.TaskPending && task.Status != enums.TaskRetryWait {
		return data_interface.StartAttemptResult{}, data_interface.ErrOwnershipLost
	}
	if task.CurrentAttemptID != nil || task.LeaseToken != "" {
		return data_interface.StartAttemptResult{}, data_interface.ErrOwnershipLost
	}
	if task.ScheduledAt == nil || now.Before(*task.ScheduledAt) || !now.Before(task.RoundDeadlineAt) || task.RoundAttemptCount >= task.MaxAttempts {
		return data_interface.StartAttemptResult{}, domainErr(constant.CodeStateConflict, "任务当前不能领取")
	}
	var channelVersion uint32
	if err = tx.QueryRowContext(ctx, `SELECT row_version FROM channels WHERE channel_id = ?`, task.ChannelID).Scan(&channelVersion); err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	if channelVersion != in.ExpectedChannelVersion || task.ReleaseID != in.ExpectedReleaseID {
		return data_interface.StartAttemptResult{}, data_interface.ErrPreconditionChanged
	}
	if !in.SafetyNotAfter.IsZero() && !now.Before(in.SafetyNotAfter.UTC()) {
		return data_interface.StartAttemptResult{}, data_interface.ErrPreconditionChanged
	}
	if in.UseRiskAuthorization && (task.RiskOperationID == "" || task.RiskAttemptID != nil || task.RoundAttemptCount != 0) {
		return data_interface.StartAttemptResult{}, domainErr(constant.CodeStateConflict, "风险授权不可用")
	}
	if task.RoundAttemptCount == math.MaxUint32 || task.TotalAttemptCount == math.MaxUint32 || task.RowVersion == math.MaxUint32 {
		return data_interface.StartAttemptResult{}, domainErr(constant.CodeInternal, "次数或版本溢出")
	}
	token, err := newToken()
	if err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	expires := now.Add(in.LeaseTimeout).UTC().Truncate(time.Microsecond)
	attemptNo := task.RoundAttemptCount + 1
	res, err := tx.ExecContext(ctx, `
		INSERT INTO delivery_attempts (
			delivery_id, round_no, attempt_no, outbox_id, trace_id, worker_id, deployment_version,
			status, send_state, started_at)
		VALUES (?,?,?,?,?,?,?, 'PROCESSING', 'UNCERTAIN', ?)`,
		task.DeliveryID, task.RoundNo, attemptNo, in.OutboxID, emptyNil(in.TraceID), emptyNil(in.WorkerID), emptyNil(in.DeploymentVersion), utcWall(now))
	if err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	attemptID, err := insertedID(res)
	if err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	risk := any(nil)
	if in.UseRiskAuthorization {
		risk = attemptID
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE delivery_tasks SET
			status = 'PROCESSING', round_attempt_count = round_attempt_count + 1, total_attempt_count = total_attempt_count + 1,
			scheduled_at = NULL, next_retry_at = NULL, next_check_at = NULL,
			current_attempt_id = ?, lease_token = ?, lease_expires_at = ?,
			row_version = row_version + 1, risk_authorized_attempt_id = COALESCE(?, risk_authorized_attempt_id)
		WHERE delivery_id = ? AND status IN ('PENDING', 'RETRY_WAIT') AND round_no = ? AND schedule_version = ?
			AND row_version = ? AND current_attempt_id IS NULL AND lease_token IS NULL`,
		attemptID, token, utcWall(expires), risk, task.DeliveryID, task.RoundNo, task.ScheduleVersion, task.RowVersion)
	if err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return data_interface.StartAttemptResult{}, data_interface.ErrOwnershipLost
	}
	if err = insertHistory(ctx, tx, task, enums.PhaseAttemptStarted, task.Status, enums.TaskProcessing, "尝试已领取", task.ScheduleVersion, attemptID, in.OutboxID, nil, now); err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	return data_interface.StartAttemptResult{
		Ownership: domain.Ownership{
			DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
			AttemptID: attemptID, LeaseToken: token, LeaseExpiresAt: expires,
		},
		AttemptNo: attemptNo, StartedAt: now, RowVersion: task.RowVersion + 1,
		RoundAttemptCount: task.RoundAttemptCount + 1, TotalAttemptCount: task.TotalAttemptCount + 1,
		ChannelVersion: channelVersion,
	}, nil
}

func invocationTx(ctx context.Context, tx *sql.Tx, in data_interface.RecordInvocationInput) error {
	now, err := txNow(ctx, tx)
	if err != nil {
		return err
	}
	task, err := lockTask(ctx, tx, in.Ownership.DeliveryID)
	if err != nil {
		return err
	}
	if !sameOwner(task, in.Ownership) || task.LeaseExpiresAt == nil || !now.Before(*task.LeaseExpiresAt) {
		return data_interface.ErrOwnershipLost
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE delivery_attempts SET
			send_state = 'HTTP_INVOKED', request_started_at = ?, method = ?, target_path = ?,
			request_summary = ?, request_summary_truncated = ?, request_size_bytes = ?
		WHERE attempt_id = ? AND delivery_id = ? AND status = 'PROCESSING' AND send_state IN ('UNCERTAIN', 'HTTP_INVOKED')`,
		utcWall(in.RequestStartedAt.UTC()), emptyNil(in.Method), emptyNil(in.TargetPath), emptyNil(in.RequestSummary), in.RequestTruncated, in.RequestSize,
		in.Ownership.AttemptID, in.Ownership.DeliveryID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		var state string
		err = tx.QueryRowContext(ctx, `SELECT send_state FROM delivery_attempts WHERE attempt_id = ?`, in.Ownership.AttemptID).Scan(&state)
		if err == nil && state == string(enums.SendHTTPInvoked) {
			return nil
		}
		return data_interface.ErrOwnershipLost
	}
	return nil
}

func finishTx(ctx context.Context, tx *sql.Tx, in data_interface.FinishInput) (data_interface.TaskRecord, error) {
	now, err := txNow(ctx, tx)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	task, err := lockTask(ctx, tx, in.Ownership.DeliveryID)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	finished, err := attemptFinished(ctx, tx, in.Ownership.AttemptID)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	if finished {
		return loadTask(ctx, tx, task.DeliveryID)
	}
	if !sameOwner(task, in.Ownership) || task.LeaseExpiresAt == nil || !now.Before(*task.LeaseExpiresAt) {
		return data_interface.TaskRecord{}, data_interface.ErrOwnershipLost
	}
	if err = domain.CheckTaskTimes(timesFromPlan(in.Plan)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	if err = updateAttemptResult(ctx, tx, in, now); err != nil {
		return data_interface.TaskRecord{}, err
	}
	if err = applyTaskPlan(ctx, tx, task, in.Plan, &in.Ownership, now); err != nil {
		return data_interface.TaskRecord{}, err
	}
	task.ScheduleVersion = nextVersion(task.ScheduleVersion, in.Plan.BumpSchedule)
	task.Status = in.Plan.Status
	if err = insertHistory(ctx, tx, task, enums.PhaseAttemptFinished, enums.TaskProcessing, in.Plan.Status, in.Plan.Reason, task.ScheduleVersion, in.Ownership.AttemptID, task.OutboxID, in.EvidenceScope, now); err != nil {
		return data_interface.TaskRecord{}, err
	}
	if extra := planPhase(in.Plan); extra != "" {
		if err = insertHistory(ctx, tx, task, extra, enums.TaskProcessing, in.Plan.Status, in.Plan.Reason, task.ScheduleVersion, in.Ownership.AttemptID, task.OutboxID, nil, now); err != nil {
			return data_interface.TaskRecord{}, err
		}
	}
	if in.Plan.BumpSchedule {
		if err = cancelOutbox(ctx, tx, task.OutboxID, now); err != nil {
			return data_interface.TaskRecord{}, err
		}
		if needsOutbox(in.Plan.Status) {
			if _, err = insertOutbox(ctx, tx, task, in.Plan, now); err != nil {
				return data_interface.TaskRecord{}, err
			}
		}
	}
	return loadTask(ctx, tx, task.DeliveryID)
}

func waitTx(ctx context.Context, tx *sql.Tx, in data_interface.WaitOrBlockInput) (data_interface.TaskRecord, error) {
	now, err := txNow(ctx, tx)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	task, err := lockTask(ctx, tx, in.DeliveryID)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	if task.EventID != in.EventID || task.RoundNo != in.RoundNo || task.ScheduleVersion != in.ScheduleVersion || task.RowVersion != in.ExpectedRowVersion || task.OutboxID != in.OutboxID {
		return data_interface.TaskRecord{}, data_interface.ErrOwnershipLost
	}
	if task.Status != enums.TaskPending && task.Status != enums.TaskRetryWait || task.CurrentAttemptID != nil {
		return data_interface.TaskRecord{}, data_interface.ErrOwnershipLost
	}
	if err = domain.CheckTaskTimes(timesFromPlan(in.Plan)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	if err = applyTaskPlan(ctx, tx, task, in.Plan, nil, now); err != nil {
		return data_interface.TaskRecord{}, err
	}
	task.ScheduleVersion = nextVersion(task.ScheduleVersion, in.Plan.BumpSchedule)
	phase := planPhase(in.Plan)
	if phase == "" {
		phase = enums.PhaseRoundClosed
	}
	if err = insertHistory(ctx, tx, task, phase, task.Status, in.Plan.Status, in.Plan.Reason, task.ScheduleVersion, 0, task.OutboxID, nil, now); err != nil {
		return data_interface.TaskRecord{}, err
	}
	if in.Plan.BumpSchedule {
		if err = cancelOutbox(ctx, tx, task.OutboxID, now); err != nil {
			return data_interface.TaskRecord{}, err
		}
		if needsOutbox(in.Plan.Status) {
			if _, err = insertOutbox(ctx, tx, task, in.Plan, now); err != nil {
				return data_interface.TaskRecord{}, err
			}
		}
	}
	return loadTask(ctx, tx, task.DeliveryID)
}

func resumeTx(ctx context.Context, tx *sql.Tx, in data_interface.ResumeInput) (data_interface.TaskRecord, error) {
	now, err := txNow(ctx, tx)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	task, err := lockTask(ctx, tx, in.DeliveryID)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	if task.Status != enums.TaskBlocked || task.RoundNo != in.RoundNo || task.ScheduleVersion != in.ScheduleVersion || task.RowVersion != in.ExpectedRowVersion {
		return data_interface.TaskRecord{}, domainErr(constant.CodeStateConflict, "阻塞恢复条件不匹配")
	}
	if err = domain.CheckTaskTimes(timesFromPlan(in.Plan)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	if err = applyTaskPlan(ctx, tx, task, in.Plan, nil, now); err != nil {
		return data_interface.TaskRecord{}, err
	}
	task.ScheduleVersion = nextVersion(task.ScheduleVersion, in.Plan.BumpSchedule)
	phase := enums.PhaseUnblocked
	if in.Plan.Status == enums.TaskFailed || in.Plan.Status == enums.TaskUnknown {
		phase = enums.PhaseRoundClosed
	}
	if err = insertHistory(ctx, tx, task, phase, enums.TaskBlocked, in.Plan.Status, in.Plan.Reason, task.ScheduleVersion, 0, 0, nil, now); err != nil {
		return data_interface.TaskRecord{}, err
	}
	if needsOutbox(in.Plan.Status) {
		if _, err = insertOutbox(ctx, tx, task, in.Plan, now); err != nil {
			return data_interface.TaskRecord{}, err
		}
	}
	return loadTask(ctx, tx, task.DeliveryID)
}

func lateTx(ctx context.Context, tx *sql.Tx, in data_interface.LateEvidenceInput) error {
	now, err := txNow(ctx, tx)
	if err != nil {
		return err
	}
	task, err := lockTask(ctx, tx, in.DeliveryID)
	if err != nil {
		return err
	}
	// 同内容重复追加只保留一次；不推进任务。
	key := lateKey(in)
	if done, err := keyExists(ctx, tx, task.DeliveryID, key); err != nil || done {
		return err
	}
	seq, err := NextHistorySeq(ctx, tx, task.DeliveryID)
	if err != nil {
		return err
	}
	writeID, err := keyWriteID(task.DeliveryID, key)
	if err != nil {
		return err
	}
	var raw any
	if len(in.Evidence) > 0 && json.Valid(in.Evidence) {
		raw = string(in.Evidence)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO delivery_history (
			delivery_id, history_seq, write_id, phase, from_status, to_status, reason,
			round_no, schedule_version, attempt_id, outbox_id, evidence, occurred_at, actor)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		task.DeliveryID, seq, writeID, string(enums.PhaseLateResponse), string(task.Status), string(task.Status), clipReason(in.Reason),
		task.RoundNo, task.ScheduleVersion, nullU64(in.AttemptID), nullU64(task.OutboxID), raw, utcWall(now), "worker")
	if isUnique(err) {
		return nil
	}
	return err
}

func applyTaskPlan(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, plan domain.Plan, own *domain.Ownership, now time.Time) error {
	if task.RowVersion == math.MaxUint32 || (plan.BumpSchedule && task.ScheduleVersion == math.MaxUint32) {
		return domainErr(constant.CodeInternal, "版本溢出")
	}
	schedule := task.ScheduleVersion
	if plan.BumpSchedule {
		schedule++
	}
	row := task.RowVersion + 1
	var last any
	if own != nil {
		last = own.AttemptID
	} else if task.LastAttemptID != nil {
		last = *task.LastAttemptID
	}
	errorCode := plan.ErrorCode
	errorReason := plan.Reason
	if plan.ClearLastError {
		errorCode, errorReason = "", ""
	}
	blockedSince := task.BlockedSince
	blockedReason := ""
	resumeStatus := ""
	var resumeNotBefore *time.Time
	if plan.Status == enums.TaskBlocked {
		blockedReason = clipReason(plan.Reason)
		resumeStatus = plan.ResumeStatus
		resumeNotBefore = plan.ResumeNotBefore
		if blockedSince == nil {
			blockedSince = &now
		}
	}
	unresolved := task.Unresolved
	if plan.Status == enums.TaskSucceeded || plan.Unresolved != nil {
		unresolved = toIfaceUnresolved(plan.Unresolved)
	}
	raw, err := marshalUnresolved(unresolved)
	if err != nil {
		return err
	}
	var res sql.Result
	if own != nil {
		res, err = tx.ExecContext(ctx, `
			UPDATE delivery_tasks SET
				status = ?, result_certainty = ?, last_result_certainty = ?,
				scheduled_at = ?, next_retry_at = ?, next_check_at = ?,
				current_attempt_id = NULL, lease_token = NULL, lease_expires_at = NULL,
				last_attempt_id = ?, last_error_code = ?, last_error_reason = ?, last_error_truncated = ?,
				blocked_reason = ?, blocked_since = ?, resume_status = ?, resume_not_before = ?,
				round_finished_at = ?, unresolved_evidence = ?, schedule_version = ?, row_version = ?
			WHERE delivery_id = ? AND round_no = ? AND schedule_version = ? AND row_version = ? AND status = ?
				AND current_attempt_id = ? AND lease_token = ?`,
			string(plan.Status), string(plan.ResultCertainty), nullCertainty(plan.LastCertainty),
			nullTime(plan.ScheduledAt), nullTime(plan.NextRetryAt), nullTime(plan.NextCheckAt),
			last, emptyNil(errorCode), emptyNil(errorReason), false,
			emptyNil(blockedReason), nullTime(blockedSince), emptyNil(resumeStatus), nullTime(resumeNotBefore),
			nullTime(plan.RoundFinishedAt), raw, schedule, row,
			task.DeliveryID, task.RoundNo, task.ScheduleVersion, task.RowVersion, string(task.Status),
			own.AttemptID, own.LeaseToken)
	} else {
		res, err = tx.ExecContext(ctx, `
			UPDATE delivery_tasks SET
				status = ?, result_certainty = ?, last_result_certainty = ?,
				scheduled_at = ?, next_retry_at = ?, next_check_at = ?,
				current_attempt_id = NULL, lease_token = NULL, lease_expires_at = NULL,
				last_attempt_id = ?, last_error_code = ?, last_error_reason = ?, last_error_truncated = ?,
				blocked_reason = ?, blocked_since = ?, resume_status = ?, resume_not_before = ?,
				round_finished_at = ?, unresolved_evidence = ?, schedule_version = ?, row_version = ?
			WHERE delivery_id = ? AND round_no = ? AND schedule_version = ? AND row_version = ? AND status = ?
				AND current_attempt_id IS NULL AND lease_token IS NULL`,
			string(plan.Status), string(plan.ResultCertainty), nullCertainty(plan.LastCertainty),
			nullTime(plan.ScheduledAt), nullTime(plan.NextRetryAt), nullTime(plan.NextCheckAt),
			last, emptyNil(errorCode), emptyNil(errorReason), false,
			emptyNil(blockedReason), nullTime(blockedSince), emptyNil(resumeStatus), nullTime(resumeNotBefore),
			nullTime(plan.RoundFinishedAt), raw, schedule, row,
			task.DeliveryID, task.RoundNo, task.ScheduleVersion, task.RowVersion, string(task.Status))
	}
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return data_interface.ErrOwnershipLost
	}
	return nil
}

func updateAttemptResult(ctx context.Context, tx *sql.Tx, in data_interface.FinishInput, now time.Time) error {
	var evidence any
	if len(in.EvidenceScope) > 0 && json.Valid(in.EvidenceScope) {
		evidence = string(in.EvidenceScope)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE delivery_attempts SET
			status = 'FINISHED', send_state = ?, outcome = ?, result_certainty = ?, error_code = ?, error_reason = ?,
			error_summary_truncated = ?, request_summary = ?, response_summary = ?,
			request_summary_truncated = ?, response_summary_truncated = ?, response_truncated = ?,
			http_status = ?, provider_request_id = ?, retryable = ?, evidence_scope = ?,
			finished_at = ?, duration_ms = ?, attempt_duration_ms = ?, timing_incomplete = ?
		WHERE attempt_id = ? AND delivery_id = ? AND status = 'PROCESSING'`,
		string(in.SendState), string(in.Outcome), string(in.ResultCertainty), emptyNil(in.ErrorCode), emptyNil(in.ErrorReason),
		in.ErrorTruncated, emptyNil(in.RequestSummary), emptyNil(in.ResponseSummary),
		in.RequestTruncated, in.ResponseTruncated, in.ResponseTruncated,
		nullInt(in.HTTPStatus), emptyNil(in.ProviderRequestID), in.Retryable, evidence,
		utcWall(now), millis(in.HTTPDuration), millis(in.AttemptDuration), in.TimingIncomplete,
		in.Ownership.AttemptID, in.Ownership.DeliveryID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return data_interface.ErrOwnershipLost
	}
	return nil
}

func lockTask(ctx context.Context, tx *sql.Tx, deliveryID uint64) (data_interface.TaskRecord, error) {
	if err := LockTasks(ctx, tx, []uint64{deliveryID}); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return data_interface.TaskRecord{}, domainErr(constant.CodeNotFound, "任务不存在")
		}
		return data_interface.TaskRecord{}, err
	}
	return loadTask(ctx, tx, deliveryID)
}

func loadTask(ctx context.Context, tx *sql.Tx, deliveryID uint64) (data_interface.TaskRecord, error) {
	rec, err := scanTask(tx.QueryRowContext(ctx, taskSelect+` WHERE t.delivery_id = ?`, deliveryID))
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	rec.Attempts, err = loadAttempts(ctx, tx, deliveryID)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	rec.History, err = loadHistory(ctx, tx, deliveryID)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	return rec, nil
}

// taskSelect 是任务快照的统一列与连接；scanTask 与之一一对应。
const taskSelect = `
		SELECT t.delivery_id, t.event_id, e.caller_id, t.route_target_id, t.channel_id, c.row_version, e.routing_release_id,
			t.provider_id, t.provider_account_id, t.operation, t.adapter_type, t.adapter_version, t.mapping_id, t.mapping_version,
			t.fixed_target, t.payload_canonical, t.round_policy, t.idempotency_key, t.status, t.result_certainty, t.last_result_certainty,
			t.round_no, t.schedule_version, t.row_version, t.history_seq, t.round_attempt_count, t.total_attempt_count, t.max_attempts,
			t.round_deadline_at, t.scheduled_at, t.next_retry_at, t.next_check_at, t.current_attempt_id, t.last_attempt_id,
			t.lease_token, t.lease_expires_at, t.blocked_reason, t.blocked_since, t.resume_status, t.resume_not_before, t.round_finished_at,
			t.last_error_code, t.last_error_reason, t.last_error_truncated, t.risk_operation_id, t.risk_authorized_attempt_id, t.unresolved_evidence,
			(SELECT o.outbox_id FROM outbox_messages o
				WHERE o.delivery_id = t.delivery_id AND o.round_no = t.round_no AND o.schedule_version = t.schedule_version
					AND o.publish_status <> 'CANCELLED'
				ORDER BY o.outbox_id DESC LIMIT 1)
		FROM delivery_tasks t
		JOIN business_events e ON e.event_id = t.event_id
		JOIN channels c ON c.channel_id = t.channel_id`

func scanTask(row rowScanner) (data_interface.TaskRecord, error) {
	var rec data_interface.TaskRecord
	var fixed, payload, policy, unresolved []byte
	var lastCert, leaseToken, blockedReason, resumeStatus, lastCode, lastReason, riskOp sql.NullString
	var scheduled, nextRetry, nextCheck, leaseExpires, blockedSince, resumeBefore, finished sql.NullTime
	var currentAttempt, lastAttempt, riskAttempt, outbox sql.NullInt64
	var lastTruncated bool
	err := row.Scan(
		&rec.DeliveryID, &rec.EventID, &rec.CallerID, &rec.RouteTargetID, &rec.ChannelID, &rec.ChannelVersion, &rec.ReleaseID,
		&rec.ProviderID, &rec.ProviderAccountID, &rec.Operation, &rec.AdapterType, &rec.AdapterVersion, &rec.MappingID, &rec.MappingVersion,
		&fixed, &payload, &policy, &rec.IdempotencyKey, &rec.Status, &rec.ResultCertainty, &lastCert,
		&rec.RoundNo, &rec.ScheduleVersion, &rec.RowVersion, &rec.HistorySeq, &rec.RoundAttemptCount, &rec.TotalAttemptCount, &rec.MaxAttempts,
		&rec.RoundDeadlineAt, &scheduled, &nextRetry, &nextCheck, &currentAttempt, &lastAttempt,
		&leaseToken, &leaseExpires, &blockedReason, &blockedSince, &resumeStatus, &resumeBefore, &finished,
		&lastCode, &lastReason, &lastTruncated, &riskOp, &riskAttempt, &unresolved, &outbox,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return data_interface.TaskRecord{}, domainErr(constant.CodeNotFound, "任务不存在")
	}
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	rec.FixedTarget = append([]byte(nil), fixed...)
	rec.PayloadCanonical = append([]byte(nil), payload...)
	rec.RoundPolicy = append([]byte(nil), policy...)
	rec.LastCertainty = enums.ResultCertainty(lastCert.String)
	rec.ScheduledAt = timePtr(scheduled)
	rec.NextRetryAt = timePtr(nextRetry)
	rec.NextCheckAt = timePtr(nextCheck)
	rec.LeaseExpiresAt = timePtr(leaseExpires)
	rec.BlockedSince = timePtr(blockedSince)
	rec.ResumeNotBefore = timePtr(resumeBefore)
	rec.RoundFinishedAt = timePtr(finished)
	rec.CurrentAttemptID = u64Ptr(currentAttempt)
	rec.LastAttemptID = u64Ptr(lastAttempt)
	rec.RiskAttemptID = u64Ptr(riskAttempt)
	rec.LeaseToken = leaseToken.String
	rec.BlockedReason = blockedReason.String
	rec.ResumeStatus = resumeStatus.String
	rec.LastErrorCode = lastCode.String
	rec.LastErrorReason = lastReason.String
	rec.LastErrorTruncated = lastTruncated
	rec.RiskOperationID = riskOp.String
	if outbox.Valid {
		rec.OutboxID = uint64(outbox.Int64)
	}
	rec.Unresolved, err = unmarshalUnresolved(string(unresolved))
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	return rec, nil
}

func loadAttempts(ctx context.Context, tx *sql.Tx, deliveryID uint64) ([]data_interface.AttemptRecord, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT attempt_id, delivery_id, round_no, attempt_no, outbox_id, trace_id, worker_id, deployment_version,
			status, send_state, outcome, result_certainty, error_code, error_reason, error_summary_truncated,
			request_summary, response_summary, request_summary_truncated, response_summary_truncated,
			http_status, provider_request_id, retryable, evidence_scope, started_at, request_started_at, finished_at,
			duration_ms, attempt_duration_ms, timing_incomplete
		FROM delivery_attempts WHERE delivery_id = ? ORDER BY attempt_id`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []data_interface.AttemptRecord
	for rows.Next() {
		var item data_interface.AttemptRecord
		var outbox, httpStatus sql.NullInt64
		var trace, worker, deploy, send, outcome, cert, code, reason, req, resp, provider sql.NullString
		var evidence sql.NullString
		var errTrunc, reqTrunc, respTrunc, retryable, timing sql.NullBool
		var started time.Time
		var requestStarted, finished sql.NullTime
		var httpMs, attemptMs sql.NullInt64
		if err = rows.Scan(
			&item.AttemptID, &item.DeliveryID, &item.RoundNo, &item.AttemptNo, &outbox, &trace, &worker, &deploy,
			&item.Status, &send, &outcome, &cert, &code, &reason, &errTrunc,
			&req, &resp, &reqTrunc, &respTrunc, &httpStatus, &provider, &retryable, &evidence, &started, &requestStarted, &finished,
			&httpMs, &attemptMs, &timing,
		); err != nil {
			return nil, err
		}
		if outbox.Valid {
			item.OutboxID = uint64(outbox.Int64)
		}
		item.TraceID, item.WorkerID, item.DeploymentVersion = trace.String, worker.String, deploy.String
		item.SendState = enums.SendState(send.String)
		item.Outcome = enums.Outcome(outcome.String)
		item.ResultCertainty = enums.ResultCertainty(cert.String)
		item.ErrorCode, item.ErrorReason = code.String, reason.String
		item.ErrorTruncated = errTrunc.Bool
		item.RequestSummary, item.ResponseSummary = req.String, resp.String
		item.RequestTruncated, item.ResponseTruncated = reqTrunc.Bool, respTrunc.Bool
		if httpStatus.Valid {
			code := int(httpStatus.Int64)
			item.HTTPStatus = &code
		}
		item.ProviderRequestID = provider.String
		item.Retryable = retryable.Bool
		item.EvidenceScope = []byte(evidence.String)
		item.StartedAt = started.UTC()
		item.RequestStartedAt = timePtr(requestStarted)
		item.FinishedAt = timePtr(finished)
		item.HTTPDuration = durationPtr(httpMs)
		item.AttemptDuration = durationPtr(attemptMs)
		item.TimingIncomplete = timing.Bool
		items = append(items, item)
	}
	return items, rows.Err()
}

func loadHistory(ctx context.Context, tx *sql.Tx, deliveryID uint64) ([]data_interface.HistoryRecord, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT history_seq, write_id, phase, from_status, to_status, reason, round_no, schedule_version,
			attempt_id, outbox_id, evidence, occurred_at
		FROM delivery_history WHERE delivery_id = ? ORDER BY history_seq`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []data_interface.HistoryRecord
	for rows.Next() {
		var item data_interface.HistoryRecord
		var from, to, reason, evidence sql.NullString
		var round, schedule sql.NullInt64
		var attempt, outbox sql.NullInt64
		var occurred sql.NullTime
		if err = rows.Scan(&item.Seq, &item.WriteID, &item.Phase, &from, &to, &reason, &round, &schedule, &attempt, &outbox, &evidence, &occurred); err != nil {
			return nil, err
		}
		item.From = enums.TaskStatus(from.String)
		item.To = enums.TaskStatus(to.String)
		item.Reason = reason.String
		if round.Valid {
			item.RoundNo = uint32(round.Int64)
		}
		if schedule.Valid {
			item.ScheduleVersion = uint32(schedule.Int64)
		}
		if attempt.Valid {
			item.AttemptID = uint64(attempt.Int64)
		}
		if outbox.Valid {
			item.OutboxID = uint64(outbox.Int64)
		}
		item.Evidence = []byte(evidence.String)
		if occurred.Valid {
			item.OccurredAt = occurred.Time.UTC()
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func insertHistory(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, phase enums.HistoryPhase, from, to enums.TaskStatus, reason string, schedule uint32, attemptID, outboxID uint64, evidence []byte, now time.Time) error {
	seq, err := NextHistorySeq(ctx, tx, task.DeliveryID)
	if err != nil {
		return err
	}
	writeID, err := domain.WriteID(task.DeliveryID, phase, attemptID, "", outboxID, uint32(seq))
	if err != nil {
		return err
	}
	var raw any
	if len(evidence) > 0 && json.Valid(evidence) {
		raw = string(evidence)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO delivery_history (
			delivery_id, history_seq, write_id, phase, from_status, to_status, reason,
			round_no, schedule_version, attempt_id, outbox_id, evidence, occurred_at, actor)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		task.DeliveryID, seq, writeID, string(phase), string(from), string(to), clipReason(reason),
		task.RoundNo, schedule, nullU64(attemptID), nullU64(outboxID), raw, utcWall(now), "worker")
	return err
}

func insertOutbox(ctx context.Context, tx *sql.Tx, task data_interface.TaskRecord, plan domain.Plan, now time.Time) (uint64, error) {
	when := now
	if plan.ScheduledAt != nil {
		when = plan.ScheduledAt.UTC()
	}
	kind := "immediate"
	if when.After(now) {
		kind = "delayed"
	}
	placeholder := "tmp-" + itoaU64(task.DeliveryID) + "-" + strconv.FormatUint(uint64(task.ScheduleVersion), 10)
	res, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_messages (
			message_id, delivery_id, round_no, schedule_version, message_type, scheduled_at,
			publish_status, publish_count, next_publish_at, publish_revision, repair_count)
		VALUES (?,?,?,?,?,?, 'PENDING', 0, ?, 1, 0)`,
		placeholder, task.DeliveryID, task.RoundNo, task.ScheduleVersion, kind, utcWall(when), utcWall(when))
	if err != nil {
		return 0, err
	}
	id, err := insertedID(res)
	if err != nil {
		return 0, err
	}
	messageID, err := domain.OutboxMessageID(id)
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE outbox_messages SET message_id = ? WHERE outbox_id = ?`, messageID, id); err != nil {
		return 0, err
	}
	return id, nil
}

func cancelOutbox(ctx context.Context, tx *sql.Tx, outboxID uint64, now time.Time) error {
	if outboxID == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET publish_status = 'CANCELLED' WHERE outbox_id = ?`, outboxID)
	_ = now
	return err
}

func attemptFinished(ctx context.Context, tx *sql.Tx, attemptID uint64) (bool, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM delivery_attempts WHERE attempt_id = ?`, attemptID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, data_interface.ErrOwnershipLost
	}
	if err != nil {
		return false, err
	}
	return status == "FINISHED", nil
}

func sameOwner(task data_interface.TaskRecord, own domain.Ownership) bool {
	return task.DeliveryID == own.DeliveryID && task.RoundNo == own.RoundNo && task.ScheduleVersion == own.ScheduleVersion &&
		task.Status == enums.TaskProcessing && task.CurrentAttemptID != nil && *task.CurrentAttemptID == own.AttemptID &&
		task.LeaseToken == own.LeaseToken
}

func timesFromPlan(plan domain.Plan) domain.TaskTimes {
	times := domain.TaskTimes{
		Status: plan.Status, ScheduledAt: plan.ScheduledAt, NextRetryAt: plan.NextRetryAt, NextCheckAt: plan.NextCheckAt,
		RoundFinishedAt: plan.RoundFinishedAt,
	}
	if plan.Status == enums.TaskBlocked {
		times.BlockedReason = plan.Reason
		if times.BlockedReason == "" {
			times.BlockedReason = "blocked"
		}
	}
	return times
}

func planPhase(plan domain.Plan) enums.HistoryPhase {
	switch plan.Status {
	case enums.TaskRetryWait:
		if plan.Kind == enums.ActionWait {
			return enums.PhaseWaitPlanned
		}
		return enums.PhaseRetryPlanned
	case enums.TaskBlocked:
		return enums.PhaseBlocked
	case enums.TaskFailed, enums.TaskUnknown, enums.TaskSucceeded:
		return ""
	default:
		return ""
	}
}

func needsOutbox(status enums.TaskStatus) bool {
	return status == enums.TaskPending || status == enums.TaskRetryWait
}

func nextVersion(current uint32, bump bool) uint32 {
	if bump {
		return current + 1
	}
	return current
}

func txNow(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var wall string
	if err := tx.QueryRowContext(ctx, `SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%d %H:%i:%s.%f')`).Scan(&wall); err != nil {
		return time.Time{}, err
	}
	return parseUTCWall(wall)
}

func newToken() (string, error) {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", domainErr(constant.CodeInternal, "领取标识生成失败")
	}
	return hex.EncodeToString(buf[:]), nil
}

func nullTime(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return utcWall(t.UTC())
}

func timePtr(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time.UTC().Truncate(time.Microsecond)
	return &t
}

func u64Ptr(v sql.NullInt64) *uint64 {
	if !v.Valid || v.Int64 <= 0 {
		return nil
	}
	n := uint64(v.Int64)
	return &n
}

func nullU64(v uint64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullInt(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

func emptyNil(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullCertainty(v enums.ResultCertainty) any {
	if v == "" {
		return nil
	}
	return string(v)
}

func millis(d *time.Duration) any {
	if d == nil {
		return nil
	}
	ms := d.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return ms
}

func durationPtr(v sql.NullInt64) *time.Duration {
	if !v.Valid {
		return nil
	}
	d := time.Duration(v.Int64) * time.Millisecond
	return &d
}

func clipReason(v string) string {
	out, _ := domain.TruncateUTF8(v, 500)
	return out
}

type unresolvedJSON struct {
	AttemptID string `json:"attempt_id"`
	RoundNo   uint32 `json:"round_no"`
}

func marshalUnresolved(items []data_interface.UnresolvedRef) (any, error) {
	if len(items) == 0 {
		return nil, nil
	}
	body := make([]unresolvedJSON, len(items))
	for i, item := range items {
		text, err := domain.FormatInternalID(item.AttemptID)
		if err != nil {
			return nil, err
		}
		body[i] = unresolvedJSON{AttemptID: text, RoundNo: item.RoundNo}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}

func unmarshalUnresolved(raw string) ([]data_interface.UnresolvedRef, error) {
	if raw == "" || raw == "null" {
		return nil, nil
	}
	var body []unresolvedJSON
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		return nil, domainErr(constant.CodeInternal, "未决证据无法读取")
	}
	out := make([]data_interface.UnresolvedRef, len(body))
	for i, item := range body {
		id, err := domain.ParseInternalID("attempt_id", item.AttemptID)
		if err != nil {
			return nil, err
		}
		out[i] = data_interface.UnresolvedRef{AttemptID: id, RoundNo: item.RoundNo}
	}
	return out, nil
}

func toIfaceUnresolved(items []domain.UnresolvedRef) []data_interface.UnresolvedRef {
	out := make([]data_interface.UnresolvedRef, len(items))
	for i, item := range items {
		out[i] = data_interface.UnresolvedRef{AttemptID: item.AttemptID, RoundNo: item.RoundNo}
	}
	return out
}

func mapDelivery(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, data_interface.ErrOwnershipLost) || errors.Is(err, data_interface.ErrPreconditionChanged) ||
		errors.Is(err, data_interface.ErrCommitUncertain) || errors.Is(err, data_interface.ErrUnconfirmed) {
		return err
	}
	var de *domain.Error
	if errors.As(err, &de) {
		return err
	}
	if Classify(err) == FailureCommitUncertain {
		return data_interface.ErrCommitUncertain
	}
	return domainErr(constant.CodeDependencyUnavailable, "数据库不可用")
}
