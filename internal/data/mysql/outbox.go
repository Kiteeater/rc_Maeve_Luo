package mysql

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// OutboxStore 实现 S04 的发布领取、结果记录和同消息再发布。
// 领取事务只锁 Outbox；结果与再发布事务按 S00 锁顺序先锁任务再锁 Outbox。
type OutboxStore struct {
	db   *sql.DB
	exec *Executor
}

func NewOutboxStore(db *sql.DB, exec *Executor) *OutboxStore {
	return &OutboxStore{db: db, exec: exec}
}

type outboxRow struct {
	id, eventID, deliveryID uint64
	messageID               string
	round, schedule         uint32
	scheduledAt             time.Time
	status                  string
	count, revision         uint32
	token                   sql.NullString
	leaseExpires            sql.NullTime
	nextPublish             sql.NullTime
	taskStatus              string
	taskRound, taskSchedule uint32
}

const outboxSelect = `
	SELECT o.outbox_id, t.event_id, o.delivery_id, o.message_id, o.round_no, o.schedule_version, o.scheduled_at,
		o.publish_status, o.publish_count, o.publish_revision, o.lease_token, o.lease_expires_at, o.next_publish_at,
		t.status, t.round_no, t.schedule_version
	FROM outbox_messages o JOIN delivery_tasks t ON t.delivery_id = o.delivery_id`

func scanOutbox(sc interface{ Scan(...any) error }) (outboxRow, error) {
	var r outboxRow
	err := sc.Scan(&r.id, &r.eventID, &r.deliveryID, &r.messageID, &r.round, &r.schedule, &r.scheduledAt,
		&r.status, &r.count, &r.revision, &r.token, &r.leaseExpires, &r.nextPublish,
		&r.taskStatus, &r.taskRound, &r.taskSchedule)
	return r, err
}

// ClaimPublishBatch 用两条索引分支（到期待发布 / 过期领取）配合 SKIP LOCKED 领取。
// 计划超出 Broker 窗口的行只推迟 next_publish_at，不增加 publish_count。
func (s *OutboxStore) ClaimPublishBatch(ctx context.Context, in data_interface.ClaimPublishInput) ([]data_interface.ClaimedPublish, error) {
	if in.Limit < 1 {
		return nil, nil
	}
	var out []data_interface.ClaimedPublish
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out = out[:0]
		now, err := txNow(ctx, tx)
		if err != nil {
			return err
		}
		ids, err := claimCandidates(ctx, tx, now, in.Limit)
		if err != nil {
			return err
		}
		for _, id := range ids {
			row, err := scanOutbox(tx.QueryRowContext(ctx, outboxSelect+` WHERE o.outbox_id = ?`, id))
			if err != nil {
				return err
			}
			if !domain.PublishStillCurrent(row.taskStatus, uint64(row.taskRound), uint64(row.taskSchedule), row.id, row.id, row.round, row.schedule) ||
				row.taskRound != row.round || row.taskSchedule != row.schedule {
				if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET publish_status = 'CANCELLED',
					lease_token = NULL, lease_expires_at = NULL WHERE outbox_id = ?`, row.id); err != nil {
					return err
				}
				continue
			}
			action, _, next, err := domain.DeliveryWindow(now, row.scheduledAt, in.MaxDelay, in.ClockSafety)
			if err != nil {
				return err
			}
			if action == domain.WindowDefer {
				if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET publish_status = 'PENDING', next_publish_at = ?,
					lease_token = NULL, lease_expires_at = NULL WHERE outbox_id = ?`, utcWall(next), row.id); err != nil {
					return err
				}
				continue
			}
			token, err := newToken()
			if err != nil {
				return err
			}
			expires := now.Add(in.Lease)
			if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET publish_status = 'PUBLISHING',
				publish_count = publish_count + 1, publish_revision = publish_revision + 1,
				lease_token = ?, lease_expires_at = ?, last_publish_started_at = ? WHERE outbox_id = ?`,
				token, utcWall(expires), utcWall(now), row.id); err != nil {
				return err
			}
			cmd, body, err := domain.NewDeliveryCommand(row.id, row.eventID, row.deliveryID, row.round, row.schedule)
			if err != nil {
				return err
			}
			if cmd.MessageID != row.messageID {
				return errors.New("mysql: outbox message_id does not match o1_<outbox_id>")
			}
			out = append(out, data_interface.ClaimedPublish{
				Command: cmd, Body: body, Token: token, Revision: row.revision + 1, PublishCount: row.count + 1,
				ScheduledAt: row.scheduledAt.UTC(), LeaseExpires: expires,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func claimCandidates(ctx context.Context, tx *sql.Tx, now time.Time, limit int) ([]uint64, error) {
	queries := []string{
		`SELECT outbox_id FROM outbox_messages WHERE publish_status = 'PENDING' AND next_publish_at <= ?
			ORDER BY next_publish_at, outbox_id LIMIT ? FOR UPDATE SKIP LOCKED`,
		`SELECT outbox_id FROM outbox_messages WHERE publish_status = 'FAILED' AND next_publish_at <= ?
			ORDER BY next_publish_at, outbox_id LIMIT ? FOR UPDATE SKIP LOCKED`,
		`SELECT outbox_id FROM outbox_messages WHERE publish_status = 'PUBLISHING' AND lease_expires_at <= ?
			ORDER BY lease_expires_at, outbox_id LIMIT ? FOR UPDATE SKIP LOCKED`,
	}
	var ids []uint64
	for _, q := range queries {
		if len(ids) >= limit {
			break
		}
		rows, err := tx.QueryContext(ctx, q, utcWall(now), limit-len(ids))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id uint64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// RecordPublishResult 校验 token/revision/发布序号/期限后写结果和发布历史；失去所有权时只追加迟到证据。
func (s *OutboxStore) RecordPublishResult(ctx context.Context, in data_interface.RecordPublishInput) (data_interface.PublishRecord, error) {
	var rec data_interface.PublishRecord
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now, err := txNow(ctx, tx)
		if err != nil {
			return err
		}
		var deliveryID uint64
		if err := tx.QueryRowContext(ctx, `SELECT delivery_id FROM outbox_messages WHERE outbox_id = ?`, in.OutboxID).Scan(&deliveryID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return data_interface.ErrOutboxNotFound
			}
			return err
		}
		if err := LockTasks(ctx, tx, []uint64{deliveryID}); err != nil {
			return err
		}
		row, err := scanOutbox(tx.QueryRowContext(ctx, outboxSelect+` WHERE o.outbox_id = ? FOR UPDATE`, in.OutboxID))
		if err != nil {
			return err
		}
		view := domain.PublishView{Status: row.status, Token: row.token.String, Revision: row.revision, Count: row.count}
		if row.leaseExpires.Valid {
			view.LeaseExpires = row.leaseExpires.Time
		}
		if row.nextPublish.Valid {
			view.NextPublishAt = row.nextPublish.Time
		}
		eff, err := domain.ApplyPublishResult(view, now, in.Token, in.Revision, in.PublishCount, in.Outcome, in.NextPublishAt)
		if err != nil {
			return err
		}
		rec = data_interface.PublishRecord{Applied: eff.Applied, Late: eff.Late, Status: eff.Status}
		phase := enums.PhasePublishSucceeded
		if in.Outcome != domain.PublishPublished {
			phase = enums.PhasePublishFailed
		}
		reason := in.ErrorSummary
		if eff.Late {
			reason = "late publish result: " + in.Outcome
			phase = enums.PhaseLateResponse
		} else {
			var published any
			if eff.Status == domain.PublishPublished {
				published = utcWall(now)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET publish_status = ?, next_publish_at = ?,
				lease_token = NULL, lease_expires_at = NULL, last_error = ?, last_published_at = COALESCE(?, last_published_at)
				WHERE outbox_id = ?`, eff.Status, nullTime(&eff.NextPublishAt), nullStr(in.ErrorSummary), published, in.OutboxID); err != nil {
				return err
			}
		}
		return appendPublishHistory(ctx, tx, row, phase, reason, in.PublishCount, now)
	})
	return rec, err
}

// RescheduleSameCommand 处理早到指令：锁任务和 Outbox，确认仍是当前计划且未到期、无有效领取，
// 将同一 Outbox 置回 PENDING、增加 revision、作废旧发布领取并追加 WAIT_PLANNED 历史。
func (s *OutboxStore) RescheduleSameCommand(ctx context.Context, in data_interface.RescheduleInput) (data_interface.RescheduleResult, error) {
	var res data_interface.RescheduleResult
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now, err := txNow(ctx, tx)
		if err != nil {
			return err
		}
		if err := LockTasks(ctx, tx, []uint64{in.Command.DeliveryID}); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return data_interface.ErrOutboxNotFound
			}
			return err
		}
		row, err := scanOutbox(tx.QueryRowContext(ctx, outboxSelect+` WHERE o.outbox_id = ? FOR UPDATE`, in.Command.OutboxID))
		if errors.Is(err, sql.ErrNoRows) {
			return data_interface.ErrOutboxNotFound
		}
		if err != nil {
			return err
		}
		if err := domain.CorrelateCommand(in.Command, row.messageID, row.id, row.eventID, row.deliveryID, row.round, row.schedule); err != nil {
			return err
		}
		var taskToken sql.NullString
		var taskLease sql.NullTime
		if err := tx.QueryRowContext(ctx, `SELECT lease_token, lease_expires_at FROM delivery_tasks WHERE delivery_id = ?`,
			row.deliveryID).Scan(&taskToken, &taskLease); err != nil {
			return err
		}
		notBefore := in.NotBefore
		if notBefore.Before(row.scheduledAt) {
			notBefore = row.scheduledAt
		}
		kind := domain.RescheduleKind(row.taskStatus, row.taskRound, row.taskSchedule, row.round, row.schedule,
			notBefore, now, taskToken.String, timePtr(taskLease))
		res.Disposition = kind
		if kind != domain.RescheduleOK {
			return nil
		}
		next := now
		action, _, deferred, err := domain.DeliveryWindow(now, notBefore, in.MaxDelay, in.ClockSafety)
		if err != nil {
			return err
		}
		if action == domain.WindowDefer {
			next = deferred
		}
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET publish_status = 'PENDING', next_publish_at = ?,
			publish_revision = publish_revision + 1, lease_token = NULL, lease_expires_at = NULL WHERE outbox_id = ?`,
			utcWall(next), row.id); err != nil {
			return err
		}
		res.Revision = row.revision + 1
		res.NextPublishAt = next
		return appendPublishHistory(ctx, tx, row, enums.PhaseWaitPlanned, "republish same command: "+in.Reason, row.revision+1, now)
	})
	return res, err
}

// LoadBinding 读取 Outbox 行及任务的关联标识。
func (s *OutboxStore) LoadBinding(ctx context.Context, outboxID uint64) (data_interface.CommandBinding, error) {
	row, err := scanOutbox(s.db.QueryRowContext(ctx, outboxSelect+` WHERE o.outbox_id = ?`, outboxID))
	if errors.Is(err, sql.ErrNoRows) {
		return data_interface.CommandBinding{}, data_interface.ErrOutboxNotFound
	}
	if err != nil {
		return data_interface.CommandBinding{}, err
	}
	return data_interface.CommandBinding{
		OutboxID: row.id, MessageID: row.messageID, EventID: row.eventID, DeliveryID: row.deliveryID,
		RoundNo: row.round, ScheduleVersion: row.schedule,
	}, nil
}

// appendPublishHistory 的 write_id 由 (任务, 阶段, outbox_id, 发布序号) 决定；同一结果事务重试不产生重复历史。
func appendPublishHistory(ctx context.Context, tx *sql.Tx, row outboxRow, phase enums.HistoryPhase, reason string, generation uint32, now time.Time) error {
	writeID, err := domain.WriteID(row.deliveryID, phase, 0, "", row.id, generation)
	if err != nil {
		return err
	}
	var exists int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM delivery_history WHERE write_id = ?`, writeID).Scan(&exists)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	seq, err := NextHistorySeq(ctx, tx, row.deliveryID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO delivery_history (
			delivery_id, history_seq, write_id, phase, from_status, to_status, reason,
			round_no, schedule_version, attempt_id, outbox_id, evidence, occurred_at, actor)
		VALUES (?,?,?,?,?,?,?,?,?,NULL,?,NULL,?,?)`,
		row.deliveryID, seq, writeID, string(phase), row.taskStatus, row.taskStatus, clipReason(reason),
		row.round, row.schedule, row.id, utcWall(now), "publisher")
	return err
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
