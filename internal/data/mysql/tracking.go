package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// TrackingStore 实现 S08 只读查询。所有 SQL 在进入仓储时携带 Scope，空范围直接拒绝。
type TrackingStore struct {
	db *sql.DB
}

func NewTrackingStore(db *sql.DB) *TrackingStore { return &TrackingStore{db: db} }

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func errEmptyScope() error { return domainErr(constant.CodeForbidden, "查询范围为空") }

func scopeArgs(scopes []string) []any {
	out := make([]any, len(scopes))
	for i, s := range scopes {
		out[i] = s
	}
	return out
}

// eventFilters 追加事件级过滤（精确匹配）与固定时间窗。
func eventFilters(f domain.QueryFilter, where []string, args []any) ([]string, []any) {
	add := func(col string, v any) {
		where = append(where, col+" = ?")
		args = append(args, v)
	}
	if f.CallerID != "" {
		add("e.caller_id", f.CallerID)
	}
	if f.SourceBusiness != "" {
		add("e.source_business", f.SourceBusiness)
	}
	if f.SourceEventID != "" {
		add("e.source_event_id", f.SourceEventID)
	}
	if f.EventID != 0 {
		add("e.event_id", f.EventID)
	}
	if f.BusinessObjectType != "" {
		add("e.business_object_type", f.BusinessObjectType)
	}
	if f.BusinessObjectID != "" {
		add("e.business_object_id", f.BusinessObjectID)
	}
	if !f.Unbounded {
		where = append(where, "e.accepted_at >= ?", "e.accepted_at < ?")
		args = append(args, utcWall(f.From), utcWall(f.To))
	}
	return where, args
}

func statusArgs(f domain.QueryFilter) []any {
	out := make([]any, len(f.Statuses))
	for i, s := range f.Statuses {
		out[i] = s
	}
	return out
}

// BuildDeliveriesQuery 生成任务列表 seek 查询：(accepted_at, delivery_id) 倒序，读取 size+1。
func BuildDeliveriesQuery(q data_interface.TrackingQuery) (string, []any, error) {
	if len(q.Scopes) == 0 {
		return "", nil, errEmptyScope()
	}
	where := []string{"e.caller_id IN (" + placeholders(len(q.Scopes)) + ")"}
	args := scopeArgs(q.Scopes)
	where, args = eventFilters(q.Filter, where, args)
	if q.Filter.ProviderID != "" {
		where = append(where, "t.provider_id = ?")
		args = append(args, q.Filter.ProviderID)
	}
	if len(q.Filter.Statuses) > 0 {
		where = append(where, "t.status IN ("+placeholders(len(q.Filter.Statuses))+")")
		args = append(args, statusArgs(q.Filter)...)
	}
	if len(q.After) == 2 {
		at := utcWall(time.Unix(0, q.After[0]))
		where = append(where, "(e.accepted_at < ? OR (e.accepted_at = ? AND t.delivery_id < ?))")
		args = append(args, at, at, q.After[1])
	}
	args = append(args, q.Limit)
	return `SELECT t.delivery_id, e.accepted_at FROM delivery_tasks t JOIN business_events e ON e.event_id = t.event_id
		WHERE ` + strings.Join(where, " AND ") + ` ORDER BY e.accepted_at DESC, t.delivery_id DESC LIMIT ?`, args, nil
}

// BuildEventsQuery 生成事件列表查询；provider/status 必须由同一个子任务同时满足（TRK-14）。
func BuildEventsQuery(q data_interface.TrackingQuery) (string, []any, error) {
	if len(q.Scopes) == 0 {
		return "", nil, errEmptyScope()
	}
	where := []string{"e.caller_id IN (" + placeholders(len(q.Scopes)) + ")"}
	args := scopeArgs(q.Scopes)
	where, args = eventFilters(q.Filter, where, args)
	if q.Filter.ProviderID != "" || len(q.Filter.Statuses) > 0 {
		sub := []string{"t.event_id = e.event_id"}
		if q.Filter.ProviderID != "" {
			sub = append(sub, "t.provider_id = ?")
			args = append(args, q.Filter.ProviderID)
		}
		if len(q.Filter.Statuses) > 0 {
			sub = append(sub, "t.status IN ("+placeholders(len(q.Filter.Statuses))+")")
			args = append(args, statusArgs(q.Filter)...)
		}
		where = append(where, "EXISTS (SELECT 1 FROM delivery_tasks t WHERE "+strings.Join(sub, " AND ")+")")
	}
	if len(q.After) == 2 {
		at := utcWall(time.Unix(0, q.After[0]))
		where = append(where, "(e.accepted_at < ? OR (e.accepted_at = ? AND e.event_id < ?))")
		args = append(args, at, at, q.After[1])
	}
	args = append(args, q.Limit)
	return `SELECT ` + eventColumns + ` FROM business_events e WHERE ` + strings.Join(where, " AND ") +
		` ORDER BY e.accepted_at DESC, e.event_id DESC LIMIT ?`, args, nil
}

const eventColumns = `e.event_id, e.caller_id, e.source_business, e.source_event_id, e.event_type, e.schema_version,
	e.occurred_at, e.accepted_at, e.business_object_type, e.business_object_id, e.routing_release_id`

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func scanEvent(r rowScanner) (data_interface.EventRow, error) {
	var e data_interface.EventRow
	err := r.Scan(&e.EventID, &e.CallerID, &e.SourceBusiness, &e.SourceEventID, &e.EventType, &e.SchemaVersion,
		&e.OccurredAt, &e.AcceptedAt, &e.BusinessObjectType, &e.BusinessObjectID, &e.RoutingReleaseID)
	e.OccurredAt, e.AcceptedAt = e.OccurredAt.UTC(), e.AcceptedAt.UTC()
	return e, err
}

// statusCounts 聚合事件全部子任务状态（不受 provider/status 过滤裁剪）。
func statusCounts(ctx context.Context, q querier, ids []uint64) (map[uint64]map[string]int, error) {
	out := map[uint64]map[string]int{}
	if len(ids) == 0 {
		return out, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
		out[id] = domain.StatusCounts(nil)
	}
	rows, err := q.QueryContext(ctx, `SELECT event_id, status, COUNT(*) FROM delivery_tasks WHERE event_id IN (`+placeholders(len(ids))+`) GROUP BY event_id, status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id uint64
		var status string
		var n int
		if err := rows.Scan(&id, &status, &n); err != nil {
			return nil, err
		}
		out[id][status] = n
	}
	return out, rows.Err()
}

func applyCounts(e *data_interface.EventRow, counts map[uint64]map[string]int) {
	e.StatusCounts = counts[e.EventID]
	e.DeliveryCount = 0
	for _, n := range e.StatusCounts {
		e.DeliveryCount += n
	}
}

func (s *TrackingStore) QueryEvents(ctx context.Context, q data_interface.TrackingQuery) ([]data_interface.EventRow, error) {
	query, args, err := BuildEventsQuery(q)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var out []data_interface.EventRow
	var ids []uint64
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, e)
		ids = append(ids, e.EventID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	counts, err := statusCounts(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		applyCounts(&out[i], counts)
	}
	return out, nil
}

const deliveryExtra = `SELECT e.source_business, e.source_event_id, e.business_object_type, e.business_object_id, e.accepted_at, t.created_at, t.updated_at
	FROM delivery_tasks t JOIN business_events e ON e.event_id = t.event_id WHERE t.delivery_id = ?`

func loadDeliveryRow(ctx context.Context, q querier, id uint64) (data_interface.DeliveryRow, error) {
	task, err := scanTask(q.QueryRowContext(ctx, taskSelect+` WHERE t.delivery_id = ?`, id))
	if err != nil {
		return data_interface.DeliveryRow{}, err
	}
	row := data_interface.DeliveryRow{Task: task}
	err = q.QueryRowContext(ctx, deliveryExtra, id).Scan(&row.SourceBusiness, &row.SourceEventID, &row.BusinessObjectType,
		&row.BusinessObjectID, &row.AcceptedAt, &row.CreatedAt, &row.UpdatedAt)
	row.AcceptedAt, row.CreatedAt, row.UpdatedAt = row.AcceptedAt.UTC(), row.CreatedAt.UTC(), row.UpdatedAt.UTC()
	return row, err
}

func (s *TrackingStore) QueryDeliveries(ctx context.Context, q data_interface.TrackingQuery) ([]data_interface.DeliveryRow, error) {
	query, args, err := BuildDeliveriesQuery(q)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for rows.Next() {
		var id uint64
		var at time.Time
		if err := rows.Scan(&id, &at); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]data_interface.DeliveryRow, 0, len(ids))
	for _, id := range ids {
		r, err := loadDeliveryRow(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func (s *TrackingStore) LoadEventView(ctx context.Context, scopes []string, eventID uint64, limit int) (data_interface.EventRow, []data_interface.DeliveryRow, error) {
	if len(scopes) == 0 {
		return data_interface.EventRow{}, nil, errEmptyScope()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return data_interface.EventRow{}, nil, err
	}
	defer tx.Rollback()
	args := append([]any{eventID}, scopeArgs(scopes)...)
	ev, err := scanEvent(tx.QueryRowContext(ctx, `SELECT `+eventColumns+` FROM business_events e WHERE e.event_id = ? AND e.caller_id IN (`+placeholders(len(scopes))+`)`, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return data_interface.EventRow{}, nil, domainErr(constant.CodeNotFound, "事件不存在")
	}
	if err != nil {
		return data_interface.EventRow{}, nil, err
	}
	counts, err := statusCounts(ctx, tx, []uint64{eventID})
	if err != nil {
		return data_interface.EventRow{}, nil, err
	}
	applyCounts(&ev, counts)
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id FROM delivery_tasks WHERE event_id = ? ORDER BY delivery_id DESC LIMIT ?`, eventID, limit)
	if err != nil {
		return data_interface.EventRow{}, nil, err
	}
	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return data_interface.EventRow{}, nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	out := make([]data_interface.DeliveryRow, 0, len(ids))
	for _, id := range ids {
		r, err := loadDeliveryRow(ctx, tx, id)
		if err != nil {
			return data_interface.EventRow{}, nil, err
		}
		out = append(out, r)
	}
	return ev, out, nil
}

// checkParent 先检查父任务范围；不存在与越权都返回 not_found。
func checkParent(ctx context.Context, q querier, scopes []string, deliveryID uint64) error {
	if len(scopes) == 0 {
		return errEmptyScope()
	}
	var got uint64
	args := append([]any{deliveryID}, scopeArgs(scopes)...)
	err := q.QueryRowContext(ctx, `SELECT t.delivery_id FROM delivery_tasks t JOIN business_events e ON e.event_id = t.event_id
		WHERE t.delivery_id = ? AND e.caller_id IN (`+placeholders(len(scopes))+`)`, args...).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return domainErr(constant.CodeNotFound, "任务不存在")
	}
	return err
}

func (s *TrackingStore) LoadDelivery(ctx context.Context, scopes []string, deliveryID uint64) (data_interface.DeliveryRow, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return data_interface.DeliveryRow{}, err
	}
	defer tx.Rollback()
	if err := checkParent(ctx, tx, scopes, deliveryID); err != nil {
		return data_interface.DeliveryRow{}, err
	}
	return loadDeliveryRow(ctx, tx, deliveryID)
}

func (s *TrackingStore) ListAttempts(ctx context.Context, scopes []string, deliveryID uint64, q data_interface.TrackingQuery) ([]data_interface.AttemptRow, error) {
	if err := checkParent(ctx, s.db, scopes, deliveryID); err != nil {
		return nil, err
	}
	where := []string{"delivery_id = ?"}
	args := []any{deliveryID}
	if q.Filter.RoundNo > 0 {
		where = append(where, "round_no = ?")
		args = append(args, q.Filter.RoundNo)
	}
	if len(q.After) == 2 {
		where = append(where, "(round_no > ? OR (round_no = ? AND attempt_no > ?))")
		args = append(args, q.After[0], q.After[0], q.After[1])
	}
	args = append(args, q.Limit)
	rows, err := s.db.QueryContext(ctx, `
		SELECT attempt_id, delivery_id, round_no, attempt_no, outbox_id, trace_id, worker_id, deployment_version,
			status, send_state, outcome, result_certainty, error_code, error_reason, error_summary_truncated,
			request_summary, response_summary, request_summary_truncated, response_summary_truncated,
			http_status, provider_request_id, retryable, evidence_scope, started_at, request_started_at, finished_at,
			duration_ms, attempt_duration_ms, timing_incomplete,
			method, target_path, request_size_bytes, provider_code, response_size_bytes, provider_wait_hint, response_received_at, response_truncated
		FROM delivery_attempts WHERE `+strings.Join(where, " AND ")+` ORDER BY round_no, attempt_no LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []data_interface.AttemptRow
	for rows.Next() {
		var a data_interface.AttemptRow
		var outbox, httpStatus, reqSize, respSize, httpMs, attemptMs sql.NullInt64
		var trace, worker, deploy, send, outcome, cert, code, reason, req, resp, provider, evidence sql.NullString
		var method, path, pcode, hint sql.NullString
		var errTrunc, reqTrunc, respTrunc, retryable, timing, bodyTrunc sql.NullBool
		var requestStarted, finished, received sql.NullTime
		if err := rows.Scan(&a.AttemptID, &a.DeliveryID, &a.RoundNo, &a.AttemptNo, &outbox, &trace, &worker, &deploy,
			&a.Status, &send, &outcome, &cert, &code, &reason, &errTrunc, &req, &resp, &reqTrunc, &respTrunc,
			&httpStatus, &provider, &retryable, &evidence, &a.StartedAt, &requestStarted, &finished, &httpMs, &attemptMs, &timing,
			&method, &path, &reqSize, &pcode, &respSize, &hint, &received, &bodyTrunc); err != nil {
			return nil, err
		}
		if outbox.Valid {
			a.OutboxID = uint64(outbox.Int64)
		}
		a.TraceID, a.WorkerID, a.DeploymentVersion = trace.String, worker.String, deploy.String
		a.SendState, a.Outcome, a.ResultCertainty = enums.SendState(send.String), enums.Outcome(outcome.String), enums.ResultCertainty(cert.String)
		a.ErrorCode, a.ErrorReason, a.ErrorTruncated = code.String, reason.String, errTrunc.Bool
		a.RequestSummary, a.ResponseSummary, a.RequestTruncated, a.ResponseTruncated = req.String, resp.String, reqTrunc.Bool, respTrunc.Bool
		if httpStatus.Valid {
			v := int(httpStatus.Int64)
			a.HTTPStatus = &v
		}
		a.ProviderRequestID, a.Retryable = provider.String, retryable.Bool
		if evidence.Valid {
			a.EvidenceScope = []byte(evidence.String)
		}
		a.StartedAt = a.StartedAt.UTC()
		a.RequestStartedAt, a.FinishedAt, a.ResponseReceivedAt = timePtr(requestStarted), timePtr(finished), timePtr(received)
		a.HTTPDuration, a.AttemptDuration, a.TimingIncomplete = durationPtr(httpMs), durationPtr(attemptMs), timing.Bool
		a.Method, a.TargetPath, a.ProviderCode, a.ProviderWaitHint = method.String, path.String, pcode.String, hint.String
		if bodyTrunc.Valid {
			v := bodyTrunc.Bool
			a.BodyTruncated = &v
		}
		if reqSize.Valid {
			v := reqSize.Int64
			a.RequestSize = &v
		}
		if respSize.Valid {
			v := respSize.Int64
			a.ResponseSize = &v
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *TrackingStore) ListHistory(ctx context.Context, scopes []string, deliveryID uint64, q data_interface.TrackingQuery) ([]data_interface.HistoryRow, error) {
	if err := checkParent(ctx, s.db, scopes, deliveryID); err != nil {
		return nil, err
	}
	after := int64(0)
	if len(q.After) == 1 {
		after = q.After[0]
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT history_seq, write_id, phase, from_status, to_status, reason, round_no, schedule_version,
			attempt_id, outbox_id, evidence, occurred_at, operation_id, actor, recorded_at
		FROM delivery_history WHERE delivery_id = ? AND history_seq > ? ORDER BY history_seq LIMIT ?`, deliveryID, after, q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []data_interface.HistoryRow
	for rows.Next() {
		var h data_interface.HistoryRow
		var from, to, reason, evidence, op, actor sql.NullString
		var round, schedule, attempt, outbox sql.NullInt64
		var occurred sql.NullTime
		if err := rows.Scan(&h.Seq, &h.WriteID, &h.Phase, &from, &to, &reason, &round, &schedule, &attempt, &outbox, &evidence,
			&occurred, &op, &actor, &h.RecordedAt); err != nil {
			return nil, err
		}
		h.From, h.To, h.Reason = enums.TaskStatus(from.String), enums.TaskStatus(to.String), reason.String
		h.RoundNo, h.ScheduleVersion = uint32(round.Int64), uint32(schedule.Int64)
		h.AttemptID, h.OutboxID = uint64(attempt.Int64), uint64(outbox.Int64)
		if evidence.Valid {
			h.Evidence = []byte(evidence.String)
		}
		if occurred.Valid {
			h.OccurredAt = occurred.Time.UTC()
		}
		h.OperationID, h.Actor, h.RecordedAt = op.String, actor.String, h.RecordedAt.UTC()
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *TrackingStore) Overview(ctx context.Context, q data_interface.OverviewQuery) (data_interface.OverviewCounts, error) {
	if len(q.Scopes) == 0 {
		return data_interface.OverviewCounts{}, errEmptyScope()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return data_interface.OverviewCounts{}, err
	}
	defer tx.Rollback()
	var out data_interface.OverviewCounts
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(6)`).Scan(&now); err != nil {
		return out, err
	}
	out.GeneratedAt = now.UTC()
	sc := "e.caller_id IN (" + placeholders(len(q.Scopes)) + ")"
	sa := scopeArgs(q.Scopes)
	count := func(query string, extra ...any) (*int64, error) {
		var n int64
		if err := tx.QueryRowContext(ctx, query, append(append([]any{}, sa...), extra...)...).Scan(&n); err != nil {
			return nil, err
		}
		return &n, nil
	}
	join := ` FROM delivery_tasks t JOIN business_events e ON e.event_id = t.event_id WHERE ` + sc
	var oldest sql.NullTime
	var pending int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), MIN(o.created_at) FROM outbox_messages o
		JOIN delivery_tasks t ON t.delivery_id = o.delivery_id AND t.round_no = o.round_no AND t.schedule_version = o.schedule_version
		JOIN business_events e ON e.event_id = t.event_id
		WHERE `+sc+` AND o.publish_status IN ('PENDING','PUBLISHING','FAILED')`, sa...).Scan(&pending, &oldest); err != nil {
		return out, err
	}
	out.PendingPublish = &pending
	if oldest.Valid {
		age := now.Sub(oldest.Time)
		out.OldestPublishAge = &age
	} else {
		zero := time.Duration(0)
		out.OldestPublishAge = &zero
	}
	if out.DueNotExecuted, err = count(`SELECT COUNT(*)`+join+` AND t.status IN ('PENDING','RETRY_WAIT') AND t.scheduled_at <= ?`, utcWall(now)); err != nil {
		return out, err
	}
	if out.ExpiredLeases, err = count(`SELECT COUNT(*)`+join+` AND t.status = 'PROCESSING' AND t.lease_expires_at <= ?`, utcWall(now)); err != nil {
		return out, err
	}
	if out.LongBlocked, err = count(`SELECT COUNT(*)`+join+` AND t.status = 'BLOCKED' AND t.blocked_since <= ?`, utcWall(now.Add(-q.BlockedAfter))); err != nil {
		return out, err
	}
	if out.LongUnknown, err = count(`SELECT COUNT(*)`+join+` AND t.status = 'UNKNOWN' AND t.round_finished_at <= ?`, utcWall(now.Add(-q.UnknownAfter))); err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT t.status, COUNT(*)`+join+` GROUP BY t.status`, sa...)
	if err != nil {
		return out, err
	}
	out.StatusCounts = map[string]int64{}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return out, err
		}
		out.StatusCounts[st] = n
	}
	rows.Close()
	var total, succ int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(t.status = 'SUCCEEDED'), 0)`+join+` AND e.accepted_at >= ? AND e.accepted_at < ?`,
		append(append([]any{}, sa...), utcWall(q.From), utcWall(q.To))...).Scan(&total, &succ); err != nil {
		return out, err
	}
	if total > 0 {
		r := float64(succ) / float64(total)
		out.TaskReceiveRate = &r
	}
	var fin, recv, incomplete int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(a.outcome = 'RECEIVED'), 0), COALESCE(SUM(a.timing_incomplete), 0)
		FROM delivery_attempts a JOIN delivery_tasks t ON t.delivery_id = a.delivery_id JOIN business_events e ON e.event_id = t.event_id
		WHERE `+sc+` AND a.finished_at >= ? AND a.finished_at < ?`, append(append([]any{}, sa...), utcWall(q.From), utcWall(q.To))...).Scan(&fin, &recv, &incomplete); err != nil {
		return out, err
	}
	if fin > 0 {
		r := float64(recv) / float64(fin)
		out.AttemptRecvRate = &r
	}
	out.TimingIncomplete = &incomplete
	// HTTP 分位与补偿失败批次不在 MySQL 统计范围内，保持不可用而非 0。
	return out, nil
}

// RetentionStore 实现 S08 协调清理。
type RetentionStore struct {
	db   *sql.DB
	exec *Executor
}

func NewRetentionStore(db *sql.DB, exec *Executor) *RetentionStore {
	return &RetentionStore{db: db, exec: exec}
}

func (s *RetentionStore) ListRetentionCandidates(ctx context.Context, cutoff time.Time, limit int) ([]data_interface.RetentionCandidate, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.event_id, MAX(t.round_finished_at) FROM delivery_tasks t
		GROUP BY t.event_id
		HAVING SUM(t.status NOT IN ('SUCCEEDED','FAILED') OR t.round_finished_at IS NULL) = 0 AND MAX(t.round_finished_at) < ?
		ORDER BY t.event_id LIMIT ?`, utcWall(cutoff), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []data_interface.RetentionCandidate
	for rows.Next() {
		var c data_interface.RetentionCandidate
		if err := rows.Scan(&c.EventID, &c.LastResolved); err != nil {
			return nil, err
		}
		c.LastResolved = c.LastResolved.UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteResolvedEvent 按 S00 顺序锁事件与全部任务并复核；先删依赖实体再删任务与事件。配置不随事件删除。
func (s *RetentionStore) DeleteResolvedEvent(ctx context.Context, c data_interface.RetentionCandidate, cutoff time.Time, maxRows int) (domain.RetentionDecision, error) {
	decision := domain.RetentionSkip
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		decision = domain.RetentionSkip
		var got uint64
		err := tx.QueryRowContext(ctx, `SELECT event_id FROM business_events WHERE event_id = ? FOR UPDATE`, c.EventID).Scan(&got)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT delivery_id FROM delivery_tasks WHERE event_id = ? ORDER BY delivery_id`, c.EventID)
		if err != nil {
			return err
		}
		var ids []uint64
		for rows.Next() {
			var id uint64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := LockTasks(ctx, tx, ids); err != nil {
			return err
		}
		facts := make([]domain.RetentionTask, 0, len(ids))
		for _, id := range ids {
			var f domain.RetentionTask
			var finished sql.NullTime
			var lease sql.NullString
			var current sql.NullInt64
			if err := tx.QueryRowContext(ctx, `SELECT status, round_finished_at, COALESCE(JSON_LENGTH(unresolved_evidence), 0), lease_token, current_attempt_id
				FROM delivery_tasks WHERE delivery_id = ?`, id).Scan(&f.Status, &finished, &f.Unresolved, &lease, &current); err != nil {
				return err
			}
			f.RoundFinishedAt, f.LeaseToken, f.CurrentAttempt = timePtr(finished), lease.String, current.Valid
			facts = append(facts, f)
		}
		total := 1 + len(ids)
		if len(ids) > 0 {
			args := make([]any, len(ids))
			for i, id := range ids {
				args[i] = id
			}
			for _, table := range []string{"delivery_attempts", "delivery_history", "outbox_messages"} {
				var n int
				if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE delivery_id IN (`+placeholders(len(ids))+`)`, args...).Scan(&n); err != nil {
					return err
				}
				total += n
			}
		}
		decision = domain.CheckRetention(facts, c.LastResolved, cutoff, total, maxRows)
		if decision != domain.RetentionDelete {
			return nil
		}
		args := make([]any, len(ids))
		targets := make([]any, len(ids))
		for i, id := range ids {
			args[i] = id
			targets[i] = strconv.FormatUint(id, 10)
		}
		in := `(` + placeholders(len(ids)) + `)`
		for _, stmt := range []struct {
			sql  string
			args []any
		}{
			{`DELETE FROM delivery_attempts WHERE delivery_id IN ` + in, args},
			{`DELETE FROM delivery_history WHERE delivery_id IN ` + in, args},
			{`DELETE FROM outbox_messages WHERE delivery_id IN ` + in, args},
			{`DELETE FROM management_events WHERE target_type = 'delivery' AND target_id IN ` + in, targets},
			{`DELETE FROM delivery_tasks WHERE event_id = ?`, []any{c.EventID}},
			{`DELETE FROM business_events WHERE event_id = ?`, []any{c.EventID}},
		} {
			if _, err := tx.ExecContext(ctx, stmt.sql, stmt.args...); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return domain.RetentionSkip, err
	}
	return decision, nil
}
