package mysql

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// EventStore 在一个事务里保存事件、全部任务、首次 Outbox 和初始历史。
// 不发布 MQ。发布由 S04 完成。
type EventStore struct {
	db   *sql.DB
	exec *Executor
}

func NewEventStore(db *sql.DB, exec *Executor) *EventStore {
	return &EventStore{db: db, exec: exec}
}

// Measure 统计尚未完成发布的有效 Outbox。不含 PUBLISHED 和 CANCELLED。
// Broker 是否停机不影响计数。发布由 S04 完成。
func (s *EventStore) Measure(ctx context.Context) (int64, *time.Time, error) {
	var pending int64
	var oldest sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), DATE_FORMAT(MIN(created_at), '%Y-%m-%d %H:%i:%s.%f')
		FROM outbox_messages
		WHERE publish_status IN ('PENDING', 'PUBLISHING', 'FAILED')`).Scan(&pending, &oldest)
	if err != nil {
		return 0, nil, domainErr(constant.CodeDependencyUnavailable, "积压统计不可用")
	}
	if !oldest.Valid || oldest.String == "" {
		return pending, nil, nil
	}
	at, err := parseUTCWall(oldest.String)
	if err != nil {
		return 0, nil, domainErr(constant.CodeDependencyUnavailable, "积压统计不可用")
	}
	return pending, &at, nil
}

func (s *EventStore) FindAcceptedEvent(ctx context.Context, callerID, sourceEventID string) (data_interface.AcceptedEvent, bool, error) {
	var out data_interface.AcceptedEvent
	var found bool
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		ev, ok, err := readAccepted(ctx, tx, callerID, sourceEventID)
		if err != nil || !ok {
			found = false
			return err
		}
		out = ev
		found = true
		return nil
	})
	if err != nil {
		return data_interface.AcceptedEvent{}, false, mapInfra(err)
	}
	return out, found, nil
}

func (s *EventStore) AcceptEvent(ctx context.Context, in data_interface.AcceptEventInput) (data_interface.AcceptedEvent, error) {
	var out data_interface.AcceptedEvent
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		ev, err := insertAcceptance(ctx, tx, in)
		if err != nil {
			return err
		}
		out = ev
		return nil
	})
	if err == nil {
		return out, nil
	}
	if idx, ok := uniqueIndex(err); ok {
		if idx == "uq_event_source" {
			return data_interface.AcceptedEvent{}, data_interface.ErrEventUnique
		}
		return data_interface.AcceptedEvent{}, data_interface.ErrInconsistent
	}
	if n, ok := mysqlNumber(err); ok && n == 1452 {
		return data_interface.AcceptedEvent{}, data_interface.ErrInconsistent
	}
	return data_interface.AcceptedEvent{}, mapInfra(err)
}

func insertAcceptance(ctx context.Context, tx *sql.Tx, in data_interface.AcceptEventInput) (data_interface.AcceptedEvent, error) {
	if err := validateAcceptance(in); err != nil {
		return data_interface.AcceptedEvent{}, err
	}
	var wall string
	if err := tx.QueryRowContext(ctx, `SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%d %H:%i:%s.%f')`).Scan(&wall); err != nil {
		return data_interface.AcceptedEvent{}, err
	}
	acceptedAt, err := parseUTCWall(wall)
	if err != nil {
		return data_interface.AcceptedEvent{}, domainErr(constant.CodeDependencyUnavailable, "数据库时间不可用")
	}
	var hash sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT content_sha256 FROM routing_releases WHERE release_id = ?`, in.ReleaseID).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) || !hash.Valid {
		return data_interface.AcceptedEvent{}, domainErr(constant.CodeDependencyUnavailable, "当前发布不可读取")
	}
	if err != nil {
		return data_interface.AcceptedEvent{}, err
	}
	if subtle.ConstantTimeCompare([]byte(hash.String), []byte(in.ExpectedReleaseHash)) != 1 {
		return data_interface.AcceptedEvent{}, domainErr(constant.CodeDependencyUnavailable, "当前发布不可读取")
	}
	occurred := utcWall(in.OccurredAt)
	res, err := tx.ExecContext(ctx, `
		INSERT INTO business_events (
			caller_id, source_business, source_event_id, event_type, schema_version,
			occurred_at, business_object_type, business_object_id,
			data_snapshot, data_canonical, fingerprint, fingerprint_version,
			routing_release_id, accepted_at)
		VALUES (?,?,?,?,?, ?,?,?, CAST(? AS JSON), ?,?,?, ?,?)`,
		in.CallerID, in.SourceBusiness, in.SourceEventID, in.EventType, in.SchemaVersion,
		occurred, in.BusinessObjectType, in.BusinessObjectID,
		string(in.DataCanonical), in.DataCanonical, in.Fingerprint, in.FingerprintVersion,
		in.ReleaseID, wall)
	if err != nil {
		return data_interface.AcceptedEvent{}, err
	}
	eventID, err := insertedID(res)
	if err != nil {
		return data_interface.AcceptedEvent{}, err
	}
	targets := append([]domain.TargetPlan(nil), in.Plan.Targets...)
	slices.SortFunc(targets, func(a, b domain.TargetPlan) int {
		return strings.Compare(a.RouteTargetID, b.RouteTargetID)
	})
	deliveries := make([]data_interface.DeliveryView, 0, len(targets))
	for _, target := range targets {
		view, err := insertTarget(ctx, tx, in, eventID, acceptedAt, wall, target)
		if err != nil {
			return data_interface.AcceptedEvent{}, err
		}
		deliveries = append(deliveries, view)
	}
	return data_interface.AcceptedEvent{
		EventID:            eventID,
		CallerID:           in.CallerID,
		SourceBusiness:     in.SourceBusiness,
		SourceEventID:      in.SourceEventID,
		FingerprintVersion: in.FingerprintVersion,
		Fingerprint:        in.Fingerprint,
		DataCanonical:      append([]byte(nil), in.DataCanonical...),
		RoutingReleaseID:   in.ReleaseID,
		AcceptedAt:         acceptedAt,
		Deliveries:         deliveries,
	}, nil
}

func insertTarget(ctx context.Context, tx *sql.Tx, in data_interface.AcceptEventInput, eventID uint64, acceptedAt time.Time, wall string, target domain.TargetPlan) (data_interface.DeliveryView, error) {
	initial, err := domain.NewInitialDelivery(acceptedAt, in.Plan.RoundPolicy)
	if err != nil {
		return data_interface.DeliveryView{}, err
	}
	if initial.Status != enums.TaskPending || initial.ResultCertainty != enums.CertaintyNotReceived ||
		initial.RoundNo != 1 || initial.ScheduleVersion != 1 || initial.RowVersion != 1 ||
		initial.RoundAttemptCount != 0 || initial.TotalAttemptCount != 0 {
		return data_interface.DeliveryView{}, domainErr(constant.CodeInternal, "初始任务字段不符合受理规则")
	}
	channelID, err := domain.ChannelIDOf(target.FixedTarget)
	if err != nil {
		return data_interface.DeliveryView{}, err
	}
	deadline := utcWall(initial.RoundDeadlineAt)
	scheduled := utcWall(initial.ScheduledAt)
	res, err := tx.ExecContext(ctx, `
		INSERT INTO delivery_tasks (
			event_id, route_target_id, channel_id, provider_id, provider_account_id, operation,
			adapter_type, mapping_id, fixed_target, round_policy, payload, payload_canonical,
			mapping_version, adapter_version, idempotency_key,
			status, round_no, round_attempt_count, total_attempt_count, max_attempts,
			round_deadline_at, scheduled_at, schedule_version, row_version, result_certainty, history_seq)
		VALUES (?,?,?,?,?,?,?,?,CAST(? AS JSON),CAST(? AS JSON),CAST(? AS JSON),?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)`,
		eventID, target.RouteTargetID, channelID, target.ProviderID, target.ProviderAccountID, target.Operation,
		target.AdapterType, target.MappingID, string(target.FixedTarget), string(in.Plan.RoundPolicy), string(target.PayloadCanonical), target.PayloadCanonical,
		target.MappingVersion, target.AdapterVersion, "pending",
		string(initial.Status), initial.RoundNo, initial.RoundAttemptCount, initial.TotalAttemptCount, initial.MaxAttempts,
		deadline, scheduled, initial.ScheduleVersion, initial.RowVersion, string(initial.ResultCertainty))
	if err != nil {
		return data_interface.DeliveryView{}, err
	}
	deliveryID, err := insertedID(res)
	if err != nil {
		return data_interface.DeliveryView{}, err
	}
	businessKey, err := domain.TaskBusinessKey(in.IdempotencyNamespace, deliveryID)
	if err != nil {
		return data_interface.DeliveryView{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE delivery_tasks SET idempotency_key = ? WHERE delivery_id = ?`, businessKey, deliveryID); err != nil {
		return data_interface.DeliveryView{}, err
	}
	box := domain.NewInitialOutbox(initial.ScheduledAt)
	if box.PublishStatus != "PENDING" || box.MessageType != "immediate" {
		return data_interface.DeliveryView{}, domainErr(constant.CodeInternal, "首次指令字段不符合受理规则")
	}
	res, err = tx.ExecContext(ctx, `
		INSERT INTO outbox_messages (
			message_id, delivery_id, round_no, schedule_version, message_type,
			scheduled_at, publish_status, publish_count, next_publish_at,
			publish_revision, repair_count, created_at, updated_at)
		VALUES (?,?,?,?,?, ?,?,?,?, ?,?,?,?)`,
		"p_"+itoaU64(deliveryID), deliveryID, initial.RoundNo, initial.ScheduleVersion, box.MessageType,
		utcWall(box.ScheduledAt), box.PublishStatus, box.PublishCount, utcWall(box.NextPublishAt),
		box.PublishRevision, box.RepairCount, wall, wall)
	if err != nil {
		return data_interface.DeliveryView{}, err
	}
	outboxID, err := insertedID(res)
	if err != nil {
		return data_interface.DeliveryView{}, err
	}
	if _, _, err = domain.NewDeliveryCommand(outboxID, eventID, deliveryID, initial.RoundNo, initial.ScheduleVersion); err != nil {
		return data_interface.DeliveryView{}, err
	}
	messageID, err := domain.OutboxMessageID(outboxID)
	if err != nil {
		return data_interface.DeliveryView{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE outbox_messages SET message_id = ? WHERE outbox_id = ?`, messageID, outboxID); err != nil {
		return data_interface.DeliveryView{}, err
	}
	if err = appendInitialHistory(ctx, tx, deliveryID, outboxID, initial, wall); err != nil {
		return data_interface.DeliveryView{}, err
	}
	return data_interface.DeliveryView{
		DeliveryID:        deliveryID,
		RouteTargetID:     target.RouteTargetID,
		ProviderID:        target.ProviderID,
		ProviderAccountID: target.ProviderAccountID,
		Operation:         target.Operation,
		PayloadCanonical:  append([]byte(nil), target.PayloadCanonical...),
	}, nil
}

func appendInitialHistory(ctx context.Context, tx *sql.Tx, deliveryID, outboxID uint64, initial domain.InitialDelivery, wall string) error {
	phases := []enums.HistoryPhase{enums.PhaseTaskCreated, enums.PhaseScheduleCreated}
	reasons := []string{"任务已创建", "首次调度已创建"}
	for i, phase := range phases {
		seq, err := NextHistorySeq(ctx, tx, deliveryID)
		if err != nil {
			return err
		}
		if seq != uint32(i+1) {
			return domainErr(constant.CodeInternal, "初始历史序号不符合受理规则")
		}
		boxID := outboxID
		if phase == enums.PhaseTaskCreated {
			boxID = 0
		}
		writeID, err := domain.WriteID(deliveryID, phase, 0, "", boxID, 0)
		if err != nil {
			return domainErr(constant.CodeInternal, "历史写入标识不合法")
		}
		var storedBox any
		if phase == enums.PhaseScheduleCreated {
			storedBox = outboxID
		}
		if _, err = tx.ExecContext(ctx, `
			INSERT INTO delivery_history (
				delivery_id, history_seq, write_id, phase, to_status, reason,
				round_no, schedule_version, outbox_id, occurred_at, recorded_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			deliveryID, seq, writeID, string(phase), string(initial.Status), reasons[i],
			initial.RoundNo, initial.ScheduleVersion, storedBox, wall, wall); err != nil {
			return err
		}
	}
	return nil
}

func validateAcceptance(in data_interface.AcceptEventInput) error {
	if err := domain.ValidateCallerID(in.CallerID); err != nil {
		return domainErr(constant.CodeInvalidRequest, "调用方不合法")
	}
	if err := domain.ValidateSourceBusiness(in.SourceBusiness); err != nil {
		return domainErr(constant.CodeInvalidRequest, "来源不合法")
	}
	if len(in.AllowedSources) > 0 && !slices.Contains(in.AllowedSources, in.SourceBusiness) {
		return domainErr(constant.CodeForbidden, "来源不在授权范围")
	}
	if err := domain.ValidateExternalID("source_event_id", in.SourceEventID); err != nil {
		return domainErr(constant.CodeInvalidRequest, "来源事件标识不合法")
	}
	if in.FingerprintVersion != constant.FingerprintVersion || len(in.Fingerprint) != 64 || len(in.ExpectedReleaseHash) != 64 {
		return domainErr(constant.CodeInvalidRequest, "指纹或发布摘要不合法")
	}
	env := domain.EventEnvelope{
		CallerID: in.CallerID, SourceBusiness: in.SourceBusiness, SourceEventID: in.SourceEventID,
		EventType: in.EventType, SchemaVersion: in.SchemaVersion, OccurredAt: in.OccurredAt.UTC(),
		BusinessObjectType: in.BusinessObjectType, BusinessObjectID: in.BusinessObjectID,
		DataCanonical: in.DataCanonical, FingerprintVersion: in.FingerprintVersion,
	}
	if _, fp, err := domain.FingerprintEvent(env); err != nil {
		return err
	} else if fp != in.Fingerprint {
		return domainErr(constant.CodeInternal, "指纹与事实不一致")
	}
	ref, err := domain.FormatInternalID(in.ReleaseID)
	if err != nil || in.Plan.ReleaseRef != ref {
		return domainErr(constant.CodeDependencyUnavailable, "当前发布不可读取")
	}
	if !domain.NamespaceOK(in.IdempotencyNamespace) {
		return domainErr(constant.CodeInternal, "幂等命名空间不合法")
	}
	if in.MaxFanout < 1 || len(in.Plan.Targets) == 0 {
		return domainErr(constant.CodeNoMatchingRoute, "没有匹配的路由")
	}
	if len(in.Plan.Targets) > in.MaxFanout {
		return domainErr(constant.CodeFanoutExceeded, "扇出超过上限")
	}
	targets := append([]domain.TargetPlan(nil), in.Plan.Targets...)
	slices.SortFunc(targets, func(a, b domain.TargetPlan) int { return strings.Compare(a.RouteTargetID, b.RouteTargetID) })
	var planBytes int64
	for i, target := range targets {
		if i > 0 && target.RouteTargetID == targets[i-1].RouteTargetID {
			return domainErr(constant.CodeMappingInvalid, "路由目标重复")
		}
		if err := domain.CheckTarget(target); err != nil {
			return err
		}
		planBytes += int64(len(target.PayloadCanonical)) + int64(len(target.FixedTarget))
	}
	if planBytes > int64(in.MaxPlanBytes) {
		return domainErr(constant.CodeLimitExceeded, "固定计划超过上限")
	}
	if _, err := domain.NewInitialDelivery(time.Unix(0, 0).UTC(), in.Plan.RoundPolicy); err != nil {
		return err
	}
	return nil
}

func readAccepted(ctx context.Context, tx *sql.Tx, callerID, sourceEventID string) (data_interface.AcceptedEvent, bool, error) {
	var ev data_interface.AcceptedEvent
	var accepted string
	var data []byte
	err := tx.QueryRowContext(ctx, `
		SELECT event_id, caller_id, source_business, source_event_id,
		       fingerprint_version, fingerprint, data_canonical, routing_release_id,
		       DATE_FORMAT(accepted_at, '%Y-%m-%d %H:%i:%s.%f')
		FROM business_events
		WHERE caller_id = ? AND source_event_id = ?`, callerID, sourceEventID).Scan(
		&ev.EventID, &ev.CallerID, &ev.SourceBusiness, &ev.SourceEventID,
		&ev.FingerprintVersion, &ev.Fingerprint, &data, &ev.RoutingReleaseID, &accepted)
	if errors.Is(err, sql.ErrNoRows) {
		return data_interface.AcceptedEvent{}, false, nil
	}
	if err != nil {
		return data_interface.AcceptedEvent{}, false, err
	}
	ev.AcceptedAt, err = parseUTCWall(accepted)
	if err != nil {
		return data_interface.AcceptedEvent{}, false, domainErr(constant.CodeInternal, "受理时间不可读")
	}
	ev.DataCanonical = append([]byte(nil), data...)
	rows, err := tx.QueryContext(ctx, `
		SELECT delivery_id, route_target_id, provider_id, provider_account_id, operation, payload_canonical
		FROM delivery_tasks WHERE event_id = ? ORDER BY route_target_id`, ev.EventID)
	if err != nil {
		return data_interface.AcceptedEvent{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var view data_interface.DeliveryView
		var payload []byte
		if err := rows.Scan(&view.DeliveryID, &view.RouteTargetID, &view.ProviderID, &view.ProviderAccountID, &view.Operation, &payload); err != nil {
			return data_interface.AcceptedEvent{}, false, err
		}
		view.PayloadCanonical = append([]byte(nil), payload...)
		ev.Deliveries = append(ev.Deliveries, view)
	}
	if err := rows.Err(); err != nil {
		return data_interface.AcceptedEvent{}, false, err
	}
	if len(ev.Deliveries) == 0 {
		return data_interface.AcceptedEvent{}, false, domainErr(constant.CodeInternal, "受理数据不一致")
	}
	return ev, true, nil
}

func insertedID(res sql.Result) (uint64, error) {
	id, err := res.LastInsertId()
	if err != nil || id <= 0 {
		return 0, domainErr(constant.CodeInternal, "内部标识分配失败")
	}
	return uint64(id), nil
}

func utcWall(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format("2006-01-02 15:04:05.000000")
}

func parseUTCWall(value string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05.000000", value)
}

func itoaU64(n uint64) string {
	text, err := domain.FormatInternalID(n)
	if err != nil {
		return "0"
	}
	return text
}

func uniqueIndex(err error) (string, bool) {
	var me *mysqldrv.MySQLError
	if !errors.As(err, &me) || me.Number != 1062 {
		return "", false
	}
	const marker = "for key '"
	i := strings.LastIndex(me.Message, marker)
	if i < 0 {
		return "", true
	}
	rest := strings.TrimSuffix(me.Message[i+len(marker):], "'")
	if j := strings.LastIndex(rest, "."); j >= 0 {
		rest = rest[j+1:]
	}
	return rest, true
}

func mapInfra(err error) error {
	if err == nil {
		return nil
	}
	var de *domain.Error
	if errors.As(err, &de) {
		return err
	}
	if errors.Is(err, data_interface.ErrCommitUncertain) || errors.Is(err, data_interface.ErrEventUnique) || errors.Is(err, data_interface.ErrInconsistent) {
		return err
	}
	if Classify(err) == FailureCommitUncertain {
		return data_interface.ErrCommitUncertain
	}
	return domainErr(constant.CodeDependencyUnavailable, "数据库不可用")
}
