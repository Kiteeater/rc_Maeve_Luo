package domain

import (
	"crypto/subtle"
	"math"
	"time"
	"unicode/utf8"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
)

// 发布状态。FAILED 只表示这次发布失败，仍可重试，不是业务任务失败。
const (
	PublishPending    = "PENDING"
	PublishPublishing = "PUBLISHING"
	PublishPublished  = "PUBLISHED"
	PublishFailed     = "FAILED"
	PublishCancelled  = "CANCELLED"

	WindowImmediate = "immediate"
	WindowDelayed   = "delayed"
	WindowDefer     = "defer"

	// 再发布原因。修复入口由 S06 完成，这里只保存同一 Outbox 的安排。
	RescheduleEarly  = "early"
	RescheduleRepair = "repair"

	RescheduleStale = "stale"
	RescheduleBusy  = "busy"
	RescheduleDue   = "due"
	RescheduleOK    = "ok"
)

// DeliveryWindow 按数据库时间和 Broker 延迟上限决定即时、延迟或留到窗口内再领。
// 超出窗口时不截断成过早执行。clockSafety 从支持范围里扣掉，避免时钟误差把过远计划提前发出。
func DeliveryWindow(now, scheduled time.Time, maxDelay, clockSafety time.Duration) (action string, deliverAt, nextPublish time.Time, err error) {
	now = now.UTC().Truncate(time.Microsecond)
	scheduled = scheduled.UTC().Truncate(time.Microsecond)
	if maxDelay <= 0 || clockSafety < 0 || clockSafety > 5*time.Second || maxDelay <= clockSafety {
		return "", time.Time{}, time.Time{}, codeErr(constant.CodeInternal, "延迟窗口不合法", "ROCKETMQ_MAX_DELAY")
	}
	if scheduled.IsZero() {
		return "", time.Time{}, time.Time{}, codeErr(constant.CodeInternal, "计划时间为空", "scheduled_at")
	}
	if !scheduled.After(now) {
		return WindowImmediate, time.Time{}, now, nil
	}
	horizon := now.Add(maxDelay - clockSafety)
	if !scheduled.After(horizon) {
		return WindowDelayed, scheduled, now, nil
	}
	return WindowDefer, time.Time{}, scheduled.Add(-maxDelay).Add(clockSafety), nil
}

// ClaimSelectable 判断一行能否进入本次领取。ready 是到期的待发布，expired 是过期领取。
// 调用方必须分成两条索引查询，不能合成无索引的 OR。
func ClaimSelectable(status string, nextPublish, leaseExpires, now time.Time) string {
	now = now.UTC()
	switch status {
	case PublishPending, PublishFailed:
		if nextPublish.IsZero() || now.Before(nextPublish.UTC()) {
			return ""
		}
		return "ready"
	case PublishPublishing:
		if leaseExpires.IsZero() || now.Before(leaseExpires.UTC()) {
			return ""
		}
		return "expired"
	default:
		return ""
	}
}

// PublishView 是结果事务里看到的 Outbox 领取。
type PublishView struct {
	Status        string
	Token         string
	Revision      uint32
	Count         uint32
	LeaseExpires  time.Time
	NextPublishAt time.Time
}

// PublishEffect 是一次发布结果允许写入的变化。Late 为真时不得改状态或 next_publish_at。
type PublishEffect struct {
	Status        string
	NextPublishAt time.Time
	ClearLease    bool
	Late          bool
	Applied       bool
}

// LeaseOwns 要求状态、token、revision、发布序号和期限同时匹配。过期后即使 token 相同也不再拥有结果权。
func LeaseOwns(row PublishView, now time.Time, token string, revision, count uint32) bool {
	if row.Status != PublishPublishing || count == 0 || row.Count != count || row.Revision != revision {
		return false
	}
	if !sameToken(row.Token, token) {
		return false
	}
	if row.LeaseExpires.IsZero() || !now.UTC().Before(row.LeaseExpires.UTC()) {
		return false
	}
	return true
}

// ApplyPublishResult 把明确成功、失败或取消落到当前领取。失去所有权时只允许追加迟到证据。
func ApplyPublishResult(row PublishView, now time.Time, token string, revision, count uint32, outcome string, next time.Time) (PublishEffect, error) {
	if !LeaseOwns(row, now, token, revision, count) {
		return PublishEffect{Status: row.Status, NextPublishAt: row.NextPublishAt, Late: true}, nil
	}
	switch outcome {
	case PublishPublished:
		return PublishEffect{Status: PublishPublished, NextPublishAt: row.NextPublishAt, ClearLease: true, Applied: true}, nil
	case PublishFailed:
		if next.IsZero() {
			return PublishEffect{}, codeErr(constant.CodeInternal, "发布退避时间为空", "next_publish_at")
		}
		return PublishEffect{Status: PublishFailed, NextPublishAt: next.UTC().Truncate(time.Microsecond), ClearLease: true, Applied: true}, nil
	case PublishCancelled:
		return PublishEffect{Status: PublishCancelled, NextPublishAt: row.NextPublishAt, ClearLease: true, Applied: true}, nil
	default:
		return PublishEffect{}, codeErr(constant.CodeInternal, "发布结果不合法", "publish_status")
	}
}

// PublishStillCurrent 判断这条 Outbox 仍是任务的当前计划。终态和阻塞不再发布。
func PublishStillCurrent(taskStatus string, taskRound, taskSchedule, taskOutbox, outboxID uint64, outRound, outSchedule uint32) bool {
	if taskOutbox != outboxID || uint64(outRound) != taskRound || uint64(outSchedule) != taskSchedule {
		return false
	}
	switch taskStatus {
	case "SUCCEEDED", "FAILED", "UNKNOWN", "BLOCKED":
		return false
	default:
		return true
	}
}

// RescheduleKind 判断早到或补发能否改同一条 Outbox。due 表示交给当前执行路径，不安排未来发布。
func RescheduleKind(taskStatus string, taskRound, taskSchedule, outRound, outSchedule uint32, scheduled, now time.Time, leaseToken string, leaseExpires *time.Time) string {
	now = now.UTC()
	scheduled = scheduled.UTC()
	switch taskStatus {
	case "SUCCEEDED", "FAILED", "UNKNOWN", "BLOCKED":
		return RescheduleStale
	}
	if taskRound != outRound || taskSchedule != outSchedule {
		return RescheduleStale
	}
	if taskStatus == "PROCESSING" && leaseToken != "" && leaseExpires != nil && now.Before(leaseExpires.UTC()) {
		return RescheduleBusy
	}
	if !scheduled.After(now) {
		return RescheduleDue
	}
	return RescheduleOK
}

// ParseCommand 解析协议版本 1。未知版本不当作首版；重复键、未知字段和尾随 JSON 由规范化解析拒绝。
func ParseCommand(body []byte) (DeliveryCommand, error) {
	if len(body) == 0 || len(body) > DeliveryCommandMaxBytes || !utf8.Valid(body) {
		return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "指令无法识别", "body")
	}
	value, err := canonicaljson.Parse(body)
	if err != nil || value.Kind() != canonicaljson.KindObject {
		return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "指令无法识别", "body")
	}
	fields, ok := value.Object()
	if !ok || len(fields) != 7 {
		return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "指令字段不匹配", "body")
	}
	got := make(map[string]canonicaljson.Value, len(fields))
	for _, field := range fields {
		got[field.Key] = field.Value
	}
	for _, key := range []string{"protocol_version", "outbox_id", "message_id", "event_id", "delivery_id", "round_no", "schedule_version"} {
		if _, ok := got[key]; !ok {
			return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "指令字段不匹配", key)
		}
	}
	version, ok := got["protocol_version"].Int64()
	if !ok {
		return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "指令无法识别", "protocol_version")
	}
	if version != 1 {
		return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "未知协议版本", "protocol_version")
	}
	outboxID, err := idField("outbox_id", got["outbox_id"])
	if err != nil {
		return DeliveryCommand{}, err
	}
	eventID, err := idField("event_id", got["event_id"])
	if err != nil {
		return DeliveryCommand{}, err
	}
	deliveryID, err := idField("delivery_id", got["delivery_id"])
	if err != nil {
		return DeliveryCommand{}, err
	}
	roundNo, ok := countField(got["round_no"])
	if !ok {
		return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "指令无法识别", "round_no")
	}
	scheduleVersion, ok := countField(got["schedule_version"])
	if !ok {
		return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "指令无法识别", "schedule_version")
	}
	messageID, ok := got["message_id"].Text()
	if !ok {
		return DeliveryCommand{}, codeErr(constant.CodeInvalidRequest, "指令无法识别", "message_id")
	}
	cmd := DeliveryCommand{
		ProtocolVersion: 1, OutboxID: outboxID, MessageID: messageID,
		EventID: eventID, DeliveryID: deliveryID, RoundNo: roundNo, ScheduleVersion: scheduleVersion,
	}
	if err := ValidateDeliveryCommand(cmd); err != nil {
		return DeliveryCommand{}, err
	}
	return cmd, nil
}

// CorrelateCommand 要求消息里的关联标识和 Outbox 行一致。不一致时不能执行，也不能当成旧版本直接确认。
func CorrelateCommand(cmd DeliveryCommand, messageID string, outboxID, eventID, deliveryID uint64, roundNo, scheduleVersion uint32) error {
	if err := ValidateDeliveryCommand(cmd); err != nil {
		return err
	}
	if cmd.OutboxID != outboxID || cmd.EventID != eventID || cmd.DeliveryID != deliveryID ||
		cmd.RoundNo != roundNo || cmd.ScheduleVersion != scheduleVersion || cmd.MessageID != messageID {
		return codeErr(constant.CodeInvalidRequest, "指令关联不一致", "message_id")
	}
	return nil
}

func idField(name string, value canonicaljson.Value) (uint64, error) {
	text, ok := value.Text()
	if !ok {
		return 0, codeErr(constant.CodeInvalidRequest, "指令无法识别", name)
	}
	id, err := ParseInternalID(name, text)
	if err != nil {
		return 0, codeErr(constant.CodeInvalidRequest, "指令无法识别", name)
	}
	return id, nil
}

func countField(value canonicaljson.Value) (uint32, bool) {
	n, ok := value.Int64()
	if !ok || n < 1 || n > math.MaxUint32 {
		return 0, false
	}
	return uint32(n), true
}

func sameToken(a, b string) bool {
	if a == "" || len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// PublishBackoff 计算第 count 次发布失败后的等待：initial*2^(count-1)，不超过 cap，再乘 50%～100% 抖动。
// 发布退避只影响发布节奏，不耗尽持久化消息的生命周期。
func PublishBackoff(count uint32, initial, cap time.Duration, jitter float64) time.Duration {
	if count < 1 {
		count = 1
	}
	d := initial
	for i := uint32(1); i < count && d < cap; i++ {
		d *= 2
	}
	if d > cap {
		d = cap
	}
	if jitter < 0.5 {
		jitter = 0.5
	}
	if jitter > 1 {
		jitter = 1
	}
	return time.Duration(float64(d) * jitter)
}
