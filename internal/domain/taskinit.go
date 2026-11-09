package domain

import (
	"crypto/subtle"
	"encoding/json"
	"time"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/enums"
)

// DeliveryCommandMaxBytes 是 S04 指令 UTF-8 上限。本文件只构造消息，发布由 S04 完成。
const DeliveryCommandMaxBytes = 4096

// InitialDelivery 是受理时写入的任务初始字段。状态机由 S03 完成。
type InitialDelivery struct {
	Status            enums.TaskStatus
	ResultCertainty   enums.ResultCertainty
	RoundNo           uint32
	ScheduleVersion   uint32
	RowVersion        uint32
	RoundAttemptCount uint32
	TotalAttemptCount uint32
	MaxAttempts       uint32
	ScheduledAt       time.Time
	RoundDeadlineAt   time.Time
}

// InitialOutbox 是首次即时指令的发布字段。发布由 S04 完成。
type InitialOutbox struct {
	MessageType     string
	PublishStatus   string
	PublishCount    uint32
	PublishRevision uint32
	RepairCount     uint32
	ScheduledAt     time.Time
	NextPublishAt   time.Time
}

// DeliveryCommand 是调度指令。只含关联标识，不含载荷、URL 或凭证。发布由 S04 完成。
type DeliveryCommand struct {
	ProtocolVersion int
	OutboxID        uint64
	MessageID       string
	EventID         uint64
	DeliveryID      uint64
	RoundNo         uint32
	ScheduleVersion uint32
}

// NewInitialDelivery 按冻结的轮次策略生成初始任务。状态机由 S03 完成。
func NewInitialDelivery(acceptedAt time.Time, roundPolicy []byte) (InitialDelivery, error) {
	attempts, timeout, err := roundBudget(roundPolicy)
	if err != nil {
		return InitialDelivery{}, err
	}
	acceptedAt = acceptedAt.UTC().Truncate(time.Microsecond)
	if acceptedAt.IsZero() {
		return InitialDelivery{}, codeErr(constant.CodeInternal, "受理时间为空", "")
	}
	deadline := acceptedAt.Add(time.Duration(timeout) * time.Second)
	if !deadline.After(acceptedAt) {
		return InitialDelivery{}, codeErr(constant.CodeMappingInvalid, "轮次截止时间不合法", "round_deadline_at")
	}
	return InitialDelivery{
		Status:            enums.TaskPending,
		ResultCertainty:   enums.CertaintyNotReceived,
		RoundNo:           1,
		ScheduleVersion:   1,
		RowVersion:        1,
		RoundAttemptCount: 0,
		TotalAttemptCount: 0,
		MaxAttempts:       attempts,
		ScheduledAt:       acceptedAt,
		RoundDeadlineAt:   deadline,
	}, nil
}

// NewInitialOutbox 给出首次指令的待发布字段。计划时间已到期，类型为即时。发布由 S04 完成。
func NewInitialOutbox(scheduledAt time.Time) InitialOutbox {
	scheduledAt = scheduledAt.UTC().Truncate(time.Microsecond)
	return InitialOutbox{
		MessageType:     "immediate",
		PublishStatus:   "PENDING",
		PublishCount:    0,
		PublishRevision: 1,
		RepairCount:     0,
		ScheduledAt:     scheduledAt,
		NextPublishAt:   scheduledAt,
	}
}

// NewDeliveryCommand 构造协议版本 1 的指令。message_id 为 o1_<outbox_id>。发布由 S04 完成。
func NewDeliveryCommand(outboxID, eventID, deliveryID uint64, roundNo, scheduleVersion uint32) (DeliveryCommand, []byte, error) {
	outboxText, err := FormatInternalID(outboxID)
	if err != nil {
		return DeliveryCommand{}, nil, codeErr(constant.CodeInternal, "指令标识不合法", "outbox_id")
	}
	messageID, err := OutboxMessageID(outboxID)
	if err != nil {
		return DeliveryCommand{}, nil, err
	}
	eventText, err := FormatInternalID(eventID)
	if err != nil {
		return DeliveryCommand{}, nil, codeErr(constant.CodeInternal, "事件标识不合法", "event_id")
	}
	deliveryText, err := FormatInternalID(deliveryID)
	if err != nil {
		return DeliveryCommand{}, nil, codeErr(constant.CodeInternal, "任务标识不合法", "delivery_id")
	}
	if roundNo == 0 || scheduleVersion == 0 {
		return DeliveryCommand{}, nil, codeErr(constant.CodeInternal, "调度版本不合法", "schedule_version")
	}
	cmd := DeliveryCommand{
		ProtocolVersion: 1,
		OutboxID:        outboxID,
		MessageID:       messageID,
		EventID:         eventID,
		DeliveryID:      deliveryID,
		RoundNo:         roundNo,
		ScheduleVersion: scheduleVersion,
	}
	body, err := json.Marshal(struct {
		ProtocolVersion int    `json:"protocol_version"`
		OutboxID        string `json:"outbox_id"`
		MessageID       string `json:"message_id"`
		EventID         string `json:"event_id"`
		DeliveryID      string `json:"delivery_id"`
		RoundNo         uint32 `json:"round_no"`
		ScheduleVersion uint32 `json:"schedule_version"`
	}{
		ProtocolVersion: cmd.ProtocolVersion,
		OutboxID:        outboxText,
		MessageID:       messageID,
		EventID:         eventText,
		DeliveryID:      deliveryText,
		RoundNo:         roundNo,
		ScheduleVersion: scheduleVersion,
	})
	if err != nil {
		return DeliveryCommand{}, nil, codeErr(constant.CodeInternal, "指令无法编码", "")
	}
	if len(body) > DeliveryCommandMaxBytes {
		return DeliveryCommand{}, nil, codeErr(constant.CodeLimitExceeded, "指令超过上限", "")
	}
	return cmd, body, nil
}

// OutboxMessageID 是应用稳定消息标识，不使用 Broker 物理消息 ID。
func OutboxMessageID(outboxID uint64) (string, error) {
	text, err := FormatInternalID(outboxID)
	if err != nil {
		return "", codeErr(constant.CodeInternal, "指令标识不合法", "outbox_id")
	}
	return "o1_" + text, nil
}

// TaskBusinessKey 是任务级稳定业务键 n1_<namespace>_<delivery_id>。供应商协议映射由 S05 完成。
func TaskBusinessKey(namespace string, deliveryID uint64) (string, error) {
	if !NamespaceOK(namespace) {
		return "", codeErr(constant.CodeInternal, "幂等命名空间不合法", "namespace")
	}
	text, err := FormatInternalID(deliveryID)
	if err != nil {
		return "", codeErr(constant.CodeInternal, "任务标识不合法", "delivery_id")
	}
	return "n1_" + namespace + "_" + text, nil
}

// NamespaceOK 判断部署命名空间是否为 1～24 位 ASCII 字母数字。
func NamespaceOK(value string) bool {
	if len(value) < 1 || len(value) > 24 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < '0' || (c > '9' && c < 'A') || (c > 'Z' && c < 'a') || c > 'z' {
			return false
		}
	}
	return true
}

// FingerprintEvent 用算法版本 1 计算事件指纹。输入是完整事实对象的规范化字节。
func FingerprintEvent(env EventEnvelope) (canonical []byte, fingerprint string, err error) {
	if env.FingerprintVersion != 0 && env.FingerprintVersion != constant.FingerprintVersion {
		return nil, "", codeErr(constant.CodeDependencyUnavailable, "无法用当前算法比较已受理事件", "fingerprint_version")
	}
	data, err := canonicaljson.Parse(env.DataCanonical)
	if err != nil || data.Kind() != canonicaljson.KindObject {
		return nil, "", codeErr(constant.CodeInvalidRequest, "事件数据不是 JSON 对象", "data")
	}
	obj := canonicaljson.Object([]canonicaljson.Field{
		{Key: "business_object_id", Value: canonicaljson.String(env.BusinessObjectID)},
		{Key: "business_object_type", Value: canonicaljson.String(env.BusinessObjectType)},
		{Key: "caller_id", Value: canonicaljson.String(env.CallerID)},
		{Key: "data", Value: data},
		{Key: "event_type", Value: canonicaljson.String(env.EventType)},
		{Key: "occurred_at", Value: canonicaljson.String(FormatUTCMicros(env.OccurredAt))},
		{Key: "schema_version", Value: canonicaljson.String(env.SchemaVersion)},
		{Key: "source_business", Value: canonicaljson.String(env.SourceBusiness)},
		{Key: "source_event_id", Value: canonicaljson.String(env.SourceEventID)},
	})
	canonical = canonicaljson.AppendCanonical(nil, obj)
	return canonical, canonicaljson.Fingerprint(canonical), nil
}

// ChannelIDOf 从固定目标里读取通道 ID。缺失或类型不对视为映射不完整。
func ChannelIDOf(fixed []byte) (uint64, error) {
	value, err := canonicaljson.Parse(fixed)
	if err != nil {
		return 0, codeErr(constant.CodeMappingInvalid, "固定目标不合法", "fixed_target")
	}
	member, ok := value.Member("channel_id")
	if !ok {
		return 0, codeErr(constant.CodeMappingInvalid, "固定目标缺少通道", "channel_id")
	}
	text, ok := member.Text()
	if !ok {
		return 0, codeErr(constant.CodeMappingInvalid, "固定目标通道不合法", "channel_id")
	}
	id, err := ParseInternalID("channel_id", text)
	if err != nil {
		return 0, codeErr(constant.CodeMappingInvalid, "固定目标通道不合法", "channel_id")
	}
	return id, nil
}

// CheckTarget 确认目标具备固定语义、账号归属和有效引用。
func CheckTarget(target TargetPlan) error {
	if err := ValidateRouteTargetID(target.RouteTargetID); err != nil {
		return codeErr(constant.CodeMappingInvalid, "路由目标不合法", "route_target_id")
	}
	if err := ValidateProviderID(target.ProviderID); err != nil {
		return codeErr(constant.CodeMappingInvalid, "供应商不合法", "provider_id")
	}
	if err := ValidateProviderAccountID(target.ProviderAccountID); err != nil {
		return codeErr(constant.CodeMappingInvalid, "供应商账号不合法", "provider_account_id")
	}
	if err := ValidateOperation(target.Operation); err != nil {
		return codeErr(constant.CodeMappingInvalid, "操作不合法", "operation")
	}
	if err := ValidateASCIIIdent("adapter_type", target.AdapterType, maxASCIIIdent64); err != nil {
		return codeErr(constant.CodeMappingInvalid, "适配器类型不合法", "adapter_type")
	}
	if err := ValidateAdapterVersion(target.AdapterVersion); err != nil || target.MappingID == "" {
		return codeErr(constant.CodeMappingInvalid, "映射引用不合法", "mapping_id")
	}
	if err := ValidateMappingVersion(target.MappingVersion); err != nil {
		return codeErr(constant.CodeMappingInvalid, "映射版本不合法", "mapping_version")
	}
	if _, err := ChannelIDOf(target.FixedTarget); err != nil {
		return err
	}
	if len(target.PayloadCanonical) == 0 {
		return codeErr(constant.CodeMappingInvalid, "映射载荷为空", "payload")
	}
	return nil
}

// SameFingerprint 比较已保存的算法版本和指纹。版本不同时不改用新算法判冲突。
func SameFingerprint(storedVersion int, stored, got string) (bool, error) {
	if storedVersion != constant.FingerprintVersion {
		return false, codeErr(constant.CodeDependencyUnavailable, "无法用当前算法比较已受理事件", "fingerprint_version")
	}
	if len(stored) != len(got) || len(stored) != 64 {
		return false, nil
	}
	return subtle.ConstantTimeCompare([]byte(stored), []byte(got)) == 1, nil
}

func roundBudget(roundPolicy []byte) (uint32, int64, error) {
	value, err := canonicaljson.Parse(roundPolicy)
	if err != nil || value.Kind() != canonicaljson.KindObject {
		return 0, 0, codeErr(constant.CodeMappingInvalid, "轮次策略不合法", "round_policy")
	}
	attemptsValue, ok := value.Member("max_attempts")
	if !ok {
		return 0, 0, codeErr(constant.CodeMappingInvalid, "轮次策略缺少尝试上限", "max_attempts")
	}
	attempts, ok := attemptsValue.Int64()
	if !ok || attempts < 1 || attempts > 32 {
		return 0, 0, codeErr(constant.CodeMappingInvalid, "轮次尝试上限不合法", "max_attempts")
	}
	timeoutValue, ok := value.Member("round_timeout_seconds")
	if !ok {
		return 0, 0, codeErr(constant.CodeMappingInvalid, "轮次策略缺少截止时间", "round_timeout_seconds")
	}
	timeout, ok := timeoutValue.Int64()
	if !ok || timeout < 60 || timeout > 604800 {
		return 0, 0, codeErr(constant.CodeMappingInvalid, "轮次截止时间不合法", "round_timeout_seconds")
	}
	return uint32(attempts), timeout, nil
}
