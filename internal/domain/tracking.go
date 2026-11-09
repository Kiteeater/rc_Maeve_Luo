package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"notifier/internal/constant"
	"notifier/internal/enums"
)

// AllTaskStatuses 是 status_counts 固定返回的 7 个状态（TRK-D01）。
var AllTaskStatuses = []enums.TaskStatus{
	enums.TaskPending, enums.TaskProcessing, enums.TaskRetryWait, enums.TaskBlocked,
	enums.TaskSucceeded, enums.TaskFailed, enums.TaskUnknown,
}

// StatusCounts 补齐全部状态，数量为 0 也保留，不用布尔 success 隐藏部分成功。
func StatusCounts(statuses []enums.TaskStatus) map[string]int {
	out := make(map[string]int, len(AllTaskStatuses))
	for _, s := range AllTaskStatuses {
		out[string(s)] = 0
	}
	for _, s := range statuses {
		out[string(s)]++
	}
	return out
}

// NeedsManualAction 在 UNKNOWN 或恢复升级标记存在时为 true。
func NeedsManualAction(status enums.TaskStatus, escalated bool) bool {
	return status == enums.TaskUnknown || escalated
}

// ListKind 区分列表排序契约。
type ListKind string

const (
	ListEvents     ListKind = "events"
	ListDeliveries ListKind = "deliveries"
	ListAttempts   ListKind = "attempts"
	ListHistory    ListKind = "history"
)

// TrackingLimits 是 S08 §9 查询参数。
type TrackingLimits struct {
	DefaultPageSize int
	MaxPageSize     int
	DefaultWindow   time.Duration
	MaxWindow       time.Duration
	CursorTTL       time.Duration
}

// QueryFilter 是规范化后的列表过滤；字段均为精确匹配。
type QueryFilter struct {
	CallerID           string   `json:"caller_id,omitempty"`
	SourceBusiness     string   `json:"source_business,omitempty"`
	SourceEventID      string   `json:"source_event_id,omitempty"`
	EventID            uint64   `json:"event_id,omitempty"`
	BusinessObjectType string   `json:"business_object_type,omitempty"`
	BusinessObjectID   string   `json:"business_object_id,omitempty"`
	ProviderID         string   `json:"provider_id,omitempty"`
	Statuses           []string `json:"status,omitempty"`
	RoundNo            uint32   `json:"round_no,omitempty"`
	PageSize           int      `json:"page_size"`
	// From/To 是有效时间窗（左闭右开）；Unbounded 表示唯一事件过滤省略时间窗。
	From      time.Time `json:"-"`
	To        time.Time `json:"-"`
	Unbounded bool      `json:"-"`
}

// TimeRange 是游标内固定的时间边界。
type TimeRange struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

func trkInvalid(msg, path string) error { return codeErr(constant.CodeInvalidRequest, msg, path) }

// ParseStatuses 解析逗号分隔状态：最多 7 个、合法且不重复。
func ParseStatuses(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	if len(parts) > len(AllTaskStatuses) {
		return nil, trkInvalid("状态过多", "status")
	}
	seen := map[string]bool{}
	valid := map[string]bool{}
	for _, s := range AllTaskStatuses {
		valid[string(s)] = true
	}
	for _, p := range parts {
		if !valid[p] || seen[p] {
			return nil, trkInvalid("状态不合法或重复", "status")
		}
		seen[p] = true
	}
	sort.Strings(parts)
	return parts, nil
}

// ResolvePageSize 校验页大小；尝试/历史固定上限 200。
func ResolvePageSize(kind ListKind, raw string, lim TrackingLimits) (int, error) {
	maxSize := lim.MaxPageSize
	if kind == ListAttempts || kind == ListHistory {
		maxSize = 200
	}
	if raw == "" {
		if lim.DefaultPageSize > maxSize {
			return maxSize, nil
		}
		return lim.DefaultPageSize, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > maxSize {
		return 0, trkInvalid("page_size 超出范围", "page_size")
	}
	return n, nil
}

// ResolveWindow 计算有效时间窗（TRK-Q01、§7）：只能成对提供、from<to、显式窗口不超过上限；
// 未提供时，唯一事件过滤省略时间窗，其余固定 [now-默认窗口, now)。超限返回 400，不截断。
func ResolveWindow(fromRaw, toRaw string, uniqueEvent bool, now time.Time, lim TrackingLimits) (TimeRange, bool, error) {
	if (fromRaw == "") != (toRaw == "") {
		return TimeRange{}, false, trkInvalid("accepted_from 与 accepted_to 必须同时提供", "accepted_from")
	}
	if fromRaw == "" {
		if uniqueEvent {
			return TimeRange{}, true, nil
		}
		now = now.UTC()
		return TimeRange{From: now.Add(-lim.DefaultWindow), To: now}, false, nil
	}
	from, err1 := time.Parse(time.RFC3339Nano, fromRaw)
	to, err2 := time.Parse(time.RFC3339Nano, toRaw)
	if err1 != nil || err2 != nil || !from.Before(to) {
		return TimeRange{}, false, trkInvalid("时间范围不合法", "accepted_from")
	}
	if to.Sub(from) > lim.MaxWindow {
		return TimeRange{}, false, trkInvalid("时间范围超过上限", "accepted_to")
	}
	return TimeRange{From: from.UTC(), To: to.UTC()}, false, nil
}

// ScopeHash 绑定调用方/运维查询范围。
func ScopeHash(scopes []string) string {
	c := append([]string(nil), scopes...)
	sort.Strings(c)
	sum := sha256.Sum256([]byte(strings.Join(c, "\x00")))
	return hex.EncodeToString(sum[:])
}

// FilterHash 绑定规范化过滤与页大小（不含时间边界，时间边界单独固定在游标内）。
func FilterHash(kind ListKind, f QueryFilter) string {
	raw, _ := json.Marshal(struct {
		Kind ListKind    `json:"kind"`
		F    QueryFilter `json:"f"`
	}{kind, f})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// CursorPayload 是 §7 游标载荷。LastSort 为完整排序键（时间纳秒 + 唯一 ID，或轮次/序号）。
type CursorPayload struct {
	Version   int        `json:"version"`
	KeyID     string     `json:"key_id"`
	ScopeHash string     `json:"caller_scope_hash"`
	FilterSHA string     `json:"filter_sha256"`
	Order     string     `json:"order"`
	LastSort  []int64    `json:"last_sort_values"`
	Range     *TimeRange `json:"effective_time_range,omitempty"`
	IssuedAt  int64      `json:"issued_at"`
	ExpiresAt int64      `json:"expires_at"`
}

// CursorKeys 是可轮换的签名密钥：Active 用于签发，旧 key_id 在保留期内仍可验证。
type CursorKeys struct {
	Active string
	Keys   map[string][]byte
}

// OrderOf 返回固定排序名。
func OrderOf(kind ListKind) string {
	switch kind {
	case ListEvents:
		return "accepted_at_desc,event_id_desc"
	case ListDeliveries:
		return "accepted_at_desc,delivery_id_desc"
	case ListAttempts:
		return "round_no_asc,attempt_no_asc"
	default:
		return "history_seq_asc"
	}
}

// SignCursor 对规范化载荷做 HMAC-SHA256，再 base64url 编码。
func SignCursor(keys CursorKeys, p CursorPayload) (string, error) {
	secret, ok := keys.Keys[keys.Active]
	if !ok || len(secret) == 0 {
		return "", codeErr(constant.CodeDependencyUnavailable, "游标签名密钥不可用", "")
	}
	p.Version, p.KeyID = 1, keys.Active
	body, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	enc := base64.RawURLEncoding
	return enc.EncodeToString(body) + "." + enc.EncodeToString(mac.Sum(nil)), nil
}

func invalidCursor() error {
	return codeErr(constant.CodeInvalidCursor, "游标无效或已失效", "cursor")
}

// VerifyCursor 验证版本、签名、过期、范围、过滤与排序，并校验字段类型和数值范围（TRK-02、TRK-13）。
func VerifyCursor(keys CursorKeys, raw string, now time.Time, scopeHash, filterSHA string, kind ListKind) (CursorPayload, error) {
	if len(raw) > 4096 {
		return CursorPayload{}, invalidCursor()
	}
	bodyEnc, sigEnc, ok := strings.Cut(raw, ".")
	if !ok {
		return CursorPayload{}, invalidCursor()
	}
	enc := base64.RawURLEncoding
	body, err1 := enc.DecodeString(bodyEnc)
	sig, err2 := enc.DecodeString(sigEnc)
	if err1 != nil || err2 != nil {
		return CursorPayload{}, invalidCursor()
	}
	var p CursorPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return CursorPayload{}, invalidCursor()
	}
	secret, ok := keys.Keys[p.KeyID]
	if !ok || len(secret) == 0 {
		return CursorPayload{}, invalidCursor()
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return CursorPayload{}, invalidCursor()
	}
	want := 2
	if kind == ListHistory {
		want = 1
	}
	if p.Version != 1 || p.ScopeHash != scopeHash || p.FilterSHA != filterSHA || p.Order != OrderOf(kind) ||
		len(p.LastSort) != want || p.IssuedAt <= 0 || p.ExpiresAt <= p.IssuedAt || now.Unix() >= p.ExpiresAt {
		return CursorPayload{}, invalidCursor()
	}
	for _, v := range p.LastSort {
		if v < 0 {
			return CursorPayload{}, invalidCursor()
		}
	}
	if p.Range != nil && !p.Range.From.Before(p.Range.To) {
		return CursorPayload{}, invalidCursor()
	}
	return p, nil
}

// RetentionTask 是清理资格判断所需的任务事实。
type RetentionTask struct {
	Status          enums.TaskStatus
	RoundFinishedAt *time.Time
	Unresolved      int
	LeaseToken      string
	CurrentAttempt  bool
}

// RetentionDecision 是单事件清理结论。
type RetentionDecision string

const (
	RetentionDelete   RetentionDecision = "DELETE"
	RetentionSkip     RetentionDecision = "SKIPPED"
	RetentionTooLarge RetentionDecision = "TOO_LARGE"
)

// CheckRetention 判断事件能否清理（TRK-R01/R02）：全部任务成功/失败、无活动/领取/未决，
// 且最后结案早于 cutoff；最后结案与发现时不同（期间重投/核对）则 SKIPPED；超过行数则整体保留。
func CheckRetention(tasks []RetentionTask, discoveredLast time.Time, cutoff time.Time, rows, maxRows int) RetentionDecision {
	if len(tasks) == 0 {
		return RetentionSkip
	}
	var last time.Time
	for _, t := range tasks {
		if (t.Status != enums.TaskSucceeded && t.Status != enums.TaskFailed) || t.RoundFinishedAt == nil ||
			t.Unresolved > 0 || t.LeaseToken != "" || t.CurrentAttempt {
			return RetentionSkip
		}
		if t.RoundFinishedAt.After(last) {
			last = *t.RoundFinishedAt
		}
	}
	if !last.Equal(discoveredLast) || !last.Before(cutoff) {
		return RetentionSkip
	}
	if rows > maxRows {
		return RetentionTooLarge
	}
	return RetentionDelete
}

// ReleaseCleanable 仅在版本不是当前发布且没有保留中的引用时可清理（TRK-R03）。
func ReleaseCleanable(isCurrent bool, references int) bool {
	return !isCurrent && references == 0
}

// ComponentState 是就绪组件状态。
type ComponentState string

const (
	ComponentReady       ComponentState = "ready"
	ComponentDegraded    ComponentState = "degraded"
	ComponentUnavailable ComponentState = "unavailable"
)

// ReadinessFacts 是有界探针结果。
type ReadinessFacts struct {
	MySQL, Migrations, Release, Identities bool
	MQ                                     bool
	Backlog, BacklogThreshold              int64
	BacklogKnown                           bool
}

// ReadinessReport 是 /readyz 响应模型。
type ReadinessReport struct {
	Ready      bool
	Admission  string
	ReasonCode string
	Components map[string]ComponentState
}

// EvaluateReadiness 实现 TRK-O03：主库/迁移/当前发布/身份缺失阻止就绪；MQ 故障只使后台角色降级，
// 积压超过阈值时暂停新受理。
func EvaluateReadiness(f ReadinessFacts) ReadinessReport {
	r := ReadinessReport{Ready: true, Admission: "open", Components: map[string]ComponentState{
		"api": ComponentReady, "publisher": ComponentReady, "consumer": ComponentReady, "recovery": ComponentReady,
	}}
	block := func(code string) {
		if r.Ready {
			r.Ready, r.Admission, r.ReasonCode = false, "paused", code
		}
	}
	switch {
	case !f.MySQL:
		block("mysql_unavailable")
		for k := range r.Components {
			r.Components[k] = ComponentUnavailable
		}
	case !f.Migrations:
		block("migrations_pending")
	case !f.Release:
		block("release_unavailable")
	case !f.Identities:
		block("identities_unavailable")
	}
	if !f.MQ {
		r.Components["publisher"], r.Components["consumer"] = ComponentDegraded, ComponentDegraded
	}
	if !f.BacklogKnown || f.Backlog > f.BacklogThreshold {
		block("admission_backlog")
	}
	if !r.Ready && r.Components["api"] == ComponentReady {
		r.Components["api"] = ComponentDegraded
	}
	return r
}
