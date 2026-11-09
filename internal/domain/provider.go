package domain

import (
	"strconv"
	"strings"
	"time"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/enums"
)

// FixedTarget 是任务固定的目标语义（S01 发布冻结），执行时不从可变通道重新解释。
type FixedTarget struct {
	ChannelID         uint64
	ProviderID        string
	ProviderAccountID string
	Operation         string
	AdapterType       string
	AdapterVersion    string
	CredentialRef     string
	BaseURL           string
	Path              string
	AuthType          string
	IdemMode          string
	IdemWindow        time.Duration
}

// ParseFixedTarget 解析 fixed_target 规范化字节。
func ParseFixedTarget(fixed []byte) (FixedTarget, error) {
	bad := func(f string) (FixedTarget, error) {
		return FixedTarget{}, codeErr(constant.CodeInternal, "固定目标不合法", f)
	}
	root, err := canonicaljson.Parse(fixed)
	if err != nil || root.Kind() != canonicaljson.KindObject {
		return bad("fixed_target")
	}
	id, err := ChannelIDOf(fixed)
	if err != nil {
		return FixedTarget{}, err
	}
	out := FixedTarget{ChannelID: id}
	for key, dst := range map[string]*string{
		"provider_id": &out.ProviderID, "provider_account_id": &out.ProviderAccountID, "operation": &out.Operation,
		"adapter_type": &out.AdapterType, "adapter_version": &out.AdapterVersion, "credential_ref": &out.CredentialRef,
	} {
		v, ok := root.Member(key)
		if !ok {
			return bad(key)
		}
		if *dst, ok = v.Text(); !ok {
			return bad(key)
		}
	}
	sem, ok := root.Member("semantic_config")
	if !ok || sem.Kind() != canonicaljson.KindObject {
		return bad("semantic_config")
	}
	for key, dst := range map[string]*string{"base_url": &out.BaseURL, "path": &out.Path, "auth_type": &out.AuthType} {
		v, ok := sem.Member(key)
		if !ok {
			return bad("semantic_config." + key)
		}
		if *dst, ok = v.Text(); !ok {
			return bad("semantic_config." + key)
		}
	}
	idem, ok := sem.Member("idempotency")
	if !ok {
		return bad("semantic_config.idempotency")
	}
	mode, _ := idem.Member("mode")
	out.IdemMode, _ = mode.Text()
	win, _ := idem.Member("window_seconds")
	n, _ := win.Int64()
	out.IdemWindow = time.Duration(n) * time.Second
	return out, nil
}

// ReplaySafe 判断供应商幂等能否覆盖未决结果：窗口起点取全部未决尝试最早可能发送时间，
// 当前时间加本次最大预算必须严格早于窗口终点（PRV-10、PRV-12）。
func ReplaySafe(mode string, window time.Duration, windowStart, now time.Time, budget time.Duration) (bool, time.Time) {
	if mode != "key" || window <= 0 || windowStart.IsZero() {
		return false, time.Time{}
	}
	end := windowStart.Add(window)
	if now.Add(budget).Before(end) {
		return true, end
	}
	return false, time.Time{}
}

// TransportFacts 是一次 HTTP 的传输事实。
type TransportFacts struct {
	Invoked      bool // 是否已进入 HTTP 调用
	Status       int  // 未收到响应头为 0
	Body         []byte
	ReadComplete bool
	TooLarge     bool
	BytesRead    int64
	TransportErr string
	Redirect     bool
	ReceivedAt   time.Time
	RetryAfter   string
}

// ProtocolResult 是 mock-json@1 协议解释（S00 标准结果）。
type ProtocolResult struct {
	Outcome           enums.Outcome
	Certainty         enums.ResultCertainty
	Retryable         bool
	ErrorCode         string
	ProviderCode      string
	ProviderRequestID string
	WaitUntil         time.Time
}

// InterpretMockJSON 按 S05 第 11 节解释响应：HTTP 200 不自动等于接收；无法证明未接收时为不确定。
func InterpretMockJSON(f TransportFacts) ProtocolResult {
	unc := func(code string) ProtocolResult {
		return ProtocolResult{Outcome: enums.OutcomeUncertain, Certainty: enums.CertaintyUncertain, Retryable: true, ErrorCode: code}
	}
	if !f.Invoked {
		return ProtocolResult{Outcome: enums.OutcomeLocalFailure, Certainty: enums.CertaintyNotReceived, ErrorCode: constant.ErrBuildFailed}
	}
	if f.Redirect {
		return unc(constant.ErrRedirectRefused)
	}
	if f.Status == 0 {
		return unc(constant.ErrConnection)
	}
	if f.TooLarge || !f.ReadComplete {
		return unc(constant.ErrProtocolInvalid)
	}
	root, err := canonicaljson.Parse(f.Body)
	if err != nil || root.Kind() != canonicaljson.KindObject {
		return unc(constant.ErrProtocolInvalid)
	}
	accepted, okA := boolMember(root, "accepted")
	code, okC := textMember(root, "code")
	reqID, _ := textMember(root, "request_id")
	res := ProtocolResult{ProviderCode: code, ProviderRequestID: reqID}
	switch {
	case f.Status >= 200 && f.Status < 300 && okA && accepted:
		res.Outcome, res.Certainty = enums.OutcomeReceived, enums.CertaintyReceived
		return res
	case okA && !accepted && okC && code == "TEMPORARY":
		res.Outcome, res.Certainty, res.Retryable, res.ErrorCode = enums.OutcomeRetryableReject, enums.CertaintyNotReceived, true, constant.ErrProviderRetryable
		res.WaitUntil = RetryAfter(f.RetryAfter, f.ReceivedAt)
		return res
	case okA && !accepted && okC && code == "PERMANENT" && f.Status < 500:
		res.Outcome, res.Certainty, res.ErrorCode = enums.OutcomePermanentReject, enums.CertaintyNotReceived, constant.ErrProviderPermanent
		if f.Status == 401 || f.Status == 403 {
			res.ErrorCode = constant.ErrAuthRejected
		}
		return res
	}
	out := unc(constant.ErrProtocolInvalid)
	out.ProviderCode, out.ProviderRequestID = code, reqID
	return out
}

// RetryAfter 支持非负十进制秒数或 HTTP 日期，依据响应接收时刻换算；无法解析返回零值。
func RetryAfter(v string, received time.Time) time.Time {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}
	}
	if n, err := strconv.ParseUint(v, 10, 32); err == nil {
		return received.Add(time.Duration(n) * time.Second)
	}
	if t, err := time.Parse(time.RFC1123, v); err == nil {
		if t.Before(received) {
			return received
		}
		return t.UTC()
	}
	return time.Time{}
}

// WhitelistSummary 只提取白名单字段生成摘要，按 UTF-8 字节安全截断并标记（PRV-08）。
// 非 JSON 响应不记录正文，只记录长度。
func WhitelistSummary(body []byte, allow []string, max int) (string, bool) {
	root, err := canonicaljson.Parse(body)
	if err != nil || root.Kind() != canonicaljson.KindObject {
		if len(body) == 0 {
			return "", false
		}
		return "non-json body, " + strconv.Itoa(len(body)) + " bytes", false
	}
	var fields []canonicaljson.Field
	for _, k := range allow {
		if v, ok := root.Member(k); ok {
			fields = append(fields, canonicaljson.Field{Key: k, Value: v})
		}
	}
	out := string(canonicaljson.AppendCanonical(nil, canonicaljson.Object(fields)))
	return TruncateUTF8(out, max)
}

// BucketState 是令牌桶状态，tokens 以千分之一令牌定点表示。
type BucketState struct {
	MilliTokens   int64
	LastRefillMS  int64
	PolicyVersion uint32
}

// TakeToken 与 Redis 脚本同一算法：时钟回退不产生额度；策略版本变化时重建但不超过新 burst，
// 也不额外补满（沿用已有令牌，取与新 burst 的较小值）。返回是否通过及最早可再试时间（毫秒）。
func TakeToken(st BucketState, exists bool, nowMS int64, rate, burst int64, version uint32) (BucketState, bool, int64) {
	full := burst * 1000
	if !exists {
		st = BucketState{MilliTokens: full, LastRefillMS: nowMS, PolicyVersion: version}
	} else if st.PolicyVersion != version {
		if st.MilliTokens > full {
			st.MilliTokens = full
		}
		st.PolicyVersion = version
	}
	if nowMS > st.LastRefillMS {
		st.MilliTokens += (nowMS - st.LastRefillMS) * rate
		if st.MilliTokens > full {
			st.MilliTokens = full
		}
		st.LastRefillMS = nowMS
	}
	if st.MilliTokens >= 1000 {
		st.MilliTokens -= 1000
		return st, true, 0
	}
	need := 1000 - st.MilliTokens
	wait := (need + rate - 1) / rate
	return st, false, nowMS + wait
}

func boolMember(v canonicaljson.Value, key string) (bool, bool) {
	m, ok := v.Member(key)
	if !ok {
		return false, false
	}
	return m.Bool()
}

func textMember(v canonicaljson.Value, key string) (string, bool) {
	m, ok := v.Member(key)
	if !ok {
		return "", false
	}
	return m.Text()
}
