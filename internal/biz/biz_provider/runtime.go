// Package biz_provider 编排供应商执行边界（S05）：当前约束、限流、凭证准备、请求构造、单次调用和协议解释。
// 不拥有任务重试调度或状态写入权。
package biz_provider

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// Config 是 S05 运行参数。
type Config struct {
	HTTPBudget      time.Duration // 单次 HTTP 总预算（DELIVERY_HTTP_TIMEOUT）
	TokenSkew       time.Duration // PROVIDER_TOKEN_EXPIRY_SKEW
	MaxRequestBytes int           // PROVIDER_MAX_REQUEST_BYTES
	SummaryMax      int
	LimiterRetry    time.Duration // 限流器不可用时的有界等待
}

// 摘要白名单：mock-json@1 只记录这三个字段。
var responseWhitelist = []string{"accepted", "code", "request_id"}

// Runtime 实现 data_interface.ProviderRuntime。
type Runtime struct {
	Registry data_interface.AdapterRegistry
	Channels data_interface.ChannelStateReader
	Limiter  data_interface.RateLimiter
	Creds    data_interface.CredentialResolver
	HTTP     data_interface.HTTPCaller
	Config   Config
	Now      func() time.Time
}

func block(code, reason string) data_interface.PrepareResponse {
	return data_interface.PrepareResponse{Kind: domain.PrepareBlock, ErrorCode: code, Reason: reason}
}

// Prepare 依次检查适配器、通道当前约束、目标许可、重发安全、限流和凭证。不发业务请求。
func (r *Runtime) Prepare(ctx context.Context, in data_interface.PrepareRequest) (data_interface.PrepareResponse, error) {
	task := in.Task
	now := in.Now
	if now.IsZero() {
		now = r.now()
	}
	ft, err := domain.ParseFixedTarget(task.FixedTarget)
	if err != nil {
		return block(constant.ErrConfigurationUnavail, "固定目标不可解析"), nil
	}
	if _, ok := r.Registry.Lookup(ft.AdapterType, ft.AdapterVersion); !ok {
		return block(constant.ErrAdapterUnavailable, "原适配器版本未注册"), nil
	}
	ch, err := r.Channels.LoadChannelState(ctx, ft.ChannelID)
	if err != nil {
		return block(constant.ErrConfigurationUnavail, "通道不可用"), nil
	}
	if ch.ProviderID != ft.ProviderID || ch.ProviderAccountID != ft.ProviderAccountID || ch.Operation != ft.Operation ||
		ch.AdapterType != ft.AdapterType {
		return block(constant.ErrAccountMismatch, "通道归属与任务不一致"), nil
	}
	if !ch.Enabled {
		return block(constant.ErrChannelDisabled, "通道已禁用"), nil
	}
	if !in.CallerAllowed || !contains(ch.AllowedCallers, task.CallerID) {
		return block(constant.ErrCallerForbidden, "调用方无权使用通道"), nil
	}
	if !originAllowed(ft.BaseURL, ch.AllowedOrigins) {
		return block(constant.ErrTargetForbidden, "原目标不再允许"), nil
	}
	resp := data_interface.PrepareResponse{Kind: domain.PrepareReady, ChannelVersion: ch.RowVersion, ReleaseID: task.ReleaseID}
	if len(task.Unresolved) > 0 {
		start := windowStart(task)
		covers, until := domain.ReplaySafe(ft.IdemMode, ft.IdemWindow, start, now, r.Config.HTTPBudget)
		resp.WindowStart, resp.SafeUntil, resp.IdempotencyCovers = start, until, covers
		manual := task.RiskOperationID != "" && task.RiskAttemptID == nil && task.RoundAttemptCount == 0
		if !covers && !manual {
			return data_interface.PrepareResponse{Kind: domain.PrepareUnsafe, ErrorCode: constant.ErrUnsafeResend,
				Reason: "供应商去重窗口不足以覆盖未决结果", WindowStart: start}, nil
		}
	}
	ok, notBefore, err := r.Limiter.Take(ctx, ch.QuotaKey, ch.RatePerSecond, ch.Burst, ch.PolicyVersion)
	if err != nil {
		return data_interface.PrepareResponse{Kind: domain.PrepareWait, ErrorCode: constant.ErrRateLimitWait,
			Reason: "限流器不可用，暂停发送", RateLimitUntil: now.Add(r.Config.LimiterRetry)}, nil
	}
	if !ok {
		return data_interface.PrepareResponse{Kind: domain.PrepareWait, ErrorCode: constant.ErrRateLimitWait,
			Reason: "额度不足", RateLimitUntil: notBefore}, nil
	}
	h, err := r.Creds.ResolveCredential(ctx, ft.CredentialRef, ft.ProviderAccountID)
	switch {
	case errors.Is(err, data_interface.ErrCredentialRevoked):
		return block(constant.ErrCredentialRevoked, "凭证已撤销"), nil
	case errors.Is(err, data_interface.ErrAccountMismatch):
		return block(constant.ErrAccountMismatch, "凭证账号不一致"), nil
	case errors.Is(err, data_interface.ErrCredentialReauth):
		return block(constant.ErrCredentialReauth, "凭证需要重新授权"), nil
	case err != nil:
		return data_interface.PrepareResponse{Kind: domain.PrepareWait, ErrorCode: constant.ErrAuthWait, Reason: "凭证暂不可用"}, nil
	}
	if h.Account != ft.ProviderAccountID {
		return block(constant.ErrAccountMismatch, "凭证账号不一致"), nil
	}
	if !h.ExpiresAt.IsZero() && h.ExpiresAt.Sub(now) < r.Config.HTTPBudget+r.Config.TokenSkew {
		return data_interface.PrepareResponse{Kind: domain.PrepareWait, ErrorCode: constant.ErrAuthWait, Reason: "凭证剩余寿命不足"}, nil
	}
	resp.CredentialHandle = h.Opaque
	return resp, nil
}

// Build 用固定载荷和稳定幂等键构造请求；鉴权头与签名在发送时由 data/provider 按本次时间生成。
// 发出前重查通道版本，凭证准备后通道变化则不使用过期 READY（DLV-19）。
func (r *Runtime) Build(ctx context.Context, task data_interface.TaskRecord, _ domain.Ownership, prep data_interface.PrepareResponse) (data_interface.BuiltRequest, error) {
	ft, err := domain.ParseFixedTarget(task.FixedTarget)
	if err != nil {
		return data_interface.BuiltRequest{}, err
	}
	ch, err := r.Channels.LoadChannelState(ctx, ft.ChannelID)
	if err != nil || ch.RowVersion != prep.ChannelVersion || !ch.Enabled {
		return data_interface.BuiltRequest{}, errors.New("provider: channel changed after prepare")
	}
	if r.Config.MaxRequestBytes > 0 && len(task.PayloadCanonical) > r.Config.MaxRequestBytes {
		return data_interface.BuiltRequest{}, errors.New("provider: request exceeds limit")
	}
	summary, trunc := domain.TruncateUTF8("POST "+ft.Path+" bytes="+strconv.Itoa(len(task.PayloadCanonical)), r.Config.SummaryMax)
	return data_interface.BuiltRequest{
		Method: "POST", TargetPath: ft.Path, Summary: summary, Truncated: trunc, Size: len(task.PayloadCanonical),
		Body: task.PayloadCanonical, URL: ft.BaseURL + ft.Path, AuthType: ft.AuthType,
		IdempotencyKey: task.IdempotencyKey, CredentialHandle: prep.CredentialHandle, AllowedCIDRs: ch.AllowedCIDRs,
	}, nil
}

// SendOnce 只调用一次 HTTP，不重试、不跟随重定向。
func (r *Runtime) SendOnce(ctx context.Context, built data_interface.BuiltRequest) (data_interface.SendResponse, error) {
	f := r.HTTP.Do(ctx, built)
	out := data_interface.SendResponse{HTTPStatus: f.Status, Facts: f, TransportErr: f.TransportErr}
	out.BodySummary, out.Truncated = domain.WhitelistSummary(f.Body, responseWhitelist, r.Config.SummaryMax)
	if f.TooLarge {
		out.Truncated = true
	}
	if f.Status == 0 {
		out.ErrorCode = constant.ErrConnection
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			out.ErrorCode = constant.ErrHTTPTimeout
		}
	}
	return out, nil
}

// Interpret 按 mock-json@1 协议解释单次响应。只有同一业务幂等键的明确接收能覆盖未决尝试。
func (r *Runtime) Interpret(_ context.Context, task data_interface.TaskRecord, _ data_interface.BuiltRequest, sent data_interface.SendResponse) (data_interface.Interpreted, error) {
	pr := domain.InterpretMockJSON(sent.Facts)
	out := data_interface.Interpreted{
		Outcome: pr.Outcome, Certainty: pr.Certainty, Retryable: pr.Retryable, ErrorCode: pr.ErrorCode,
		ProviderRequestID: pr.ProviderRequestID, SupplierNotBefore: pr.WaitUntil,
	}
	if pr.ErrorCode != "" {
		out.ErrorReason = "provider code " + pr.ProviderCode
	}
	ft, err := domain.ParseFixedTarget(task.FixedTarget)
	if err == nil && pr.Outcome == enums.OutcomeReceived && ft.IdemMode == "key" {
		out.CoversBusinessKey = true
		for _, u := range task.Unresolved {
			out.CoveredAttemptIDs = append(out.CoveredAttemptIDs, u.AttemptID)
		}
	}
	return out, nil
}

func (r *Runtime) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// windowStart 取全部未决尝试可能首次发送的最早时间；无法还原时保守取 started_at。
func windowStart(task data_interface.TaskRecord) time.Time {
	pending := map[uint64]bool{}
	for _, u := range task.Unresolved {
		pending[u.AttemptID] = true
	}
	var start time.Time
	for _, a := range task.Attempts {
		if !pending[a.AttemptID] {
			continue
		}
		t := a.StartedAt
		if a.RequestStartedAt != nil && a.RequestStartedAt.Before(t) {
			t = *a.RequestStartedAt
		}
		if start.IsZero() || t.Before(start) {
			start = t
		}
	}
	return start
}

func originAllowed(raw string, allowed []data_interface.Origin) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
	}
	for _, o := range allowed {
		if o.Scheme == u.Scheme && o.Host == u.Hostname() && strconv.Itoa(o.Port) == port {
			return true
		}
	}
	return false
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
