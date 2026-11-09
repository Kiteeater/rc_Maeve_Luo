package data_interface

import (
	"context"
	"errors"
	"time"

	"notifier/internal/domain"
)

// 凭证获取失败分类（S05 第 9 节）：TEMPORARY 保存 WAIT；其余 BLOCK。
var (
	ErrCredentialTemporary = errors.New("credential temporarily unavailable")
	ErrCredentialRevoked   = errors.New("credential revoked")
	ErrAccountMismatch     = errors.New("credential account mismatch")
	ErrCredentialReauth    = errors.New("credential reauthorization required")
	ErrRateLimiterDown     = errors.New("rate limiter unavailable")
)

// Origin 是允许的目标源。
type Origin struct {
	Scheme string
	Host   string
	Port   int
}

// ChannelState 是通道当前约束（MySQL 为准）。账号与操作归属不可通过通道修改重新解释旧任务。
type ChannelState struct {
	ChannelID         uint64
	RowVersion        uint32
	ProviderID        string
	ProviderAccountID string
	Operation         string
	AdapterType       string
	AdapterVersion    string
	CredentialRef     string
	Enabled           bool
	AllowedCallers    []string
	AllowedOrigins    []Origin
	AllowedCIDRs      []string
	QuotaKey          string
	RatePerSecond     int64
	Burst             int64
	PolicyVersion     uint32
}

// ChannelStateReader 读取通道当前约束。
type ChannelStateReader interface {
	LoadChannelState(ctx context.Context, channelID uint64) (ChannelState, error)
}

// RateLimiter 是按共享 quota_key 的原子令牌桶。不可用时返回 ErrRateLimiterDown，不放行。
type RateLimiter interface {
	Take(ctx context.Context, quotaKey string, rate, burst int64, policyVersion uint32) (ok bool, notBefore time.Time, err error)
}

// CredentialHandle 是短期凭证句柄。Opaque 只由 data/provider 解引用，不进入日志或持久化。
type CredentialHandle struct {
	Version   uint32
	ExpiresAt time.Time
	Account   string
	Opaque    string
}

// CredentialResolver 按逻辑引用和原账号解析凭证。
type CredentialResolver interface {
	ResolveCredential(ctx context.Context, ref, account string) (CredentialHandle, error)
}

// HTTPCaller 发送一次受控 HTTP：不跟随重定向、不隐式重试、有界读取、检查实际连接目标。
type HTTPCaller interface {
	Do(ctx context.Context, req BuiltRequest) domain.TransportFacts
}

// CredentialState 是需要 Refresh Token 的凭证持久状态。不含秘密。
type CredentialState struct {
	Ref             string
	Account         string
	Version         uint32
	State           string // ACTIVE / REFRESHING / REAUTH_REQUIRED / REVOKED
	RefreshOwner    string
	RefreshDeadline time.Time
}

// CredentialStore 以持久 CAS 协调刷新；Redis 锁只做减压，不承担正确性。
type CredentialStore interface {
	LoadCredential(ctx context.Context, ref string) (CredentialState, error)
	BeginRefresh(ctx context.Context, ref string, expectVersion uint32, owner string, deadline time.Time) (bool, error)
	CommitRefresh(ctx context.Context, ref string, expectVersion uint32, owner string) (uint32, bool, error)
	MarkReauth(ctx context.Context, ref string, owner string) error
}

// TokenIssuer 调用供应商刷新接口并把新秘密写入受保护存储；返回后才允许 CommitRefresh。
type TokenIssuer interface {
	Refresh(ctx context.Context, ref string, newVersion uint32) error
}
