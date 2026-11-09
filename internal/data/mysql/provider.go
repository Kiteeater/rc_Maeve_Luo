package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"notifier/internal/data_interface"
)

// ErrChannelNotFound 表示通道不存在。
var ErrChannelNotFound = errors.New("mysql: channel not found")

// ProviderStore 读取通道当前约束并以 CAS 维护凭证刷新状态（S05）。
type ProviderStore struct {
	db *sql.DB
}

func NewProviderStore(db *sql.DB) *ProviderStore { return &ProviderStore{db: db} }

type accessConstraints struct {
	AllowedCallers []string                `json:"allowed_callers"`
	AllowedOrigins []data_interface.Origin `json:"allowed_origins"`
	AllowedCIDRs   []string                `json:"allowed_cidrs"`
}

type runtimePolicy struct {
	QuotaKey          string `json:"quota_key"`
	RatePerSecond     int64  `json:"rate_per_second"`
	Burst             int64  `json:"burst"`
	RatePolicyVersion uint32 `json:"rate_policy_version"`
}

// LoadChannelState 每次执行读取 MySQL 当前值，不依赖可能过期的缓存放行。
func (s *ProviderStore) LoadChannelState(ctx context.Context, channelID uint64) (data_interface.ChannelState, error) {
	var st data_interface.ChannelState
	var ac, rp sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT channel_id, row_version, provider_id, provider_account_id, operation,
	adapter_type, adapter_version, credential_ref, enabled, access_constraints, runtime_policy
	FROM channels WHERE channel_id = ?`, channelID).Scan(&st.ChannelID, &st.RowVersion, &st.ProviderID,
		&st.ProviderAccountID, &st.Operation, &st.AdapterType, &st.AdapterVersion, &st.CredentialRef, &st.Enabled, &ac, &rp)
	if errors.Is(err, sql.ErrNoRows) {
		return st, ErrChannelNotFound
	}
	if err != nil {
		return st, err
	}
	var a accessConstraints
	var p runtimePolicy
	if ac.Valid {
		if err := json.Unmarshal([]byte(ac.String), &a); err != nil {
			return st, errors.New("mysql: access_constraints invalid")
		}
	}
	if rp.Valid {
		if err := json.Unmarshal([]byte(rp.String), &p); err != nil {
			return st, errors.New("mysql: runtime_policy invalid")
		}
	}
	st.AllowedCallers, st.AllowedOrigins, st.AllowedCIDRs = a.AllowedCallers, a.AllowedOrigins, a.AllowedCIDRs
	st.QuotaKey, st.RatePerSecond, st.Burst, st.PolicyVersion = p.QuotaKey, p.RatePerSecond, p.Burst, p.RatePolicyVersion
	// 无额度配置的通道不能放行。
	if st.QuotaKey == "" || st.RatePerSecond <= 0 || st.Burst <= 0 {
		st.Enabled = false
	}
	return st, nil
}

func (s *ProviderStore) LoadCredential(ctx context.Context, ref string) (data_interface.CredentialState, error) {
	var st data_interface.CredentialState
	var owner sql.NullString
	var deadline sql.NullTime
	err := s.db.QueryRowContext(ctx, `SELECT credential_ref, provider_account_id, version, state, refresh_owner, refresh_deadline
	FROM credential_refs WHERE credential_ref = ?`, ref).Scan(&st.Ref, &st.Account, &st.Version, &st.State, &owner, &deadline)
	if err != nil {
		return st, err
	}
	st.RefreshOwner = owner.String
	if deadline.Valid {
		st.RefreshDeadline = deadline.Time.UTC()
	}
	return st, nil
}

// BeginRefresh：仅 ACTIVE 且版本一致时进入 REFRESHING。过期的 REFRESHING 不被第二个实例接管（无法证明旧 Refresh Token 仍有效）。
func (s *ProviderStore) BeginRefresh(ctx context.Context, ref string, expect uint32, owner string, deadline time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE credential_refs SET state = 'REFRESHING', refresh_owner = ?, refresh_deadline = ?
	WHERE credential_ref = ? AND version = ? AND state = 'ACTIVE'`, owner, deadline.UTC(), ref, expect)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *ProviderStore) CommitRefresh(ctx context.Context, ref string, expect uint32, owner string) (uint32, bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE credential_refs SET state = 'ACTIVE', version = version + 1, refresh_owner = NULL, refresh_deadline = NULL
	WHERE credential_ref = ? AND version = ? AND state = 'REFRESHING' AND refresh_owner = ?`, ref, expect, owner)
	if err != nil {
		return 0, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return 0, false, err
	}
	return expect + 1, true, nil
}

func (s *ProviderStore) MarkReauth(ctx context.Context, ref, owner string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE credential_refs SET state = 'REAUTH_REQUIRED'
	WHERE credential_ref = ? AND state = 'REFRESHING' AND refresh_owner = ?`, ref, owner)
	return err
}
