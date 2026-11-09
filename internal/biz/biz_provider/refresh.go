package biz_provider

import (
	"context"
	"errors"
	"time"

	"notifier/internal/data_interface"
)

// Refresher 协调需要 Refresh Token 轮换的凭证刷新（PRV-04）：先持久 CAS 进入 REFRESHING，
// 调用供应商并保存新秘密后再 CAS 提交新版本；提交结果不确定或失去所有权时不盲目沿用旧 Refresh Token。
type Refresher struct {
	Store  data_interface.CredentialStore
	Issuer data_interface.TokenIssuer
	Budget time.Duration
	Now    func() time.Time
}

// Refresh 返回新版本。并发刷新时只有一个实例持有刷新权，其余返回 ErrCredentialTemporary。
func (r *Refresher) Refresh(ctx context.Context, ref, owner string) (uint32, error) {
	st, err := r.Store.LoadCredential(ctx, ref)
	if err != nil {
		return 0, err
	}
	switch st.State {
	case "REVOKED":
		return 0, data_interface.ErrCredentialRevoked
	case "REAUTH_REQUIRED":
		return 0, data_interface.ErrCredentialReauth
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now()
	}
	won, err := r.Store.BeginRefresh(ctx, ref, st.Version, owner, now.Add(r.Budget))
	if err != nil {
		return 0, err
	}
	if !won {
		return 0, data_interface.ErrCredentialTemporary
	}
	if err := r.Issuer.Refresh(ctx, ref, st.Version+1); err != nil {
		// 供应商可能已轮换而本地没有新凭证：不能证明旧 Refresh Token 仍有效，转可见异常。
		_ = r.Store.MarkReauth(ctx, ref, owner)
		return 0, data_interface.ErrCredentialReauth
	}
	v, ok, err := r.Store.CommitRefresh(ctx, ref, st.Version, owner)
	if err != nil {
		// 提交不确定：重新读取持久版本，不以旧结果覆盖。
		cur, lerr := r.Store.LoadCredential(ctx, ref)
		if lerr == nil && cur.Version == st.Version+1 && cur.State == "ACTIVE" {
			return cur.Version, nil
		}
		return 0, errors.Join(data_interface.ErrCredentialTemporary, err)
	}
	if !ok {
		return 0, data_interface.ErrCredentialTemporary
	}
	return v, nil
}
