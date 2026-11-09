package provider

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"

	"notifier/internal/conf"
	"notifier/internal/data_interface"
)

// FileSecrets 是本地/测试环境的秘密来源：JSON 文件 {credential_ref: secret}。生产由密钥服务替换。
type FileSecrets struct {
	mu      sync.RWMutex
	secrets map[string]string
}

// NewFileSecrets 读取 PROVIDER_SECRETS_FILE；未配置时为空集合（所有需要鉴权的请求 WAIT）。
func NewFileSecrets(c conf.Provider) (*FileSecrets, error) {
	fs := &FileSecrets{secrets: map[string]string{}}
	if c.SecretsFile == "" {
		return fs, nil
	}
	raw, err := os.ReadFile(c.SecretsFile)
	if err != nil {
		return nil, errors.New("provider: secrets file unreadable")
	}
	if err := json.Unmarshal(raw, &fs.secrets); err != nil {
		return nil, errors.New("provider: secrets file invalid")
	}
	return fs, nil
}

// Set 仅用于测试与本地注入。
func (f *FileSecrets) Set(ref, secret string) {
	f.mu.Lock()
	f.secrets[ref] = secret
	f.mu.Unlock()
}

// Secret 解析 "ref#version" 句柄。
func (f *FileSecrets) Secret(handle string) (string, error) {
	ref, _, ok := strings.Cut(handle, "#")
	if !ok {
		return "", data_interface.ErrCredentialTemporary
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, ok := f.secrets[ref]
	if !ok {
		return "", data_interface.ErrCredentialTemporary
	}
	return s, nil
}

// Resolver 实现 CredentialResolver：以持久凭证状态为准，按版本生成不透明句柄。
type Resolver struct {
	Store   data_interface.CredentialStore
	Secrets SecretSource
}

func (r *Resolver) ResolveCredential(ctx context.Context, ref, account string) (data_interface.CredentialHandle, error) {
	st, err := r.Store.LoadCredential(ctx, ref)
	if err != nil {
		return data_interface.CredentialHandle{}, errors.Join(data_interface.ErrCredentialTemporary, err)
	}
	switch {
	case st.Account != account:
		return data_interface.CredentialHandle{}, data_interface.ErrAccountMismatch
	case st.State == "REVOKED":
		return data_interface.CredentialHandle{}, data_interface.ErrCredentialRevoked
	case st.State == "REAUTH_REQUIRED":
		return data_interface.CredentialHandle{}, data_interface.ErrCredentialReauth
	case st.State == "REFRESHING":
		return data_interface.CredentialHandle{}, data_interface.ErrCredentialTemporary
	}
	h := ref + "#" + strconv.FormatUint(uint64(st.Version), 10)
	if _, err := r.Secrets.Secret(h); err != nil {
		return data_interface.CredentialHandle{}, data_interface.ErrCredentialTemporary
	}
	return data_interface.CredentialHandle{Version: st.Version, Account: st.Account, Opaque: h}, nil
}
