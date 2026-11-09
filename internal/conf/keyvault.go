package conf

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

// KeyVault 仅保存连接信息和密钥名称，密钥值解析到对应配置字段。
type KeyVault struct {
	URL                 string
	Timeout             time.Duration
	MySQLDSNSecret      string
	RedisPasswordSecret string
}

// SecretReader 是启动阶段的密钥读取边界，不属于业务仓储接口。
type SecretReader interface {
	GetSecret(context.Context, string) (string, error)
}

// Loader 允许测试或其他部署环境提供密钥来源。
type Loader struct {
	Secrets SecretReader
}

func (l Loader) resolveSecrets(ctx context.Context, cfg *GlobalConfig) error {
	kv := cfg.KeyVault
	if kv.URL == "" {
		if kv.MySQLDSNSecret != "" || kv.RedisPasswordSecret != "" {
			return errors.New("conf: secret references require AZURE_KEY_VAULT_URL")
		}
		return nil // 本地开发使用环境变量与默认值。
	}
	u, err := url.Parse(kv.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("conf: AZURE_KEY_VAULT_URL must be an HTTPS vault URL")
	}
	if kv.MySQLDSNSecret == "" {
		return errors.New("conf: Key Vault mode requires MYSQL_DSN_SECRET")
	}
	if cfg.Redis.Password != "" && kv.RedisPasswordSecret == "" {
		return errors.New("conf: Redis authentication in Key Vault mode requires REDIS_PASSWORD_SECRET")
	}
	if kv.Timeout <= 0 {
		return errors.New("conf: AZURE_KEY_VAULT_TIMEOUT must be positive")
	}
	reader := l.Secrets
	if reader == nil {
		reader, err = newAzureSecretReader(kv.URL)
		if err != nil {
			return fmt.Errorf("conf: initialize Key Vault: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, kv.Timeout)
	defer cancel()
	for _, secret := range []struct {
		key  string
		name string
		dst  *string
	}{
		{"MYSQL_DSN_SECRET", kv.MySQLDSNSecret, &cfg.MySQL.DSN},
		{"REDIS_PASSWORD_SECRET", kv.RedisPasswordSecret, &cfg.Redis.Password},
	} {
		if secret.name == "" {
			continue
		}
		value, err := reader.GetSecret(ctx, secret.name)
		if err != nil {
			// SDK 错误可能包含响应正文，启动日志只记录失败字段。
			if ctx.Err() != nil {
				return fmt.Errorf("conf: resolve %s: %w", secret.key, ctx.Err())
			}
			return fmt.Errorf("conf: resolve %s failed", secret.key)
		}
		if value == "" {
			return fmt.Errorf("conf: %s resolved to an empty secret", secret.key)
		}
		*secret.dst = value
	}
	return nil
}

type azureSecretReader struct {
	client *azsecrets.Client
}

func newAzureSecretReader(vaultURL string) (SecretReader, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	client, err := azsecrets.NewClient(vaultURL, credential, nil)
	if err != nil {
		return nil, err
	}
	return &azureSecretReader{client: client}, nil
}

func (r *azureSecretReader) GetSecret(ctx context.Context, name string) (string, error) {
	response, err := r.client.GetSecret(ctx, name, "", nil)
	if err != nil {
		return "", err
	}
	if response.Value == nil {
		return "", errors.New("Key Vault returned no secret value")
	}
	return *response.Value, nil
}
