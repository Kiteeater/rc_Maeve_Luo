package conf_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"notifier/internal/conf"
)

type secretReader func(context.Context, string) (string, error)

func (f secretReader) GetSecret(ctx context.Context, name string) (string, error) {
	return f(ctx, name)
}

func localConfig(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"AZURE_KEY_VAULT_URL", "AZURE_KEY_VAULT_TIMEOUT", "MYSQL_DSN_SECRET", "REDIS_PASSWORD_SECRET",
		"HTTP_ADDR", "MYSQL_DSN", "REDIS_ADDR", "REDIS_PASSWORD", "REDIS_DB",
		"ROCKETMQ_ENDPOINT", "ROCKETMQ_TOPIC_IMMEDIATE", "ROCKETMQ_TOPIC_DELAYED", "ROCKETMQ_GROUP",
		"LOG_LEVEL", "LOG_FORMAT", "DELIVERY_PARALLELISM", "DELIVERY_MAX_ATTEMPTS",
		"DELIVERY_MAX_ALLOWED_ATTEMPTS", "DELIVERY_RECEIVE_BATCH",
		"DELIVERY_HTTP_TIMEOUT", "DELIVERY_BUILD_TIMEOUT", "DELIVERY_LEASE_TIMEOUT",
		"DELIVERY_RESULT_RESERVE", "DELIVERY_MQ_INVISIBLE", "DELIVERY_ACK_TIMEOUT",
		"DELIVERY_CREDENTIAL_WAIT", "DELIVERY_CREDENTIAL_PREPARE", "DELIVERY_RECEIVE_TIMEOUT",
		"DELIVERY_MQ_MAX_EXTENSION", "DELIVERY_MAX_ALLOWED_ROUND_TIMEOUT",
		"DELIVERY_BACKOFF_INITIAL", "DELIVERY_BACKOFF_CAP", "DELIVERY_SERVER_WAIT_CAP",
		"DELIVERY_ROUND_DEADLINE", "DELIVERY_BLOCKED_CHECK", "OUTBOX_POLL_INTERVAL",
		"OUTBOX_GRACE_PERIOD", "RECOVERY_SCAN_INTERVAL", "RETENTION_OVERVIEW_CACHE", "RETENTION_RESOLVED_AFTER",
		"HTTP_REQUEST_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT", "HTTP_MAX_HEADER_BYTES",
		"MYSQL_OPERATION_TIMEOUT", "AUTH_CONFIG_REF", "SUMMARY_MAX_BYTES",
		"CONFIG_MAX_BYTES", "CONFIG_MAX_ROUTES", "CONFIG_MAX_TARGETS", "CONFIG_MAX_CONDITIONS",
		"CONFIG_MAX_MAPPING_FIELDS", "CONFIG_MAX_PATH_BYTES", "CONFIG_MAX_EVENT_BYTES", "CONFIG_MAX_FANOUT",
		"CONFIG_CALCULATION_TIMEOUT", "CONFIG_CACHE_TTL",
	} {
		t.Setenv(key, "")
	}
	// 这几项显式为空会启动失败；未设置才使用默认值。
	for _, key := range []string{"MYSQL_DSN", "REDIS_ADDR", "ROCKETMQ_ENDPOINT", "AUTH_CONFIG_REF"} {
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLocalConfig(t *testing.T) {
	localConfig(t)
	cfg, err := conf.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cfg != conf.Default() {
		t.Fatal("unset environment must preserve local defaults")
	}
	t.Setenv("HTTP_ADDR", ":8090")
	t.Setenv("REDIS_DB", "2")
	t.Setenv("DELIVERY_HTTP_TIMEOUT", "3s")
	t.Setenv("PROVIDER_HEADER_TIMEOUT", "3s")
	cfg, err = conf.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Addr != ":8090" || cfg.Redis.DB != 2 || cfg.Delivery.HTTPTimeout != 3*time.Second {
		t.Fatal("environment overrides were not applied")
	}
}

func TestInvalidEnvironment(t *testing.T) {
	for _, key := range []string{"REDIS_DB", "DELIVERY_HTTP_TIMEOUT"} {
		t.Run(key, func(t *testing.T) {
			localConfig(t)
			t.Setenv(key, "invalid")
			cfg, err := conf.Load(context.Background())
			if err == nil || !strings.Contains(err.Error(), key) || cfg != (conf.GlobalConfig{}) {
				t.Fatalf("expected a named error and no partial config, got %v", err)
			}
		})
	}
}

func TestKeyVaultOverridesLocalSecrets(t *testing.T) {
	localConfig(t)
	t.Setenv("AZURE_KEY_VAULT_URL", "https://notifier.vault.azure.net/")
	t.Setenv("MYSQL_DSN_SECRET", "mysql-dsn")
	t.Setenv("REDIS_PASSWORD_SECRET", "redis-password")
	t.Setenv("MYSQL_DSN", "local-dsn")
	t.Setenv("REDIS_PASSWORD", "local-password")
	values := map[string]string{"mysql-dsn": "vault-dsn", "redis-password": "vault-password"}
	loader := conf.Loader{Secrets: secretReader(func(ctx context.Context, name string) (string, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("secret lookup must have a startup deadline")
		}
		return values[name], nil
	})}
	cfg, err := loader.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MySQL.DSN != "vault-dsn" || cfg.Redis.Password != "vault-password" {
		t.Fatal("vault secrets must take precedence over local values")
	}
}

func TestKeyVaultFailsWithoutFallback(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		err   error
	}{
		{name: "unavailable", err: errors.New("response contains sensitive-value")},
		{name: "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			localConfig(t)
			t.Setenv("AZURE_KEY_VAULT_URL", "https://notifier.vault.azure.net")
			t.Setenv("MYSQL_DSN_SECRET", "mysql-dsn")
			loader := conf.Loader{Secrets: secretReader(func(context.Context, string) (string, error) {
				return tc.value, tc.err
			})}
			cfg, err := loader.Load(context.Background())
			if err == nil || cfg != (conf.GlobalConfig{}) {
				t.Fatal("failed secret lookup must not fall back to local credentials")
			}
			if !strings.Contains(err.Error(), "MYSQL_DSN_SECRET") || strings.Contains(err.Error(), "sensitive-value") {
				t.Fatalf("secret error must identify the field without leaking the SDK response: %v", err)
			}
		})
	}
}

func TestKeyVaultConfigurationValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		url     string
		secret  string
		timeout string
		redis   string
	}{
		{name: "reference without vault", secret: "mysql-dsn"},
		{name: "vault without reference", url: "https://notifier.vault.azure.net"},
		{name: "insecure vault", url: "http://notifier.vault.azure.net", secret: "mysql-dsn"},
		{name: "invalid timeout", url: "https://notifier.vault.azure.net", secret: "mysql-dsn", timeout: "0s"},
		{name: "Redis password without reference", url: "https://notifier.vault.azure.net", secret: "mysql-dsn", redis: "local-password"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			localConfig(t)
			t.Setenv("AZURE_KEY_VAULT_URL", tc.url)
			t.Setenv("MYSQL_DSN_SECRET", tc.secret)
			t.Setenv("AZURE_KEY_VAULT_TIMEOUT", tc.timeout)
			t.Setenv("REDIS_PASSWORD", tc.redis)
			loader := conf.Loader{Secrets: secretReader(func(context.Context, string) (string, error) {
				t.Fatal("invalid config must fail before reading secrets")
				return "", nil
			})}
			if _, err := loader.Load(context.Background()); err == nil {
				t.Fatal("expected invalid Key Vault config to fail")
			}
		})
	}
}

func TestKeyVaultHonorsCancellation(t *testing.T) {
	localConfig(t)
	t.Setenv("AZURE_KEY_VAULT_URL", "https://notifier.vault.azure.net")
	t.Setenv("MYSQL_DSN_SECRET", "mysql-dsn")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loader := conf.Loader{Secrets: secretReader(func(ctx context.Context, _ string) (string, error) {
		return "", ctx.Err()
	})}
	if _, err := loader.Load(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup must preserve context cancellation, got %v", err)
	}
}

func TestKeyVaultTimeout(t *testing.T) {
	localConfig(t)
	t.Setenv("AZURE_KEY_VAULT_URL", "https://notifier.vault.azure.net")
	t.Setenv("AZURE_KEY_VAULT_TIMEOUT", "5ms")
	t.Setenv("MYSQL_DSN_SECRET", "mysql-dsn")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	parentDeadline, _ := ctx.Deadline()
	loader := conf.Loader{Secrets: secretReader(func(ctx context.Context, _ string) (string, error) {
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Before(parentDeadline) {
			t.Fatal("secret lookup must apply the shorter configured timeout")
		}
		<-ctx.Done()
		return "", ctx.Err()
	})}
	if _, err := loader.Load(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("secret lookup must stop at its deadline, got %v", err)
	}
}

func TestKeyVaultDiscardsPartialConfig(t *testing.T) {
	localConfig(t)
	t.Setenv("AZURE_KEY_VAULT_URL", "https://notifier.vault.azure.net")
	t.Setenv("MYSQL_DSN_SECRET", "mysql-dsn")
	t.Setenv("REDIS_PASSWORD_SECRET", "redis-password")
	loader := conf.Loader{Secrets: secretReader(func(_ context.Context, name string) (string, error) {
		if name == "mysql-dsn" {
			return "resolved-dsn", nil
		}
		return "", errors.New("Redis secret unavailable")
	})}
	cfg, err := loader.Load(context.Background())
	if err == nil || !strings.Contains(err.Error(), "REDIS_PASSWORD_SECRET") || cfg != (conf.GlobalConfig{}) {
		t.Fatalf("later secret failure must discard already resolved values, got %v", err)
	}
}
