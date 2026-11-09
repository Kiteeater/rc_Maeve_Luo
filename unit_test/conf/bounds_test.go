package conf_test

import (
	"context"
	"strings"
	"testing"

	"notifier/internal/conf"
)

func TestC08_RelationshipAndBoundsRejectSecrets(t *testing.T) {
	const secret = "supersecret-s00-value"
	cases := []struct {
		name  string
		key   string
		value string
		want  []string
	}{
		{name: "http timeout eats the lease", key: "DELIVERY_HTTP_TIMEOUT", value: "29s", want: []string{"DELIVERY_HTTP_TIMEOUT", "DELIVERY_LEASE_TIMEOUT", "MYSQL_OPERATION_TIMEOUT"}},
		{name: "invisible shorter than processing budget", key: "DELIVERY_MQ_INVISIBLE", value: "20s", want: []string{"DELIVERY_MQ_INVISIBLE", "DELIVERY_LEASE_TIMEOUT", "MYSQL_OPERATION_TIMEOUT"}},
		{name: "request timeout too high", key: "HTTP_REQUEST_TIMEOUT", value: "31s", want: []string{"HTTP_REQUEST_TIMEOUT"}},
		{name: "request timeout too low", key: "HTTP_REQUEST_TIMEOUT", value: "999ms", want: []string{"HTTP_REQUEST_TIMEOUT"}},
		{name: "mysql timeout too low", key: "MYSQL_OPERATION_TIMEOUT", value: "50ms", want: []string{"MYSQL_OPERATION_TIMEOUT"}},
		{name: "mysql timeout too high", key: "MYSQL_OPERATION_TIMEOUT", value: "6s", want: []string{"MYSQL_OPERATION_TIMEOUT"}},
		{name: "shutdown too low", key: "HTTP_SHUTDOWN_TIMEOUT", value: "4s", want: []string{"HTTP_SHUTDOWN_TIMEOUT"}},
		{name: "shutdown too high", key: "HTTP_SHUTDOWN_TIMEOUT", value: "121s", want: []string{"HTTP_SHUTDOWN_TIMEOUT"}},
		{name: "header too small", key: "HTTP_MAX_HEADER_BYTES", value: "512", want: []string{"HTTP_MAX_HEADER_BYTES"}},
		{name: "header too large", key: "HTTP_MAX_HEADER_BYTES", value: "65537", want: []string{"HTTP_MAX_HEADER_BYTES"}},
		{name: "summary too small", key: "SUMMARY_MAX_BYTES", value: "255", want: []string{"SUMMARY_MAX_BYTES"}},
		{name: "summary too large", key: "SUMMARY_MAX_BYTES", value: "8193", want: []string{"SUMMARY_MAX_BYTES"}},
		{name: "empty dsn", key: "MYSQL_DSN", value: "", want: []string{"MYSQL_DSN"}},
		{name: "empty auth ref", key: "AUTH_CONFIG_REF", value: "", want: []string{"AUTH_CONFIG_REF"}},
		{name: "duration parse hides the raw value", key: "DELIVERY_LEASE_TIMEOUT", value: secret, want: []string{"DELIVERY_LEASE_TIMEOUT"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			localConfig(t)
			t.Setenv("MYSQL_DSN", "user:"+secret+"@tcp(127.0.0.1:3306)/notifier")
			t.Setenv("REDIS_PASSWORD", secret)
			t.Setenv(tc.key, tc.value)
			cfg, err := conf.Load(context.Background())
			if err == nil || cfg != (conf.GlobalConfig{}) {
				t.Fatal("out of range config must fail closed")
			}
			for _, name := range tc.want {
				if !strings.Contains(err.Error(), name) {
					t.Fatalf("error %q does not name %s", err, name)
				}
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), tc.value) && tc.value == secret {
				t.Fatalf("error leaked a secret: %v", err)
			}
		})
	}
}

func TestC08_BoundaryValuesLoad(t *testing.T) {
	localConfig(t)
	t.Setenv("HTTP_REQUEST_TIMEOUT", "1s")
	t.Setenv("HTTP_REQUEST_TIMEOUT", "30s")
	t.Setenv("MYSQL_OPERATION_TIMEOUT", "100ms")
	t.Setenv("HTTP_SHUTDOWN_TIMEOUT", "5s")
	t.Setenv("HTTP_MAX_HEADER_BYTES", "1024")
	t.Setenv("SUMMARY_MAX_BYTES", "8192")
	// S03 收紧领取与不可见关系后，边界值同时满足批次 0 公式和新公式。
	t.Setenv("DELIVERY_BUILD_TIMEOUT", "10ms")
	t.Setenv("DELIVERY_HTTP_TIMEOUT", "100ms")
	t.Setenv("DELIVERY_RESULT_RESERVE", "101ms")
	t.Setenv("DELIVERY_LEASE_TIMEOUT", "5s")
	t.Setenv("DELIVERY_ACK_TIMEOUT", "100ms")
	t.Setenv("DELIVERY_CREDENTIAL_PREPARE", "100ms")
	t.Setenv("DELIVERY_MQ_INVISIBLE", "10200ms")
	t.Setenv("DELIVERY_MQ_MAX_EXTENSION", "10200ms")
	cfg, err := conf.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.RequestTimeout.String() != "30s" || cfg.Limits.SummaryBytes != 8192 {
		t.Fatalf("boundary values were not kept: %+v", cfg.HTTP)
	}
}
