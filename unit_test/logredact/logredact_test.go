package logredact_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"notifier/internal/logredact"
)

func TestLogRedactionHidesSecrets(t *testing.T) {
	const secret = "supersecret-s00-value"
	var buf bytes.Buffer
	logger := slog.New(logredact.Handler(&buf, "json", slog.LevelInfo))
	logger.Info(
		"dial user:"+secret+"@tcp(127.0.0.1:3306)/notifier Authorization: Bearer "+secret+" password="+secret,
		slog.String("password", secret),
		slog.String("Authorization", "Bearer "+secret),
		slog.String("dsn", "notifier:"+secret+"@tcp(127.0.0.1:3306)/notifier"),
		slog.String("api_token", secret),
		slog.String("client_secret", secret),
		slog.String("credential_ref", secret),
		slog.Group("nested", slog.String("passwd", secret)),
	)
	got := buf.String()
	if strings.Contains(got, secret) {
		t.Fatalf("log leaked a secret: %s", got)
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("expected redaction marker, got %s", got)
	}
	if strings.Contains(got, "Bearer "+secret) || strings.Contains(got, ":"+secret+"@") {
		t.Fatalf("message still contains a credential: %s", got)
	}
}
