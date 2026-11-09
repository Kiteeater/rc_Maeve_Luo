// Package logredact 在写出结构化日志前屏蔽凭证、口令和 DSN 密码。
package logredact

import (
	"context"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"

	"notifier/internal/conf"
)

const redacted = "[REDACTED]"

var (
	dsnPassword = regexp.MustCompile(`([A-Za-z0-9._%+-]*):([^@\s/]+)@`)
	bearerValue = regexp.MustCompile(`(?i)bearer\s+\S+`)
	assignValue = regexp.MustCompile(`(?i)\b(password|passwd|token|secret|authorization|dsn)\b\s*[=:]\s*([^\s,;]+)`)
)

// New 按配置构造默认脱敏的 slog.Logger。
func New(c conf.Log) *slog.Logger {
	return slog.New(Handler(os.Stdout, c.Format, level(c.Level)))
}

func level(name string) slog.Level {
	switch name {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Handler 把 JSON 或 text handler 包进脱敏层。
func Handler(w io.Writer, format string, lvl slog.Level) slog.Handler {
	opts := &slog.HandlerOptions{Level: lvl}
	var base slog.Handler
	if format == "text" {
		base = slog.NewTextHandler(w, opts)
	} else {
		base = slog.NewJSONHandler(w, opts)
	}
	return Wrap(base)
}

// Wrap 包装任意 Handler。敏感键的值，以及正文里的 Bearer、DSN 密码和 key=value 秘密，都替换为 [REDACTED]。
func Wrap(next slog.Handler) slog.Handler {
	return &handler{next: next}
}

type handler struct {
	next slog.Handler
}

func (h *handler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.next.Enabled(ctx, lvl)
}

func (h *handler) Handle(ctx context.Context, record slog.Record) error {
	clean := slog.NewRecord(record.Time, record.Level, Scrub(record.Message), record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		clean.AddAttrs(redactAttr(attr))
		return true
	})
	return h.next.Handle(ctx, clean)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cleaned := make([]slog.Attr, len(attrs))
	for i, attr := range attrs {
		cleaned[i] = redactAttr(attr)
	}
	return &handler{next: h.next.WithAttrs(cleaned)}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{next: h.next.WithGroup(name)}
}

func redactAttr(attr slog.Attr) slog.Attr {
	attr.Value = attr.Value.Resolve()
	if sensitiveKey(attr.Key) {
		attr.Value = slog.StringValue(redacted)
		return attr
	}
	switch attr.Value.Kind() {
	case slog.KindString:
		attr.Value = slog.StringValue(Scrub(attr.Value.String()))
	case slog.KindGroup:
		group := attr.Value.Group()
		for i := range group {
			group[i] = redactAttr(group[i])
		}
		attr.Value = slog.GroupValue(group...)
	}
	return attr
}

func sensitiveKey(key string) bool {
	k := strings.ToLower(key)
	for _, part := range []string{"authorization", "token", "password", "passwd", "secret", "dsn", "credential"} {
		if strings.Contains(k, part) {
			return true
		}
	}
	return false
}

// Scrub 替换正文中的 Bearer、DSN 用户密码和常见 key=value 秘密。
func Scrub(message string) string {
	message = dsnPassword.ReplaceAllString(message, "$1:"+redacted+"@")
	message = bearerValue.ReplaceAllString(message, "Bearer "+redacted)
	message = assignValue.ReplaceAllString(message, "$1="+redacted)
	return message
}
