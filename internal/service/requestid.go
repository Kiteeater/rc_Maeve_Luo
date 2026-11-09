package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"notifier/internal/domain"
)

type requestIDKey struct{}

// WithRequestID 把关联用的请求 ID 放进上下文。它不是幂等键。
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom 读取当前请求 ID。没有时返回空串。
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// NormalizeRequestID 接受 1～64 字节 ASCII 标识，否则生成 32 位小写十六进制。
func NormalizeRequestID(candidate string) string {
	if err := domain.ValidateASCIIIdent("request_id", candidate, 64); err == nil {
		return candidate
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(buf[:])
}
