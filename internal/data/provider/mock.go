// Package provider 提供 S01 发布用的最小适配器注册表。
// 完整适配器由 S05 完成。这里不发送请求、不刷新凭证、不限流。
package provider

import (
	"context"
	"net/url"
	"strings"

	"notifier/internal/canonicaljson"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

const (
	mockType    = "mock-json"
	mockVersion = "1"
	maxPayload  = 256 << 10
)

// Registry 按 adapter_type 与 adapter_version 查找适配器。
type Registry struct {
	items map[string]data_interface.Adapter
}

// NewMockRegistry 只注册 mock-json/1。完整适配器由 S05 完成。
func NewMockRegistry() *Registry {
	reg := &Registry{items: map[string]data_interface.Adapter{}}
	reg.items[adapterKey(mockType, mockVersion)] = mockJSON{}
	return reg
}

func adapterKey(adapterType, adapterVersion string) string {
	return adapterType + "\x00" + adapterVersion
}

// Lookup 判断该类型和版本是否已注册。
func (r *Registry) Lookup(adapterType, adapterVersion string) (data_interface.Adapter, bool) {
	if r == nil {
		return nil, false
	}
	item, ok := r.items[adapterKey(adapterType, adapterVersion)]
	return item, ok
}

type mockJSON struct{}

// ValidateTarget 只检查 mock-json/1 的本地语义。不访问网络。
func (mockJSON) ValidateTarget(_ context.Context, spec data_interface.TargetSpec) error {
	root, err := canonicaljson.ParseObject(spec.SemanticCanonical)
	if err != nil {
		return semanticErr("/", "目标语义不是对象")
	}
	fields, _ := root.Object()
	allow := map[string]struct{}{"base_url": {}, "path": {}, "auth_type": {}, "idempotency": {}}
	for _, field := range fields {
		if secretKey(field.Key) {
			return semanticErr("/"+field.Key, "目标语义不能包含秘密")
		}
		if _, ok := allow[field.Key]; !ok {
			return semanticErr("/"+field.Key, "目标语义包含未知字段")
		}
	}
	base, ok := textField(root, "base_url")
	if !ok || !validBaseURL(base) {
		return semanticErr("/base_url", "base_url 不合法")
	}
	path, ok := textField(root, "path")
	if !ok || !validPath(path) {
		return semanticErr("/path", "path 不合法")
	}
	authType, ok := textField(root, "auth_type")
	if !ok || (authType != "bearer" && authType != "hmac_sha256" && authType != "none") {
		return semanticErr("/auth_type", "auth_type 不合法")
	}
	idem, ok := root.Member("idempotency")
	if !ok || idem.Kind() != canonicaljson.KindObject {
		return semanticErr("/idempotency", "idempotency 必须是对象")
	}
	return checkIdempotency(idem)
}

// ValidatePayload 确认映射结果是对象且不超过载荷上限。不访问网络。
func (mockJSON) ValidatePayload(_ context.Context, payload, _ []byte) error {
	if len(payload) > maxPayload {
		return payloadErr("映射载荷超过上限")
	}
	value, err := canonicaljson.Parse(payload)
	if err != nil || value.Kind() != canonicaljson.KindObject {
		return payloadErr("映射载荷必须是对象")
	}
	return nil
}

func checkIdempotency(v canonicaljson.Value) error {
	fields, _ := v.Object()
	allow := map[string]struct{}{"mode": {}, "window_seconds": {}, "starts_at": {}}
	for _, field := range fields {
		if secretKey(field.Key) {
			return semanticErr("/idempotency/"+field.Key, "目标语义不能包含秘密")
		}
		if _, ok := allow[field.Key]; !ok {
			return semanticErr("/idempotency/"+field.Key, "目标语义包含未知字段")
		}
	}
	mode, ok := textField(v, "mode")
	if !ok || (mode != "key" && mode != "none") {
		return semanticErr("/idempotency/mode", "idempotency.mode 不合法")
	}
	window, ok := v.Member("window_seconds")
	n, okN := window.Int64()
	if !ok || !okN {
		return semanticErr("/idempotency/window_seconds", "window_seconds 必须是整数")
	}
	if mode == "key" && (n < 1 || n > 604800) {
		return semanticErr("/idempotency/window_seconds", "window_seconds 超出范围")
	}
	if mode == "none" && n != 0 {
		return semanticErr("/idempotency/window_seconds", "mode 为 none 时 window_seconds 必须为 0")
	}
	starts, ok := textField(v, "starts_at")
	if !ok || starts != "first_possible_send" {
		return semanticErr("/idempotency/starts_at", "starts_at 不合法")
	}
	return nil
}

func validBaseURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

func validPath(path string) bool {
	return strings.HasPrefix(path, "/") && !strings.ContainsAny(path, "?#")
}

func secretKey(key string) bool {
	switch strings.ToLower(key) {
	case "token", "secret", "password", "authorization", "api_key", "refresh_token":
		return true
	default:
		return false
	}
}

func textField(v canonicaljson.Value, key string) (string, bool) {
	field, ok := v.Member(key)
	if !ok {
		return "", false
	}
	return field.Text()
}

func semanticErr(path, message string) error {
	err, _ := domain.NewError("semantic_invalid", message, path, nil)
	return err
}

func payloadErr(message string) error {
	err, _ := domain.NewError("mapping_invalid", message, "/", nil)
	return err
}
