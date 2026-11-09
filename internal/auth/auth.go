// Package auth 把受保护的身份配置解析成调用上下文。
//
// 服务只保存 Bearer 原始字节的 SHA-256，并用恒定时间比较。
// 真实 Token 不进入领域对象、数据库参数或日志。
// 单次进程使用启动时加载的配置；轮换和撤销通过滚动重启生效，本包不做热更新。
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/domain"
)

const bearerPrefix = "Bearer "

var (
	// ErrUnauthenticated 表示没有匹配的凭证。
	ErrUnauthenticated = errors.New("auth: unauthenticated")
	// ErrDisabled 表示凭证匹配到已禁用身份。与错误凭证区分开。
	ErrDisabled = errors.New("auth: identity disabled")
)

// Context 是认证层输出的调用上下文。凭证本身不在其中。
type Context struct {
	ActorID        string
	CallerID       string
	AllowedSources []string
	Permissions    []string
	CallerScopes   []string
}

// Allows 只判断权限码是否存在。写操作还要另查 CallerScopes，ops:read 不隐含写权限。
func (c Context) Allows(permission string) bool {
	for _, item := range c.Permissions {
		if item == permission {
			return true
		}
	}
	return false
}

// ScopeContains 判断目标调用方是否在本次身份的明确范围内。
func (c Context) ScopeContains(callerID string) bool {
	for _, item := range c.CallerScopes {
		if item == callerID {
			return true
		}
	}
	return false
}

// Identity 是一份已校验的身份。TokenSHA256 为小写十六进制。
type Identity struct {
	ActorID        string
	CallerID       string
	TokenSHA256    string
	AllowedSources []string
	CallerScopes   []string
	Permissions    []string
	Enabled        bool
}

// Set 是进程内生效的身份集合。
type Set struct {
	items []stored
}

type stored struct {
	hash    [32]byte
	enabled bool
	ctx     Context
}

// Available 在至少有一个启用身份时为真。没有可用身份时进程不得对外就绪。
func (s *Set) Available() bool {
	if s == nil {
		return false
	}
	for _, item := range s.items {
		if item.enabled {
			return true
		}
	}
	return false
}

// Authenticate 用 Bearer 原始字节的 SHA-256 与全部身份做恒定时间比较。
// 不因先行命中而提前返回。禁用身份与未知凭证是不同错误。
func (s *Set) Authenticate(token string) (Context, error) {
	if s == nil || token == "" {
		return Context{}, ErrUnauthenticated
	}
	sum := sha256.Sum256([]byte(token))
	var matched *stored
	found := 0
	for i := range s.items {
		eq := subtle.ConstantTimeCompare(sum[:], s.items[i].hash[:])
		if eq == 1 && found == 0 {
			matched = &s.items[i]
		}
		found |= eq
	}
	if found == 0 || matched == nil {
		return Context{}, ErrUnauthenticated
	}
	if !matched.enabled {
		return Context{}, ErrDisabled
	}
	return matched.ctx, nil
}

// BearerToken 只接受精确前缀 "Bearer "。前后多余空白视为未认证。
func BearerToken(header string) (string, error) {
	if !strings.HasPrefix(header, bearerPrefix) {
		return "", ErrUnauthenticated
	}
	token := header[len(bearerPrefix):]
	if token == "" || strings.ContainsAny(token, " \t") {
		return "", ErrUnauthenticated
	}
	return token, nil
}

// NewStatic 校验并装载调用方注入的身份。本地测试用它代替文件。
func NewStatic(list []Identity) (*Set, error) {
	return build(list)
}

// LoadFile 读取 AUTH_CONFIG_REF 指向的身份文件。错误不包含 Token 或哈希。
func LoadFile(path string) (*Set, error) {
	if path == "" {
		return nil, errors.New("auth: AUTH_CONFIG_REF must not be empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("auth: AUTH_CONFIG_REF: read failed")
	}
	list, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return build(list)
}

func build(list []Identity) (*Set, error) {
	if len(list) == 0 {
		return nil, errors.New("auth: AUTH_CONFIG_REF: no usable identity")
	}
	seenHash := make(map[string]struct{}, len(list))
	seenActor := make(map[string]struct{}, len(list))
	items := make([]stored, 0, len(list))
	enabled := 0
	for _, item := range list {
		if err := validateIdentity(item); err != nil {
			return nil, err
		}
		if _, ok := seenHash[item.TokenSHA256]; ok {
			return nil, errors.New("auth: AUTH_CONFIG_REF: duplicate token_sha256")
		}
		seenHash[item.TokenSHA256] = struct{}{}
		if _, ok := seenActor[item.ActorID]; ok {
			return nil, errors.New("auth: AUTH_CONFIG_REF: duplicate actor_id")
		}
		seenActor[item.ActorID] = struct{}{}
		decoded, err := hex.DecodeString(item.TokenSHA256)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != item.TokenSHA256 {
			return nil, errors.New("auth: AUTH_CONFIG_REF: invalid token_sha256")
		}
		var hash [32]byte
		copy(hash[:], decoded)
		if item.Enabled {
			enabled++
		}
		items = append(items, stored{
			hash:    hash,
			enabled: item.Enabled,
			ctx: Context{
				ActorID:        item.ActorID,
				CallerID:       item.CallerID,
				AllowedSources: append([]string(nil), item.AllowedSources...),
				Permissions:    append([]string(nil), item.Permissions...),
				CallerScopes:   append([]string(nil), item.CallerScopes...),
			},
		})
	}
	if enabled == 0 {
		return nil, errors.New("auth: AUTH_CONFIG_REF: no usable identity")
	}
	return &Set{items: items}, nil
}

func validateIdentity(item Identity) error {
	if err := domain.ValidateActorID(item.ActorID); err != nil {
		return errors.New("auth: AUTH_CONFIG_REF: invalid actor_id")
	}
	if err := domain.ValidateCallerID(item.CallerID); err != nil {
		return errors.New("auth: AUTH_CONFIG_REF: invalid caller_id")
	}
	if len(item.TokenSHA256) != sha256.Size*2 {
		return errors.New("auth: AUTH_CONFIG_REF: invalid token_sha256")
	}
	if len(item.CallerScopes) == 0 {
		return errors.New("auth: AUTH_CONFIG_REF: empty caller_scopes")
	}
	if len(item.CallerScopes) > constant.MaxCallerScopes {
		return errors.New("auth: AUTH_CONFIG_REF: caller_scopes exceed limit")
	}
	if err := uniqueIdents("caller_scopes", item.CallerScopes, domain.ValidateCallerID); err != nil {
		return err
	}
	if err := uniqueIdents("allowed_sources", item.AllowedSources, domain.ValidateSourceBusiness); err != nil {
		return err
	}
	seenPerm := make(map[string]struct{}, len(item.Permissions))
	for _, perm := range item.Permissions {
		if !constant.KnownPermission(perm) {
			return errors.New("auth: AUTH_CONFIG_REF: unknown permission")
		}
		if _, ok := seenPerm[perm]; ok {
			return errors.New("auth: AUTH_CONFIG_REF: duplicate permission")
		}
		seenPerm[perm] = struct{}{}
	}
	return nil
}

func uniqueIdents(field string, values []string, check func(string) error) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := check(value); err != nil {
			return fmt.Errorf("auth: AUTH_CONFIG_REF: invalid %s", field)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("auth: AUTH_CONFIG_REF: duplicate %s", field)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func parseConfig(raw []byte) ([]Identity, error) {
	root, err := canonicaljson.ParseObject(raw)
	if err != nil {
		return nil, errors.New("auth: AUTH_CONFIG_REF: invalid identity config")
	}
	fields, err := objectMap(root)
	if err != nil {
		return nil, errors.New("auth: AUTH_CONFIG_REF: invalid identity config")
	}
	if err := exactKeys(fields, "format_version", "identities"); err != nil {
		return nil, err
	}
	version, ok := fields["format_version"].NumberCanonical()
	if !ok || version != "1e0" {
		return nil, errors.New("auth: AUTH_CONFIG_REF: unsupported format_version")
	}
	items, ok := fields["identities"].Array()
	if !ok {
		return nil, errors.New("auth: AUTH_CONFIG_REF: invalid identities")
	}
	list := make([]Identity, 0, len(items))
	for _, item := range items {
		identity, err := parseIdentity(item)
		if err != nil {
			return nil, err
		}
		list = append(list, identity)
	}
	return list, nil
}

func parseIdentity(v canonicaljson.Value) (Identity, error) {
	fields, err := objectMap(v)
	if err != nil {
		return Identity{}, errors.New("auth: AUTH_CONFIG_REF: invalid identity")
	}
	if err := exactKeys(fields, "actor_id", "caller_id", "token_sha256", "allowed_sources", "caller_scopes", "permissions", "enabled"); err != nil {
		return Identity{}, err
	}
	actor, err := requireText(fields, "actor_id")
	if err != nil {
		return Identity{}, err
	}
	caller, err := requireText(fields, "caller_id")
	if err != nil {
		return Identity{}, err
	}
	token, err := requireText(fields, "token_sha256")
	if err != nil {
		return Identity{}, err
	}
	enabled, ok := fields["enabled"].Bool()
	if !ok {
		return Identity{}, errors.New("auth: AUTH_CONFIG_REF: invalid enabled")
	}
	sources, err := stringArray(fields, "allowed_sources")
	if err != nil {
		return Identity{}, err
	}
	scopes, err := stringArray(fields, "caller_scopes")
	if err != nil {
		return Identity{}, err
	}
	perms, err := stringArray(fields, "permissions")
	if err != nil {
		return Identity{}, err
	}
	return Identity{
		ActorID:        actor,
		CallerID:       caller,
		TokenSHA256:    token,
		AllowedSources: sources,
		CallerScopes:   scopes,
		Permissions:    perms,
		Enabled:        enabled,
	}, nil
}

func objectMap(v canonicaljson.Value) (map[string]canonicaljson.Value, error) {
	fields, ok := v.Object()
	if !ok {
		return nil, errors.New("auth: expected object")
	}
	out := make(map[string]canonicaljson.Value, len(fields))
	for _, field := range fields {
		out[field.Key] = field.Value
	}
	return out, nil
}

func exactKeys(fields map[string]canonicaljson.Value, keys ...string) error {
	want := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		want[key] = struct{}{}
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("auth: AUTH_CONFIG_REF: missing %s", key)
		}
	}
	for key := range fields {
		if _, ok := want[key]; !ok {
			return fmt.Errorf("auth: AUTH_CONFIG_REF: unknown field %s", key)
		}
	}
	return nil
}

func requireText(fields map[string]canonicaljson.Value, key string) (string, error) {
	text, ok := fields[key].Text()
	if !ok {
		return "", fmt.Errorf("auth: AUTH_CONFIG_REF: invalid %s", key)
	}
	return text, nil
}

func stringArray(fields map[string]canonicaljson.Value, key string) ([]string, error) {
	items, ok := fields[key].Array()
	if !ok {
		return nil, fmt.Errorf("auth: AUTH_CONFIG_REF: invalid %s", key)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.Text()
		if !ok {
			return nil, fmt.Errorf("auth: AUTH_CONFIG_REF: invalid %s", key)
		}
		out = append(out, text)
	}
	return out, nil
}
