// Package data_interface 定义 S01 用到的小端口。
// 适配器只暴露注册与本地校验；发送、凭证刷新和限流由 S05 完成。
package data_interface

import (
	"context"
	"errors"
	"slices"
	"time"

	"notifier/internal/canonicaljson"
	"notifier/internal/domain"
)

var (
	// ErrReleaseNotFound 表示该 release_id 没有持久化版本。
	ErrReleaseNotFound = errors.New("data_interface: release not found")
	// ErrNoCurrent 表示还没有当前发布。
	ErrNoCurrent = errors.New("data_interface: no current release")
	// ErrChannelNotFound 表示通道不存在。
	ErrChannelNotFound = errors.New("data_interface: channel not found")
	// ErrCredentialNotFound 表示凭证引用不存在。
	ErrCredentialNotFound = errors.New("data_interface: credential not found")
	// ErrCacheMiss 表示按版本的缓存没有命中，可以回源。
	ErrCacheMiss = errors.New("data_interface: cache miss")
	// ErrCacheCorrupt 表示缓存值无法对上发布 ID 或内容摘要，必须丢弃后回源。
	ErrCacheCorrupt = errors.New("data_interface: cache corrupt")
)

// ReleaseRecord 是一份不可变发布。内容以规范化字节为准。
type ReleaseRecord struct {
	ReleaseID          uint64
	VersionNo          uint32
	Status             string
	ContentCanonical   []byte
	ContentSHA256      string
	FingerprintVersion int
	ReferenceSnapshot  []byte
	CreatedAt          time.Time
	PublishedAt        *time.Time
}

// CreateDraftInput 是创建草稿的参数。引用版本由仓储在事务内读取。
type CreateDraftInput struct {
	ActorID          string
	Reason           string
	IdempotencyKey   string
	Fingerprint      string
	ContentCanonical []byte
	ContentSHA256    string
	Bindings         []domain.TargetBinding
}

// CreateDraftResult 是已提交的创建结果。相同操作键返回同一版本。
type CreateDraftResult struct {
	OperationID   string
	ReleaseID     uint64
	VersionNo     uint32
	Status        string
	ContentSHA256 string
}

// PublishInput 是发布参数。Expected 为 nil 表示首次发布。
type PublishInput struct {
	ActorID        string
	Reason         string
	IdempotencyKey string
	Fingerprint    string
	ReleaseID      uint64
	Expected       *uint64
}

// PublishResult 是已提交的发布结果。重放不得改写当前指针。
type PublishResult struct {
	OperationID string
	ReleaseID   uint64
	VersionNo   uint32
	Previous    *uint64
	PublishedAt time.Time
}

// ReleaseRepository 保存不可变草稿、当前指针和发布操作。
type ReleaseRepository interface {
	CreateDraft(ctx context.Context, in CreateDraftInput) (CreateDraftResult, error)
	Publish(ctx context.Context, in PublishInput) (PublishResult, error)
	Load(ctx context.Context, releaseID uint64) (ReleaseRecord, error)
	LoadCurrentID(ctx context.Context) (uint64, error)
}

// ChannelPin 是发布复核用的通道归属，不含启停和额度。
type ChannelPin struct {
	ChannelID         uint64
	RowVersion        uint32
	ProviderID        string
	ProviderAccountID string
	Operation         string
	AdapterType       string
	AdapterVersion    string
	CredentialRef     string
}

// CredentialPin 是逻辑凭证引用。不含秘密。完整存储由 S05 完成。
type CredentialPin struct {
	Ref               string
	Version           uint32
	ProviderID        string
	ProviderAccountID string
}

// ReferenceSet 是草稿冻结的引用版本。
type ReferenceSet struct {
	Channels    []ChannelPin
	Credentials []CredentialPin
}

// ChannelReader 读取通道归属。
type ChannelReader interface {
	GetChannel(ctx context.Context, id uint64) (ChannelPin, error)
}

// CredentialReader 读取凭证引用是否存在及其版本。
type CredentialReader interface {
	GetCredential(ctx context.Context, ref string) (CredentialPin, error)
}

// TargetSpec 是发布期交给适配器的固定目标。
type TargetSpec struct {
	RouteTargetID     string
	Operation         string
	CredentialRef     string
	SemanticCanonical []byte
}

// Adapter 只做本地校验。完整协议由 S05 完成。
type Adapter interface {
	ValidateTarget(ctx context.Context, spec TargetSpec) error
	ValidatePayload(ctx context.Context, payload, semantic []byte) error
}

// AdapterRegistry 按 adapter_type 与 adapter_version 查找已注册适配器。
type AdapterRegistry interface {
	Lookup(adapterType, adapterVersion string) (Adapter, bool)
}

// ReleaseCache 按发布 ID 缓存不可变内容。当前指针不经过这里。
type ReleaseCache interface {
	Get(ctx context.Context, releaseID uint64) (ReleaseRecord, error)
	Put(ctx context.Context, rec ReleaseRecord) error
	Delete(ctx context.Context, releaseID uint64) error
}

// SameReferences 比较两份引用版本。顺序无关。
func SameReferences(stored, live ReferenceSet) bool {
	if len(stored.Channels) != len(live.Channels) || len(stored.Credentials) != len(live.Credentials) {
		return false
	}
	channels := make(map[uint64]ChannelPin, len(live.Channels))
	for _, item := range live.Channels {
		channels[item.ChannelID] = item
	}
	for _, item := range stored.Channels {
		if channels[item.ChannelID] != item {
			return false
		}
	}
	creds := make(map[string]CredentialPin, len(live.Credentials))
	for _, item := range live.Credentials {
		creds[item.Ref] = item
	}
	for _, item := range stored.Credentials {
		if creds[item.Ref] != item {
			return false
		}
	}
	return true
}

// MarshalReferences 写成规范化 JSON，供发布事务保存。
func MarshalReferences(set ReferenceSet) ([]byte, error) {
	channels := append([]ChannelPin(nil), set.Channels...)
	slices.SortFunc(channels, func(a, b ChannelPin) int {
		if a.ChannelID < b.ChannelID {
			return -1
		}
		if a.ChannelID > b.ChannelID {
			return 1
		}
		return 0
	})
	creds := append([]CredentialPin(nil), set.Credentials...)
	slices.SortFunc(creds, func(a, b CredentialPin) int {
		if a.Ref < b.Ref {
			return -1
		}
		if a.Ref > b.Ref {
			return 1
		}
		return 0
	})
	chVals := make([]canonicaljson.Value, 0, len(channels))
	for _, item := range channels {
		id, err := canonicaljson.Parse([]byte(u64(item.ChannelID)))
		if err != nil {
			return nil, err
		}
		ver, err := canonicaljson.Uint(uint64(item.RowVersion))
		if err != nil {
			return nil, err
		}
		chVals = append(chVals, canonicaljson.Object([]canonicaljson.Field{
			{Key: "adapter_type", Value: canonicaljson.String(item.AdapterType)},
			{Key: "adapter_version", Value: canonicaljson.String(item.AdapterVersion)},
			{Key: "channel_id", Value: id},
			{Key: "credential_ref", Value: canonicaljson.String(item.CredentialRef)},
			{Key: "operation", Value: canonicaljson.String(item.Operation)},
			{Key: "provider_account_id", Value: canonicaljson.String(item.ProviderAccountID)},
			{Key: "provider_id", Value: canonicaljson.String(item.ProviderID)},
			{Key: "row_version", Value: ver},
		}))
	}
	credVals := make([]canonicaljson.Value, 0, len(creds))
	for _, item := range creds {
		ver, err := canonicaljson.Uint(uint64(item.Version))
		if err != nil {
			return nil, err
		}
		credVals = append(credVals, canonicaljson.Object([]canonicaljson.Field{
			{Key: "credential_ref", Value: canonicaljson.String(item.Ref)},
			{Key: "provider_account_id", Value: canonicaljson.String(item.ProviderAccountID)},
			{Key: "provider_id", Value: canonicaljson.String(item.ProviderID)},
			{Key: "version", Value: ver},
		}))
	}
	obj := canonicaljson.Object([]canonicaljson.Field{
		{Key: "channels", Value: canonicaljson.Array(chVals)},
		{Key: "credentials", Value: canonicaljson.Array(credVals)},
	})
	return canonicaljson.AppendCanonical(nil, obj), nil
}

// UnmarshalReferences 解析仓储中的引用快照。
func UnmarshalReferences(raw []byte) (ReferenceSet, error) {
	root, err := canonicaljson.ParseObject(raw)
	if err != nil {
		return ReferenceSet{}, err
	}
	var set ReferenceSet
	ch, ok := root.Member("channels")
	if !ok {
		return ReferenceSet{}, errors.New("data_interface: snapshot channels")
	}
	items, ok := ch.Array()
	if !ok {
		return ReferenceSet{}, errors.New("data_interface: snapshot channels")
	}
	for _, item := range items {
		pin, err := readChannel(item)
		if err != nil {
			return ReferenceSet{}, err
		}
		set.Channels = append(set.Channels, pin)
	}
	cred, ok := root.Member("credentials")
	if !ok {
		return ReferenceSet{}, errors.New("data_interface: snapshot credentials")
	}
	items, ok = cred.Array()
	if !ok {
		return ReferenceSet{}, errors.New("data_interface: snapshot credentials")
	}
	for _, item := range items {
		pin, err := readCredential(item)
		if err != nil {
			return ReferenceSet{}, err
		}
		set.Credentials = append(set.Credentials, pin)
	}
	return set, nil
}

func readChannel(v canonicaljson.Value) (ChannelPin, error) {
	var pin ChannelPin
	id, ok := v.Member("channel_id")
	n, okN := id.Int64()
	if !ok || !okN || n <= 0 {
		return pin, errors.New("data_interface: snapshot channel_id")
	}
	pin.ChannelID = uint64(n)
	ver, ok := v.Member("row_version")
	vn, okN := ver.Int64()
	if !ok || !okN || vn < 0 {
		return pin, errors.New("data_interface: snapshot row_version")
	}
	pin.RowVersion = uint32(vn)
	pin.ProviderID, ok = textMember(v, "provider_id")
	if !ok {
		return pin, errors.New("data_interface: snapshot provider_id")
	}
	pin.ProviderAccountID, ok = textMember(v, "provider_account_id")
	if !ok {
		return pin, errors.New("data_interface: snapshot provider_account_id")
	}
	pin.Operation, ok = textMember(v, "operation")
	if !ok {
		return pin, errors.New("data_interface: snapshot operation")
	}
	pin.AdapterType, ok = textMember(v, "adapter_type")
	if !ok {
		return pin, errors.New("data_interface: snapshot adapter_type")
	}
	pin.AdapterVersion, ok = textMember(v, "adapter_version")
	if !ok {
		return pin, errors.New("data_interface: snapshot adapter_version")
	}
	pin.CredentialRef, ok = textMember(v, "credential_ref")
	if !ok {
		return pin, errors.New("data_interface: snapshot credential_ref")
	}
	return pin, nil
}

func readCredential(v canonicaljson.Value) (CredentialPin, error) {
	var pin CredentialPin
	var ok bool
	pin.Ref, ok = textMember(v, "credential_ref")
	if !ok {
		return pin, errors.New("data_interface: snapshot credential_ref")
	}
	ver, ok := v.Member("version")
	n, okN := ver.Int64()
	if !ok || !okN || n < 0 {
		return pin, errors.New("data_interface: snapshot version")
	}
	pin.Version = uint32(n)
	pin.ProviderID, ok = textMember(v, "provider_id")
	if !ok {
		return pin, errors.New("data_interface: snapshot provider_id")
	}
	pin.ProviderAccountID, ok = textMember(v, "provider_account_id")
	if !ok {
		return pin, errors.New("data_interface: snapshot provider_account_id")
	}
	return pin, nil
}

func textMember(v canonicaljson.Value, key string) (string, bool) {
	field, ok := v.Member(key)
	if !ok {
		return "", false
	}
	return field.Text()
}

// CreateFingerprint 是创建草稿的操作指纹：内容摘要和原因。
func CreateFingerprint(contentSHA, reason string) string {
	obj := canonicaljson.Object([]canonicaljson.Field{
		{Key: "content_sha256", Value: canonicaljson.String(contentSHA)},
		{Key: "reason", Value: canonicaljson.String(reason)},
	})
	return canonicaljson.Fingerprint(canonicaljson.AppendCanonical(nil, obj))
}

// PublishFingerprint 是发布操作指纹。expected 为空表示首次发布。
func PublishFingerprint(releaseID uint64, expected *uint64, reason string) (string, error) {
	id, err := domain.FormatInternalID(releaseID)
	if err != nil {
		return "", err
	}
	exp := canonicaljson.Null()
	if expected != nil {
		text, err := domain.FormatInternalID(*expected)
		if err != nil {
			return "", err
		}
		exp = canonicaljson.String(text)
	}
	obj := canonicaljson.Object([]canonicaljson.Field{
		{Key: "expected_current_release_id", Value: exp},
		{Key: "reason", Value: canonicaljson.String(reason)},
		{Key: "release_id", Value: canonicaljson.String(id)},
	})
	return canonicaljson.Fingerprint(canonicaljson.AppendCanonical(nil, obj)), nil
}

func u64(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
