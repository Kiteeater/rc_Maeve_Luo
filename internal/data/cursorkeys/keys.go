// Package cursorkeys 从受保护引用读取 S08 游标签名密钥。密钥不进入 GlobalConfig 日志。
package cursorkeys

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"notifier/internal/domain"
)

type file struct {
	Active string            `json:"active"`
	Keys   map[string]string `json:"keys"` // key_id -> 十六进制密钥（至少 32 字节）
}

// Source 读取并短时缓存密钥文件；轮换时旧 key_id 保留在文件中即可继续验证。
type Source struct {
	Path string
	TTL  time.Duration

	mu     sync.Mutex
	cached domain.CursorKeys
	at     time.Time
}

func (s *Source) Load() (domain.CursorKeys, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ttl := s.TTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	if !s.at.IsZero() && time.Since(s.at) < ttl {
		return s.cached, nil
	}
	if s.Path == "" {
		return domain.CursorKeys{}, errors.New("cursorkeys: TRACKING_CURSOR_KEY_REF not set")
	}
	raw, err := os.ReadFile(s.Path)
	if err != nil {
		return domain.CursorKeys{}, errors.New("cursorkeys: key ref unreadable")
	}
	keys, err := Parse(raw)
	if err != nil {
		return domain.CursorKeys{}, err
	}
	s.cached, s.at = keys, time.Now()
	return keys, nil
}

// Parse 校验密钥文件：active 必须存在，每个密钥至少 32 字节。
func Parse(raw []byte) (domain.CursorKeys, error) {
	var f file
	if err := json.Unmarshal(raw, &f); err != nil {
		return domain.CursorKeys{}, errors.New("cursorkeys: invalid key file")
	}
	out := domain.CursorKeys{Active: f.Active, Keys: map[string][]byte{}}
	for id, h := range f.Keys {
		b, err := hex.DecodeString(h)
		if err != nil || len(b) < 32 || id == "" || len(id) > 32 {
			return domain.CursorKeys{}, errors.New("cursorkeys: invalid key entry")
		}
		out.Keys[id] = b
	}
	if _, ok := out.Keys[f.Active]; !ok {
		return domain.CursorKeys{}, errors.New("cursorkeys: active key missing")
	}
	return out, nil
}
