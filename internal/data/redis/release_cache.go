package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// ReleaseCache 按发布 ID 缓存不可变内容。当前指针不写在这里。
type ReleaseCache struct {
	client *goredis.Client
	ttl    time.Duration
}

func NewReleaseCache(client *goredis.Client, ttl time.Duration) *ReleaseCache {
	return &ReleaseCache{client: client, ttl: ttl}
}

func cacheKey(releaseID uint64) string {
	return fmt.Sprintf("notifier:config:v1:%d", releaseID)
}

type cacheBody struct {
	ReleaseID          string  `json:"release_id"`
	VersionNo          uint32  `json:"version_no"`
	Status             string  `json:"status"`
	Content            string  `json:"content"`
	ContentSHA256      string  `json:"content_sha256"`
	FingerprintVersion int     `json:"fingerprint_version"`
	CreatedAt          string  `json:"created_at"`
	PublishedAt        *string `json:"published_at"`
}

// Get 命中且摘要一致时返回该版本。损坏与未命中分开，调用方都按同一 ID 回源。
func (c *ReleaseCache) Get(ctx context.Context, releaseID uint64) (data_interface.ReleaseRecord, error) {
	if c == nil || c.client == nil || releaseID == 0 {
		return data_interface.ReleaseRecord{}, data_interface.ErrCacheMiss
	}
	raw, err := c.client.Get(ctx, cacheKey(releaseID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return data_interface.ReleaseRecord{}, data_interface.ErrCacheMiss
	}
	if err != nil {
		return data_interface.ReleaseRecord{}, err
	}
	var body cacheBody
	if err := json.Unmarshal(raw, &body); err != nil {
		return data_interface.ReleaseRecord{}, data_interface.ErrCacheCorrupt
	}
	id, err := domain.ParseInternalID("release_id", body.ReleaseID)
	if err != nil || id != releaseID || body.FingerprintVersion != constant.FingerprintVersion {
		return data_interface.ReleaseRecord{}, data_interface.ErrCacheCorrupt
	}
	content := []byte(body.Content)
	if body.ContentSHA256 == "" || canonicaljson.Fingerprint(content) != body.ContentSHA256 {
		return data_interface.ReleaseRecord{}, data_interface.ErrCacheCorrupt
	}
	created, err := domain.ParseRFC3339("created_at", body.CreatedAt)
	if err != nil {
		return data_interface.ReleaseRecord{}, data_interface.ErrCacheCorrupt
	}
	rec := data_interface.ReleaseRecord{
		ReleaseID:          id,
		VersionNo:          body.VersionNo,
		Status:             body.Status,
		ContentCanonical:   content,
		ContentSHA256:      body.ContentSHA256,
		FingerprintVersion: body.FingerprintVersion,
		CreatedAt:          created,
	}
	if body.PublishedAt != nil {
		published, err := domain.ParseRFC3339("published_at", *body.PublishedAt)
		if err != nil {
			return data_interface.ReleaseRecord{}, data_interface.ErrCacheCorrupt
		}
		rec.PublishedAt = &published
	}
	return rec, nil
}

// Put 写入失败由调用方忽略。内容不可变，TTL 只决定缓存保留时间。
func (c *ReleaseCache) Put(ctx context.Context, rec data_interface.ReleaseRecord) error {
	if c == nil || c.client == nil || rec.ReleaseID == 0 {
		return nil
	}
	id, err := domain.FormatInternalID(rec.ReleaseID)
	if err != nil {
		return err
	}
	body := cacheBody{
		ReleaseID:          id,
		VersionNo:          rec.VersionNo,
		Status:             rec.Status,
		Content:            string(rec.ContentCanonical),
		ContentSHA256:      rec.ContentSHA256,
		FingerprintVersion: rec.FingerprintVersion,
		CreatedAt:          domain.FormatUTCMicros(rec.CreatedAt),
	}
	if rec.PublishedAt != nil {
		text := domain.FormatUTCMicros(*rec.PublishedAt)
		body.PublishedAt = &text
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, cacheKey(rec.ReleaseID), raw, c.ttl).Err()
}

// Delete 删除一个版本的缓存。不影响其他版本，也不改当前指针。
func (c *ReleaseCache) Delete(ctx context.Context, releaseID uint64) error {
	if c == nil || c.client == nil || releaseID == 0 {
		return nil
	}
	return c.client.Del(ctx, cacheKey(releaseID)).Err()
}
