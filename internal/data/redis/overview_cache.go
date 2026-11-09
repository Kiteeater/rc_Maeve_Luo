package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// OverviewCache 是 S08 概览快照缓存；key 已包含权限范围 hash 与窗口。
type OverviewCache struct {
	client *goredis.Client
}

func NewOverviewCache(c *goredis.Client) *OverviewCache { return &OverviewCache{client: c} }

func (c *OverviewCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if c == nil || c.client == nil {
		return nil, false, errors.New("redis: overview cache unavailable")
	}
	b, err := c.client.Get(ctx, "notifier:"+key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (c *OverviewCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c == nil || c.client == nil {
		return errors.New("redis: overview cache unavailable")
	}
	return c.client.Set(ctx, "notifier:"+key, value, ttl).Err()
}
