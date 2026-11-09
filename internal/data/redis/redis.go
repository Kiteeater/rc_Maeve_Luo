// Package redis 构造缓存客户端。业务缓存与限流由 S01、S05、S08 完成。
package redis

import (
	"context"
	"database/sql"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/conf"
)

// Open 连接 Redis。错误只包含参数名，不包含密码。
// *sql.DB 参数保证 MySQL 拨号先于 Redis，便于启动失败时定位第一处依赖。
func Open(cfg conf.Redis, _ *sql.DB) (*goredis.Client, func(), error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, nil, errors.New("redis: REDIS_ADDR: connect failed")
	}
	return client, func() { _ = client.Close() }, nil
}

// Healthy 执行 PING。失败信息不包含地址或密码。
func Healthy(ctx context.Context, client *goredis.Client) error {
	if client == nil {
		return errors.New("redis: REDIS_ADDR: not ready")
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return errors.New("redis: REDIS_ADDR: not ready")
	}
	return nil
}
