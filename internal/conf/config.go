// Package conf 加载进程启动配置，汇总环境变量与 Azure Key Vault 密钥。
//
// 所有运行参数均可通过环境变量覆盖，未设置时使用技术方案第 13 节给出的初始建议值。
// 配置值在启动时解析，运行期间不可变；需要动态调整的运行参数属于发布快照，
// 由业务用例管理，不经过本包。GlobalConfig 通过构造函数注入，不使用可变全局变量。
package conf

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
)

// GlobalConfig 是启动完成后的配置快照，运行期间只读。
type GlobalConfig struct {
	KeyVault  KeyVault
	HTTP      HTTP
	MySQL     MySQL
	Redis     Redis
	RocketMQ  RocketMQ
	Delivery  Delivery
	Outbox    Outbox
	Recovery  Recovery
	Retention Retention
	Log       Log
}

// HTTP 接入与查询服务。
type HTTP struct {
	Addr string // 监听地址，如 ":8080"
}

// MySQL 主存储。
type MySQL struct {
	DSN string // 例：user:pass@tcp(127.0.0.1:3306)/notifier?parseTime=true&charset=utf8mb4
}

// Redis 缓存与限流。
type Redis struct {
	Addr     string
	Password string
	DB       int
}

// RocketMQ 消息收发（gRPC Proxy 接入）。
type RocketMQ struct {
	Endpoint       string // gRPC 接入端点，如 "127.0.0.1:8081"
	TopicImmediate string // 即时投递指令 Topic
	TopicDelayed   string // 延迟投递指令 Topic
	Group          string // 消费者分组
}

// Delivery 投递执行参数（技术方案第 13 节初始建议）。
type Delivery struct {
	Parallelism    int           // 并行投递 worker 数，建议 10
	HTTPTimeout    time.Duration // 供应商 HTTP 超时，建议 10s
	LeaseTimeout   time.Duration // 任务领取期限，建议 30s
	MQInvisible    time.Duration // MQ 不可见期限，建议 60s
	MaxAttempts    int           // 每轮最多尝试次数，建议 8
	BackoffInitial time.Duration // 退避起始，建议 5s
	BackoffCap     time.Duration // 本地退避上限，建议 5m
	ServerWaitCap  time.Duration // 服务端等待建议上限，建议 1h
	RoundDeadline  time.Duration // 本轮等待截止，建议 24h
	BlockedCheck   time.Duration // 阻塞条件检查间隔，建议 60s
}

// Outbox 可靠发布参数。
type Outbox struct {
	PollInterval time.Duration // 空闲轮询，建议 1s
	GracePeriod  time.Duration // 补发宽限，建议 5m
}

// Recovery 补偿扫描参数。
type Recovery struct {
	ScanInterval time.Duration // 补偿扫描，建议 30s
}

// Retention 数据保留与缓存策略。
type Retention struct {
	OverviewCache time.Duration // 运行概览缓存，建议 5s
	ResolvedAfter time.Duration // 结案记录保留期，建议 30 天
}

// Log 结构化日志。
type Log struct {
	Level  string // debug | info | warn | error
	Format string // json | text
}

// Default 返回技术方案第 13 节给出的初始建议配置。
func Default() GlobalConfig {
	return GlobalConfig{
		KeyVault: KeyVault{Timeout: 15 * time.Second},
		HTTP:     HTTP{Addr: ":8080"},
		MySQL: MySQL{
			DSN: "notifier:notifier@tcp(127.0.0.1:3306)/notifier?parseTime=true&charset=utf8mb4",
		},
		Redis: Redis{Addr: "127.0.0.1:6379"},
		RocketMQ: RocketMQ{
			Endpoint:       "127.0.0.1:8081",
			TopicImmediate: "notifier-delivery",
			TopicDelayed:   "notifier-delivery-delay",
			Group:          "notifier-worker",
		},
		Delivery: Delivery{
			Parallelism:    10,
			HTTPTimeout:    10 * time.Second,
			LeaseTimeout:   30 * time.Second,
			MQInvisible:    60 * time.Second,
			MaxAttempts:    8,
			BackoffInitial: 5 * time.Second,
			BackoffCap:     5 * time.Minute,
			ServerWaitCap:  time.Hour,
			RoundDeadline:  24 * time.Hour,
			BlockedCheck:   60 * time.Second,
		},
		Outbox: Outbox{
			PollInterval: time.Second,
			GracePeriod:  5 * time.Minute,
		},
		Recovery: Recovery{ScanInterval: 30 * time.Second},
		Retention: Retention{
			OverviewCache: 5 * time.Second,
			ResolvedAfter: 30 * 24 * time.Hour,
		},
		Log: Log{Level: "info", Format: "json"},
	}
}

// Load 汇总默认值、环境变量和 Key Vault 密钥。任一来源失败时不返回部分配置。
func Load(ctx context.Context) (GlobalConfig, error) {
	return (Loader{}).Load(ctx)
}

// Load 支持注入密钥读取器；未注入时使用 Azure SDK。
func (l Loader) Load(ctx context.Context) (GlobalConfig, error) {
	c := Default()

	c.KeyVault.URL = envStr("AZURE_KEY_VAULT_URL", c.KeyVault.URL)
	c.KeyVault.MySQLDSNSecret = envStr("MYSQL_DSN_SECRET", c.KeyVault.MySQLDSNSecret)
	c.KeyVault.RedisPasswordSecret = envStr("REDIS_PASSWORD_SECRET", c.KeyVault.RedisPasswordSecret)
	c.HTTP.Addr = envStr("HTTP_ADDR", c.HTTP.Addr)
	c.MySQL.DSN = envStr("MYSQL_DSN", c.MySQL.DSN)
	c.Redis.Addr = envStr("REDIS_ADDR", c.Redis.Addr)
	c.Redis.Password = envStr("REDIS_PASSWORD", c.Redis.Password)
	c.RocketMQ.Endpoint = envStr("ROCKETMQ_ENDPOINT", c.RocketMQ.Endpoint)
	c.RocketMQ.TopicImmediate = envStr("ROCKETMQ_TOPIC_IMMEDIATE", c.RocketMQ.TopicImmediate)
	c.RocketMQ.TopicDelayed = envStr("ROCKETMQ_TOPIC_DELAYED", c.RocketMQ.TopicDelayed)
	c.RocketMQ.Group = envStr("ROCKETMQ_GROUP", c.RocketMQ.Group)
	c.Log.Level = envStr("LOG_LEVEL", c.Log.Level)
	c.Log.Format = envStr("LOG_FORMAT", c.Log.Format)

	var err error
	if c.Redis.DB, err = envInt("REDIS_DB", c.Redis.DB); err != nil {
		return GlobalConfig{}, err
	}
	if c.Delivery.Parallelism, err = envInt("DELIVERY_PARALLELISM", c.Delivery.Parallelism); err != nil {
		return GlobalConfig{}, err
	}
	if c.Delivery.MaxAttempts, err = envInt("DELIVERY_MAX_ATTEMPTS", c.Delivery.MaxAttempts); err != nil {
		return GlobalConfig{}, err
	}

	durations := []struct {
		key string
		dst *time.Duration
	}{
		{"AZURE_KEY_VAULT_TIMEOUT", &c.KeyVault.Timeout},
		{"DELIVERY_HTTP_TIMEOUT", &c.Delivery.HTTPTimeout},
		{"DELIVERY_LEASE_TIMEOUT", &c.Delivery.LeaseTimeout},
		{"DELIVERY_MQ_INVISIBLE", &c.Delivery.MQInvisible},
		{"DELIVERY_BACKOFF_INITIAL", &c.Delivery.BackoffInitial},
		{"DELIVERY_BACKOFF_CAP", &c.Delivery.BackoffCap},
		{"DELIVERY_SERVER_WAIT_CAP", &c.Delivery.ServerWaitCap},
		{"DELIVERY_ROUND_DEADLINE", &c.Delivery.RoundDeadline},
		{"DELIVERY_BLOCKED_CHECK", &c.Delivery.BlockedCheck},
		{"OUTBOX_POLL_INTERVAL", &c.Outbox.PollInterval},
		{"OUTBOX_GRACE_PERIOD", &c.Outbox.GracePeriod},
		{"RECOVERY_SCAN_INTERVAL", &c.Recovery.ScanInterval},
		{"RETENTION_OVERVIEW_CACHE", &c.Retention.OverviewCache},
		{"RETENTION_RESOLVED_AFTER", &c.Retention.ResolvedAfter},
	}
	for _, d := range durations {
		if *d.dst, err = envDuration(d.key, *d.dst); err != nil {
			return GlobalConfig{}, err
		}
	}

	if err := l.resolveSecrets(ctx, &c); err != nil {
		return GlobalConfig{}, err
	}
	return c, nil
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config %s: 期望整数，得到 %q: %w", key, v, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config %s: 期望时长（如 10s、5m、24h），得到 %q: %w", key, v, err)
	}
	return d, nil
}
