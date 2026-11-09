package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/data_interface"
)

// tokenBucket 是 S05 §10 的原子令牌桶，与 domain.TakeToken 同一算法。定点单位：1 令牌 = 1000 毫令牌。
// 使用 Redis 服务器时间；时钟回退不产生额度；策略版本变化只重建上限，不额外补满。
var tokenBucket = goredis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local ver = ARGV[3]
local unit = 1000
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local s = redis.call('HMGET', key, 'tokens', 'last_refill_ms', 'policy_version')
local tokens = tonumber(s[1])
local last = tonumber(s[2])
if tokens == nil or last == nil then
  tokens = burst * unit
  last = now
elseif s[3] ~= ver then
  tokens = math.min(tokens, burst * unit)
end
local elapsed = now - last
if elapsed < 0 then elapsed = 0 end
tokens = math.min(burst * unit, tokens + elapsed * rate)
if now > last then last = now end
local wait = 0
local ok = 0
if tokens >= unit then
  tokens = tokens - unit
  ok = 1
else
  wait = math.ceil((unit - tokens) / rate)
end
redis.call('HSET', key, 'tokens', tokens, 'last_refill_ms', last, 'policy_version', ver)
local ttl = math.max(60000, math.ceil(2000 * burst / rate))
redis.call('PEXPIRE', key, ttl)
return {ok, now + wait}
`)

// RateLimiter 实现 data_interface.RateLimiter。
type RateLimiter struct {
	client *goredis.Client
}

func NewRateLimiter(c *goredis.Client) *RateLimiter { return &RateLimiter{client: c} }

func (l *RateLimiter) Take(ctx context.Context, quotaKey string, rate, burst int64, policyVersion uint32) (bool, time.Time, error) {
	res, err := tokenBucket.Run(ctx, l.client, []string{"notifier:quota:v1:" + quotaKey}, rate, burst, policyVersion).Int64Slice()
	if err != nil || len(res) != 2 {
		return false, time.Time{}, data_interface.ErrRateLimiterDown
	}
	return res[0] == 1, time.UnixMilli(res[1]).UTC(), nil
}
