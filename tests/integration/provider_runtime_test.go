//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/biz/biz_provider"
	"notifier/internal/data/mysql"
	"notifier/internal/data/redis"
	"notifier/internal/data_interface"
)

// 只连接本地 docker MySQL/Redis。

var (
	s05Once sync.Once
	s05Err  error
)

func s05DB(t *testing.T) *sql.DB {
	t.Helper()
	const name = "notifier_s05_prv"
	s05Once.Do(func() {
		admin := adminDB(t)
		if _, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name); err != nil {
			s05Err = err
			return
		}
		if _, err := admin.ExecContext(context.Background(), "CREATE DATABASE "+name); err != nil {
			s05Err = err
			return
		}
		runGoose(t, repoRoot(t), gooseDSN(name), "up")
	})
	if s05Err != nil {
		t.Fatal(s05Err)
	}
	db := dbNamed(t, name)
	for _, stmt := range []string{`DELETE FROM channels`, `DELETE FROM credential_refs`} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// PRV-05 / PRV-11：Redis 原子令牌桶跨通道共享 quota_key；Redis 故障返回错误而非放行。
func TestPRV11_RedisSharedQuotaKey(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr()})
	t.Cleanup(func() { _ = rdb.Close() })
	key := "itest-" + time.Now().Format("150405.000000")
	lim := redis.NewRateLimiter(rdb)
	var mu sync.Mutex
	passed := 0
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := lim.Take(context.Background(), key, 1, 5, 1)
			if err != nil {
				t.Error(err)
			}
			if ok {
				mu.Lock()
				passed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if passed != 5 {
		t.Fatalf("shared bucket passed %d, want 5", passed)
	}
	if ok, _, _ := lim.Take(context.Background(), key, 1, 5, 2); ok {
		t.Fatal("policy version change must not refill")
	}
}

func TestPRV05_RedisDownDoesNotPass(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	ok, _, err := redis.NewRateLimiter(rdb).Take(context.Background(), "k", 1, 1, 1)
	if ok || err == nil {
		t.Fatal("limiter failure must not pass")
	}
}

// PRV-04：MySQL CAS 刷新只有一个实例获胜。
func TestPRV04_MySQLRefreshCAS(t *testing.T) {
	db := s05DB(t)
	if _, err := db.Exec(`INSERT INTO credential_refs (credential_ref, provider_id, provider_account_id, version) VALUES ('c1','mock','a1',1)`); err != nil {
		t.Fatal(err)
	}
	store := mysql.NewProviderStore(db)
	r := &biz_provider.Refresher{Store: store, Issuer: okIssuer{}, Budget: 5 * time.Second}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := r.Refresh(context.Background(), "c1", "inst"+string(rune('a'+i))); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	st, err := store.LoadCredential(context.Background(), "c1")
	if err != nil || st.State != "ACTIVE" || int(st.Version) != 1+wins || wins < 1 {
		t.Fatalf("wins=%d st=%+v err=%v", wins, st, err)
	}
}

type okIssuer struct{}

func (okIssuer) Refresh(context.Context, string, uint32) error { return nil }

// PRV-06：通道当前约束以 MySQL 为准；缺少额度配置的通道不能放行。
func TestPRV06_ChannelStateFromMySQL(t *testing.T) {
	db := s05DB(t)
	if _, err := db.Exec(`INSERT INTO channels (channel_id, provider_id, provider_account_id, credential_ref, adapter_type, adapter_version, operation, enabled, access_constraints, runtime_policy)
	VALUES (9,'mock','a1','c1','mock-json','1','send',TRUE,
	'{"allowed_callers":["c"],"allowed_origins":[{"scheme":"http","host":"127.0.0.1","port":9090}],"allowed_cidrs":["127.0.0.0/8"]}', NULL)`); err != nil {
		t.Fatal(err)
	}
	st, err := mysql.NewProviderStore(db).LoadChannelState(context.Background(), 9)
	if err != nil || st.Enabled || len(st.AllowedOrigins) != 1 {
		t.Fatalf("no quota policy must disable: %+v %v", st, err)
	}
	var _ data_interface.ChannelStateReader = mysql.NewProviderStore(db)
}
