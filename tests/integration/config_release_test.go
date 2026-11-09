//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/biz/biz_config"
	"notifier/internal/constant"
	"notifier/internal/data/mysql"
	"notifier/internal/data/provider"
	"notifier/internal/data/redis"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

func TestCFG06_ReloadV1AfterRedisRestart(t *testing.T) {
	uc, _, cache, rdb := s01Env(t)
	v1 := publishContent(t, uc, tagged("v1"), nil)
	v2 := publishContent(t, uc, tagged("v2"), &v1)
	env := demoEvent(`{"user_id":"u-1"}`)
	warmed, err := uc.Calculate(context.Background(), demoPrincipal(), v1, env)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("notifier:config:v1:%d", v1)
	if n, err := rdb.Exists(context.Background(), key).Result(); err != nil || n != 1 {
		t.Fatalf("cache warm exists = %d %v", n, err)
	}
	if err := cache.Delete(context.Background(), v1); err != nil {
		t.Fatal(err)
	}
	root := repoRoot(t)
	save := exec.Command("docker", "compose", "exec", "-T", "redis", "redis-cli", "SAVE")
	save.Dir = root
	if out, err := save.CombinedOutput(); err != nil {
		t.Fatalf("redis SAVE: %v %s", err, out)
	}
	restart := exec.Command("docker", "compose", "restart", "redis")
	restart.Dir = root
	if out, err := restart.CombinedOutput(); err != nil {
		t.Fatalf("restart redis: %v %s", err, out)
	}
	waitRedis(t)
	if err := rdb.Get(context.Background(), key).Err(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("v1 key after restart = %v", err)
	}
	plan, err := uc.Calculate(context.Background(), demoPrincipal(), v1, env)
	if err != nil {
		t.Fatal(err)
	}
	payload := string(plan.Targets[0].PayloadCanonical)
	if plan.ReleaseRef != fmt.Sprint(v1) || !strings.Contains(payload, `"tag":"v1"`) || strings.Contains(payload, `"tag":"v2"`) {
		t.Fatalf("reloaded plan release %s payload %s, v2 was %d, warm %s", plan.ReleaseRef, payload, v2, warmed.Targets[0].PayloadCanonical)
	}
}

func TestCFG07_PublishRetryDoesNotRewindPointer(t *testing.T) {
	uc, _, _, _ := s01Env(t)
	v1, first := publishKeyed(t, uc, tagged("v1"), "k1", nil)
	v2, _ := publishKeyed(t, uc, tagged("v2"), "k2", &v1)
	retry, err := uc.Publish(context.Background(), demoPrincipal(), v1, "k1", "publish draft", nil)
	if err != nil {
		t.Fatal(err)
	}
	if retry.OperationID != first.OperationID || retry.ReleaseID != v1 || retry.VersionNo != first.VersionNo {
		t.Fatalf("retry = %+v want %+v", retry, first)
	}
	current, err := uc.Current(context.Background())
	if err != nil || current.ReleaseID != v2 {
		t.Fatalf("current after retry = %+v %v", current, err)
	}
	_, err = uc.Publish(context.Background(), demoPrincipal(), v2, "k2", "other reason", &v1)
	if codeOf(err) != constant.CodeIdempotencyConflict {
		t.Fatalf("different params = %v", err)
	}
	current, err = uc.Current(context.Background())
	if err != nil || current.ReleaseID != v2 {
		t.Fatalf("current after conflict = %+v %v", current, err)
	}
}

func TestCFG09_ConcurrentPublishOneConflict(t *testing.T) {
	uc, db, _, _ := s01Env(t)
	a := createDraft(t, uc, tagged("a"))
	b := createDraft(t, uc, tagged("b"))
	start := make(chan struct{})
	var wg sync.WaitGroup
	type outcome struct {
		res data_interface.PublishResult
		err error
	}
	out := make([]outcome, 2)
	ids := []uint64{a, b}
	wg.Add(2)
	for i := range ids {
		go func(i int) {
			defer wg.Done()
			<-start
			out[i].res, out[i].err = uc.Publish(context.Background(), demoPrincipal(), ids[i], fmt.Sprintf("race-%d", i), "publish draft", nil)
		}(i)
	}
	close(start)
	wg.Wait()
	wins, conflicts := 0, 0
	var winner uint64
	for _, item := range out {
		if item.err == nil {
			wins++
			winner = item.res.ReleaseID
			continue
		}
		if codeOf(item.err) == constant.CodeVersionConflict {
			conflicts++
			continue
		}
		t.Fatalf("unexpected race result %+v", item)
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins %d conflicts %d results %+v", wins, conflicts, out)
	}
	var current sql.NullInt64
	if err := db.QueryRow(`SELECT release_id FROM routing_current WHERE singleton = 1`).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if !current.Valid || uint64(current.Int64) != winner {
		t.Fatalf("current = %+v winner %d", current, winner)
	}
	loser := a
	if loser == winner {
		loser = b
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM routing_releases WHERE release_id = ?`, loser).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "draft" {
		t.Fatalf("loser status = %s", status)
	}
}

func TestCFG11_ReferenceChangeRollsBackPublish(t *testing.T) {
	uc, db, _, _ := s01Env(t)
	id := createDraft(t, uc, exampleContent())
	if _, err := db.Exec(`UPDATE credential_refs SET version = 2 WHERE credential_ref = 'provider/demo'`); err != nil {
		t.Fatal(err)
	}
	_, err := uc.Publish(context.Background(), demoPrincipal(), id, "changed-ref", "publish draft", nil)
	if codeOf(err) != constant.CodeConfigurationChanged {
		t.Fatalf("publish = %v", err)
	}
	var current sql.NullInt64
	if err := db.QueryRow(`SELECT release_id FROM routing_current WHERE singleton = 1`).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current.Valid {
		t.Fatalf("current changed to %d", current.Int64)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM routing_releases WHERE release_id = ?`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "draft" {
		t.Fatalf("draft status = %s", status)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM management_events WHERE operation_type = ? AND idempotency_key = ?`, constant.OpConfigPublish, "changed-ref").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("publish operation rows = %d", n)
	}
}

var (
	s01Once sync.Once
	s01Err  error
)

func s01Env(t *testing.T) (*biz_config.UseCase, *sql.DB, *redis.ReleaseCache, *goredis.Client) {
	t.Helper()
	const name = "notifier_s01_cfg"
	s01Once.Do(func() {
		admin := adminDB(t)
		if _, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name); err != nil {
			s01Err = err
			return
		}
		if _, err := admin.ExecContext(context.Background(), "CREATE DATABASE "+name); err != nil {
			s01Err = err
			return
		}
		runGoose(t, repoRoot(t), gooseDSN(name), "up")
	})
	if s01Err != nil {
		t.Fatal(s01Err)
	}
	db := dbNamed(t, name)
	if _, err := db.Exec(`UPDATE routing_current SET release_id = NULL WHERE singleton = 1`); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`DELETE FROM management_events`,
		`DELETE FROM routing_releases`,
		`DELETE FROM channels`,
		`DELETE FROM credential_refs`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO channels (channel_id, provider_id, provider_account_id, credential_ref, adapter_type, adapter_version, operation, row_version)
		VALUES (1, 'mock', 'demo', 'provider/demo', 'mock-json', '1', 'create-user', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO credential_refs (credential_ref, provider_id, provider_account_id, version)
		VALUES ('provider/demo', 'mock', 'demo', 1)`); err != nil {
		t.Fatal(err)
	}
	store := mysql.NewReleaseStore(db, mysql.NewExecutor(db, 5*time.Second))
	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr()})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis: %v", err)
	}
	cache := redis.NewReleaseCache(rdb, time.Hour)
	uc := biz_config.NewUseCase(store, store, store, provider.NewMockRegistry(), cache, domain.DefaultBounds(), time.Second)
	return uc, db, cache, rdb
}

func redisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}

func waitRedis(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		client := goredis.NewClient(&goredis.Options{Addr: redisAddr()})
		err := client.Ping(context.Background()).Err()
		_ = client.Close()
		if err == nil {
			return
		}
		last = err
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("redis did not return: %v", last)
}

func publishContent(t *testing.T, uc *biz_config.UseCase, content string, expected *uint64) uint64 {
	t.Helper()
	id := createDraft(t, uc, content)
	if _, err := uc.Publish(context.Background(), demoPrincipal(), id, fmt.Sprintf("pub-%d", id), "publish draft", expected); err != nil {
		t.Fatal(err)
	}
	return id
}

func publishKeyed(t *testing.T, uc *biz_config.UseCase, content, key string, expected *uint64) (uint64, data_interface.PublishResult) {
	t.Helper()
	id := createDraft(t, uc, content)
	out, err := uc.Publish(context.Background(), demoPrincipal(), id, key, "publish draft", expected)
	if err != nil {
		t.Fatal(err)
	}
	return id, out
}

func createDraft(t *testing.T, uc *biz_config.UseCase, content string) uint64 {
	t.Helper()
	out, err := uc.Create(context.Background(), demoPrincipal(), fmt.Sprintf("draft-%d", time.Now().UnixNano()), "create draft", []byte(content))
	if err != nil {
		t.Fatal(err)
	}
	return out.ReleaseID
}

func demoPrincipal() biz_config.Principal {
	return biz_config.Principal{ActorID: "ops-admin", CallerID: "caller-demo", AllowedSources: []string{"user-service"}, Scopes: []string{"caller-demo"}}
}

func demoEvent(data string) domain.EventEnvelope {
	return domain.EventEnvelope{CallerID: "caller-demo", SourceBusiness: "user-service", EventType: "UserRegistered", SchemaVersion: "1", DataCanonical: []byte(data)}
}

func codeOf(err error) string {
	var de *domain.Error
	if errors.As(err, &de) && de != nil {
		return de.Code
	}
	return ""
}

func policyJSON() string {
	return `{"backoff_cap_ms":300000,"backoff_initial_ms":5000,"max_attempts":8,"round_timeout_seconds":86400,"server_wait_cap_seconds":3600}`
}

func exampleContent() string {
	return fmt.Sprintf(`{"event_schemas":[{"event_type":"UserRegistered","schema_version":"1","shape":{"additional_properties":false,"properties":{"user_id":{"type":"string"}},"required":["user_id"],"type":"object"}}],"format_version":1,"mappings":{"crm-user":{"fields":[{"from":"/user_id","nullable":false,"required":true,"to":"/external_user_id","type":"string"}],"version":"1"}},"policy":%s,"routes":[{"caller_id":"caller-demo","conditions":[],"event_type":"UserRegistered","route_id":"user-crm","schema_version":"1","source_business":"user-service","targets":["crm-create-user"]}],"targets":{"crm-create-user":{"adapter_type":"mock-json","adapter_version":"1","channel_id":"1","credential_ref":"provider/demo","mapping_id":"crm-user","mapping_version":"1","operation":"create-user","provider_account_id":"demo","provider_id":"mock","semantic_config":{"auth_type":"bearer","base_url":"http://mockvendor:9090","idempotency":{"mode":"key","starts_at":"first_possible_send","window_seconds":86400},"path":"/notify/create-user"}}}}`, policyJSON())
}

func tagged(tag string) string {
	return fmt.Sprintf(`{"event_schemas":[{"event_type":"UserRegistered","schema_version":"1","shape":{"additional_properties":false,"properties":{"user_id":{"type":"string"}},"required":["user_id"],"type":"object"}}],"format_version":1,"mappings":{"crm-user":{"fields":[{"from":"/user_id","nullable":false,"required":true,"to":"/external_user_id","type":"string"},{"constant":%q,"nullable":false,"required":true,"to":"/tag","type":"string"}],"version":"1"}},"policy":%s,"routes":[{"caller_id":"caller-demo","conditions":[],"event_type":"UserRegistered","route_id":"user-crm","schema_version":"1","source_business":"user-service","targets":["crm-create-user"]}],"targets":{"crm-create-user":{"adapter_type":"mock-json","adapter_version":"1","channel_id":"1","credential_ref":"provider/demo","mapping_id":"crm-user","mapping_version":"1","operation":"create-user","provider_account_id":"demo","provider_id":"mock","semantic_config":{"auth_type":"bearer","base_url":"http://mockvendor:9090","idempotency":{"mode":"key","starts_at":"first_possible_send","window_seconds":86400},"path":"/notify/create-user"}}}}`, tag, policyJSON())
}
