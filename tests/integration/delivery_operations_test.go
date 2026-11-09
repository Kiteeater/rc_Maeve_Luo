//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data/mysql"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// S07 真实 MySQL 场景（暂缓运行，统一在 S08 后执行）。只连接本地 docker MySQL。

func opsReplayInput(t *testing.T, task data_interface.TaskRecord, key, fp string) data_interface.ReplayInput {
	t.Helper()
	p, err := domain.ParseFrozenPolicy(task.RoundPolicy)
	if err != nil {
		t.Fatal(err)
	}
	return data_interface.ReplayInput{
		OperationMeta: data_interface.OperationMeta{
			OperationID: "it-" + key, OperationTy: constant.OpManualRetry, Key: key, CallerScope: task.CallerID,
			Fingerprint: fp, Actor: "it", Reason: "集成测试重投", DeliveryID: task.DeliveryID, ExpectedRow: task.RowVersion + 1,
			Build: func(tk data_interface.TaskRecord, at time.Time) ([]byte, error) {
				return json.Marshal(map[string]any{"delivery_id": tk.DeliveryID, "round_no": tk.RoundNo})
			},
		},
		Policy: p, PolicyRaw: domain.EncodeFrozenPolicy(p),
	}
}

func failTask(t *testing.T, task data_interface.TaskRecord) {
	t.Helper()
	db := s02DB(t)
	if _, err := db.Exec(`UPDATE delivery_tasks SET status = 'FAILED', row_version = row_version + 1, scheduled_at = NULL WHERE delivery_id = ?`, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
}

// OPS-01：同键同参数并发重投只产生一个操作、一个新轮次、一条新指令。
func TestOPS01_MySQLConcurrentSameKey(t *testing.T) {
	db := s02DB(t)
	task, _ := s03Task(t, db, "ops-01-"+time.Now().Format("150405.000000"))
	failTask(t, task)
	store := mysql.NewOperationStore(db, mysql.NewExecutor(db, 2*time.Second))
	in := opsReplayInput(t, task, "ops01-"+time.Now().Format("150405.000000"), "fp-1")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.ReplayTask(context.Background(), in); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var ops, outbox int
	var round uint32
	_ = db.QueryRow(`SELECT COUNT(*) FROM management_events WHERE idempotency_key = ?`, in.Key).Scan(&ops)
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_messages WHERE delivery_id = ? AND round_no = 2`, task.DeliveryID).Scan(&outbox)
	_ = db.QueryRow(`SELECT round_no FROM delivery_tasks WHERE delivery_id = ?`, task.DeliveryID).Scan(&round)
	if ops != 1 || outbox != 1 || round != 2 {
		t.Fatalf("ops=%d outbox=%d round=%d", ops, outbox, round)
	}
}

// OPS-11：同调用方两个任务并发用同一操作键，至多一项成功，另一项整体回滚。
func TestOPS11_MySQLSameKeyTwoTasks(t *testing.T) {
	db := s02DB(t)
	suffix := time.Now().Format("150405.000000")
	a, _ := s03Task(t, db, "ops-11a-"+suffix)
	b, _ := s03Task(t, db, "ops-11b-"+suffix)
	failTask(t, a)
	failTask(t, b)
	store := mysql.NewOperationStore(db, mysql.NewExecutor(db, 2*time.Second))
	key := "ops11-" + suffix
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, tk := range []data_interface.TaskRecord{a, b} {
		wg.Add(1)
		go func(i int, tk data_interface.TaskRecord) {
			defer wg.Done()
			_, errs[i] = store.ReplayTask(context.Background(), opsReplayInput(t, tk, key, "fp-"+string(rune('a'+i))))
		}(i, tk)
	}
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) {
		t.Fatalf("want exactly one success: %v", errs)
	}
	var rounds int
	_ = db.QueryRow(`SELECT COUNT(*) FROM delivery_tasks WHERE delivery_id IN (?, ?) AND round_no = 2`, a.DeliveryID, b.DeliveryID).Scan(&rounds)
	if rounds != 1 {
		t.Fatalf("orphan rounds=%d", rounds)
	}
}
