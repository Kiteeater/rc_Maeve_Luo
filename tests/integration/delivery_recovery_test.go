//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"notifier/internal/data/mysql"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// S06 真实 MySQL 场景（暂缓运行，统一在 S08 后执行）。只连接本地 docker MySQL。

// RCV-01：当前 Outbox 缺失，并发扫描器只建一条指令与一条修复历史。
func TestRCV01_ConcurrentRepairInsertsOneOutbox(t *testing.T) {
	db := s02DB(t)
	task, _ := s03Task(t, db, "rcv-01-"+time.Now().Format("150405.000000"))
	if _, err := db.Exec(`DELETE FROM outbox_messages WHERE delivery_id = ?`, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE delivery_tasks SET scheduled_at = UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE WHERE delivery_id = ?`, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
	store := mysql.NewRecoveryStore(db, mysql.NewExecutor(db, 2*time.Second))
	snap, err := store.LoadRecoverySnapshot(context.Background(), task.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan data_interface.RecoveryOutcome, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := store.RecoverTask(context.Background(), data_interface.RecoverInput{
				DeliveryID: task.DeliveryID, RoundNo: snap.Task.RoundNo, ScheduleVersion: snap.Task.ScheduleVersion,
				ExpectedRowVersion: snap.Task.RowVersion, Action: domain.RecoveryRepair, Repair: domain.RepairInsert,
				RepairKey: "it-rcv01", Reason: "当前指令缺失",
			})
			if err != nil {
				t.Error(err)
			}
			results <- out
		}()
	}
	wg.Wait()
	close(results)
	applied := 0
	for r := range results {
		if r == data_interface.RecoveryApplied {
			applied++
		}
	}
	var outbox, hist int
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_messages WHERE delivery_id = ?`, task.DeliveryID).Scan(&outbox)
	_ = db.QueryRow(`SELECT COUNT(*) FROM delivery_history WHERE delivery_id = ? AND phase = 'DISPATCH_REPAIRED'`, task.DeliveryID).Scan(&hist)
	if applied != 1 || outbox != 1 || hist != 1 {
		t.Fatalf("applied=%d outbox=%d history=%d", applied, outbox, hist)
	}
}

// RCV-04 / RCV-05：领取过期按 UNCERTAIN/LEASE_EXPIRED 结束；结果已提交时旧候选为 STALE。
func TestRCV04_ExpireAttemptOnMySQL(t *testing.T) {
	db := s02DB(t)
	task, ds := s03Task(t, db, "rcv-04-"+time.Now().Format("150405.000000"))
	if _, err := db.Exec(`UPDATE delivery_tasks SET scheduled_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE delivery_id = ?`, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
	task, _ = ds.LoadTask(context.Background(), task.DeliveryID)
	start := s03Start(task)
	start.LeaseTimeout = time.Second
	res, err := ds.StartAttempt(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	store := mysql.NewRecoveryStore(db, mysql.NewExecutor(db, 2*time.Second))
	snap, err := store.LoadRecoverySnapshot(context.Background(), task.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := domain.ExpirePlan(snap.Task.Facts(), domain.ExpireFacts{Now: snap.Now, Policy: domain.FrozenPolicy{MaxAttempts: 1}, AttemptID: res.Ownership.AttemptID, JitterNumer: 5000})
	if err != nil {
		t.Fatal(err)
	}
	in := data_interface.RecoverInput{DeliveryID: task.DeliveryID, RoundNo: snap.Task.RoundNo, ScheduleVersion: snap.Task.ScheduleVersion,
		ExpectedRowVersion: snap.Task.RowVersion, Action: domain.RecoveryExpire, AttemptID: res.Ownership.AttemptID, Plan: &plan, RepairKey: "it-rcv04"}
	if out, err := store.RecoverTask(context.Background(), in); err != nil || out != data_interface.RecoveryApplied {
		t.Fatalf("out=%s err=%v", out, err)
	}
	if out, _ := store.RecoverTask(context.Background(), in); out != data_interface.RecoveryAlreadyApplied {
		t.Fatalf("replay must be ALREADY_APPLIED: %s", out)
	}
	var code, cert string
	_ = db.QueryRow(`SELECT error_code, result_certainty FROM delivery_attempts WHERE attempt_id = ?`, res.Ownership.AttemptID).Scan(&code, &cert)
	if code != "LEASE_EXPIRED" || cert != "UNCERTAIN" {
		t.Fatalf("attempt: %s %s", code, cert)
	}
}
