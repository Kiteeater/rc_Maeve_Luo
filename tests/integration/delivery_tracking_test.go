//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"notifier/internal/data/mysql"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// S08 真实 MySQL 场景。只连接本地 docker MySQL。

// TRK-01：任务查询携带范围，其他调用方读不到。
func TestTRK01_MySQLScopedDetail(t *testing.T) {
	db := s02DB(t)
	task, _ := s03Task(t, db, "trk-01-"+time.Now().Format("150405.000000"))
	store := mysql.NewTrackingStore(db)
	row, err := store.LoadDelivery(context.Background(), []string{task.CallerID}, task.DeliveryID)
	if err != nil || row.Task.DeliveryID != task.DeliveryID || row.SourceEventID == "" {
		t.Fatalf("own scope: %v %+v", err, row.Task.DeliveryID)
	}
	if _, err := store.LoadDelivery(context.Background(), []string{"someone-else"}, task.DeliveryID); err == nil {
		t.Fatal("cross scope must be not_found")
	}
	rows, err := store.QueryDeliveries(context.Background(), data_interface.TrackingQuery{Scopes: []string{task.CallerID},
		Filter: domain.QueryFilter{EventID: task.EventID, Unbounded: true}, Limit: 10})
	if err != nil || len(rows) == 0 {
		t.Fatalf("list: %v %d", err, len(rows))
	}
	ev, ds, err := store.LoadEventView(context.Background(), []string{task.CallerID}, task.EventID, 51)
	if err != nil || ev.DeliveryCount != len(ds) || len(ev.StatusCounts) != 7 {
		t.Fatalf("event view: %v %+v", err, ev)
	}
	if _, err := store.Overview(context.Background(), data_interface.OverviewQuery{Scopes: []string{task.CallerID}, From: time.Now().Add(-time.Hour), To: time.Now()}); err != nil {
		t.Fatalf("overview: %v", err)
	}
}

// TRK-09：未结案任务的事件不能被清理。
func TestTRK09_MySQLActiveEventSkipped(t *testing.T) {
	db := s02DB(t)
	task, _ := s03Task(t, db, "trk-09-"+time.Now().Format("150405.000000"))
	store := mysql.NewRetentionStore(db, mysql.NewExecutor(db, 2*time.Second))
	got, err := store.DeleteResolvedEvent(context.Background(), data_interface.RetentionCandidate{EventID: task.EventID, LastResolved: time.Now().Add(-1000 * time.Hour)}, time.Now(), 5000)
	if err != nil || got != domain.RetentionSkip {
		t.Fatalf("got %s %v", got, err)
	}
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM delivery_tasks WHERE delivery_id = ?`, task.DeliveryID).Scan(&n)
	if n != 1 {
		t.Fatal("task deleted")
	}
}
