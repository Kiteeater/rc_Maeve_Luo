//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os/exec"
	"strings"
	"testing"
	"time"

	"notifier/internal/data/mysql"
)

func TestBatch0_EmptyDatabaseGooseUpDownUp(t *testing.T) {
	const name = "notifier_s00_mig"
	admin := adminDB(t)
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name)
	})

	root := repoRoot(t)
	dsn := gooseDSN(name)
	runGoose(t, root, dsn, "up-to", "2")
	db := dbNamed(t, name)
	if err := insertAttempt(ctx, db, "SENT"); err != nil {
		t.Fatal(err)
	}

	runGoose(t, root, dsn, "up")
	var sendState string
	if err := db.QueryRowContext(ctx, `SELECT send_state FROM delivery_attempts`).Scan(&sendState); err != nil {
		t.Fatal(err)
	}
	if sendState != "HTTP_INVOKED" {
		t.Fatalf("send_state = %s, want HTTP_INVOKED", sendState)
	}
	assertColumn(t, db, name, "delivery_attempts", "send_state", "varchar", 32)
	assertColumn(t, db, name, "delivery_attempts", "request_summary", "text", 0)
	assertColumn(t, db, name, "delivery_tasks", "status", "varchar", 32)
	assertColumn(t, db, name, "routing_releases", "content_canonical", "mediumblob", 0)
	assertColumn(t, db, name, "management_events", "expected_schedule_version", "int", 0)

	summary := strings.Repeat("a", 8192)
	res, err := db.ExecContext(ctx, `
		UPDATE delivery_attempts
		SET request_summary = ?, request_summary_truncated = TRUE
		WHERE attempt_no = 1`, summary)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("summary update affected %d", n)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM delivery_attempts`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM delivery_tasks`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM business_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM channels`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM routing_releases`); err != nil {
		t.Fatal(err)
	}

	runGoose(t, root, dsn, "down-to", "0")
	runGoose(t, root, dsn, "up")

	taskID, err := insertTask(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	deliveryID := uint64(taskID)
	ex := mysql.NewExecutor(db, 2*time.Second)
	var first, second uint32
	err = ex.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var seqErr error
		first, seqErr = mysql.NextHistorySeq(ctx, tx, deliveryID)
		return seqErr
	})
	if err != nil || first != 1 {
		t.Fatalf("first history seq = %d %v", first, err)
	}
	err = ex.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var seqErr error
		second, seqErr = mysql.NextHistorySeq(ctx, tx, deliveryID)
		return seqErr
	})
	if err != nil || second != 2 {
		t.Fatalf("second history seq = %d %v", second, err)
	}
}

func insertAttempt(ctx context.Context, db *sql.DB, sendState string) error {
	taskID, err := insertTask(ctx, db)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO delivery_attempts (delivery_id, round_no, attempt_no, started_at, send_state)
		VALUES (?, 1, 1, UTC_TIMESTAMP(6), ?)`, taskID, sendState)
	return err
}

func insertTask(ctx context.Context, db *sql.DB) (int64, error) {
	res, err := db.ExecContext(ctx, `INSERT INTO routing_releases (version_no, content) VALUES (1, JSON_OBJECT('k', 'v'))`)
	if err != nil {
		return 0, err
	}
	rel, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	res, err = db.ExecContext(ctx, `
		INSERT INTO channels (provider_id, provider_account_id, operation, credential_ref, adapter_type, adapter_version)
		VALUES ('prov', 'acct', 'notify', 'ref', 'http', 'v1')`)
	if err != nil {
		return 0, err
	}
	ch, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	res, err = db.ExecContext(ctx, `
		INSERT INTO business_events (
			caller_id, source_business, source_event_id, event_type, schema_version, occurred_at,
			business_object_type, business_object_id, data_snapshot, data_canonical, fingerprint, routing_release_id)
		VALUES ('caller', 'src', 'evt-1', 'demo', 'v1', UTC_TIMESTAMP(6), 'order', 'obj-1',
			JSON_OBJECT('n', 1), CAST('{"n":1e0}' AS BINARY), REPEAT('a', 64), ?)`, rel)
	if err != nil {
		return 0, err
	}
	ev, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	res, err = db.ExecContext(ctx, `
		INSERT INTO delivery_tasks (
			event_id, route_target_id, channel_id, provider_id, provider_account_id, operation,
			adapter_type, mapping_id, fixed_target, round_policy, payload, payload_canonical,
			mapping_version, adapter_version, idempotency_key, max_attempts, round_deadline_at)
		VALUES (?, 'target-1', ?, 'prov', 'acct', 'notify', 'http', 'map-1',
			JSON_OBJECT('op', 'notify'), JSON_OBJECT('max', 1), JSON_OBJECT('n', 1),
			CAST('{"n":1e0}' AS BINARY), 'v1', 'v1', 'idem-1', 3, UTC_TIMESTAMP(6))`, ev, ch)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func runGoose(t *testing.T, root, dsn string, args ...string) {
	t.Helper()
	cmdArgs := append([]string{"run", "github.com/pressly/goose/v3/cmd/goose", "-dir", root + "/migrations", "mysql", dsn}, args...)
	cmd := exec.Command("go", cmdArgs...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("goose %s: %v\n%s", strings.Join(args, " "), err, redact(string(out)))
	}
}

func assertColumn(t *testing.T, db *sql.DB, schema, table, column, dataType string, minChars int) {
	t.Helper()
	var gotType string
	var chars sql.NullInt64
	err := db.QueryRowContext(context.Background(), `
		SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
		schema, table, column).Scan(&gotType, &chars)
	if err != nil {
		t.Fatalf("%s.%s: %v", table, column, err)
	}
	if gotType != dataType {
		t.Fatalf("%s.%s type = %s, want %s", table, column, gotType, dataType)
	}
	if minChars > 0 && (!chars.Valid || chars.Int64 < int64(minChars)) {
		t.Fatalf("%s.%s length = %v, want >= %d", table, column, chars, minChars)
	}
}
