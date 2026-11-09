//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os/exec"
	"testing"
	"time"
)

func TestBatch0_ZZ_MySQLRestartKeepsData(t *testing.T) {
	db := dbNamed(t, "notifier")
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS s00_restart_probe (
		id INT PRIMARY KEY,
		note VARCHAR(32) NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		probe, err := sql.Open("mysql", gooseDSN("notifier"))
		if err != nil {
			return
		}
		defer probe.Close()
		_, _ = probe.ExecContext(context.Background(), `DROP TABLE IF EXISTS s00_restart_probe`)
	})
	if _, err := db.ExecContext(ctx, `INSERT INTO s00_restart_probe (id, note) VALUES (1, 'kept')
		ON DUPLICATE KEY UPDATE note = 'kept'`); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("docker", "compose", "restart", "mysql")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restart mysql: %v\n%s", err, out)
	}

	deadline := time.Now().Add(90 * time.Second)
	var note string
	var last error
	for time.Now().Before(deadline) {
		probe, err := sql.Open("mysql", gooseDSN("notifier"))
		if err != nil {
			last = err
			time.Sleep(time.Second)
			continue
		}
		err = probe.QueryRowContext(context.Background(), `SELECT note FROM s00_restart_probe WHERE id = 1`).Scan(&note)
		_ = probe.Close()
		if err == nil && note == "kept" {
			return
		}
		last = err
		time.Sleep(time.Second)
	}
	t.Fatalf("probe row did not survive restart: %q %v", note, last)
}
