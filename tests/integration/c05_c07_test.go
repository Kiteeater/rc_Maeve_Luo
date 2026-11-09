//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"notifier/internal/canonicaljson"
	"notifier/internal/data/mysql"
)

func TestC05_TransactionRollsBackWhenHistoryOrBizWriteFails(t *testing.T) {
	const name = "notifier_s00_c05"
	admin := adminDB(t)
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name) })

	db := dbNamed(t, name)
	_, err := db.ExecContext(ctx, `
		CREATE TABLE s00_c05_biz (
			id BIGINT PRIMARY KEY,
			note VARCHAR(32) NOT NULL
		);
		CREATE TABLE s00_c05_hist (
			id BIGINT PRIMARY KEY,
			biz_id BIGINT NOT NULL,
			note VARCHAR(32) NOT NULL
		)`)
	if err != nil {
		t.Fatal(err)
	}
	ex := mysql.NewExecutor(db, 2*time.Second)

	err = ex.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO s00_c05_biz (id, note) VALUES (1, 'biz')`); err != nil {
			return err
		}
		return errors.New("history write failed")
	})
	if err == nil {
		t.Fatal("history failure must surface")
	}
	assertCount(t, db, "s00_c05_biz", 0)
	assertCount(t, db, "s00_c05_hist", 0)

	err = ex.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO s00_c05_biz (id, note) VALUES (2, 'biz')`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO s00_c05_hist (id, biz_id, note) VALUES (2, 2, NULL)`)
		return err
	})
	if err == nil {
		t.Fatal("invalid history insert must fail")
	}
	assertCount(t, db, "s00_c05_biz", 0)
	assertCount(t, db, "s00_c05_hist", 0)

	var calls int
	err = ex.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		calls++
		if calls < 3 {
			return &mysqldrv.MySQLError{Number: 1213, Message: "deadlock"}
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO s00_c05_biz (id, note) VALUES (3, 'ok')`)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO s00_c05_hist (id, biz_id, note) VALUES (3, 3, 'hist')`)
		return err
	})
	if err != nil || calls != 3 {
		t.Fatalf("deadlock retry calls=%d err=%v", calls, err)
	}
	assertCount(t, db, "s00_c05_biz", 1)

	calls = 0
	err = ex.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		calls++
		if _, err := tx.ExecContext(ctx, `INSERT INTO s00_c05_biz (id, note) VALUES (9, 'a')`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO s00_c05_biz (id, note) VALUES (9, 'b')`)
		return err
	})
	if err == nil || calls != 1 || mysql.Classify(err) != mysql.FailureUnique {
		t.Fatalf("unique conflict calls=%d kind=%v err=%v", calls, mysql.Classify(err), err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM s00_c05_biz WHERE id = 9`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("unique conflict left a row: %d %v", n, err)
	}
}

func TestC07_CanonicalBlobFingerprintSurvivesMySQLJSONProjection(t *testing.T) {
	const name = "notifier_s00_c07"
	admin := adminDB(t)
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name) })

	raw := []byte(`{"d":0.01230,"n":9007199254740993}`)
	canonical, err := canonicaljson.Canonical(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := canonicaljson.Canonical(canonical)
	if err != nil || string(again) != string(canonical) {
		t.Fatalf("round trip changed canonical bytes: %s", again)
	}
	if strings.Contains(string(canonical), "9007199254740992") || !strings.Contains(string(canonical), "9007199254740993") {
		t.Fatalf("integer changed: %s", canonical)
	}
	if !strings.Contains(string(canonical), "123e-4") {
		t.Fatalf("decimal changed: %s", canonical)
	}
	wantFP := canonicaljson.Fingerprint(canonical)

	db := dbNamed(t, name)
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE s00_c07 (
			id INT PRIMARY KEY,
			exact MEDIUMBLOB NOT NULL,
			projected JSON NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO s00_c07 (id, exact, projected) VALUES (1, ?, CAST(? AS JSON))`, canonical, canonical); err != nil {
		t.Fatal(err)
	}
	var blob []byte
	var projected string
	if err := db.QueryRowContext(ctx, `SELECT exact, CAST(projected AS CHAR) FROM s00_c07 WHERE id = 1`).Scan(&blob, &projected); err != nil {
		t.Fatal(err)
	}
	if string(blob) != string(canonical) || canonicaljson.Fingerprint(blob) != wantFP {
		t.Fatalf("blob fingerprint changed: %s", blob)
	}
	if projected == string(canonical) || strings.Contains(projected, "9007199254740993") || strings.Contains(projected, "123e-4") {
		t.Fatalf("JSON projection was expected to differ, got %s", projected)
	}
}

func assertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil || n != want {
		t.Fatalf("%s count = %d, want %d, err %v", table, n, want, err)
	}
}
