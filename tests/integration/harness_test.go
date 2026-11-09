//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate integration tests")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func mysqlEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func adminDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?parseTime=true&charset=utf8mb4&multiStatements=true",
		mysqlEnv("MYSQL_USER", "root"),
		mysqlEnv("MYSQL_PASSWORD", "root"),
		mysqlEnv("MYSQL_HOST", "127.0.0.1"),
		mysqlEnv("MYSQL_PORT", "3306"),
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("mysql: MYSQL_HOST: connect failed: %v", err)
	}
	return db
}

func dbNamed(t *testing.T, name string) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&multiStatements=true",
		mysqlEnv("MYSQL_USER", "root"),
		mysqlEnv("MYSQL_PASSWORD", "root"),
		mysqlEnv("MYSQL_HOST", "127.0.0.1"),
		mysqlEnv("MYSQL_PORT", "3306"),
		name,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func gooseDSN(name string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
		mysqlEnv("MYSQL_USER", "root"),
		mysqlEnv("MYSQL_PASSWORD", "root"),
		mysqlEnv("MYSQL_HOST", "127.0.0.1"),
		mysqlEnv("MYSQL_PORT", "3306"),
		name,
	)
}

func redact(s string) string {
	return strings.ReplaceAll(s, mysqlEnv("MYSQL_PASSWORD", "root"), "[REDACTED]")
}
