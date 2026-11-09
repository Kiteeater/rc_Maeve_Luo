// Package mysql 提供连接池、通用事务执行器和历史序号分配。
// 具体业务事务由 S02～S07 完成。
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"notifier/internal/auth"
	"notifier/internal/conf"
)

// Open 按 MYSQL_OPERATION_TIMEOUT 建立连接池并确认能连通。
// 依赖 *auth.Set 只是为了让身份配置先于拨号失败，错误里不出现 DSN。
func Open(cfg conf.MySQL, _ *auth.Set) (*sql.DB, func(), error) {
	dsn, err := withTimeouts(cfg.DSN, cfg.OperationTimeout)
	if err != nil {
		return nil, nil, errors.New("mysql: MYSQL_DSN: invalid")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, nil, errors.New("mysql: MYSQL_DSN: open failed")
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.OperationTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, errors.New("mysql: MYSQL_DSN: connect failed")
	}
	return db, func() { _ = db.Close() }, nil
}

func withTimeouts(dsn string, budget time.Duration) (string, error) {
	parsed, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	parsed.Timeout = budget
	parsed.ReadTimeout = budget
	parsed.WriteTimeout = budget
	parsed.ParseTime = true
	return parsed.FormatDSN(), nil
}

// FailureKind 区分唯一键冲突、死锁和连接中断。
type FailureKind int

const (
	FailureOther FailureKind = iota
	FailureUnique
	FailureDeadlock
	FailureConnLost
	FailureCommitUncertain
)

// CommitUncertainError 表示提交时连接丢失，不能判断事务是否落地。调用方不得重做 HTTP 或 MQ。
type CommitUncertainError struct {
	cause error
}

func (e *CommitUncertainError) Error() string { return "mysql: commit uncertain" }

func (e *CommitUncertainError) Unwrap() error { return e.cause }

// Classify 给调用方区分可重试的数据库错误。不包含驱动原文，避免带出连接信息。
func Classify(err error) FailureKind {
	if err == nil {
		return FailureOther
	}
	var uncertain *CommitUncertainError
	if errors.As(err, &uncertain) {
		return FailureCommitUncertain
	}
	if isDeadlock(err) {
		return FailureDeadlock
	}
	if isUnique(err) {
		return FailureUnique
	}
	if isConnLost(err) {
		return FailureConnLost
	}
	return FailureOther
}

// Executor 在单个事务里执行回调。只有确认回滚的死锁或连接中断才重试，最多 2 次。
// 不重试唯一键冲突、锁等待，也不重试提交结果不确定的错误。不得把 HTTP/MQ 调用放进回调。
type Executor struct {
	db      *sql.DB
	timeout time.Duration
}

func NewExecutor(db *sql.DB, timeout time.Duration) *Executor {
	return &Executor{db: db, timeout: timeout}
}

const maxAttempts = 3 // 首次加上最多 2 次重试

func (e *Executor) RunTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	var last error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		opCtx, cancel := context.WithTimeout(ctx, e.timeout)
		tx, err := e.db.BeginTx(opCtx, nil)
		if err != nil {
			cancel()
			last = err
			if attempt < maxAttempts && ctx.Err() == nil && isConnLost(err) {
				continue
			}
			return err
		}
		err = fn(opCtx, tx)
		if err != nil {
			rbErr := tx.Rollback()
			cancel()
			if rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				return fmt.Errorf("mysql: rollback failed")
			}
			last = err
			if attempt < maxAttempts && ctx.Err() == nil && (isDeadlock(err) || isConnLost(err)) {
				continue
			}
			return err
		}
		if err = tx.Commit(); err != nil {
			cancel()
			if isConnLost(err) {
				return &CommitUncertainError{cause: err}
			}
			last = err
			if attempt < maxAttempts && ctx.Err() == nil && isDeadlock(err) {
				continue
			}
			return err
		}
		cancel()
		return nil
	}
	return last
}

func isUnique(err error) bool {
	n, ok := mysqlNumber(err)
	return ok && n == 1062
}

func isDeadlock(err error) bool {
	n, ok := mysqlNumber(err)
	return ok && n == 1213
}

func isConnLost(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysqldrv.ErrInvalidConn) {
		return true
	}
	if n, ok := mysqlNumber(err); ok && (n == 2006 || n == 2013) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func mysqlNumber(err error) (uint16, bool) {
	var me *mysqldrv.MySQLError
	if errors.As(err, &me) {
		return me.Number, true
	}
	return 0, false
}
