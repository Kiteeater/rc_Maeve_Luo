// 通知服务入口：负责依赖组装与进程生命周期。
//
// 依赖图声明在 wire.go，wire_gen.go 由 make wire 生成。
package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/auth"
	"notifier/internal/conf"
	"notifier/internal/data/redis"
	"notifier/internal/data/rocketmq"
	"notifier/internal/logredact"
	"notifier/internal/server"
	"notifier/internal/service"
)

func main() {
	if err := run(); err != nil {
		slog.Error("notifier exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	app, cleanup, err := initializeApp(ctx)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return err
	}
	slog.SetDefault(app.log)
	return app.run(ctx)
}

type application struct {
	app *server.App
	log *slog.Logger
}

func newApplication(httpCfg conf.HTTP, logCfg conf.Log, srv *http.Server, roles *server.Group) *application {
	logger := logredact.New(logCfg)
	return &application{
		log: logger,
		app: &server.App{
			HTTP:            srv,
			Roles:           roles,
			ShutdownTimeout: httpCfg.ShutdownTimeout,
			Log:             logger,
		},
	}
}

func (a *application) run(ctx context.Context) error { return a.app.Run(ctx) }

func loadIdentities(cfg conf.Auth) (*auth.Set, error) {
	return auth.LoadFile(cfg.ConfigRef)
}

func emptyRoles() *server.Group { return server.NewGroup() }

// newReadiness 检查 MySQL、Redis、RocketMQ 和可用身份。探针响应不回传这里的错误文本。
func newReadiness(db *sql.DB, rdb *goredis.Client, mq *rocketmq.Client, ids *auth.Set, cfg conf.MySQL) service.Readiness {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.OperationTimeout)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return errors.New("mysql: MYSQL_DSN: not ready")
		}
		if err := redis.Healthy(ctx, rdb); err != nil {
			return err
		}
		if err := mq.Healthy(ctx); err != nil {
			return err
		}
		if ids == nil || !ids.Available() {
			return errors.New("auth: AUTH_CONFIG_REF: no usable identity")
		}
		return nil
	}
}
