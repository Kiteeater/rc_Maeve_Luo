// 通知服务入口：负责依赖组装与进程生命周期。
//
// 依赖图声明在 wire.go，wire_gen.go 由 make wire 生成。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notifier/internal/conf"
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
	app, err := initializeApp(ctx)
	if err != nil {
		return err
	}
	slog.SetDefault(app.logger)
	return app.run(ctx)
}

type application struct {
	http   *http.Server
	logger *slog.Logger
}

func newApplication(logConfig conf.Log, srv *http.Server) *application {
	return &application{http: srv, logger: newLogger(logConfig)}
}

func (a *application) run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		a.logger.Info("http server listening", "addr", a.http.Addr)
		errCh <- a.http.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		a.logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := a.http.Shutdown(shutdownCtx); err != nil {
		_ = a.http.Close()
		return err
	}
	a.logger.Info("notifier stopped")
	return nil
}

// newLogger 依据配置构造结构化日志器，输出 JSON（默认）或文本。
func newLogger(c conf.Log) *slog.Logger {
	var level slog.Level
	switch c.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if c.Format == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(h)
}
