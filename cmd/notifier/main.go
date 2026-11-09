// 通知服务入口：负责依赖组装与进程生命周期。
//
// 本文件只做启动编排：解析配置、初始化结构化日志、启动 HTTP 接入服务，
// 并监听系统信号实现优雅退出。MySQL / Redis / RocketMQ 连接、Outbox 发布器、
// 任务消费者与补偿扫描器的组装在对应包实现后在此接入。
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

	"notifier/internal/config"
	"notifier/internal/httpapi"
)

func main() {
	if err := run(); err != nil {
		slog.Error("notifier exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	slog.SetDefault(newLogger(cfg.Log))

	// TODO(实现阶段)：在此按顺序组装
	//  - MySQL 连接池与迁移状态校验
	//  - Redis 客户端与配置缓存
	//  - RocketMQ 生产/消费客户端
	//  - Outbox 发布器、worker、补偿扫描器（各自可停止的后台循环）

	// ready 用于 /readyz 判定核心依赖是否就绪；待上述依赖接入后补充真实检查。
	api := httpapi.New(func() error { return nil })

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		slog.Info("http server listening", "addr", cfg.HTTP.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	slog.Info("notifier stopped")
	return nil
}

// newLogger 依据配置构造结构化日志器，输出 JSON（默认）或文本。
func newLogger(c config.Log) *slog.Logger {
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
