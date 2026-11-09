package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// App 管理 HTTP 入口和后台角色的启动与有界退出。
// 收到停止信号后先关闭入口，再取消角色，整段受 HTTP_SHUTDOWN_TIMEOUT 约束。
type App struct {
	HTTP            *http.Server
	Roles           *Group
	ShutdownTimeout time.Duration
	Log             *slog.Logger
}

// Run 监听 HTTP 并运行后台角色，直到 ctx 取消或某一方失败。
func (a *App) Run(ctx context.Context) error {
	if a.Log == nil {
		a.Log = slog.Default()
	}
	if a.Roles == nil {
		a.Roles = NewGroup()
	}
	roleCtx, roleCancel := context.WithCancel(context.Background())
	defer roleCancel()

	httpErr := make(chan error, 1)
	go func() {
		a.Log.Info("http server listening", "addr", a.HTTP.Addr)
		err := a.HTTP.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpErr <- err
	}()

	roleErr := make(chan error, 1)
	go func() {
		roleErr <- a.Roles.Run(roleCtx)
	}()

	select {
	case err := <-httpErr:
		roleCancel()
		<-roleErr
		return err
	case err := <-roleErr:
		_ = a.shutdownWithin(a.ShutdownTimeout)
		return err
	case <-ctx.Done():
	}

	a.Log.Info("shutdown signal received")
	// 入口关闭和角色排空共用同一个 HTTP_SHUTDOWN_TIMEOUT。
	stopCtx, cancel := context.WithTimeout(context.Background(), a.ShutdownTimeout)
	defer cancel()
	if err := a.HTTP.Shutdown(stopCtx); err != nil {
		roleCancel()
		_ = a.HTTP.Close()
		return fmt.Errorf("server: HTTP_SHUTDOWN_TIMEOUT: %w", err)
	}
	roleCancel()
	select {
	case err := <-roleErr:
		if err != nil {
			return err
		}
		a.Log.Info("notifier stopped")
		return nil
	case <-stopCtx.Done():
		return fmt.Errorf("server: HTTP_SHUTDOWN_TIMEOUT exceeded while draining roles")
	}
}

func (a *App) shutdownWithin(budget time.Duration) error {
	stopCtx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := a.HTTP.Shutdown(stopCtx); err != nil {
		_ = a.HTTP.Close()
		return fmt.Errorf("server: HTTP_SHUTDOWN_TIMEOUT: %w", err)
	}
	return nil
}
