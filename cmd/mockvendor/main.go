// mockvendor 是独立的模拟供应商进程，用于集成验证与故障演练。
//
// 它只监听环回地址。容器端口映射访问不到 127.0.0.1，宿主机验证使用 make mockvendor。
package main

import (
	"log/slog"
	"os"
	"time"

	"notifier/internal/mockvendor"
)

func main() {
	opts := mockvendor.Options{
		Addr:            os.Getenv("MOCKVENDOR_ADDR"),
		StatePath:       envStr("MOCKVENDOR_STATE_PATH", ".mockvendor-state.json"),
		DefaultScenario: os.Getenv("MOCKVENDOR_SCENARIO"),
	}
	if raw := os.Getenv("MOCKVENDOR_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			slog.Error("mockvendor config", "error", "MOCKVENDOR_TIMEOUT: expected duration")
			os.Exit(1)
		}
		opts.Timeout = d
	}
	srv, err := mockvendor.New(opts)
	if err != nil {
		slog.Error("mockvendor config", "error", err)
		os.Exit(1)
	}
	slog.Info("mock vendor listening", "addr", srv.Addr())
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("mock vendor exited", "error", err)
		os.Exit(1)
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
