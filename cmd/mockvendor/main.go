// mockvendor 是独立的模拟供应商进程，用于集成验证与故障演练。
//
// 它只用于测试通知服务的投递、重试与结果判定，不属于通知服务的业务代码。
// 首次仅提供最简接收端点；故障注入与接收记录按 tests/integration 需要扩展。
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := envStr("MOCKVENDOR_ADDR", ":9090")

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("received notification", "method", r.Method, "path", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("mock vendor listening", "addr", addr)
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
