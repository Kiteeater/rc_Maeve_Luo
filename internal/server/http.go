// Package server 构造传输服务，负责路由与中间件注册。
package server

import (
	"net/http"
	"time"

	"notifier/internal/conf"
	"notifier/internal/service"
)

// NewHTTPServer 只组装 HTTP 传输，不承载协议转换或业务规则。
func NewHTTPServer(cfg conf.HTTP, health *service.HealthService) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health.Healthz)
	mux.HandleFunc("GET /readyz", health.Readyz)
	mux.HandleFunc("/v1/", service.NotImplemented)
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
