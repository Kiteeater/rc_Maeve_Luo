// Package server 构造传输服务，负责路由、中间件与后台角色生命周期。
package server

import (
	"net/http"

	"notifier/internal/conf"
	"notifier/internal/service"
)

// NewHTTPServer 只组装 HTTP 传输，不承载协议转换或业务规则。
func NewHTTPServer(cfg conf.HTTP, health *service.HealthService) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health.Healthz)
	mux.HandleFunc("GET /readyz", health.Readyz)
	mux.Handle("/v1/", RequireJSON(http.HandlerFunc(service.NotImplemented)))
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           RequestID(mux),
		ReadHeaderTimeout: cfg.RequestTimeout,
		ReadTimeout:       cfg.RequestTimeout,
		WriteTimeout:      cfg.RequestTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
}
