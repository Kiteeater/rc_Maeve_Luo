// Package server 构造传输服务，负责路由、中间件与后台角色生命周期。
package server

import (
	"net/http"

	"notifier/internal/auth"
	"notifier/internal/conf"
	"notifier/internal/constant"
	"notifier/internal/service"
)

// NewHTTPServer 只组装 HTTP 传输，不承载协议转换或业务规则。
// releases 为空时只保留健康检查和未实现的 /v1/ 入口。
func NewHTTPServer(cfg conf.HTTP, health *service.HealthService, releases *service.ReleaseService, events *service.EventService, identities *auth.Set) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health.Healthz)
	mux.HandleFunc("GET /readyz", health.Readyz)
	if releases != nil {
		protect := func(next http.Handler) http.Handler {
			return requirePermission(identities, constant.PermConfigPublish, next)
		}
		mux.Handle("POST /v1/config/releases/validate", protect(RequireJSON(http.HandlerFunc(releases.Validate))))
		mux.Handle("POST /v1/config/releases", protect(RequireJSON(http.HandlerFunc(releases.Create))))
		mux.Handle("POST /v1/config/releases/{id}/publish", protect(RequireJSON(http.HandlerFunc(releases.Publish))))
		mux.Handle("GET /v1/config/releases/current", protect(http.HandlerFunc(releases.Current)))
		mux.Handle("GET /v1/config/releases/{id}", protect(http.HandlerFunc(releases.Get)))
	}
	if events != nil {
		mux.Handle("POST /v1/events", requirePermission(identities, constant.PermEventsWrite, RequireJSON(http.HandlerFunc(events.Submit))))
	}
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
