// Package httpapi 负责 HTTP 接入的认证、路由与输入输出边界。
//
// 本包只处理传输层职责：把请求解析为领域输入、把领域结果序列化为响应，
// 并提供健康检查与统一的错误响应格式。路由、映射、投递等业务逻辑由内部各包承担。
//
// 接口面见 TECH_DESIGN.md 第 12 节；本脚手架仅接好 /healthz、/readyz 与 /v1 占位，
// 业务端点留待实现阶段挂载。
package httpapi

import "net/http"

// Server 聚合 HTTP 层依赖。
//
// 实现阶段在此注入领域服务（event.Service、deliverytask.Service 等），
// 并将对应处理函数挂载到 Handler 返回的路由树。
type Server struct {
	ready func() error // 就绪探针：核心依赖是否可用；nil 表示始终就绪
}

// New 构造 HTTP 服务。ready 用于 /readyz 判定核心依赖是否就绪。
func New(ready func() error) *Server {
	if ready == nil {
		ready = func() error { return nil }
	}
	return &Server{ready: ready}
}

// Handler 返回完整路由树。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	// 业务接口占位：实现阶段替换为具体路由与鉴权中间件。
	mux.Handle("/v1/", notImplemented("接口尚未实现"))

	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if err := s.ready(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"reason": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
