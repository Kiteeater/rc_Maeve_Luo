// Package service 负责协议输入输出转换，不注册路由、不访问数据库。
package service

import "net/http"

// Readiness 检查核心依赖是否可用。基础设施接入后由依赖组装提供真实检查。
type Readiness func() error

// HealthService 提供 HTTP 健康检查的协议响应。
type HealthService struct {
	ready Readiness
}

func NewHealthService(ready Readiness) *HealthService {
	if ready == nil {
		ready = func() error { return nil }
	}
	return &HealthService{ready: ready}
}

func (s *HealthService) Healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *HealthService) Readyz(w http.ResponseWriter, _ *http.Request) {
	if err := s.ready(); err != nil {
		// 探针响应不暴露连接信息或底层错误。
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"reason": "core dependencies unavailable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
