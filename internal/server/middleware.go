package server

import (
	"net/http"

	"notifier/internal/service"
)

// RequestID 保证每个响应都带 X-Request-ID。非法入参会换成 32 位十六进制随机值。
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := service.NormalizeRequestID(r.Header.Get("X-Request-ID"))
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(service.WithRequestID(r.Context(), id)))
	})
}

// RequireJSON 让写方法只接受 application/json。读方法保持原样，供后续查询接口使用。
func RequireJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			if err := service.CheckContentType(r); err != nil {
				service.WriteError(w, r, err)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
