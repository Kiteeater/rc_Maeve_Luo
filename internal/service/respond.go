package service

import (
	"encoding/json"
	"net/http"
)

// errorEnvelope 是统一的错误响应结构。
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

// errorBody 是错误响应的可定位信息。
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeJSON 以 JSON 写出响应体；编码失败时返回统一 500 占位。
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal","message":"encode response failed"}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// NotImplemented 提供业务接口尚未实现时的统一 501 响应。
func NotImplemented(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotImplemented, errorEnvelope{
		Error: errorBody{Code: "not_implemented", Message: "接口尚未实现"},
	})
}
