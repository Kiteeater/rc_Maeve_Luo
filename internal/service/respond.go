package service

import (
	"encoding/json"
	"net/http"

	"notifier/internal/domain"
)

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
func NotImplemented(w http.ResponseWriter, r *http.Request) {
	err, createErr := domain.NewError(CodeNotImplemented, "接口尚未实现", "", nil)
	if createErr != nil {
		writeJSON(w, http.StatusNotImplemented, errorEnvelope{Error: errorBody{Code: CodeNotImplemented, Message: "接口尚未实现"}})
		return
	}
	WriteError(w, r, err)
}
