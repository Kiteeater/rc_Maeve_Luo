package service

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"notifier/internal/canonicaljson"
	"notifier/internal/domain"
)

// 错误码与 HTTP 状态的对应关系见 S00 第 8 节。未知码按 internal/500 处理。
const (
	CodeInvalidRequest           = "invalid_request"
	CodeInvalidCursor            = "invalid_cursor"
	CodeUnauthenticated          = "unauthenticated"
	CodeForbidden                = "forbidden"
	CodeNotFound                 = "not_found"
	CodeIdempotencyConflict      = "idempotency_conflict"
	CodeVersionConflict          = "version_conflict"
	CodeStateConflict            = "state_conflict"
	CodePayloadTooLarge          = "payload_too_large"
	CodeUnsupportedMediaType     = "unsupported_media_type"
	CodeUnsupportedSchema        = "unsupported_schema"
	CodeNoMatchingRoute          = "no_matching_route"
	CodeMappingInvalid           = "mapping_invalid"
	CodeUnsafeReplay             = "unsafe_replay"
	CodeRateLimited              = "rate_limited"
	CodeDependencyUnavailable    = "dependency_unavailable"
	CodeAdmissionPaused          = "admission_paused"
	CodeInternal                 = "internal"
	CodeNotImplemented           = "not_implemented"
	CodeConfigInvalid            = "config_invalid"
	CodeConfigurationChanged     = "configuration_changed"
	CodeConfigurationUnavailable = "configuration_unavailable"
	CodePlanTimeout              = "plan_timeout"
	CodeLimitExceeded            = "limit_exceeded"
	CodeEventDataInvalid         = "event_data_invalid"
	CodeFanoutExceeded           = "fanout_exceeded"
	CodeAcceptanceUnconfirmed    = "acceptance_unconfirmed"
)

// StatusOf 返回稳定错误码对应的 HTTP 状态。
func StatusOf(code string) int {
	switch code {
	case CodeInvalidRequest, CodeInvalidCursor:
		return http.StatusBadRequest
	case CodeUnauthenticated:
		return http.StatusUnauthorized
	case CodeForbidden:
		return http.StatusForbidden
	case CodeNotFound:
		return http.StatusNotFound
	case CodeIdempotencyConflict, CodeVersionConflict, CodeStateConflict, CodeConfigurationChanged:
		return http.StatusConflict
	case CodePayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	case CodeUnsupportedMediaType:
		return http.StatusUnsupportedMediaType
	case CodeUnsupportedSchema, CodeEventDataInvalid, CodeNoMatchingRoute, CodeMappingInvalid, CodeFanoutExceeded, CodeUnsafeReplay, CodeConfigInvalid, CodeLimitExceeded:
		return http.StatusUnprocessableEntity
	case CodeRateLimited:
		return http.StatusTooManyRequests
	case CodeDependencyUnavailable, CodeAdmissionPaused, CodeAcceptanceUnconfirmed, CodeConfigurationUnavailable, CodePlanTimeout:
		return http.StatusServiceUnavailable
	case CodeNotImplemented:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code      string       `json:"code"`
	Message   string       `json:"message"`
	FieldPath string       `json:"field_path,omitempty"`
	RequestID string       `json:"request_id,omitempty"`
	Details   []detailBody `json:"details,omitempty"`
}

type detailBody struct {
	FieldPath string `json:"field_path,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

// WriteError 按领域错误写出 envelope。message 使用领域错误里的安全文本。
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	var de *domain.Error
	if !errors.As(err, &de) || de == nil {
		de = &domain.Error{Code: CodeInternal, Message: "内部错误"}
	}
	code := de.Code
	if StatusOf(code) == http.StatusInternalServerError && code != CodeInternal {
		code = CodeInternal
	}
	body := errorBody{Code: code, Message: de.Message, FieldPath: de.FieldPath}
	if r != nil {
		body.RequestID = RequestIDFrom(r.Context())
	}
	if len(de.Details) > 0 {
		body.Details = make([]detailBody, len(de.Details))
		for i, item := range de.Details {
			body.Details[i] = detailBody{FieldPath: item.FieldPath, Code: item.Code, Message: item.Message}
		}
	}
	writeJSON(w, StatusOf(code), errorEnvelope{Error: body})
}

// CheckContentType 只接受 application/json，可选 charset=utf-8。
func CheckContentType(r *http.Request) error {
	media := r.Header.Get("Content-Type")
	media, _, _ = strings.Cut(media, ";")
	params := contentParams(r.Header.Get("Content-Type"))
	if !strings.EqualFold(strings.TrimSpace(media), "application/json") {
		err, _ := domain.NewError(CodeUnsupportedMediaType, "仅接受 application/json", "", nil)
		return err
	}
	if charset, ok := params["charset"]; ok && !strings.EqualFold(charset, "utf-8") {
		err, _ := domain.NewError(CodeUnsupportedMediaType, "仅接受 application/json", "", nil)
		return err
	}
	return nil
}

func contentParams(header string) map[string]string {
	out := map[string]string{}
	parts := strings.Split(header, ";")
	for _, part := range parts[1:] {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		out[strings.ToLower(strings.TrimSpace(key))] = strings.Trim(strings.TrimSpace(value), `"`)
	}
	return out
}

// ReadBody 按实际读取字节判断大小，不信任 Content-Length。
// 超过 maxBytes 返回 payload_too_large。
func ReadBody(r *http.Request, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		maxBytes = 0
	}
	limited := io.LimitReader(r.Body, maxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		de, _ := domain.NewError(CodeInvalidRequest, "无法读取请求体", "", nil)
		return nil, de
	}
	if int64(len(body)) > maxBytes {
		de, _ := domain.NewError(CodePayloadTooLarge, "请求体超过上限", "", nil)
		return nil, de
	}
	return body, nil
}

// RejectUnknownFields 拒绝 envelope 中的未知字段。不回显字段值。
func RejectUnknownFields(raw []byte, allowed []string) error {
	value, err := canonicaljson.ParseObject(raw)
	if err != nil {
		de, _ := domain.NewError(CodeInvalidRequest, "JSON 不是完整对象", "", nil)
		return de
	}
	fields, ok := value.Object()
	if !ok {
		de, _ := domain.NewError(CodeInvalidRequest, "JSON 不是完整对象", "", nil)
		return de
	}
	allow := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allow[key] = struct{}{}
	}
	for _, field := range fields {
		if _, ok := allow[field.Key]; !ok {
			de, _ := domain.NewError(CodeInvalidRequest, "请求包含未知字段", field.Key, nil)
			return de
		}
	}
	return nil
}

// RejectQuery 拒绝未知查询参数和重复参数。错误不包含参数值。
func RejectQuery(r *http.Request, allowed []string) error {
	query := r.URL.Query()
	allow := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allow[key] = struct{}{}
	}
	for key, values := range query {
		if _, ok := allow[key]; !ok {
			de, _ := domain.NewError(CodeInvalidRequest, "未知查询参数", key, nil)
			return de
		}
		if len(values) > 1 {
			de, _ := domain.NewError(CodeInvalidRequest, "查询参数重复", key, nil)
			return de
		}
	}
	return nil
}
