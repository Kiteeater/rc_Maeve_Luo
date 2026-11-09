package service_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"notifier/internal/domain"
	"notifier/internal/service"
)

func TestHTTPErrorEnvelopeAndStatus(t *testing.T) {
	if service.StatusOf(service.CodePayloadTooLarge) != http.StatusRequestEntityTooLarge {
		t.Fatal("413 mapping")
	}
	if service.StatusOf(service.CodeUnsupportedMediaType) != http.StatusUnsupportedMediaType {
		t.Fatal("415 mapping")
	}
	if service.StatusOf(service.CodeIdempotencyConflict) != http.StatusConflict {
		t.Fatal("409 mapping")
	}
	err, createErr := domain.NewError(service.CodeInvalidRequest, "请求包含未知字段", "extra", []domain.Detail{{
		FieldPath: "data.n", Code: service.CodeInvalidRequest, Message: "无效",
	}})
	if createErr != nil {
		t.Fatal(createErr)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	req = req.WithContext(service.WithRequestID(req.Context(), "req-1"))
	rec := httptest.NewRecorder()
	service.WriteError(rec, req, err)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"request_id":"req-1"`) {
		t.Fatalf("envelope = %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"field_path":"extra"`) || strings.Contains(rec.Body.String(), "secret-value") {
		t.Fatalf("field path or leak: %s", rec.Body.String())
	}
}

func TestReadBodyAndQueryAndUnknownField(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/events?limit=1&limit=2", strings.NewReader("12345"))
	req.ContentLength = 1
	if _, err := service.ReadBody(req, 4); err == nil || service.StatusOf(codeOf(t, err)) != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body = %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader("1234"))
	body, err := service.ReadBody(req, 4)
	if err != nil || string(body) != "1234" {
		t.Fatalf("exact body = %q %v", body, err)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/events?limit=1&limit=2", nil)
	if err := service.RejectQuery(req, []string{"limit"}); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("duplicate query = %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/events?unknown=secret-value", nil)
	err = service.RejectQuery(req, []string{"limit"})
	if err == nil || strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("unknown query leaked or passed: %v", err)
	}
	err = service.RejectUnknownFields([]byte(`{"a":1,"extra":"secret-value"}`), []string{"a"})
	if err == nil || strings.Contains(err.Error(), "secret-value") || !strings.Contains(err.Error(), "extra") {
		t.Fatalf("unknown field = %v", err)
	}
	if err := service.RejectUnknownFields([]byte(`{"a":1}{"b":2}`), []string{"a"}); err == nil {
		t.Fatal("trailing data must be rejected")
	}
}

func TestContentType(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if err := service.CheckContentType(req); err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain")
	if service.StatusOf(codeOf(t, service.CheckContentType(req))) != http.StatusUnsupportedMediaType {
		t.Fatal("non-json must be 415")
	}
	req.Header.Set("Content-Type", "application/json; charset=latin1")
	if service.StatusOf(codeOf(t, service.CheckContentType(req))) != http.StatusUnsupportedMediaType {
		t.Fatal("non-utf8 charset must be 415")
	}
}

func codeOf(t *testing.T, err error) string {
	t.Helper()
	var de *domain.Error
	if !errorAs(err, &de) {
		t.Fatalf("not a domain error: %v", err)
	}
	return de.Code
}

func errorAs(err error, target **domain.Error) bool {
	if err == nil {
		return false
	}
	de, ok := err.(*domain.Error)
	if !ok {
		return false
	}
	*target = de
	return true
}
