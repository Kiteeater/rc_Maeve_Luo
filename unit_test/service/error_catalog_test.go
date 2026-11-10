package service_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"notifier/internal/constant"
	"notifier/internal/domain"
	"notifier/internal/service"
)

func TestPreciseErrorReasonsKeepExistingHTTPContract(t *testing.T) {
	cases := []struct {
		reason   string
		code     string
		category string
		status   int
	}{
		{constant.ReasonEventIDConflict, constant.CodeIdempotencyConflict, "event", 409},
		{constant.ReasonOperationKey, constant.CodeIdempotencyConflict, "operation", 409},
		{constant.ReasonBacklogCount, constant.CodeAdmissionPaused, "admission", 503},
		{constant.ReasonBacklogAge, constant.CodeAdmissionPaused, "admission", 503},
		{constant.ReasonReleasePublished, constant.CodeStateConflict, "configuration", 409},
		{constant.ReasonTaskLeaseActive, constant.CodeStateConflict, "operation", 409},
		{constant.ReasonEvidenceForeignAttempt, constant.CodeEvidenceInvalid, "operation", 422},
	}
	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			err, createErr := domain.NewError(tc.reason, "UNTRUSTED_MESSAGE", "source_event_id", nil)
			if createErr != nil {
				t.Fatal(createErr)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
			req = req.WithContext(service.WithRequestID(req.Context(), "request-123"))
			rec := httptest.NewRecorder()
			service.WriteError(rec, req, fmt.Errorf("use case: %w", err))
			var envelope map[string]map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			fields := envelope["error"]
			if rec.Code != tc.status || fields["code"] != tc.code || fields["reason_code"] != tc.reason || fields["category"] != tc.category {
				t.Fatalf("classification = %d %s", rec.Code, rec.Body.String())
			}
			if fields["request_id"] != "request-123" || fields["field_path"] != "source_event_id" {
				t.Fatalf("correlation or field location lost: %s", rec.Body.String())
			}
			if fields["message"] == "" || strings.Contains(rec.Body.String(), "UNTRUSTED_MESSAGE") {
				t.Fatalf("catalog message missing or overridden: %s", rec.Body.String())
			}
		})
	}
}

func TestUnknownAndInternalErrorsDoNotExposeOriginalDetails(t *testing.T) {
	cases := []error{
		errors.New("SECRET_SQL"),
		&domain.Error{Code: "unregistered", Message: "SECRET_SQL", FieldPath: "SECRET_PATH", Details: []domain.Detail{{Message: "SECRET_DETAIL"}}},
		&domain.Error{Code: constant.CodeInternal, Message: "SECRET_SQL", FieldPath: "SECRET_PATH", Details: []domain.Detail{{Message: "SECRET_DETAIL"}}},
		&domain.Error{Code: constant.CodeInvalidRequest, ReasonCode: constant.ReasonEventIDConflict, Message: "SECRET_SQL"},
		&domain.Error{Code: constant.CodeInvalidRequest, ReasonCode: "unregistered", Message: "SECRET_SQL"},
	}
	for _, cause := range cases {
		req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
		req = req.WithContext(service.WithRequestID(req.Context(), "request-500"))
		rec := httptest.NewRecorder()
		service.WriteError(rec, req, fmt.Errorf("wrapped: %w", cause))
		var envelope map[string]map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		fields := envelope["error"]
		if rec.Code != 500 || string(fields["code"]) != `"internal"` || string(fields["reason_code"]) != `"internal"` || string(fields["category"]) != `"internal"` {
			t.Fatalf("internal classification = %d %s", rec.Code, rec.Body.String())
		}
		if string(fields["request_id"]) != `"request-500"` || string(fields["message"]) != `"内部错误"` {
			t.Fatalf("internal response = %s", rec.Body.String())
		}
		if _, ok := fields["field_path"]; ok {
			t.Fatalf("internal path exposed: %s", rec.Body.String())
		}
		if _, ok := fields["details"]; ok {
			t.Fatalf("internal details exposed: %s", rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "SECRET") {
			t.Fatalf("internal message exposed: %s", rec.Body.String())
		}
	}
}

func TestRequestValidationHasDistinctReasons(t *testing.T) {
	cases := []struct {
		err    error
		reason string
	}{
		{service.RejectUnknownFields([]byte(`{"a":1} trailing`), []string{"a"}), constant.ReasonInvalidJSON},
		{service.RejectUnknownFields([]byte(`{"extra":1}`), []string{"a"}), constant.ReasonUnknownField},
		{service.RejectQuery(httptest.NewRequest("GET", "/?a=1&a=2", nil), []string{"a"}), constant.ReasonDuplicateQuery},
		{service.RejectQuery(httptest.NewRequest("GET", "/?extra=1", nil), []string{"a"}), constant.ReasonUnknownQuery},
	}
	for _, tc := range cases {
		var de *domain.Error
		if !errors.As(tc.err, &de) || de.Code != constant.CodeInvalidRequest || de.ReasonCode != tc.reason {
			t.Fatalf("request validation reason %s: %v", tc.reason, tc.err)
		}
	}
}
