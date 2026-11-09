package server_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"notifier/internal/conf"
	"notifier/internal/server"
	"notifier/internal/service"
)

func TestHTTPRoutes(t *testing.T) {
	for _, tc := range []struct {
		method string
		path   string
		status int
		body   string
	}{
		{http.MethodGet, "/healthz", http.StatusOK, "ok"},
		{http.MethodGet, "/readyz", http.StatusOK, "ready"},
		{http.MethodPost, "/v1/events", http.StatusNotImplemented, "not_implemented"},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, ""},
		{http.MethodGet, "/missing", http.StatusNotFound, ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			srv := server.NewHTTPServer(conf.Default().HTTP, service.NewHealthService(nil))
			response := httptest.NewRecorder()
			request := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.method == http.MethodPost {
				request.Header.Set("Content-Type", "application/json")
			}
			srv.Handler.ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d", response.Code, tc.status)
			}
			if tc.body != "" {
				if response.Header().Get("Content-Type") != "application/json; charset=utf-8" {
					t.Fatal("protocol response must declare JSON")
				}
				var body struct {
					Status string `json:"status"`
					Error  struct {
						Code string `json:"code"`
					} `json:"error"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if tc.status == http.StatusNotImplemented {
					if body.Error.Code != tc.body {
						t.Fatalf("error code = %q, want %q", body.Error.Code, tc.body)
					}
				} else if body.Status != tc.body {
					t.Fatalf("status body = %q, want %q", body.Status, tc.body)
				}
			}
		})
	}
}

func TestReadinessFailureDoesNotExposeDependencyErrors(t *testing.T) {
	health := service.NewHealthService(func() error { return errors.New("mysql: secret-password") })
	srv := server.NewHTTPServer(conf.Default().HTTP, health)
	response := httptest.NewRecorder()
	srv.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "secret-password") {
		t.Fatalf("unexpected readiness failure response: %s", response.Body.String())
	}
	response = httptest.NewRecorder()
	srv.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusOK {
		t.Fatal("dependency failure must not fail process liveness")
	}
}
