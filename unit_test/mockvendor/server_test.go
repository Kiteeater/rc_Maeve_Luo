package mockvendor_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"notifier/internal/mockvendor"
)

func TestMockVendorRejectsNonLoopback(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:9090", "192.0.2.10:9090", "not-a-host:9090"} {
		if _, err := mockvendor.New(mockvendor.Options{Addr: addr}); err == nil {
			t.Fatalf("%s must be rejected", addr)
		}
	}
	for _, addr := range []string{"", "127.0.0.1:9090", "localhost:9090", "[::1]:9090"} {
		srv, err := mockvendor.New(mockvendor.Options{Addr: addr})
		if err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
		if !strings.HasPrefix(srv.Addr(), "127.0.0.1:") && srv.Addr() != "[::1]:9090" {
			t.Fatalf("%s became %s", addr, srv.Addr())
		}
	}
}

func TestMockVendorCountsAndScenarios(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	srv := newVendor(t, path, 20*time.Millisecond)
	handler := srv.Handler()

	post(t, handler, "same-key", mockvendor.ScenarioAccept, http.StatusOK, `"accepted":true`)
	post(t, handler, "same-key", mockvendor.ScenarioReject, http.StatusOK, `"accepted":false`)
	post(t, handler, "other-key", mockvendor.ScenarioRateLimit, http.StatusTooManyRequests, "RATE_LIMIT")
	post(t, handler, "other-key", mockvendor.ScenarioServerError, http.StatusInternalServerError, "SERVER_ERROR")
	post(t, handler, "redir", mockvendor.ScenarioRedirect, http.StatusFound, "")
	post(t, handler, "late", mockvendor.ScenarioTimeout, http.StatusOK, `"late":true`)

	counts := getCounts(t, handler)
	if counts.Global != 6 || counts.Keys["same-key"] != 2 || counts.Keys["other-key"] != 2 {
		t.Fatalf("counts = %+v", counts)
	}

	restarted := newVendor(t, path, time.Second)
	again := getCounts(t, restarted.Handler())
	if again.Global != 6 || again.Keys["same-key"] != 2 {
		t.Fatalf("state did not survive reload: %+v", again)
	}
	post(t, restarted.Handler(), "same-key", mockvendor.ScenarioAccept, http.StatusOK, `"accepted":true`)
	final := getCounts(t, restarted.Handler())
	if final.Global != 7 || final.Keys["same-key"] != 3 {
		t.Fatalf("count did not continue: %+v", final)
	}
	raw, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(raw), `"global":7`) {
		t.Fatalf("state file = %s, %v", raw, err)
	}
}

func TestMockVendorAcceptThenDisconnect(t *testing.T) {
	dir := t.TempDir()
	srv := newVendor(t, filepath.Join(dir, "state.json"), time.Second)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	req, err := http.NewRequest(http.MethodPost, httpSrv.URL+"/vendor", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Idempotency-Key", "drop")
	req.Header.Set("X-Mock-Scenario", mockvendor.ScenarioAcceptThenDisconnect)
	resp, err := httpSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"accepted":true`) {
		t.Fatalf("status %d body %s", resp.StatusCode, body)
	}
	counts := getCounts(t, srv.Handler())
	if counts.Global != 1 || counts.Keys["drop"] != 1 {
		t.Fatalf("disconnect still counts: %+v", counts)
	}
}

func newVendor(t *testing.T, path string, timeout time.Duration) *mockvendor.Server {
	t.Helper()
	srv, err := mockvendor.New(mockvendor.Options{Addr: "127.0.0.1:9090", StatePath: path, Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func post(t *testing.T, handler http.Handler, key, scenario string, status int, bodyPart string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/hooks?scenario="+scenario, strings.NewReader(`{"n":1}`))
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != status {
		t.Fatalf("%s status = %d, want %d, body %s", scenario, rec.Code, status, rec.Body.String())
	}
	if bodyPart != "" && !strings.Contains(rec.Body.String(), bodyPart) {
		t.Fatalf("%s body = %s", scenario, rec.Body.String())
	}
	if scenario == mockvendor.ScenarioRedirect && rec.Header().Get("Location") == "" {
		t.Fatal("redirect missing Location")
	}
}

type countBody struct {
	Global int            `json:"global"`
	Keys   map[string]int `json:"keys"`
}

func getCounts(t *testing.T, handler http.Handler) countBody {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/counts", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("counts status %d", rec.Code)
	}
	var body countBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body
}
