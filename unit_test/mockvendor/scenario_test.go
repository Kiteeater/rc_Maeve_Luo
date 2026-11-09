package mockvendor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"notifier/internal/mockvendor"
)

func put(t *testing.T, h http.Handler, name, body string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/__test/scenarios/"+name, strings.NewReader(body)))
	return rec.Code
}

func send(h http.Handler, path, key, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Idempotency-Key", key)
	h.ServeHTTP(rec, req)
	return rec
}

func receipt(t *testing.T, h http.Handler, key string) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/__test/receipts?idempotency_key="+key, nil))
	if rec.Code != 200 {
		t.Fatalf("receipt %s: %d", key, rec.Code)
	}
	var m map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &m)
	return m
}

func TestPRV14_DedupReceiptsSurviveRestart(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state.json")
	srv, err := mockvendor.New(mockvendor.Options{Addr: "127.0.0.1:0", StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()
	if code := put(t, h, "notify", `{"operation":"send","window_seconds":3600,"steps":[{"http_status":200,"accepted":true,"code":"OK"}]}`); code != 204 {
		t.Fatalf("put scenario: %d", code)
	}
	if code := put(t, h, "bad", `{"steps":[]}`); code != 400 {
		t.Fatalf("empty steps must be rejected: %d", code)
	}
	a := send(h, "/notify", "k1", `{"m":1}`)
	b := send(h, "/notify", "k1", `{"m":1}`)
	if a.Code != 200 || b.Code != 200 {
		t.Fatalf("codes %d %d", a.Code, b.Code)
	}
	var ra, rb map[string]any
	_ = json.Unmarshal(a.Body.Bytes(), &ra)
	_ = json.Unmarshal(b.Body.Bytes(), &rb)
	if ra["request_id"] == "" || ra["request_id"] != rb["request_id"] {
		t.Fatalf("same key+body must return original request_id: %v %v", ra, rb)
	}
	if c := send(h, "/notify", "k1", `{"m":2}`); c.Code != http.StatusConflict {
		t.Fatalf("same key different body → 409, got %d", c.Code)
	}
	// 接受后断连：实际接收增加，业务执行仅一次。
	if code := put(t, h, "drop", `{"operation":"send","window_seconds":3600,"steps":[{"http_status":200,"accepted":true,"disconnect_after_accept":true}]}`); code != 204 {
		t.Fatal("put drop")
	}
	ts := httptest.NewServer(h)
	defer ts.Close()
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/drop", strings.NewReader(`{"m":1}`))
		req.Header.Set("Idempotency-Key", "k2")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			buf := make([]byte, 64)
			_, _ = resp.Body.Read(buf)
			resp.Body.Close()
		}
	}
	// 进程重启：新实例从独立状态文件恢复记录。
	srv2, err := mockvendor.New(mockvendor.Options{Addr: "127.0.0.1:0", StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	h2 := srv2.Handler()
	r1 := receipt(t, h2, "k1")
	if r1["received_count"].(float64) != 3 || r1["executed_count"].(float64) != 1 {
		t.Fatalf("k1 receipt: %v", r1)
	}
	r2 := receipt(t, h2, "k2")
	if r2["received_count"].(float64) != 2 || r2["executed_count"].(float64) != 1 {
		t.Fatalf("k2 receipt: %v", r2)
	}
	rec := httptest.NewRecorder()
	h2.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/__test/receipts?idempotency_key=nope", nil))
	if rec.Code != 404 {
		t.Fatalf("unknown key → 404, got %d", rec.Code)
	}
}
