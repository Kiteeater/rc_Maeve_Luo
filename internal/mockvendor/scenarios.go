package mockvendor

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Step 是 S05 §11 场景步骤。
type Step struct {
	HTTPStatus            int            `json:"http_status"`
	Accepted              bool           `json:"accepted"`
	Code                  string         `json:"code"`
	DelayMS               int            `json:"delay_ms"`
	DisconnectAfterAccept bool           `json:"disconnect_after_accept"`
	BodyBytes             int            `json:"body_bytes"`
	ExtraFields           map[string]any `json:"extra_fields"`
}

// Scenario 由 PUT /__test/scenarios/{name} 设置，按目标路径 /{name} 绑定。
type Scenario struct {
	Operation     string `json:"operation"`
	WindowSeconds int    `json:"window_seconds"`
	RepeatLast    *bool  `json:"repeat_last"`
	Steps         []Step `json:"steps"`
	seen          int
}

// Receipt 是独立持久化的接收记录。
type Receipt struct {
	IdempotencyKey  string    `json:"idempotency_key"`
	ReceivedCount   int       `json:"received_count"`
	ExecutedCount   int       `json:"executed_count"`
	BodySHA256      string    `json:"body_sha256"`
	RequestID       string    `json:"request_id"`
	FirstReceivedAt time.Time `json:"first_received_at"`
	LastReceivedAt  time.Time `json:"last_received_at"`
	ExecutedAt      time.Time `json:"executed_at"`
}

const maxStepBody = 8 << 20

type scenarioBook struct {
	mu        sync.Mutex
	scenarios map[string]*Scenario
	receipts  map[string]*Receipt
	seq       int
	now       func() time.Time
	path      string
}

type receiptFile struct {
	Seq      int                 `json:"seq"`
	Receipts map[string]*Receipt `json:"receipts"`
}

// newScenarioBook 从独立的接收记录文件恢复，进程重启后仍可核对实际接收与执行次数（PRV-14）。
func newScenarioBook(path string) *scenarioBook {
	b := &scenarioBook{scenarios: map[string]*Scenario{}, receipts: map[string]*Receipt{}, now: time.Now, path: path}
	if path == "" {
		return b
	}
	if raw, err := os.ReadFile(path); err == nil {
		var f receiptFile
		if json.Unmarshal(raw, &f) == nil && f.Receipts != nil {
			b.receipts, b.seq = f.Receipts, f.Seq
		}
	}
	return b
}

// persistLocked 在持锁时原子写入接收记录。
func (b *scenarioBook) persistLocked() {
	if b.path == "" {
		return
	}
	raw, err := json.Marshal(receiptFile{Seq: b.seq, Receipts: b.receipts})
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(b.path), 0o755)
	tmp := b.path + ".tmp"
	if os.WriteFile(tmp, raw, 0o600) == nil {
		_ = os.Rename(tmp, b.path)
	}
}

func validScenario(sc Scenario) error {
	if len(sc.Steps) == 0 {
		return fmt.Errorf("steps must be non-empty")
	}
	for _, st := range sc.Steps {
		if st.HTTPStatus < 200 || st.HTTPStatus > 599 || st.DelayMS < 0 || st.DelayMS > 120000 || st.BodyBytes < 0 || st.BodyBytes > maxStepBody {
			return fmt.Errorf("step out of range")
		}
	}
	return nil
}

func (s *Server) putScenario(w http.ResponseWriter, r *http.Request) {
	var sc Scenario
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&sc); err != nil || validScenario(sc) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid scenario"})
		return
	}
	b := s.book
	b.mu.Lock()
	// 更新场景不清空已有接收/执行记录。
	b.scenarios[r.PathValue("name")] = &sc
	b.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getReceipt(w http.ResponseWriter, r *http.Request) {
	b := s.book
	b.mu.Lock()
	rc, ok := b.receipts[r.URL.Query().Get("idempotency_key")]
	var cp Receipt
	if ok {
		cp = *rc
	}
	b.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown key"})
		return
	}
	writeJSON(w, http.StatusOK, cp)
}

// serveScenario 返回 true 表示请求路径绑定了场景并已处理。
func (s *Server) serveScenario(w http.ResponseWriter, r *http.Request) bool {
	name := strings.Trim(r.URL.Path, "/")
	b := s.book
	b.mu.Lock()
	sc, ok := b.scenarios[name]
	if !ok {
		b.mu.Unlock()
		return false
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	key := r.Header.Get("Idempotency-Key")
	now := b.now().UTC()
	idx := sc.seen
	sc.seen++
	var step Step
	switch {
	case idx < len(sc.Steps):
		step = sc.Steps[idx]
	case sc.RepeatLast == nil || *sc.RepeatLast:
		step = sc.Steps[len(sc.Steps)-1]
	default:
		step = Step{HTTPStatus: 200, Accepted: true}
	}
	rc := b.receipts[key]
	if rc == nil {
		rc = &Receipt{IdempotencyKey: key, FirstReceivedAt: now, BodySHA256: hash}
		b.receipts[key] = rc
	}
	rc.ReceivedCount++
	rc.LastReceivedAt = now
	conflict := false
	accepted := step.Accepted && step.HTTPStatus >= 200 && step.HTTPStatus < 300
	if accepted {
		window := time.Duration(sc.WindowSeconds) * time.Second
		switch {
		case rc.ExecutedCount > 0 && rc.BodySHA256 != hash && (window <= 0 || now.Before(rc.ExecutedAt.Add(window))):
			conflict = true
		case rc.ExecutedCount > 0 && window > 0 && now.Before(rc.ExecutedAt.Add(window)):
			// 同键同 body 在窗口内：返回原 request_id，不重复执行。
		default:
			b.seq++
			rc.ExecutedCount++
			rc.ExecutedAt = now
			rc.BodySHA256 = hash
			rc.RequestID = fmt.Sprintf("req-%d", b.seq)
		}
	}
	reqID := rc.RequestID
	b.persistLocked()
	b.mu.Unlock()

	if step.DelayMS > 0 {
		t := time.NewTimer(time.Duration(step.DelayMS) * time.Millisecond)
		select {
		case <-r.Context().Done():
			t.Stop()
			return true
		case <-t.C:
		}
	}
	if conflict {
		writeJSON(w, http.StatusConflict, map[string]any{"accepted": false, "code": "CONFLICT", "request_id": reqID})
		return true
	}
	resp := map[string]any{"accepted": step.Accepted, "code": step.Code, "request_id": reqID}
	for k, v := range step.ExtraFields {
		resp[k] = v
	}
	raw, _ := json.Marshal(resp)
	if step.BodyBytes > 0 {
		raw = []byte(`{"accepted":true,"pad":"` + strings.Repeat("x", step.BodyBytes) + `"}`)
	}
	if step.DisconnectAfterAccept {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, buf, err := hj.Hijack(); err == nil {
				writeHalf(buf, step.HTTPStatus, raw)
				_ = conn.Close()
				return true
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(step.HTTPStatus)
	_, _ = w.Write(raw)
	return true
}

// writeHalf 声明完整长度但只写一半 body 后断开，模拟接收后响应丢失。
func writeHalf(buf *bufio.ReadWriter, status int, raw []byte) {
	_, _ = fmt.Fprintf(buf, "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", status, http.StatusText(status), len(raw))
	_, _ = buf.Write(raw[:len(raw)/2])
	_ = buf.Flush()
}
