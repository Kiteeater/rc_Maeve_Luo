// Package mockvendor 是独立的模拟供应商。它记录幂等键和次数，并按场景返回，不属于通知服务的业务分层。
package mockvendor

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	ScenarioAccept               = "accept"
	ScenarioReject               = "reject"
	ScenarioRateLimit            = "rate_limit"
	ScenarioServerError          = "server_error"
	ScenarioTimeout              = "timeout"
	ScenarioAcceptThenDisconnect = "accept_then_disconnect"
	ScenarioRedirect             = "redirect"
)

// Options 控制监听地址、状态文件和超时场景的等待时间。
type Options struct {
	Addr            string
	StatePath       string
	Timeout         time.Duration
	DefaultScenario string
}

// Server 只接受环回地址。计数写入本地文件，进程重启后仍在。
type Server struct {
	addr    string
	timeout time.Duration
	deflt   string
	state   *store
	book    *scenarioBook
	http    *http.Server
}

// New 校验监听地址。空主机名会收成 127.0.0.1。0.0.0.0 和其他非环回地址会被拒绝。
func New(opts Options) (*Server, error) {
	addr, err := loopbackAddr(opts.Addr)
	if err != nil {
		return nil, err
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.DefaultScenario == "" {
		opts.DefaultScenario = ScenarioAccept
	}
	st, err := loadStore(opts.StatePath)
	if err != nil {
		return nil, err
	}
	s := &Server{addr: addr, timeout: opts.Timeout, deflt: opts.DefaultScenario, state: st, book: newScenarioBook(receiptPath(opts.StatePath))}
	s.http = &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s, nil
}

// Addr 是实际监听地址。
func (s *Server) Addr() string { return s.addr }

// ListenAndServe 在环回地址上阻塞服务。
func (s *Server) ListenAndServe() error { return s.http.ListenAndServe() }

// Handler 暴露路由，测试可以用 httptest，不必真正监听。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/counts", s.counts)
	mux.HandleFunc("PUT /__test/scenarios/{name}", s.putScenario)
	mux.HandleFunc("GET /__test/receipts", s.getReceipt)
	mux.HandleFunc("/", s.receive)
	return mux
}

func (s *Server) counts(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.state.snapshot())
}

func (s *Server) receive(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/admin/counts" {
		s.counts(w, r)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	s.state.add(key)
	if s.serveScenario(w, r) {
		return
	}
	scenario := r.Header.Get("X-Mock-Scenario")
	if scenario == "" {
		scenario = r.URL.Query().Get("scenario")
	}
	if scenario == "" {
		scenario = s.deflt
	}
	switch scenario {
	case ScenarioAccept:
		writeJSON(w, http.StatusOK, map[string]any{"accepted": true})
	case ScenarioReject:
		writeJSON(w, http.StatusOK, map[string]any{"accepted": false, "code": "PERMANENT"})
	case ScenarioRateLimit:
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"accepted": false, "code": "RATE_LIMIT"})
	case ScenarioServerError:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"accepted": false, "code": "SERVER_ERROR"})
	case ScenarioTimeout:
		timer := time.NewTimer(s.timeout)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			writeJSON(w, http.StatusOK, map[string]any{"accepted": true, "late": true})
		}
	case ScenarioAcceptThenDisconnect:
		hj, ok := w.(http.Hijacker)
		if !ok {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"accepted": false, "code": "HIJACK_UNAVAILABLE"})
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			return
		}
		_, _ = io.WriteString(buf, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"accepted\":true}")
		_ = buf.Flush()
		_ = conn.Close()
	case ScenarioRedirect:
		w.Header().Set("Location", "/redirected")
		w.WriteHeader(http.StatusFound)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"accepted": false, "code": "UNKNOWN_SCENARIO"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"accepted":false}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func loopbackAddr(addr string) (string, error) {
	if addr == "" {
		return "127.0.0.1:9090", nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", errors.New("mockvendor: invalid address")
	}
	if host == "" || host == "localhost" {
		return "127.0.0.1:" + port, nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", errors.New("mockvendor: address must be loopback")
	}
	return net.JoinHostPort(ip.String(), port), nil
}

type snapshot struct {
	Global int            `json:"global"`
	Keys   map[string]int `json:"keys"`
}

type store struct {
	mu     sync.Mutex
	path   string
	global int
	keys   map[string]int
}

func loadStore(path string) (*store, error) {
	st := &store{path: path, keys: map[string]int{}}
	if path == "" {
		return st, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return st, nil
		}
		return nil, err
	}
	var body snapshot
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	st.global = body.Global
	if body.Keys != nil {
		st.keys = body.Keys
	}
	return st, nil
}

func (s *store) add(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.global++
	s.keys[key]++
	s.persist()
}

func (s *store) snapshot() snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make(map[string]int, len(s.keys))
	for k, v := range s.keys {
		keys[k] = v
	}
	return snapshot{Global: s.global, Keys: keys}
}

func (s *store) persist() {
	if s.path == "" {
		return
	}
	body, err := json.Marshal(snapshot{Global: s.global, Keys: s.keys})
	if err != nil {
		return
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, s.path)
}

func receiptPath(state string) string {
	if state == "" {
		return ""
	}
	return state + ".receipts.json"
}
