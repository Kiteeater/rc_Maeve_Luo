package server_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"notifier/internal/conf"
	"notifier/internal/server"
	"notifier/internal/service"
)

func TestRequestIDAndUnsupportedMediaType(t *testing.T) {
	srv := server.NewHTTPServer(conf.Default().HTTP, service.NewHealthService(nil), nil, nil, nil, nil, nil)
	rec, req := newPost(t, "/v1/events", "")
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("missing content type = %d %s", rec.Code, rec.Body.String())
	}
	if len(rec.Header().Get("X-Request-ID")) != 32 {
		t.Fatalf("generated request id = %q", rec.Header().Get("X-Request-ID"))
	}

	rec, req = newPost(t, "/v1/events", "application/json; charset=utf-8")
	req.Header.Set("X-Request-ID", "trace-1")
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented || rec.Header().Get("X-Request-ID") != "trace-1" {
		t.Fatalf("json request = %d id %s", rec.Code, rec.Header().Get("X-Request-ID"))
	}
	if !strings.Contains(rec.Body.String(), `"request_id":"trace-1"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}

	rec, req = newPost(t, "/v1/events", "application/json")
	req.Header.Set("X-Request-ID", "bad id")
	srv.Handler.ServeHTTP(rec, req)
	id := rec.Header().Get("X-Request-ID")
	if id == "bad id" || len(id) != 32 {
		t.Fatalf("illegal request id was kept: %q", id)
	}
}

func TestMaxHeaderBytes(t *testing.T) {
	cfg := conf.Default().HTTP
	cfg.MaxHeaderBytes = 1024
	cfg.RequestTimeout = time.Second
	srv := server.NewHTTPServer(cfg, service.NewHealthService(nil), nil, nil, nil, nil, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	// net/http 的首包限制是 MaxHeaderBytes 再加 4096 字节缓冲，测试头必须越过这一总和。
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "GET /healthz HTTP/1.1\r\nHost: %s\r\nX-Big: %s\r\n\r\n", ln.Addr().String(), strings.Repeat("a", 8192)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil && n == 0 {
		t.Fatal(err)
	}
	if !strings.Contains(string(buf[:n]), "431") {
		t.Fatalf("oversized header response = %q", buf[:n])
	}
}

func TestHTTPShutdownTimeoutNamesTheParameter(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	release := make(chan struct{})
	app := &server.App{
		HTTP:            &http.Server{Addr: addr, Handler: http.NewServeMux()},
		Roles:           server.NewGroup(slowRole{release: release}),
		ShutdownTimeout: 200 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- app.Run(ctx) }()
	waitForListen(t, addr)
	cancel()
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "HTTP_SHUTDOWN_TIMEOUT") {
			t.Fatalf("shutdown error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not return")
	}
	select {
	case <-release:
	case <-time.After(3 * time.Second):
		t.Fatal("role did not finish")
	}
}

func TestBackgroundRoleStopsWhenContextIsCanceled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	app := &server.App{
		HTTP:            &http.Server{Addr: addr, Handler: http.NewServeMux()},
		Roles:           server.NewGroup(quickRole{}),
		ShutdownTimeout: 2 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- app.Run(ctx) }()
	waitForListen(t, addr)
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bounded shutdown did not return")
	}
}

type slowRole struct{ release chan struct{} }

func (slowRole) Name() string { return "slow" }

func (r slowRole) Run(ctx context.Context) error {
	<-ctx.Done()
	time.Sleep(time.Second)
	close(r.release)
	return nil
}

type quickRole struct{}

func (quickRole) Name() string { return "quick" }

func (quickRole) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func newPost(t *testing.T, path, contentType string) (*httptest.ResponseRecorder, *http.Request) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return httptest.NewRecorder(), req
}

func waitForListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not listen on %s", addr)
}
