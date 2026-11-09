package biz_provider_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"notifier/internal/biz/biz_provider"
	"notifier/internal/conf"
	"notifier/internal/data/provider"
	"notifier/internal/data_interface"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type reg struct{ missing bool }

func (r reg) Lookup(string, string) (data_interface.Adapter, bool) { return nil, !r.missing }

type chans struct {
	mu sync.Mutex
	st data_interface.ChannelState
}

func (c *chans) LoadChannelState(context.Context, uint64) (data_interface.ChannelState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.st, nil
}

type limiter struct {
	err  error
	deny time.Time
	keys []string
}

func (l *limiter) Take(_ context.Context, key string, _, _ int64, _ uint32) (bool, time.Time, error) {
	l.keys = append(l.keys, key)
	if l.err != nil {
		return false, time.Time{}, l.err
	}
	if !l.deny.IsZero() {
		return false, l.deny, nil
	}
	return true, time.Time{}, nil
}

// memCreds 是内存 CredentialStore，带 CAS 语义。
type memCreds struct {
	mu sync.Mutex
	st map[string]data_interface.CredentialState
}

func newMemCreds(ref, account string) *memCreds {
	return &memCreds{st: map[string]data_interface.CredentialState{ref: {Ref: ref, Account: account, Version: 1, State: "ACTIVE"}}}
}

func (m *memCreds) set(ref string, f func(*data_interface.CredentialState)) {
	m.mu.Lock()
	s := m.st[ref]
	f(&s)
	m.st[ref] = s
	m.mu.Unlock()
}

func (m *memCreds) LoadCredential(_ context.Context, ref string) (data_interface.CredentialState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.st[ref]
	if !ok {
		return s, errors.New("not found")
	}
	return s, nil
}

func (m *memCreds) BeginRefresh(_ context.Context, ref string, v uint32, owner string, d time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.st[ref]
	if s.Version != v || s.State != "ACTIVE" {
		return false, nil
	}
	s.State, s.RefreshOwner, s.RefreshDeadline = "REFRESHING", owner, d
	m.st[ref] = s
	return true, nil
}

func (m *memCreds) CommitRefresh(_ context.Context, ref string, v uint32, owner string) (uint32, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.st[ref]
	if s.Version != v || s.State != "REFRESHING" || s.RefreshOwner != owner {
		return 0, false, nil
	}
	s.Version, s.State, s.RefreshOwner = v+1, "ACTIVE", ""
	m.st[ref] = s
	return s.Version, true, nil
}

func (m *memCreds) MarkReauth(_ context.Context, ref, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.st[ref]
	if s.State == "REFRESHING" && s.RefreshOwner == owner {
		s.State = "REAUTH_REQUIRED"
		m.st[ref] = s
	}
	return nil
}

// env 组装真实 Runtime + 受控 Transport + 本地 httptest 供应商。
type env struct {
	rt      *biz_provider.Runtime
	ch      *chans
	lim     *limiter
	creds   *memCreds
	secrets *provider.FileSecrets
	srv     *httptest.Server
	hits    *int
	mu      *sync.Mutex
}

func newEnv(t *testing.T, h http.HandlerFunc) *env {
	t.Helper()
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())
	ch := &chans{st: data_interface.ChannelState{
		ChannelID: 7, RowVersion: 3, ProviderID: "mock", ProviderAccountID: "acct-1", Operation: "send",
		AdapterType: "mock-json", AdapterVersion: "1", CredentialRef: "cred-1", Enabled: true,
		AllowedCallers: []string{"caller-a"}, AllowedOrigins: []data_interface.Origin{{Scheme: "http", Host: "127.0.0.1", Port: port}},
		AllowedCIDRs: []string{"127.0.0.0/8"}, QuotaKey: "q1", RatePerSecond: 10, Burst: 10, PolicyVersion: 1,
	}}
	secrets, _ := provider.NewFileSecrets(conf.Provider{})
	secrets.Set("cred-1", "s3cr3t-token")
	creds := newMemCreds("cred-1", "acct-1")
	pc := conf.Default().Provider
	pc.MaxResponseBytes = 4096
	lim := &limiter{}
	rt := &biz_provider.Runtime{
		Registry: reg{}, Channels: ch, Limiter: lim,
		Creds: &provider.Resolver{Store: creds, Secrets: secrets},
		HTTP:  provider.NewTransport(pc, secrets),
		Config: biz_provider.Config{HTTPBudget: 2 * time.Second, TokenSkew: 30 * time.Second, MaxRequestBytes: 1 << 16,
			SummaryMax: 256, LimiterRetry: time.Second},
	}
	return &env{rt: rt, ch: ch, lim: lim, creds: creds, secrets: secrets, srv: srv, hits: &hits, mu: &mu}
}

func (e *env) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return *e.hits
}

func fixed(base, auth, mode string, window int) []byte {
	return []byte(`{"adapter_type":"mock-json","adapter_version":"1","channel_id":"7","credential_ref":"cred-1","operation":"send",` +
		`"provider_account_id":"acct-1","provider_id":"mock","semantic_config":{"auth_type":"` + auth + `","base_url":"` + base +
		`","idempotency":{"mode":"` + mode + `","starts_at":"first_possible_send","window_seconds":` + strconv.Itoa(window) + `},"path":"/notify"}}`)
}

func (e *env) task(auth, mode string, window int) data_interface.TaskRecord {
	return data_interface.TaskRecord{
		FixedTarget: fixed(e.srv.URL, auth, mode, window), PayloadCanonical: []byte(`{"msg":"hi"}`),
		IdempotencyKey: "idem-key-1", CallerID: "caller-a",
	}
}

func (e *env) prepare(t *testing.T, task data_interface.TaskRecord, now time.Time) data_interface.PrepareResponse {
	t.Helper()
	p, err := e.rt.Prepare(context.Background(), data_interface.PrepareRequest{Task: task, Now: now, CallerAllowed: true})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// run 走完整一次 Prepare/Build/SendOnce/Interpret。
func (e *env) run(t *testing.T, task data_interface.TaskRecord) (data_interface.SendResponse, data_interface.Interpreted) {
	t.Helper()
	ctx := context.Background()
	p := e.prepare(t, task, time.Now().UTC())
	b, err := e.rt.Build(ctx, task, nilOwnership, p)
	if err != nil {
		t.Fatalf("build: %v (prepare %+v)", err, p)
	}
	sent, _ := e.rt.SendOnce(ctx, b)
	in, _ := e.rt.Interpret(ctx, task, b, sent)
	return sent, in
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
