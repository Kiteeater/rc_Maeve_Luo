package biz_provider_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"notifier/internal/biz/biz_provider"
	"notifier/internal/constant"
	"notifier/internal/data/provider"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

var nilOwnership domain.Ownership

func TestPRV01_HTTP200BusinessRejectIsNotReceived(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, `{"accepted":false,"code":"PERMANENT","request_id":"r1"}`)
	})
	_, in := e.run(t, e.task("bearer", "key", 3600))
	if in.Outcome != enums.OutcomePermanentReject || in.CoversBusinessKey {
		t.Fatalf("200 business reject must not be RECEIVED: %+v", in)
	}
}

func TestPRV02_RetryReacquiresCredentialAndResigns(t *testing.T) {
	type seen struct{ ts, sig, key, body string }
	var mu sync.Mutex
	var got []seen
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, seen{r.Header.Get("X-Signature-Time"), r.Header.Get("X-Signature"), r.Header.Get("Idempotency-Key"), string(b)})
		mu.Unlock()
		writeJSON(w, 200, `{"accepted":false,"code":"TEMPORARY","request_id":"r"}`)
	})
	task := e.task("hmac_sha256", "key", 3600)
	_, _ = e.run(t, task)
	// 延迟重试前密钥轮换：新版本、同账号；重新取得凭证并按新密钥签名。
	e.creds.set("cred-1", func(s *data_interface.CredentialState) { s.Version = 2 })
	e.secrets.Set("cred-1", "rotated-secret")
	time.Sleep(1100 * time.Millisecond)
	_, _ = e.run(t, task)
	if len(got) != 2 {
		t.Fatalf("want 2 requests, got %d", len(got))
	}
	if got[0].key != got[1].key || got[0].body != got[1].body || got[0].key != "idem-key-1" {
		t.Fatalf("payload/idempotency key must stay fixed: %+v", got)
	}
	if got[0].ts == got[1].ts || got[0].sig == got[1].sig {
		t.Fatalf("signature must be regenerated per request: %+v", got)
	}
	if want := provider.Signature("rotated-secret", got[1].ts, "POST", "/notify", []byte(got[1].body)); want != got[1].sig {
		t.Fatal("second request must be signed with the newly acquired credential")
	}
}

func TestPRV03_AuthRejectedSavedWithoutImplicitResend(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 401, `{"accepted":false,"code":"PERMANENT","request_id":"r"}`)
	})
	_, in := e.run(t, e.task("bearer", "key", 3600))
	if in.ErrorCode != constant.ErrAuthRejected || in.Outcome == enums.OutcomeReceived {
		t.Fatalf("auth reject: %+v", in)
	}
	if e.count() != 1 {
		t.Fatalf("no implicit refresh+resend: hits=%d", e.count())
	}
}

type issuer struct {
	mu    sync.Mutex
	calls int
	fail  bool
	gate  chan struct{}
}

func (i *issuer) Refresh(context.Context, string, uint32) error {
	if i.gate != nil {
		<-i.gate // 刷新进行中，其他实例并发尝试
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	if i.fail {
		return errors.New("lost response")
	}
	return nil
}

func TestPRV04_ConcurrentRefreshSingleWinner(t *testing.T) {
	store := newMemCreds("cred-1", "acct-1")
	iss := &issuer{gate: make(chan struct{})}
	r := &biz_provider.Refresher{Store: store, Issuer: iss, Budget: 5 * time.Second}
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func(i int) {
			_, err := r.Refresh(context.Background(), "cred-1", "inst-"+strconv.Itoa(i))
			results <- err
		}(i)
	}
	wins := 0
	for i := 0; i < 16; i++ {
		if i == 15 {
			close(iss.gate)
		}
		if err := <-results; err == nil {
			wins++
		} else if !errors.Is(err, data_interface.ErrCredentialTemporary) {
			t.Fatalf("loser must see temporary: %v", err)
		}
	}
	iss.gate = nil
	st, _ := store.LoadCredential(context.Background(), "cred-1")
	if wins != 1 || iss.calls != 1 || st.Version != 2 || st.State != "ACTIVE" {
		t.Fatalf("wins=%d calls=%d state=%+v", wins, iss.calls, st)
	}
	// 刷新响应丢失：无法证明旧 Refresh Token 仍有效 → REAUTH_REQUIRED，后续实例不得盲目刷新。
	iss.fail = true
	if _, err := r.Refresh(context.Background(), "cred-1", "inst-x"); !errors.Is(err, data_interface.ErrCredentialReauth) {
		t.Fatalf("want reauth, got %v", err)
	}
	if _, err := r.Refresh(context.Background(), "cred-1", "inst-y"); !errors.Is(err, data_interface.ErrCredentialReauth) || iss.calls != 2 {
		t.Fatalf("second instance must not refresh blindly: %v calls=%d", err, iss.calls)
	}
}

func TestPRV05_LimiterFailureDoesNotPass(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, `{"accepted":true}`) })
	e.lim.err = data_interface.ErrRateLimiterDown
	p := e.prepare(t, e.task("bearer", "key", 3600), t0)
	if p.Kind != domain.PrepareWait || p.ErrorCode != constant.ErrRateLimiterDown || p.RateLimitUntil.IsZero() || p.CredentialHandle != "" {
		t.Fatalf("limiter down must WAIT bounded: %+v", p)
	}
	if e.count() != 0 {
		t.Fatal("no business request on limiter failure")
	}
}

func TestTimeoutClassificationSurvivesProtocolInterpretation(t *testing.T) {
	runtime := &biz_provider.Runtime{}
	result, err := runtime.Interpret(context.Background(), data_interface.TaskRecord{}, data_interface.BuiltRequest{}, data_interface.SendResponse{
		ErrorCode: constant.ErrHTTPTimeout, Facts: domain.TransportFacts{Invoked: true},
	})
	if err != nil || result.ErrorCode != constant.ErrHTTPTimeout || result.Certainty != enums.CertaintyUncertain {
		t.Fatalf("timeout classification = %+v %v", result, err)
	}
	if result.ErrorReason == "" || strings.Contains(result.ErrorReason, "provider code") {
		t.Fatalf("timeout has no meaningful description: %+v", result)
	}
}

func TestPRV06_CurrentConstraintsBlock(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*env)
		code string
	}{
		{"disabled", func(e *env) { e.ch.st.Enabled = false }, constant.ErrChannelDisabled},
		{"caller_revoked", func(e *env) { e.ch.st.AllowedCallers = []string{"other"} }, constant.ErrCallerForbidden},
		{"adapter_missing", func(e *env) { e.rt.Registry = reg{missing: true} }, constant.ErrAdapterUnavailable},
		{"origin_removed", func(e *env) { e.ch.st.AllowedOrigins = nil }, constant.ErrTargetForbidden},
		{"account_changed", func(e *env) { e.ch.st.ProviderAccountID = "acct-2" }, constant.ErrAccountMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, `{"accepted":true}`) })
			tc.mut(e)
			p := e.prepare(t, e.task("bearer", "key", 3600), t0)
			if p.Kind != domain.PrepareBlock || p.ErrorCode != tc.code {
				t.Fatalf("want BLOCK %s, got %+v", tc.code, p)
			}
		})
	}
	// 准备后通道变化：Build 不使用过期 READY。
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, `{"accepted":true}`) })
	task := e.task("bearer", "key", 3600)
	p := e.prepare(t, task, t0)
	e.ch.st.RowVersion++
	if _, err := e.rt.Build(context.Background(), task, nilOwnership, p); err == nil {
		t.Fatal("stale READY must not build")
	}
}

func hijackHalf(w http.ResponseWriter) {
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n{\"accepted\":tr")
	_ = buf.Flush()
	_ = conn.Close()
}

func TestPRV07_AcceptedThenDisconnectIsUncertainWithSafetyBasis(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) { hijackHalf(w) })
	task := e.task("bearer", "key", 3600)
	_, in := e.run(t, task)
	if in.Outcome != enums.OutcomeUncertain || in.Certainty != enums.CertaintyUncertain {
		t.Fatalf("want UNCERTAIN, got %+v", in)
	}
	// 下一次准备附带重发安全依据。
	started := time.Now().UTC()
	task.Unresolved = []data_interface.UnresolvedRef{{AttemptID: 1}}
	task.Attempts = []data_interface.AttemptRecord{{AttemptID: 1, StartedAt: started}}
	p := e.prepare(t, task, started.Add(time.Second))
	if p.Kind != domain.PrepareReady || !p.IdempotencyCovers || !p.SafeUntil.Equal(started.Add(time.Hour)) {
		t.Fatalf("want READY with safety basis: %+v", p)
	}
}

func TestPRV08_SensitiveAndOversizedResponses(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, `{"accepted":true,"code":"OK","request_id":"r1","token":"tok-SECRET","phone":"13800000000"}`)
	})
	sent, in := e.run(t, e.task("bearer", "key", 3600))
	if strings.Contains(sent.BodySummary, "SECRET") || strings.Contains(sent.BodySummary, "13800000000") || in.Outcome != enums.OutcomeReceived {
		t.Fatalf("summary must be whitelisted: %q %+v", sent.BodySummary, in)
	}
	big := newEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, `{"accepted":true,"pad":"`+strings.Repeat("x", 10000)+`"}`)
	})
	sent, in = big.run(t, big.task("bearer", "key", 3600))
	if !sent.Facts.TooLarge || sent.Facts.BytesRead != 4097 || !sent.Truncated || in.ErrorCode != constant.ErrResponseTooLarge || in.Outcome != enums.OutcomeUncertain {
		t.Fatalf("oversized: read=%d %+v %+v", sent.Facts.BytesRead, sent.Facts.TooLarge, in)
	}
}

func TestPRV09_NoRedirectFollowOrImplicitRetry(t *testing.T) {
	var redirected int
	var mu sync.Mutex
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			mu.Lock()
			redirected++
			mu.Unlock()
		}
		w.Header().Set("Location", "/elsewhere")
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	_, in := e.run(t, e.task("bearer", "key", 3600))
	if in.ErrorCode != constant.ErrRedirectRefused || in.Outcome != enums.OutcomeUncertain || redirected != 0 || e.count() != 1 {
		t.Fatalf("redirect: %+v redirected=%d hits=%d", in, redirected, e.count())
	}
	// 底层连接异常：只发一次，不因 Idempotency-Key 被 net/http 视为可重放而重发。
	c := newEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close()
	})
	_, in = c.run(t, c.task("bearer", "key", 3600))
	if in.Outcome != enums.OutcomeUncertain || c.count() != 1 {
		t.Fatalf("conn reset: %+v hits=%d", in, c.count())
	}
}

func TestPRV10_ExpiredDedupWindowIsUnsafe(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, `{"accepted":true}`) })
	task := e.task("bearer", "key", 60)
	task.Unresolved = []data_interface.UnresolvedRef{{AttemptID: 1}, {AttemptID: 2}}
	early := t0.Add(-10 * time.Minute)
	task.Attempts = []data_interface.AttemptRecord{{AttemptID: 1, StartedAt: early}, {AttemptID: 2, StartedAt: t0}}
	if p := e.prepare(t, task, t0.Add(time.Second)); p.Kind != domain.PrepareUnsafe || p.ErrorCode != constant.ErrUnsafeResend {
		t.Fatalf("window from earliest unresolved attempt has passed: %+v", p)
	}
	none := e.task("bearer", "none", 0)
	none.Unresolved, none.Attempts = task.Unresolved, task.Attempts
	if p := e.prepare(t, none, t0); p.Kind != domain.PrepareUnsafe {
		t.Fatalf("no provider idempotency → UNSAFE: %+v", p)
	}
	// 人工首个风险尝试授权可作为依据（交 S07）。
	none.RiskOperationID = "op-1"
	if p := e.prepare(t, none, t0); p.Kind != domain.PrepareReady {
		t.Fatalf("manual authorization basis: %+v", p)
	}
}

func TestPRV11_SharedQuotaKeySingleBucket(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) {})
	_ = e.prepare(t, e.task("bearer", "key", 3600), t0)
	if len(e.lim.keys) != 1 || e.lim.keys[0] != "q1" {
		t.Fatalf("limiter must use the channel's shared quota_key: %v", e.lim.keys)
	}
	// 两个同账号不同操作通道共享 q1：在同一时刻总共只能取 burst 个令牌。
	var st domain.BucketState
	exists, passed := false, 0
	for i := 0; i < 20; i++ {
		var ok bool
		st, ok, _ = domain.TakeToken(st, exists, 1000, 5, 5, 1)
		exists = true
		if ok {
			passed++
		}
	}
	if passed != 5 {
		t.Fatalf("shared bucket passed %d, want burst 5", passed)
	}
	// 策略版本变化不额外补满桶；时钟回退不产生额度。
	st, ok, _ := domain.TakeToken(st, true, 1000, 5, 5, 2)
	if ok {
		t.Fatal("policy version change must not refill")
	}
	if _, ok, _ = domain.TakeToken(st, true, 500, 5, 5, 2); ok {
		t.Fatal("clock rollback must not create tokens")
	}
	e.lim.deny = t0.Add(200 * time.Millisecond)
	if p := e.prepare(t, e.task("bearer", "key", 3600), t0); p.Kind != domain.PrepareWait || !p.RateLimitUntil.Equal(e.lim.deny) {
		t.Fatalf("rate limited → WAIT until refill: %+v", p)
	}
}

func TestPRV12_WindowRemainingShorterThanBudgetIsUnsafe(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) {})
	task := e.task("bearer", "key", 60)
	task.Unresolved = []data_interface.UnresolvedRef{{AttemptID: 1}}
	task.Attempts = []data_interface.AttemptRecord{{AttemptID: 1, StartedAt: t0}}
	// 剩余 1 秒 < 预算 2 秒：尚未过期也不能发。
	if p := e.prepare(t, task, t0.Add(59*time.Second)); p.Kind != domain.PrepareUnsafe {
		t.Fatalf("remaining < budget: %+v", p)
	}
	// 刚好等于终点也禁止。
	if p := e.prepare(t, task, t0.Add(58*time.Second)); p.Kind != domain.PrepareUnsafe {
		t.Fatalf("exact boundary: %+v", p)
	}
	if p := e.prepare(t, task, t0.Add(57*time.Second)); p.Kind != domain.PrepareReady {
		t.Fatalf("strictly before end: %+v", p)
	}
}

func TestPRV13_RotationKeepsAccountRevocationBlocks(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, _ *http.Request) {})
	task := e.task("bearer", "key", 3600)
	p1 := e.prepare(t, task, t0)
	e.creds.set("cred-1", func(s *data_interface.CredentialState) { s.Version = 5 })
	p2 := e.prepare(t, task, t0)
	if p1.Kind != domain.PrepareReady || p2.Kind != domain.PrepareReady || p1.CredentialHandle == p2.CredentialHandle {
		t.Fatalf("rotation must produce a new versioned handle: %+v %+v", p1, p2)
	}
	e.creds.set("cred-1", func(s *data_interface.CredentialState) { s.Account = "acct-other" })
	if p := e.prepare(t, task, t0); p.Kind != domain.PrepareBlock || p.ErrorCode != constant.ErrAccountMismatch {
		t.Fatalf("account mismatch: %+v", p)
	}
	e.creds.set("cred-1", func(s *data_interface.CredentialState) { s.Account, s.State = "acct-1", "REVOKED" })
	if p := e.prepare(t, task, t0); p.Kind != domain.PrepareBlock || p.ErrorCode != constant.ErrCredentialRevoked {
		t.Fatalf("revoked must block immediately: %+v", p)
	}
	e.creds.set("cred-1", func(s *data_interface.CredentialState) { s.State = "REFRESHING" })
	if p := e.prepare(t, task, t0); p.Kind != domain.PrepareWait || p.ErrorCode != constant.ErrAuthWait {
		t.Fatalf("temporary credential failure → WAIT: %+v", p)
	}
	if e.count() != 0 {
		t.Fatal("credential failures must not create business requests")
	}
}
