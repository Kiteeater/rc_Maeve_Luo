package service_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"notifier/internal/auth"
	"notifier/internal/biz/biz_event"
	"notifier/internal/conf"
	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/server"
	"notifier/internal/service"
)

func TestEVT10_HTTPRejectsUnauthenticatedAndForbidden(t *testing.T) {
	handler, _ := eventHandler(t, 1<<20)
	rec := post(handler, "/v1/events", `{}`, "")
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), `"unauthenticated"`) {
		t.Fatalf("no token = %d %s", rec.Code, rec.Body.String())
	}
	rec = post(handler, "/v1/events", `{}`, "no-write")
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), `"forbidden"`) {
		t.Fatalf("no permission = %d %s", rec.Code, rec.Body.String())
	}
	rec = post(handler, "/v1/events?debug=1", `{}`, "writer")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("query = %d %s", rec.Code, rec.Body.String())
	}
}

func TestEVT11_BodyAtLimitAndOnePast(t *testing.T) {
	body := `{"source_business":"user-service","source_event_id":"edge-body","event_type":"UserRegistered","schema_version":"1","occurred_at":"2026-11-09T09:30:00Z","business_object_type":"user","business_object_id":"user-001","data":{}}`
	handler, repo := eventHandler(t, len(body))
	rec := post(handler, "/v1/events", body, "writer")
	if rec.Code != http.StatusAccepted || repo.accepts != 1 || !strings.Contains(rec.Body.String(), `"delivery_count":1`) {
		t.Fatalf("boundary = %d %s accepts %d", rec.Code, rec.Body.String(), repo.accepts)
	}
	over, overRepo := eventHandler(t, len(body))
	rec = post(over, "/v1/events", body+" ", "writer")
	if rec.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rec.Body.String(), `"payload_too_large"`) || overRepo.accepts != 0 {
		t.Fatalf("over = %d %s accepts %d", rec.Code, rec.Body.String(), overRepo.accepts)
	}
}

func eventHandler(t *testing.T, maxBody int) (http.Handler, *httpRepo) {
	t.Helper()
	repo := &httpRepo{}
	uc := biz_event.NewUseCase(repo, httpBacklog{}, httpRelease{}, httpPlan{}, biz_event.Limits{
		MaxBody: maxBody, MaxFanout: 50, MaxPlanBytes: 2 << 20, MaxPending: 100, MaxOldest: time.Hour,
		SampleInterval: time.Hour, SampleMaxAge: time.Hour, Namespace: "local",
	}, time.Now)
	sumWriter := authSHA("writer")
	sumReader := authSHA("no-write")
	set, err := auth.NewStatic([]auth.Identity{
		{ActorID: "writer", CallerID: "caller-1", TokenSHA256: sumWriter, AllowedSources: []string{"user-service"}, CallerScopes: []string{"caller-1"}, Permissions: []string{constant.PermEventsWrite}, Enabled: true},
		{ActorID: "reader", CallerID: "caller-1", TokenSHA256: sumReader, AllowedSources: []string{"user-service"}, CallerScopes: []string{"caller-1"}, Permissions: []string{constant.PermDataRead}, Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return server.NewHTTPServer(conf.Default().HTTP, service.NewHealthService(nil), nil, service.NewEventService(uc, maxBody), nil, nil, set).Handler, repo
}

func post(handler http.Handler, path, body, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func authSHA(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

type httpRepo struct {
	accepts int
	ev      *data_interface.AcceptedEvent
}

func (r *httpRepo) FindAcceptedEvent(ctx context.Context, callerID, sourceEventID string) (data_interface.AcceptedEvent, bool, error) {
	if r.ev == nil {
		return data_interface.AcceptedEvent{}, false, nil
	}
	return *r.ev, true, nil
}

func (r *httpRepo) AcceptEvent(ctx context.Context, in data_interface.AcceptEventInput) (data_interface.AcceptedEvent, error) {
	r.accepts++
	ev := data_interface.AcceptedEvent{
		EventID: 1001, CallerID: in.CallerID, SourceBusiness: in.SourceBusiness, SourceEventID: in.SourceEventID,
		FingerprintVersion: in.FingerprintVersion, Fingerprint: in.Fingerprint, RoutingReleaseID: in.ReleaseID,
		AcceptedAt: time.Date(2026, 11, 9, 1, 30, 1, 0, time.UTC),
		Deliveries: []data_interface.DeliveryView{{DeliveryID: 2001, RouteTargetID: "only", ProviderID: "ad", ProviderAccountID: "demo", Operation: "registration"}},
	}
	r.ev = &ev
	return ev, nil
}

type httpPlan struct{}

func (httpPlan) Calculate(ctx context.Context, principal biz_event.Principal, releaseID uint64, env domain.EventEnvelope) (domain.DeliveryPlan, error) {
	return domain.DeliveryPlan{
		ReleaseRef: "1", RoundPolicy: []byte(`{"max_attempts":8,"round_timeout_seconds":3600}`),
		Targets: []domain.TargetPlan{{
			RouteTargetID: "only", ProviderID: "ad", ProviderAccountID: "demo", Operation: "registration",
			MappingID: "map-1", MappingVersion: "1", AdapterType: "mock-json", AdapterVersion: "1",
			PayloadCanonical: []byte(`{}`),
			FixedTarget:      []byte(`{"channel_id":"1"}`),
		}},
	}, nil
}

type httpRelease struct{}

func (httpRelease) LoadCurrentID(ctx context.Context) (uint64, error) { return 1, nil }

func (httpRelease) Load(ctx context.Context, releaseID uint64) (data_interface.ReleaseRecord, error) {
	return data_interface.ReleaseRecord{ReleaseID: 1, ContentSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, nil
}

type httpBacklog struct{}

func (httpBacklog) Measure(ctx context.Context) (int64, *time.Time, error) { return 0, nil, nil }
