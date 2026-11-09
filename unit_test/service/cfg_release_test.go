package service_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"notifier/internal/auth"
	"notifier/internal/biz/biz_config"
	"notifier/internal/canonicaljson"
	"notifier/internal/conf"
	"notifier/internal/constant"
	"notifier/internal/data/provider"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/server"
	"notifier/internal/service"
)

func TestCFG01_TwoTargetsSameProviderDifferentOperation(t *testing.T) {
	uc, _ := newUC(t, domain.DefaultBounds())
	content := releaseDoc(
		schemaProps(`"campaign":{"type":"string"},"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-ads", "[]", `["ads-track"]`)+","+route("user-crm", "[]", `["crm-create-user"]`),
		target("ads-track", "2", "track-user", "ads-user")+","+target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("ads-user", "["+fieldFrom("/ads_user_id", "/user_id", "string", true)+"]")+","+mapping("crm-user", "["+fieldFrom("/external_user_id", "/user_id", "string", true)+"]"),
	)
	id := publishNew(t, uc, content, nil)
	env := event(`{"campaign":"spring","user_id":"u-1"}`)
	first := mustPlan(t, uc, id, env)
	second := mustPlan(t, uc, id, env)
	if string(planCanon(t, first)) != string(planCanon(t, second)) {
		t.Fatal("same input and release produced different plan bytes")
	}
	if len(first.Targets) != 2 {
		t.Fatalf("targets = %d", len(first.Targets))
	}
	if first.Targets[0].RouteTargetID != "ads-track" || first.Targets[1].RouteTargetID != "crm-create-user" {
		t.Fatalf("targets = %s %s", first.Targets[0].RouteTargetID, first.Targets[1].RouteTargetID)
	}
	if first.Targets[0].ProviderID != first.Targets[1].ProviderID || first.Targets[0].Operation == first.Targets[1].Operation {
		t.Fatalf("provider/operation = %s/%s %s/%s", first.Targets[0].ProviderID, first.Targets[0].Operation, first.Targets[1].ProviderID, first.Targets[1].Operation)
	}
}

func TestCFG02_SameTargetAndConflictingDefinition(t *testing.T) {
	uc, st := newUC(t, domain.DefaultBounds())
	content := releaseDoc(
		schemaProps(`"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-a", "[]", `["crm-create-user"]`)+","+route("user-b", "[]", `["crm-create-user"]`),
		target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("crm-user", "["+fieldFrom("/external_user_id", "/user_id", "string", true)+"]"),
	)
	id := publishNew(t, uc, content, nil)
	plan := mustPlan(t, uc, id, event(`{"user_id":"u-1"}`))
	if len(plan.Targets) != 1 || plan.Targets[0].RouteTargetID != "crm-create-user" {
		t.Fatalf("duplicate route target was not collapsed: %+v", plan.Targets)
	}

	current := mustCurrent(t, uc)
	handler := newHTTP(t, uc)
	body := `{"operation_idempotency_key":"dup-def","reason":"conflict","content":{"format_version":1,"format_version":1}}`
	rec := doJSON(t, handler, http.MethodPost, "/v1/config/releases", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("duplicate keys status = %d %s", rec.Code, rec.Body.String())
	}
	again := mustCurrent(t, uc)
	if again != current || st.count() != 1 {
		t.Fatalf("conflicting definition changed current %d or count %d", again, st.count())
	}
}

func TestCFG03_MissingNullAndTypeMismatch(t *testing.T) {
	uc, _ := newUC(t, domain.DefaultBounds())
	baseMap := mapping("crm-user", "["+fieldFrom("/external_user_id", "/user_id", "string", true)+"]")
	baseTarget := target("crm-create-user", "1", "create-user", "crm-user")

	t.Run("condition missing", func(t *testing.T) {
		content := releaseDoc(
			schemaProps(`"flag":{"type":"string"},"user_id":{"type":"string"}`, `["user_id"]`),
			route("user-crm", `[{"op":"eq","path":"/flag","value":"yes"}]`, `["crm-create-user"]`),
			baseTarget, baseMap,
		)
		id := publishNew(t, uc, content, nil)
		_, err := uc.Calculate(context.Background(), demoPrincipal(), id, event(`{"user_id":"u-1"}`))
		if errCode(err) != constant.CodeNoMatchingRoute {
			t.Fatalf("missing field code = %s %v", errCode(err), err)
		}
	})
	t.Run("condition explicit null", func(t *testing.T) {
		content := releaseDoc(
			schemaProps(`"flag":{"nullable":true,"type":"string"},"user_id":{"type":"string"}`, `["user_id"]`),
			route("user-crm", `[{"op":"eq","path":"/flag","value":null}]`, `["crm-create-user"]`),
			baseTarget, baseMap,
		)
		id := publishNew(t, uc, content, nil)
		if _, err := uc.Calculate(context.Background(), demoPrincipal(), id, event(`{"flag":null,"user_id":"u-1"}`)); err != nil {
			t.Fatal(err)
		}
		other := releaseDoc(
			schemaProps(`"flag":{"nullable":true,"type":"string"},"user_id":{"type":"string"}`, `["user_id"]`),
			route("user-crm", `[{"op":"eq","path":"/flag","value":"yes"}]`, `["crm-create-user"]`),
			baseTarget, baseMap,
		)
		otherID := publishNew(t, uc, other, nil)
		_, err := uc.Calculate(context.Background(), demoPrincipal(), otherID, event(`{"flag":null,"user_id":"u-1"}`))
		if errCode(err) != constant.CodeNoMatchingRoute {
			t.Fatalf("null compared as string code = %s", errCode(err))
		}
	})
	t.Run("condition type", func(t *testing.T) {
		content := releaseDoc(
			schemaProps(`"user_id":{"type":"string"}`, `["user_id"]`),
			route("user-crm", `[{"op":"eq","path":"/n","value":1}]`, `["crm-create-user"]`),
			baseTarget, baseMap,
		)
		content = strings.Replace(content, `"additional_properties":false`, `"additional_properties":true`, 1)
		id := publishNew(t, uc, content, nil)
		_, err := uc.Calculate(context.Background(), demoPrincipal(), id, event(`{"n":"1","user_id":"u-1"}`))
		if errCode(err) != constant.CodeNoMatchingRoute {
			t.Fatalf("number/string condition code = %s %v", errCode(err), err)
		}
		if _, err := uc.Calculate(context.Background(), demoPrincipal(), id, event(`{"n":1,"user_id":"u-1"}`)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("mapping null and default", func(t *testing.T) {
		fields := `[{"default":"anon","from":"/nickname","nullable":true,"required":false,"to":"/nickname","type":"string"}]`
		content := releaseDoc(
			schemaProps(`"nickname":{"nullable":true,"type":"string"},"user_id":{"type":"string"}`, `["user_id"]`),
			route("user-crm", "[]", `["crm-create-user"]`),
			baseTarget, mapping("crm-user", fields),
		)
		id := publishNew(t, uc, content, nil)
		missing := mustPlan(t, uc, id, event(`{"user_id":"u-1"}`))
		if string(missing.Targets[0].PayloadCanonical) != `{"nickname":"anon"}` {
			t.Fatalf("default payload = %s", missing.Targets[0].PayloadCanonical)
		}
		nullPlan := mustPlan(t, uc, id, event(`{"nickname":null,"user_id":"u-1"}`))
		if string(nullPlan.Targets[0].PayloadCanonical) != `{"nickname":null}` {
			t.Fatalf("explicit null was replaced: %s", nullPlan.Targets[0].PayloadCanonical)
		}
		forbidden := releaseDoc(
			schemaProps(`"nickname":{"nullable":true,"type":"string"},"user_id":{"type":"string"}`, `["user_id"]`),
			route("user-crm", "[]", `["crm-create-user"]`),
			baseTarget,
			mapping("crm-user", `[{"from":"/nickname","nullable":false,"required":true,"to":"/nickname","type":"string"}]`),
		)
		forbiddenID := publishNew(t, uc, forbidden, nil)
		_, err := uc.Calculate(context.Background(), demoPrincipal(), forbiddenID, event(`{"nickname":null,"user_id":"u-1"}`))
		if errCode(err) != constant.CodeMappingInvalid {
			t.Fatalf("null forbidden code = %s", errCode(err))
		}
	})
	t.Run("mapping type", func(t *testing.T) {
		content := releaseDoc(
			schemaProps(`"nickname":{"type":"number"},"user_id":{"type":"string"}`, `["user_id"]`),
			route("user-crm", "[]", `["crm-create-user"]`),
			baseTarget,
			mapping("crm-user", `[{"from":"/nickname","required":true,"source_type":"string","to":"/nickname","type":"string"}]`),
		)
		id := publishNew(t, uc, content, nil)
		_, err := uc.Calculate(context.Background(), demoPrincipal(), id, event(`{"nickname":1,"user_id":"u-1"}`))
		if errCode(err) != constant.CodeMappingInvalid {
			t.Fatalf("type mismatch code = %s %v", errCode(err), err)
		}
	})
}

func TestCFG04_OneTargetFailsWholePlan(t *testing.T) {
	uc, _ := newUC(t, domain.DefaultBounds())
	content := releaseDoc(
		schemaProps(`"campaign":{"type":"string"},"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-ads", "[]", `["ads-track"]`)+","+route("user-crm", "[]", `["crm-create-user"]`),
		target("ads-track", "2", "track-user", "ads-user")+","+target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("ads-user", "["+fieldFrom("/campaign_id", "/campaign", "string", true)+"]")+","+mapping("crm-user", "["+fieldFrom("/external_user_id", "/user_id", "string", true)+"]"),
	)
	id := publishNew(t, uc, content, nil)
	_, err := uc.Calculate(context.Background(), demoPrincipal(), id, event(`{"user_id":"u-1"}`))
	var de *domain.Error
	if !errors.As(err, &de) || de.Code != constant.CodeMappingInvalid {
		t.Fatalf("plan err = %v", err)
	}
	found := false
	for _, detail := range de.Details {
		if strings.Contains(detail.Message, "ads-track") && detail.FieldPath == "/campaign_id" {
			found = true
		}
	}
	if !found {
		t.Fatalf("details = %+v", de.Details)
	}
}

func TestCFG05_SelectedVersionStaysAfterNewerPublish(t *testing.T) {
	uc, _ := newUC(t, domain.DefaultBounds())
	v1 := publishNew(t, uc, tagged("v1"), nil)
	v2 := publishNew(t, uc, tagged("v2"), &v1)
	env := event(`{"user_id":"u-1"}`)
	plan := mustPlan(t, uc, v1, env)
	if plan.ReleaseRef != fmt.Sprint(v1) || !strings.Contains(string(plan.Targets[0].PayloadCanonical), `"tag":"v1"`) {
		t.Fatalf("v1 plan = %s %s", plan.ReleaseRef, plan.Targets[0].PayloadCanonical)
	}
	if strings.Contains(string(plan.Targets[0].PayloadCanonical), `"tag":"v2"`) {
		t.Fatal("v1 plan used v2")
	}
	again := mustPlan(t, uc, v1, env)
	if string(planCanon(t, plan)) != string(planCanon(t, again)) {
		t.Fatal("v1 plan bytes changed")
	}
	next := mustPlan(t, uc, v2, env)
	if !strings.Contains(string(next.Targets[0].PayloadCanonical), `"tag":"v2"`) || next.VersionNo != 2 {
		t.Fatalf("v2 plan = %d %s", next.VersionNo, next.Targets[0].PayloadCanonical)
	}
}

func TestCFG08_UnregisteredAdapterMissingCredentialOrScript(t *testing.T) {
	uc, st := newUC(t, domain.DefaultBounds())
	current := publishNew(t, uc, exampleContent(), nil)
	cases := []string{
		strings.Replace(exampleContent(), `"adapter_version":"1"`, `"adapter_version":"9"`, 1),
		strings.Replace(exampleContent(), `"credential_ref":"provider/demo"`, `"credential_ref":"missing/ref"`, 1),
		strings.Replace(exampleContent(), `"auth_type":"bearer"`, `"auth_type":"bearer","script":"nope"`, 1),
	}
	for i, content := range cases {
		_, err := uc.Create(context.Background(), demoPrincipal(), fmt.Sprintf("bad-%d", i), "reject bad release", []byte(content))
		if errCode(err) != constant.CodeConfigInvalid {
			t.Fatalf("case %d code = %s %v", i, errCode(err), err)
		}
		if mustCurrent(t, uc) != current || st.count() != 1 {
			t.Fatalf("case %d changed current", i)
		}
	}
	var de *domain.Error
	_, err := uc.Create(context.Background(), demoPrincipal(), "bad-adapter", "reject bad release", []byte(cases[0]))
	if !errors.As(err, &de) || !detailCode(de, "unregistered_adapter") {
		t.Fatalf("adapter details = %v", err)
	}
	_, err = uc.Create(context.Background(), demoPrincipal(), "bad-cred", "reject bad release", []byte(cases[1]))
	if !errors.As(err, &de) || !detailCode(de, "credential_missing") {
		t.Fatalf("credential details = %v", err)
	}
	_, err = uc.Create(context.Background(), demoPrincipal(), "bad-script", "reject bad release", []byte(cases[2]))
	if !errors.As(err, &de) || !detailCode(de, "script_forbidden") {
		t.Fatalf("script details = %v", err)
	}
}

func TestCFG10_ExampleReleaseValidatePublishAndPlan(t *testing.T) {
	uc, st := newUC(t, domain.DefaultBounds())
	handler := newHTTP(t, uc)
	content := exampleContent()
	rec := doJSON(t, handler, http.MethodPost, "/v1/config/releases/validate", `{"content":`+content+`}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"valid":true`) || !strings.Contains(rec.Body.String(), `"errors":[]`) {
		t.Fatalf("validate = %d %s", rec.Code, rec.Body.String())
	}
	created := doJSON(t, handler, http.MethodPost, "/v1/config/releases", wrapCreate("cfg-10", "publish example", content))
	if created.Code != http.StatusCreated {
		t.Fatalf("create = %d %s", created.Code, created.Body.String())
	}
	var first struct {
		OperationID   string `json:"operation_id"`
		ReleaseID     string `json:"release_id"`
		VersionNo     uint32 `json:"version_no"`
		Status        string `json:"status"`
		ContentSHA256 string `json:"content_sha256"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.ReleaseID != "1" || first.VersionNo != 1 || first.Status != "draft" || len(first.ContentSHA256) != 64 {
		t.Fatalf("create body = %+v", first)
	}
	retry := doJSON(t, handler, http.MethodPost, "/v1/config/releases", wrapCreate("cfg-10", "publish example", content))
	var second struct {
		ReleaseID string `json:"release_id"`
		VersionNo uint32 `json:"version_no"`
	}
	if retry.Code != http.StatusCreated || json.Unmarshal(retry.Body.Bytes(), &second) != nil || second.ReleaseID != first.ReleaseID || second.VersionNo != first.VersionNo || st.count() != 1 {
		t.Fatalf("retry = %d %s count %d", retry.Code, retry.Body.String(), st.count())
	}
	conflict := doJSON(t, handler, http.MethodPost, "/v1/config/releases", wrapCreate("cfg-10", "different reason", content))
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), constant.CodeIdempotencyConflict) || st.count() != 1 {
		t.Fatalf("conflict = %d %s", conflict.Code, conflict.Body.String())
	}
	published := doJSON(t, handler, http.MethodPost, "/v1/config/releases/1/publish", `{"operation_idempotency_key":"pub-10","reason":"go live","expected_current_release_id":null}`)
	if published.Code != http.StatusOK || !strings.Contains(published.Body.String(), `"previous_release_id":null`) || !strings.Contains(published.Body.String(), `"version_no":1`) {
		t.Fatalf("publish = %d %s", published.Code, published.Body.String())
	}
	current := doJSON(t, handler, http.MethodGet, "/v1/config/releases/current", "")
	if current.Code != http.StatusOK || !strings.Contains(current.Body.String(), `"status":"published"`) || !strings.Contains(current.Body.String(), `"format_version":1`) {
		t.Fatalf("current = %d %s", current.Code, current.Body.String())
	}
	got := doJSON(t, handler, http.MethodGet, "/v1/config/releases/1", "")
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"base_url":"http://mockvendor:9090"`) {
		t.Fatalf("get = %d %s", got.Code, got.Body.String())
	}
	env := event(`{"campaign":"spring","user_id":"u-1"}`)
	plan := mustPlan(t, uc, 1, env)
	again := mustPlan(t, uc, 1, env)
	if string(planCanon(t, plan)) != string(planCanon(t, again)) {
		t.Fatal("example plan is not deterministic")
	}
	if len(plan.Targets) != 1 || plan.Targets[0].Operation != "create-user" || plan.Targets[0].AdapterType != "mock-json" {
		t.Fatalf("plan target = %+v", plan.Targets)
	}
	if !strings.Contains(string(plan.Targets[0].FixedTarget), `"credential_ref":"provider/demo"`) || !strings.Contains(string(plan.Targets[0].FixedTarget), `"base_url":"http://mockvendor:9090"`) {
		t.Fatalf("fixed = %s", plan.Targets[0].FixedTarget)
	}
	if !strings.Contains(string(plan.RoundPolicy), `"max_attempts":8`) || !strings.Contains(string(plan.Targets[0].PayloadCanonical), `"external_user_id":"u-1"`) {
		t.Fatalf("policy/payload = %s %s", plan.RoundPolicy, plan.Targets[0].PayloadCanonical)
	}
}

func TestCFG12_LimitsPointerEnumAndKeyword(t *testing.T) {
	bounds := domain.DefaultBounds()
	bounds.MaxRoutes = 1
	bounds.MaxConditions = 200
	uc, _ := newUC(t, bounds)
	over := releaseDoc(
		schemaProps(`"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-a", "[]", `["crm-create-user"]`)+","+route("user-b", "[]", `["crm-create-user"]`),
		target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("crm-user", "["+fieldFrom("/external_user_id", "/user_id", "string", true)+"]"),
	)
	overOut, err := uc.Validate(context.Background(), demoPrincipal(), []byte(over))
	if err != nil || !hasIssue(overOut.Issues, constant.CodeLimitExceeded, "/routes") {
		t.Fatalf("routes = %+v %v", overOut.Issues, err)
	}
	badPtr := releaseDoc(
		schemaProps(`"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-crm", `[{"op":"eq","path":"/0","value":"x"}]`, `["crm-create-user"]`),
		target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("crm-user", "["+fieldFrom("/external_user_id", "/user_id", "string", true)+"]"),
	)
	ptrOut, err := uc.Validate(context.Background(), demoPrincipal(), []byte(badPtr))
	if err != nil || !hasIssue(ptrOut.Issues, "invalid_pointer", "/routes/0/conditions/0/path") {
		t.Fatalf("pointer = %+v %v", ptrOut.Issues, err)
	}
	dupEnum := releaseDoc(
		schemaProps(`"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-crm", "[]", `["crm-create-user"]`),
		target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("crm-user", `[{"enum_map":[{"from":"a","to":"A"},{"from":"a","to":"B"}],"from":"/user_id","required":true,"source_type":"string","to":"/external_user_id","type":"string"}]`),
	)
	enumOut, err := uc.Validate(context.Background(), demoPrincipal(), []byte(dupEnum))
	if err != nil || !hasIssue(enumOut.Issues, "duplicate", "enum_map") {
		t.Fatalf("enum = %+v %v", enumOut.Issues, err)
	}
	keyword := strings.Replace(exampleContent(), `"type":"object"`, `"$ref":"outside","type":"object"`, 1)
	keyOut, err := uc.Validate(context.Background(), demoPrincipal(), []byte(keyword))
	if err != nil || !hasIssue(keyOut.Issues, "unsupported_keyword", "$ref") {
		t.Fatalf("keyword = %+v %v", keyOut.Issues, err)
	}

	var conds []string
	for i := 0; i < 101; i++ {
		conds = append(conds, `{"op":"eq","path":"0","value":"x"}`)
	}
	many := releaseDoc(
		schemaProps(`"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-crm", "["+strings.Join(conds, ",")+"]", `["crm-create-user"]`),
		target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("crm-user", "["+fieldFrom("/external_user_id", "/user_id", "string", true)+"]"),
	)
	rec := doJSON(t, newHTTP(t, uc), http.MethodPost, "/v1/config/releases/validate", `{"content":`+many+`}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("validate = %d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Valid     bool `json:"valid"`
		Truncated bool `json:"truncated"`
		Errors    []struct {
			FieldPath string `json:"field_path"`
			Code      string `json:"code"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Valid || !body.Truncated || len(body.Errors) != 100 || body.Errors[0].Code == "" || body.Errors[0].FieldPath == "" {
		t.Fatalf("truncated = %+v", body)
	}
}

func publishNew(t *testing.T, uc *biz_config.UseCase, content string, expected *uint64) uint64 {
	t.Helper()
	key := fmt.Sprintf("create-%d", time.Now().UnixNano())
	created, err := uc.Create(context.Background(), demoPrincipal(), key, "create draft", []byte(content))
	if err != nil {
		t.Fatal(err)
	}
	pubKey := fmt.Sprintf("publish-%d", created.ReleaseID)
	if expected == nil {
		if view, err := uc.Current(context.Background()); err == nil {
			expected = &view.ReleaseID
		}
	}
	if _, err := uc.Publish(context.Background(), demoPrincipal(), created.ReleaseID, pubKey, "publish draft", expected); err != nil {
		t.Fatal(err)
	}
	return created.ReleaseID
}

func mustPlan(t *testing.T, uc *biz_config.UseCase, id uint64, env domain.EventEnvelope) domain.DeliveryPlan {
	t.Helper()
	plan, err := uc.Calculate(context.Background(), demoPrincipal(), id, env)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func mustCurrent(t *testing.T, uc *biz_config.UseCase) uint64 {
	t.Helper()
	view, err := uc.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return view.ReleaseID
}

func demoPrincipal() biz_config.Principal {
	return biz_config.Principal{
		ActorID: "ops-admin", CallerID: "caller-demo",
		AllowedSources: []string{"user-service"}, Scopes: []string{"caller-demo"},
	}
}

func event(data string) domain.EventEnvelope {
	return domain.EventEnvelope{
		CallerID: "caller-demo", SourceBusiness: "user-service",
		EventType: "UserRegistered", SchemaVersion: "1", DataCanonical: []byte(data),
	}
}

func newUC(t *testing.T, bounds domain.Bounds) (*biz_config.UseCase, *memStore) {
	t.Helper()
	st := newMem()
	st.channels[1] = data_interface.ChannelPin{
		ChannelID: 1, RowVersion: 1, ProviderID: "mock", ProviderAccountID: "demo",
		Operation: "create-user", AdapterType: "mock-json", AdapterVersion: "1", CredentialRef: "provider/demo",
	}
	st.channels[2] = data_interface.ChannelPin{
		ChannelID: 2, RowVersion: 1, ProviderID: "mock", ProviderAccountID: "demo",
		Operation: "track-user", AdapterType: "mock-json", AdapterVersion: "1", CredentialRef: "provider/demo",
	}
	st.creds["provider/demo"] = data_interface.CredentialPin{Ref: "provider/demo", Version: 1, ProviderID: "mock", ProviderAccountID: "demo"}
	uc := biz_config.NewUseCase(st, st, st, provider.NewMockRegistry(), nil, bounds, time.Second)
	return uc, st
}

func newHTTP(t *testing.T, uc *biz_config.UseCase) http.Handler {
	t.Helper()
	sum := sha256.Sum256([]byte("cfg-token"))
	set, err := auth.NewStatic([]auth.Identity{{
		ActorID: "ops-admin", CallerID: "caller-demo", TokenSHA256: hex.EncodeToString(sum[:]),
		AllowedSources: []string{"user-service"}, CallerScopes: []string{"caller-demo"},
		Permissions: []string{constant.PermConfigPublish}, Enabled: true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return server.NewHTTPServer(conf.Default().HTTP, service.NewHealthService(nil), service.NewReleaseService(uc, 1<<20), nil, nil, nil, set).Handler
}

func doJSON(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer cfg-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func errCode(err error) string {
	var de *domain.Error
	if errors.As(err, &de) && de != nil {
		return de.Code
	}
	return ""
}

func detailCode(err *domain.Error, code string) bool {
	if err == nil {
		return false
	}
	for _, detail := range err.Details {
		if detail.Code == code {
			return true
		}
	}
	return false
}

func hasIssue(issues []domain.Issue, code, pathPart string) bool {
	for _, item := range issues {
		if item.Code == code && strings.Contains(item.FieldPath, pathPart) {
			return true
		}
	}
	return false
}

func planCanon(t *testing.T, plan domain.DeliveryPlan) []byte {
	t.Helper()
	targets := make([]canonicaljson.Value, len(plan.Targets))
	for i, item := range plan.Targets {
		payload, err := canonicaljson.Parse(item.PayloadCanonical)
		if err != nil {
			t.Fatal(err)
		}
		fixed, err := canonicaljson.Parse(item.FixedTarget)
		if err != nil {
			t.Fatal(err)
		}
		targets[i] = canonicaljson.Object([]canonicaljson.Field{
			{Key: "fixed_target", Value: fixed},
			{Key: "operation", Value: canonicaljson.String(item.Operation)},
			{Key: "payload", Value: payload},
			{Key: "provider_id", Value: canonicaljson.String(item.ProviderID)},
			{Key: "route_target_id", Value: canonicaljson.String(item.RouteTargetID)},
		})
	}
	policy, err := canonicaljson.Parse(plan.RoundPolicy)
	if err != nil {
		t.Fatal(err)
	}
	version, err := canonicaljson.Uint(uint64(plan.VersionNo))
	if err != nil {
		t.Fatal(err)
	}
	format, err := canonicaljson.Uint(uint64(plan.FormatVersion))
	if err != nil {
		t.Fatal(err)
	}
	obj := canonicaljson.Object([]canonicaljson.Field{
		{Key: "format_version", Value: format},
		{Key: "policy", Value: policy},
		{Key: "release_id", Value: canonicaljson.String(plan.ReleaseRef)},
		{Key: "targets", Value: canonicaljson.Array(targets)},
		{Key: "version_no", Value: version},
	})
	return canonicaljson.AppendCanonical(nil, obj)
}

func policyJSON() string {
	return `{"backoff_cap_ms":300000,"backoff_initial_ms":5000,"max_attempts":8,"round_timeout_seconds":86400,"server_wait_cap_seconds":3600}`
}

func schemaProps(props, required string) string {
	return fmt.Sprintf(`{"event_type":"UserRegistered","schema_version":"1","shape":{"additional_properties":false,"properties":{%s},"required":%s,"type":"object"}}`, props, required)
}

func route(id, conditions, targets string) string {
	return fmt.Sprintf(`{"caller_id":"caller-demo","conditions":%s,"event_type":"UserRegistered","route_id":%q,"schema_version":"1","source_business":"user-service","targets":%s}`, conditions, id, targets)
}

func target(id, channel, op, mappingID string) string {
	return fmt.Sprintf(`%q:{"adapter_type":"mock-json","adapter_version":"1","channel_id":%q,"credential_ref":"provider/demo","mapping_id":%q,"mapping_version":"1","operation":%q,"provider_account_id":"demo","provider_id":"mock","semantic_config":{"auth_type":"bearer","base_url":"http://mockvendor:9090","idempotency":{"mode":"key","starts_at":"first_possible_send","window_seconds":86400},"path":"/notify/%s"}}`, id, channel, mappingID, op, op)
}

func mapping(id, fields string) string {
	return fmt.Sprintf(`%q:{"fields":%s,"version":"1"}`, id, fields)
}

func fieldFrom(to, from, typ string, required bool) string {
	return fmt.Sprintf(`{"from":%q,"nullable":false,"required":%t,"to":%q,"type":%q}`, from, required, to, typ)
}

func releaseDoc(schema, routes, targets, mappings string) string {
	return fmt.Sprintf(`{"event_schemas":[%s],"format_version":1,"mappings":{%s},"policy":%s,"routes":[%s],"targets":{%s}}`, schema, mappings, policyJSON(), routes, targets)
}

func exampleContent() string {
	return releaseDoc(
		schemaProps(`"campaign":{"type":"string"},"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-crm", "[]", `["crm-create-user"]`),
		target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("crm-user", "["+fieldFrom("/external_user_id", "/user_id", "string", true)+"]"),
	)
}

func tagged(tag string) string {
	fields := "[" + fieldFrom("/external_user_id", "/user_id", "string", true) + "," + fmt.Sprintf(`{"constant":%q,"nullable":false,"required":true,"to":"/tag","type":"string"}`, tag) + "]"
	return releaseDoc(
		schemaProps(`"user_id":{"type":"string"}`, `["user_id"]`),
		route("user-crm", "[]", `["crm-create-user"]`),
		target("crm-create-user", "1", "create-user", "crm-user"),
		mapping("crm-user", fields),
	)
}

func wrapCreate(key, reason, content string) string {
	return fmt.Sprintf(`{"content":%s,"operation_idempotency_key":%q,"reason":%q}`, content, key, reason)
}

type memOp struct {
	fp      string
	create  *data_interface.CreateDraftResult
	publish *data_interface.PublishResult
}

type memStore struct {
	mu       sync.Mutex
	nextID   uint64
	releases map[uint64]data_interface.ReleaseRecord
	current  *uint64
	ops      map[string]memOp
	channels map[uint64]data_interface.ChannelPin
	creds    map[string]data_interface.CredentialPin
}

func newMem() *memStore {
	return &memStore{
		releases: map[uint64]data_interface.ReleaseRecord{},
		ops:      map[string]memOp{},
		channels: map[uint64]data_interface.ChannelPin{},
		creds:    map[string]data_interface.CredentialPin{},
	}
}

func (m *memStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.releases)
}

func (m *memStore) CreateDraft(_ context.Context, in data_interface.CreateDraftInput) (data_interface.CreateDraftResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := constant.OpConfigCreate + "\x00" + in.IdempotencyKey
	if op, ok := m.ops[key]; ok {
		if op.fp != in.Fingerprint || op.create == nil {
			return data_interface.CreateDraftResult{}, derr(constant.CodeIdempotencyConflict, "相同操作键的参数不一致")
		}
		return *op.create, nil
	}
	m.nextID++
	var max uint32
	for _, rec := range m.releases {
		if rec.VersionNo > max {
			max = rec.VersionNo
		}
	}
	rec := data_interface.ReleaseRecord{
		ReleaseID: m.nextID, VersionNo: max + 1, Status: "draft",
		ContentCanonical: append([]byte(nil), in.ContentCanonical...), ContentSHA256: in.ContentSHA256,
		FingerprintVersion: constant.FingerprintVersion, CreatedAt: time.Now().UTC(),
	}
	m.releases[rec.ReleaseID] = rec
	out := data_interface.CreateDraftResult{
		OperationID: fmt.Sprintf("op-%d", rec.ReleaseID), ReleaseID: rec.ReleaseID,
		VersionNo: rec.VersionNo, Status: "draft", ContentSHA256: rec.ContentSHA256,
	}
	m.ops[key] = memOp{fp: in.Fingerprint, create: &out}
	return out, nil
}

func (m *memStore) Publish(_ context.Context, in data_interface.PublishInput) (data_interface.PublishResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := constant.OpConfigPublish + "\x00" + in.IdempotencyKey
	if op, ok := m.ops[key]; ok {
		if op.fp != in.Fingerprint || op.publish == nil {
			return data_interface.PublishResult{}, derr(constant.CodeIdempotencyConflict, "相同操作键的参数不一致")
		}
		return *op.publish, nil
	}
	rec, ok := m.releases[in.ReleaseID]
	if !ok {
		return data_interface.PublishResult{}, data_interface.ErrReleaseNotFound
	}
	if rec.Status != "draft" {
		return data_interface.PublishResult{}, derr(constant.CodeStateConflict, "草稿不能再次发布")
	}
	if !sameID(m.current, in.Expected) {
		return data_interface.PublishResult{}, derr(constant.CodeVersionConflict, "当前发布版本已变化")
	}
	now := time.Now().UTC()
	if m.current != nil {
		old := m.releases[*m.current]
		old.Status = "superseded"
		m.releases[*m.current] = old
	}
	rec.Status = "published"
	rec.PublishedAt = &now
	m.releases[rec.ReleaseID] = rec
	current := rec.ReleaseID
	prev := m.current
	m.current = &current
	out := data_interface.PublishResult{
		OperationID: fmt.Sprintf("pub-%d", rec.ReleaseID), ReleaseID: rec.ReleaseID,
		VersionNo: rec.VersionNo, Previous: prev, PublishedAt: now,
	}
	m.ops[key] = memOp{fp: in.Fingerprint, publish: &out}
	return out, nil
}

func (m *memStore) Load(_ context.Context, releaseID uint64) (data_interface.ReleaseRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.releases[releaseID]
	if !ok {
		return data_interface.ReleaseRecord{}, data_interface.ErrReleaseNotFound
	}
	rec.ContentCanonical = append([]byte(nil), rec.ContentCanonical...)
	return rec, nil
}

func (m *memStore) LoadCurrentID(context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return 0, data_interface.ErrNoCurrent
	}
	return *m.current, nil
}

func (m *memStore) GetChannel(_ context.Context, id uint64) (data_interface.ChannelPin, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pin, ok := m.channels[id]
	if !ok {
		return data_interface.ChannelPin{}, data_interface.ErrChannelNotFound
	}
	return pin, nil
}

func (m *memStore) GetCredential(_ context.Context, ref string) (data_interface.CredentialPin, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pin, ok := m.creds[ref]
	if !ok {
		return data_interface.CredentialPin{}, data_interface.ErrCredentialNotFound
	}
	return pin, nil
}

func sameID(current, expected *uint64) bool {
	if current == nil || expected == nil {
		return current == nil && expected == nil
	}
	return *current == *expected
}

func derr(code, message string) error {
	err, _ := domain.NewError(code, message, "", nil)
	return err
}
