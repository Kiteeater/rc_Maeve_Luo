package service

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"

	"notifier/internal/biz/biz_operations"
	"notifier/internal/domain"
)

// OperationService 转换 S07 人工重投与核对结案协议。规则在 biz_operations。
type OperationService struct {
	uc      *biz_operations.UseCase
	maxBody int64
}

func NewOperationService(uc *biz_operations.UseCase, maxBody int) *OperationService {
	if maxBody < 0 {
		maxBody = 0
	}
	return &OperationService{uc: uc, maxBody: int64(maxBody)}
}

type retryBody struct {
	Key         string `json:"operation_idempotency_key"`
	ExpectedRow uint32 `json:"expected_row_version"`
	Reason      string `json:"reason"`
	RiskAck     bool   `json:"risk_acknowledged"`
}

type evidenceBody struct {
	Type              string   `json:"type"`
	Reference         string   `json:"reference"`
	Summary           string   `json:"summary"`
	CoveredAttemptIDs []string `json:"covered_attempt_ids"`
	IdempotencyKey    string   `json:"idempotency_key"`
	Scope             string   `json:"scope"`
}

type resolveBody struct {
	Key         string       `json:"operation_idempotency_key"`
	ExpectedRow uint32       `json:"expected_row_version"`
	Resolution  string       `json:"resolution"`
	Reason      string       `json:"reason"`
	Evidence    evidenceBody `json:"evidence"`
}

// opStatus 按 S07 §7：channel_blocked / configuration_unavailable 在人工操作接口为 422。
func opStatus(code string) int {
	if code == CodeConfigurationUnavailable {
		return http.StatusUnprocessableEntity
	}
	return StatusOf(code)
}

func invalid(msg, path string) error {
	err, _ := domain.NewError(CodeInvalidRequest, msg, path, nil)
	return err
}

func (s *OperationService) prepare(w http.ResponseWriter, r *http.Request, dst any) (biz_operations.Principal, uint64, bool) {
	fail := func(err error) (biz_operations.Principal, uint64, bool) {
		writeErrorStatus(w, r, err, opStatus)
		return biz_operations.Principal{}, 0, false
	}
	if err := RejectQuery(r, nil); err != nil {
		return fail(err)
	}
	principal, ok := PrincipalFrom(r.Context())
	if !ok {
		de, _ := domain.NewError(CodeUnauthenticated, "未认证", "", nil)
		return fail(de)
	}
	id, err := domain.ParseInternalID("id", r.PathValue("id"))
	if err != nil {
		return fail(invalid("任务 ID 不合法", "id"))
	}
	body, err := ReadBody(r, s.maxBody)
	if err != nil {
		return fail(err)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fail(invalid("请求体不合法", ""))
	}
	return biz_operations.Principal{ActorID: principal.ActorID, Scopes: principal.Scopes, Permissions: principal.Permissions}, id, true
}

func (s *OperationService) respond(w http.ResponseWriter, r *http.Request, status int, out biz_operations.Result, err error) {
	if err != nil {
		writeErrorStatus(w, r, err, opStatus)
		return
	}
	if out.Replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(out.Body)
}

// Retry 处理 POST /v1/deliveries/{id}/retry，成功 202（不是送达确认）。
func (s *OperationService) Retry(w http.ResponseWriter, r *http.Request) {
	var req retryBody
	p, id, ok := s.prepare(w, r, &req)
	if !ok {
		return
	}
	out, err := s.uc.Retry(r.Context(), p, id, biz_operations.RetryRequest{Key: req.Key, ExpectedRow: req.ExpectedRow, Reason: req.Reason, RiskAck: req.RiskAck})
	s.respond(w, r, http.StatusAccepted, out, err)
}

// Resolve 处理 POST /v1/deliveries/{id}/resolve，成功 200。
func (s *OperationService) Resolve(w http.ResponseWriter, r *http.Request) {
	var req resolveBody
	p, id, ok := s.prepare(w, r, &req)
	if !ok {
		return
	}
	ev := domain.Evidence{Type: req.Evidence.Type, Reference: req.Evidence.Reference, Summary: req.Evidence.Summary,
		IdempotencyKey: req.Evidence.IdempotencyKey, Scope: req.Evidence.Scope}
	for _, raw := range req.Evidence.CoveredAttemptIDs {
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || v == 0 {
			de, _ := domain.NewError(CodeEvidenceInvalid, "尝试 ID 不合法", "evidence.covered_attempt_ids", nil)
			writeErrorStatus(w, r, de, opStatus)
			return
		}
		ev.CoveredAttemptIDs = append(ev.CoveredAttemptIDs, v)
	}
	out, err := s.uc.Resolve(r.Context(), p, id, biz_operations.ResolveRequest{Key: req.Key, ExpectedRow: req.ExpectedRow, Resolution: req.Resolution, Reason: req.Reason, Evidence: ev})
	s.respond(w, r, http.StatusOK, out, err)
}
