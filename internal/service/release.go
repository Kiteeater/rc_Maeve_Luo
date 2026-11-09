package service

import (
	"encoding/json"
	"net/http"

	"notifier/internal/biz/biz_config"
	"notifier/internal/canonicaljson"
	"notifier/internal/domain"
)

// ReleaseService 转换配置发布协议。规则在 biz_config。
type ReleaseService struct {
	uc      *biz_config.UseCase
	maxRead int64
}

func NewReleaseService(uc *biz_config.UseCase, maxBytes int) *ReleaseService {
	const extra = 64 * 1024
	const hard = 4<<20 + extra
	n := int64(maxBytes) + extra
	if maxBytes < 0 {
		n = extra
	}
	if n > hard {
		n = hard
	}
	return &ReleaseService{uc: uc, maxRead: n}
}

type issueBody struct {
	Code          string `json:"code"`
	FieldPath     string `json:"field_path"`
	RouteTargetID string `json:"route_target_id"`
	Message       string `json:"message"`
}

type validateBody struct {
	Valid     bool        `json:"valid"`
	Errors    []issueBody `json:"errors"`
	Truncated bool        `json:"truncated"`
}

// Validate 返回 200。内容不合法仍是 200，JSON 语法错误是 400。
func (s *ReleaseService) Validate(w http.ResponseWriter, r *http.Request) {
	principal, content, ok := s.readContent(w, r)
	if !ok {
		return
	}
	out, err := s.uc.Validate(r.Context(), principal, content)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	errorsBody := make([]issueBody, len(out.Issues))
	for i, item := range out.Issues {
		errorsBody[i] = issueBody{Code: item.Code, FieldPath: item.FieldPath, RouteTargetID: item.RouteTargetID, Message: item.Message}
	}
	writeJSON(w, http.StatusOK, validateBody{Valid: out.Valid, Errors: errorsBody, Truncated: out.Truncated})
}

// Create 创建草稿。校验失败是 422，成功是 201。
func (s *ReleaseService) Create(w http.ResponseWriter, r *http.Request) {
	principal, raw, ok := s.readObject(w, r, []string{"operation_idempotency_key", "reason", "content"})
	if !ok {
		return
	}
	key, reason, content, ok := s.createFields(w, r, raw)
	if !ok {
		return
	}
	out, err := s.uc.Create(r.Context(), principal, key, reason, content)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	id, err := domain.FormatInternalID(out.ReleaseID)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, struct {
		OperationID   string `json:"operation_id"`
		ReleaseID     string `json:"release_id"`
		VersionNo     uint32 `json:"version_no"`
		Status        string `json:"status"`
		ContentSHA256 string `json:"content_sha256"`
	}{out.OperationID, id, out.VersionNo, out.Status, out.ContentSHA256})
}

// Publish 发布草稿。expected_current_release_id 必须出现，JSON null 表示首次发布。
func (s *ReleaseService) Publish(w http.ResponseWriter, r *http.Request) {
	principal, raw, ok := s.readObject(w, r, []string{"operation_idempotency_key", "reason", "expected_current_release_id"})
	if !ok {
		return
	}
	releaseID, err := domain.ParseInternalID("release_id", r.PathValue("id"))
	if err != nil {
		de, _ := domain.NewError(CodeInvalidRequest, "发布标识不合法", "release_id", nil)
		WriteError(w, r, de)
		return
	}
	key, reason, expected, ok := publishFields(raw)
	if !ok {
		de, _ := domain.NewError(CodeInvalidRequest, "发布参数不合法", "expected_current_release_id", nil)
		WriteError(w, r, de)
		return
	}
	out, err := s.uc.Publish(r.Context(), principal, releaseID, key, reason, expected)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	id, err := domain.FormatInternalID(out.ReleaseID)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	var previous *string
	if out.Previous != nil {
		text, err := domain.FormatInternalID(*out.Previous)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		previous = &text
	}
	writeJSON(w, http.StatusOK, struct {
		OperationID string  `json:"operation_id"`
		ReleaseID   string  `json:"release_id"`
		VersionNo   uint32  `json:"version_no"`
		Previous    *string `json:"previous_release_id"`
		PublishedAt string  `json:"published_at"`
	}{out.OperationID, id, out.VersionNo, previous, domain.FormatUTCMicros(out.PublishedAt)})
}

// Get 按 ID 读取完整版本。
func (s *ReleaseService) Get(w http.ResponseWriter, r *http.Request) {
	if err := RejectQuery(r, nil); err != nil {
		WriteError(w, r, err)
		return
	}
	if _, ok := s.principal(w, r); !ok {
		return
	}
	id, err := domain.ParseInternalID("release_id", r.PathValue("id"))
	if err != nil {
		de, _ := domain.NewError(CodeInvalidRequest, "发布标识不合法", "release_id", nil)
		WriteError(w, r, de)
		return
	}
	s.writeView(w, r, id)
}

// Current 读取当前发布。
func (s *ReleaseService) Current(w http.ResponseWriter, r *http.Request) {
	if err := RejectQuery(r, nil); err != nil {
		WriteError(w, r, err)
		return
	}
	if _, ok := s.principal(w, r); !ok {
		return
	}
	view, err := s.uc.Current(r.Context())
	if err != nil {
		WriteError(w, r, err)
		return
	}
	s.writeRelease(w, r, view)
}

func (s *ReleaseService) writeView(w http.ResponseWriter, r *http.Request, id uint64) {
	view, err := s.uc.Get(r.Context(), id)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	s.writeRelease(w, r, view)
}

func (s *ReleaseService) writeRelease(w http.ResponseWriter, r *http.Request, view biz_config.ReleaseView) {
	id, err := domain.FormatInternalID(view.ReleaseID)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	var published *string
	if view.PublishedAt != nil {
		text := domain.FormatUTCMicros(*view.PublishedAt)
		published = &text
	}
	writeJSON(w, http.StatusOK, struct {
		ReleaseID     string          `json:"release_id"`
		VersionNo     uint32          `json:"version_no"`
		Status        string          `json:"status"`
		FormatVersion int             `json:"format_version"`
		ContentSHA256 string          `json:"content_sha256"`
		Content       json.RawMessage `json:"content"`
		CreatedAt     string          `json:"created_at"`
		PublishedAt   *string         `json:"published_at"`
	}{
		ReleaseID: id, VersionNo: view.VersionNo, Status: view.Status, FormatVersion: view.FormatVersion,
		ContentSHA256: view.ContentSHA256, Content: json.RawMessage(view.Content),
		CreatedAt: domain.FormatUTCMicros(view.CreatedAt), PublishedAt: published,
	})
}

func (s *ReleaseService) readContent(w http.ResponseWriter, r *http.Request) (biz_config.Principal, []byte, bool) {
	principal, raw, ok := s.readObject(w, r, []string{"content"})
	if !ok {
		return biz_config.Principal{}, nil, false
	}
	content, ok := contentBytes(raw)
	if !ok {
		de, _ := domain.NewError(CodeInvalidRequest, "content 必须是对象", "content", nil)
		WriteError(w, r, de)
		return biz_config.Principal{}, nil, false
	}
	return principal, content, true
}

func (s *ReleaseService) createFields(w http.ResponseWriter, r *http.Request, raw canonicaljson.Value) (string, string, []byte, bool) {
	key, ok := textMember(raw, "operation_idempotency_key")
	reason, ok2 := textMember(raw, "reason")
	content, ok3 := contentBytes(raw)
	if !ok || !ok2 || !ok3 {
		de, _ := domain.NewError(CodeInvalidRequest, "创建参数不合法", "", nil)
		WriteError(w, r, de)
		return "", "", nil, false
	}
	return key, reason, content, true
}

func publishFields(raw canonicaljson.Value) (string, string, *uint64, bool) {
	key, ok := textMember(raw, "operation_idempotency_key")
	reason, ok2 := textMember(raw, "reason")
	exp, ok3 := raw.Member("expected_current_release_id")
	if !ok || !ok2 || !ok3 {
		return "", "", nil, false
	}
	if exp.Kind() == canonicaljson.KindNull {
		return key, reason, nil, true
	}
	text, ok := exp.Text()
	if !ok {
		return "", "", nil, false
	}
	id, err := domain.ParseInternalID("expected_current_release_id", text)
	if err != nil {
		return "", "", nil, false
	}
	return key, reason, &id, true
}

func (s *ReleaseService) readObject(w http.ResponseWriter, r *http.Request, allowed []string) (biz_config.Principal, canonicaljson.Value, bool) {
	principal, ok := s.principal(w, r)
	if !ok {
		return biz_config.Principal{}, canonicaljson.Value{}, false
	}
	if err := RejectQuery(r, nil); err != nil {
		WriteError(w, r, err)
		return biz_config.Principal{}, canonicaljson.Value{}, false
	}
	body, err := ReadBody(r, s.maxRead)
	if err != nil {
		WriteError(w, r, err)
		return biz_config.Principal{}, canonicaljson.Value{}, false
	}
	if err := RejectUnknownFields(body, allowed); err != nil {
		WriteError(w, r, err)
		return biz_config.Principal{}, canonicaljson.Value{}, false
	}
	root, err := canonicaljson.ParseObject(body)
	if err != nil {
		de, _ := domain.NewError(CodeInvalidRequest, "JSON 不是完整对象", "", nil)
		WriteError(w, r, de)
		return biz_config.Principal{}, canonicaljson.Value{}, false
	}
	return principal, root, true
}

func (s *ReleaseService) principal(w http.ResponseWriter, r *http.Request) (biz_config.Principal, bool) {
	p, ok := PrincipalFrom(r.Context())
	if !ok {
		de, _ := domain.NewError(CodeUnauthenticated, "未认证", "", nil)
		WriteError(w, r, de)
		return biz_config.Principal{}, false
	}
	return biz_config.Principal{
		ActorID: p.ActorID, CallerID: p.CallerID, AllowedSources: p.AllowedSources, Scopes: p.Scopes,
	}, true
}

func contentBytes(raw canonicaljson.Value) ([]byte, bool) {
	content, ok := raw.Member("content")
	if !ok || content.Kind() != canonicaljson.KindObject {
		return nil, false
	}
	return canonicaljson.AppendCanonical(nil, content), true
}

func textMember(raw canonicaljson.Value, key string) (string, bool) {
	field, ok := raw.Member(key)
	if !ok {
		return "", false
	}
	return field.Text()
}
