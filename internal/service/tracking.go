package service

import (
	"context"
	"net/http"

	"notifier/internal/biz/biz_tracking"
	"notifier/internal/constant"
	"notifier/internal/domain"
)

// TrackingService 转换 S08 查询协议。范围、游标与字段语义在 biz_tracking。
type TrackingService struct {
	q *biz_tracking.Query
}

func NewTrackingService(q *biz_tracking.Query) *TrackingService { return &TrackingService{q: q} }

func (s *TrackingService) principal(w http.ResponseWriter, r *http.Request) (biz_tracking.Principal, biz_tracking.Params, bool) {
	p, ok := PrincipalFrom(r.Context())
	if !ok {
		de, _ := domain.NewError(CodeUnauthenticated, "未认证", "", nil)
		WriteError(w, r, de)
		return biz_tracking.Principal{}, nil, false
	}
	params := biz_tracking.Params{}
	for k, vs := range r.URL.Query() {
		if len(vs) != 1 {
			de, _ := domain.NewError(constant.ReasonDuplicateQuery, "查询参数重复", k, nil)
			WriteError(w, r, de)
			return biz_tracking.Principal{}, nil, false
		}
		params[k] = vs[0]
	}
	return biz_tracking.Principal{CallerID: p.CallerID, Scopes: p.Scopes, Permissions: p.Permissions}, params, true
}

func (s *TrackingService) pathID(w http.ResponseWriter, r *http.Request, field string) (uint64, bool) {
	id, err := domain.ParseInternalID(field, r.PathValue("id"))
	if err != nil {
		de, _ := domain.NewError(CodeNotFound, "资源不存在", "", nil)
		WriteError(w, r, de)
		return 0, false
	}
	return id, true
}

func reply[T any](w http.ResponseWriter, r *http.Request, v T, err error) {
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func noParams(w http.ResponseWriter, r *http.Request, in biz_tracking.Params) bool {
	for k := range in {
		de, _ := domain.NewError(constant.ReasonUnknownQuery, "详情不接受查询参数", k, nil)
		WriteError(w, r, de)
		return false
	}
	return true
}

func (s *TrackingService) Events(w http.ResponseWriter, r *http.Request) {
	if p, in, ok := s.principal(w, r); ok {
		v, err := s.q.Events(r.Context(), p, in)
		reply(w, r, v, err)
	}
}

func (s *TrackingService) Event(w http.ResponseWriter, r *http.Request) {
	p, in, ok := s.principal(w, r)
	if !ok || !noParams(w, r, in) {
		return
	}
	if id, ok := s.pathID(w, r, "id"); ok {
		v, err := s.q.Event(r.Context(), p, id)
		reply(w, r, v, err)
	}
}

func (s *TrackingService) Deliveries(w http.ResponseWriter, r *http.Request) {
	if p, in, ok := s.principal(w, r); ok {
		v, err := s.q.Deliveries(r.Context(), p, in)
		reply(w, r, v, err)
	}
}

func (s *TrackingService) Delivery(w http.ResponseWriter, r *http.Request) {
	p, in, ok := s.principal(w, r)
	if !ok || !noParams(w, r, in) {
		return
	}
	if id, ok := s.pathID(w, r, "id"); ok {
		v, err := s.q.Delivery(r.Context(), p, id)
		reply(w, r, v, err)
	}
}

func (s *TrackingService) Attempts(w http.ResponseWriter, r *http.Request) {
	if p, in, ok := s.principal(w, r); ok {
		if id, ok := s.pathID(w, r, "id"); ok {
			v, err := s.q.Attempts(r.Context(), p, id, in)
			reply(w, r, v, err)
		}
	}
}

func (s *TrackingService) History(w http.ResponseWriter, r *http.Request) {
	if p, in, ok := s.principal(w, r); ok {
		if id, ok := s.pathID(w, r, "id"); ok {
			v, err := s.q.History(r.Context(), p, id, in)
			reply(w, r, v, err)
		}
	}
}

func (s *TrackingService) Overview(w http.ResponseWriter, r *http.Request) {
	if p, in, ok := s.principal(w, r); ok {
		v, err := s.q.Overview(r.Context(), p, in)
		reply(w, r, v, err)
	}
}

// ReadinessReporter 提供 TRK-O03 就绪报告；nil 时沿用基础 Readiness。
type ReadinessReporter func(ctx context.Context) domain.ReadinessReport
