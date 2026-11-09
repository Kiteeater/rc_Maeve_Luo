package service

import (
	"net/http"

	"notifier/internal/biz/biz_event"
	"notifier/internal/domain"
)

// EventService 转换事件提交协议。规则在 biz_event。
type EventService struct {
	uc      *biz_event.UseCase
	maxBody int64
}

func NewEventService(uc *biz_event.UseCase, maxBody int) *EventService {
	if maxBody < 0 {
		maxBody = 0
	}
	return &EventService{uc: uc, maxBody: int64(maxBody)}
}

type deliveryBody struct {
	DeliveryID        string `json:"delivery_id"`
	RouteTargetID     string `json:"route_target_id"`
	ProviderID        string `json:"provider_id"`
	ProviderAccountID string `json:"provider_account_id"`
	Operation         string `json:"operation"`
}

type acceptanceBody struct {
	EventID          string         `json:"event_id"`
	SourceBusiness   string         `json:"source_business"`
	SourceEventID    string         `json:"source_event_id"`
	AcceptedAt       string         `json:"accepted_at"`
	RoutingReleaseID string         `json:"routing_release_id"`
	DeliveryCount    int            `json:"delivery_count"`
	Deliveries       []deliveryBody `json:"deliveries"`
}

// Submit 读取有界 JSON 并返回 202。受理响应不是送达结果。
func (s *EventService) Submit(w http.ResponseWriter, r *http.Request) {
	if err := RejectQuery(r, nil); err != nil {
		WriteError(w, r, err)
		return
	}
	body, err := ReadBody(r, s.maxBody)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	principal, ok := PrincipalFrom(r.Context())
	if !ok {
		de, _ := domain.NewError(CodeUnauthenticated, "未认证", "", nil)
		WriteError(w, r, de)
		return
	}
	out, err := s.uc.Submit(r.Context(), biz_event.Principal{
		ActorID: principal.ActorID, CallerID: principal.CallerID,
		AllowedSources: principal.AllowedSources, Scopes: principal.Scopes,
	}, body)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	payload, err := acceptanceJSON(out)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, payload)
}

func acceptanceJSON(out biz_event.Acceptance) (acceptanceBody, error) {
	eventID, err := domain.FormatInternalID(out.EventID)
	if err != nil {
		de, _ := domain.NewError(CodeInternal, "内部错误", "", nil)
		return acceptanceBody{}, de
	}
	releaseID, err := domain.FormatInternalID(out.RoutingReleaseID)
	if err != nil {
		de, _ := domain.NewError(CodeInternal, "内部错误", "", nil)
		return acceptanceBody{}, de
	}
	items := make([]deliveryBody, len(out.Deliveries))
	for i, item := range out.Deliveries {
		id, err := domain.FormatInternalID(item.DeliveryID)
		if err != nil {
			de, _ := domain.NewError(CodeInternal, "内部错误", "", nil)
			return acceptanceBody{}, de
		}
		items[i] = deliveryBody{
			DeliveryID: id, RouteTargetID: item.RouteTargetID, ProviderID: item.ProviderID,
			ProviderAccountID: item.ProviderAccountID, Operation: item.Operation,
		}
	}
	return acceptanceBody{
		EventID: eventID, SourceBusiness: out.SourceBusiness, SourceEventID: out.SourceEventID,
		AcceptedAt: domain.FormatUTCMicros(out.AcceptedAt), RoutingReleaseID: releaseID,
		DeliveryCount: len(items), Deliveries: items,
	}, nil
}
