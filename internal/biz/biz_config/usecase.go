// Package biz_config 实现不可变配置的校验、草稿、发布和按版本计划计算。
// 引用版本的最终复核在发布事务内完成，这里不提前比较版本。
package biz_config

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// Principal 是已认证的调用方。权限码由传输层检查，范围在这里检查。
type Principal struct {
	ActorID        string
	CallerID       string
	AllowedSources []string
	Scopes         []string
}

// Validation 是静态校验加引用检查的结果。内容不合法时不返回错误。
type Validation struct {
	Valid     bool
	Issues    []domain.Issue
	Truncated bool
}

// ReleaseView 是一次按版本或当前指针的读取。
type ReleaseView struct {
	ReleaseID     uint64
	VersionNo     uint32
	Status        string
	FormatVersion int
	ContentSHA256 string
	Content       []byte
	CreatedAt     time.Time
	PublishedAt   *time.Time
}

// UseCase 依赖小端口。缓存可以为空，此时计划计算总是读 MySQL。
type UseCase struct {
	repo        data_interface.ReleaseRepository
	channels    data_interface.ChannelReader
	creds       data_interface.CredentialReader
	adapters    data_interface.AdapterRegistry
	cache       data_interface.ReleaseCache
	bounds      domain.Bounds
	calcTimeout time.Duration
}

func NewUseCase(
	repo data_interface.ReleaseRepository,
	channels data_interface.ChannelReader,
	creds data_interface.CredentialReader,
	adapters data_interface.AdapterRegistry,
	cache data_interface.ReleaseCache,
	bounds domain.Bounds,
	calcTimeout time.Duration,
) *UseCase {
	return &UseCase{
		repo: repo, channels: channels, creds: creds, adapters: adapters,
		cache: cache, bounds: bounds, calcTimeout: calcTimeout,
	}
}

type inspected struct {
	doc       *domain.ReleaseDocument
	issues    []domain.Issue
	truncated bool
}

// Validate 做本地校验和引用检查，不写库。
func (u *UseCase) Validate(ctx context.Context, principal Principal, content []byte) (Validation, error) {
	ins, err := u.inspect(ctx, principal, content)
	if err != nil {
		return Validation{}, err
	}
	if len(ins.issues) > 0 {
		return Validation{Issues: ins.issues, Truncated: ins.truncated}, nil
	}
	return Validation{Valid: true, Issues: []domain.Issue{}}, nil
}

// Create 保存一份不可变草稿。相同操作键和参数返回原结果。
func (u *UseCase) Create(ctx context.Context, principal Principal, key, reason string, content []byte) (data_interface.CreateDraftResult, error) {
	if err := checkKeyReason(key, reason); err != nil {
		return data_interface.CreateDraftResult{}, err
	}
	ins, err := u.inspect(ctx, principal, content)
	if err != nil {
		return data_interface.CreateDraftResult{}, err
	}
	if len(ins.issues) > 0 {
		return data_interface.CreateDraftResult{}, invalidContent(ins.issues, ins.truncated)
	}
	return u.repo.CreateDraft(ctx, data_interface.CreateDraftInput{
		ActorID:          principal.ActorID,
		Reason:           reason,
		IdempotencyKey:   key,
		Fingerprint:      data_interface.CreateFingerprint(ins.doc.SHA256, reason),
		ContentCanonical: ins.doc.Canonical,
		ContentSHA256:    ins.doc.SHA256,
		Bindings:         ins.doc.Bindings(),
	})
}

// Publish 切换当前指针。引用版本是否变化只由仓储事务判断。
func (u *UseCase) Publish(ctx context.Context, principal Principal, releaseID uint64, key, reason string, expected *uint64) (data_interface.PublishResult, error) {
	if err := checkKeyReason(key, reason); err != nil {
		return data_interface.PublishResult{}, err
	}
	if _, err := domain.FormatInternalID(releaseID); err != nil {
		return data_interface.PublishResult{}, fail(constant.CodeNotFound, "发布不存在", "release_id")
	}
	fp, err := data_interface.PublishFingerprint(releaseID, expected, reason)
	if err != nil {
		return data_interface.PublishResult{}, fail(constant.CodeInvalidRequest, "发布参数不合法", "")
	}
	out, err := u.repo.Publish(ctx, data_interface.PublishInput{
		ActorID:        principal.ActorID,
		Reason:         reason,
		IdempotencyKey: key,
		Fingerprint:    fp,
		ReleaseID:      releaseID,
		Expected:       expected,
	})
	if errors.Is(err, data_interface.ErrReleaseNotFound) {
		return data_interface.PublishResult{}, fail(constant.CodeNotFound, "发布不存在", "release_id")
	}
	return out, err
}

// Get 从 MySQL 读取指定版本，不使用缓存里的状态。
func (u *UseCase) Get(ctx context.Context, releaseID uint64) (ReleaseView, error) {
	rec, err := u.repo.Load(ctx, releaseID)
	if errors.Is(err, data_interface.ErrReleaseNotFound) {
		return ReleaseView{}, fail(constant.CodeNotFound, "发布不存在", "release_id")
	}
	if err != nil {
		return ReleaseView{}, err
	}
	return viewOf(rec)
}

// Current 从 MySQL 确认当前指针后再读内容。
func (u *UseCase) Current(ctx context.Context) (ReleaseView, error) {
	id, err := u.repo.LoadCurrentID(ctx)
	if errors.Is(err, data_interface.ErrNoCurrent) {
		return ReleaseView{}, fail(constant.CodeConfigurationUnavailable, "当前没有已发布配置", "")
	}
	if err != nil {
		return ReleaseView{}, err
	}
	return u.Get(ctx, id)
}

// Calculate 对选定的不可变版本计算计划。缓存缺失时按同一 ID 回源。
func (u *UseCase) Calculate(ctx context.Context, principal Principal, releaseID uint64, env domain.EventEnvelope) (domain.DeliveryPlan, error) {
	if err := authorizeEvent(principal, env); err != nil {
		return domain.DeliveryPlan{}, err
	}
	if u.calcTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, u.calcTimeout)
		defer cancel()
	}
	rec, err := u.loadForPlan(ctx, releaseID)
	if err != nil {
		return domain.DeliveryPlan{}, mapDeadline(ctx, err)
	}
	checked, err := domain.ValidateContent(rec.ContentCanonical, u.bounds)
	if err != nil || checked.Doc == nil || len(checked.Issues) > 0 {
		return domain.DeliveryPlan{}, fail(constant.CodeConfigurationUnavailable, "发布版本不可用", "")
	}
	plan, err := domain.BuildPlan(ctx, checked.Doc, rec.ReleaseID, rec.VersionNo, env, u.bounds)
	if err != nil {
		return domain.DeliveryPlan{}, mapDeadline(ctx, err)
	}
	if err := u.checkPayloads(ctx, checked.Doc, plan); err != nil {
		return domain.DeliveryPlan{}, err
	}
	if u.cache != nil {
		_ = u.cache.Put(ctx, rec)
	}
	return plan, nil
}

func (u *UseCase) inspect(ctx context.Context, principal Principal, content []byte) (inspected, error) {
	checked, err := domain.ValidateContent(content, u.bounds)
	if err != nil {
		return inspected{}, fail(constant.CodeInvalidRequest, "JSON 不是完整对象", "")
	}
	if !scopesAllow(principal, checked.Callers) {
		return inspected{}, fail(constant.CodeForbidden, "调用方不在授权范围", "")
	}
	if len(checked.Issues) > 0 {
		return inspected{issues: checked.Issues, truncated: checked.Truncated}, nil
	}
	issues, err := u.checkRefs(ctx, checked.Doc)
	if err != nil {
		return inspected{}, err
	}
	if len(issues) > 0 {
		issues, truncated := capIssues(issues)
		return inspected{issues: issues, truncated: truncated}, nil
	}
	return inspected{doc: checked.Doc}, nil
}

func (u *UseCase) checkRefs(ctx context.Context, doc *domain.ReleaseDocument) ([]domain.Issue, error) {
	var issues []domain.Issue
	for _, binding := range doc.Bindings() {
		adapter, ok := u.adapters.Lookup(binding.AdapterType, binding.AdapterVersion)
		if !ok {
			issues = append(issues, domain.Issue{
				Code: "unregistered_adapter", FieldPath: "/targets/" + binding.RouteTargetID + "/adapter_type",
				RouteTargetID: binding.RouteTargetID, Message: "适配器未注册",
			})
		} else if err := adapter.ValidateTarget(ctx, data_interface.TargetSpec{
			RouteTargetID: binding.RouteTargetID, Operation: binding.Operation,
			CredentialRef: binding.CredentialRef, SemanticCanonical: binding.Semantic,
		}); err != nil {
			issues = append(issues, adapterIssue(binding.RouteTargetID, err))
		}
		pin, err := u.channels.GetChannel(ctx, binding.ChannelID)
		if errors.Is(err, data_interface.ErrChannelNotFound) {
			issues = append(issues, domain.Issue{
				Code: "channel_not_found", FieldPath: "/targets/" + binding.RouteTargetID + "/channel_id",
				RouteTargetID: binding.RouteTargetID, Message: "通道不存在",
			})
		} else if err != nil {
			return nil, err
		} else if pin.ProviderID != binding.ProviderID || pin.ProviderAccountID != binding.ProviderAccountID ||
			pin.Operation != binding.Operation || pin.AdapterType != binding.AdapterType ||
			pin.AdapterVersion != binding.AdapterVersion || pin.CredentialRef != binding.CredentialRef {
			issues = append(issues, domain.Issue{
				Code: "channel_mismatch", FieldPath: "/targets/" + binding.RouteTargetID + "/channel_id",
				RouteTargetID: binding.RouteTargetID, Message: "通道归属不一致",
			})
		}
		cred, err := u.creds.GetCredential(ctx, binding.CredentialRef)
		if errors.Is(err, data_interface.ErrCredentialNotFound) {
			issues = append(issues, domain.Issue{
				Code: "credential_missing", FieldPath: "/targets/" + binding.RouteTargetID + "/credential_ref",
				RouteTargetID: binding.RouteTargetID, Message: "凭证引用不存在",
			})
		} else if err != nil {
			return nil, err
		} else if cred.ProviderID != binding.ProviderID || cred.ProviderAccountID != binding.ProviderAccountID {
			issues = append(issues, domain.Issue{
				Code: "credential_mismatch", FieldPath: "/targets/" + binding.RouteTargetID + "/credential_ref",
				RouteTargetID: binding.RouteTargetID, Message: "凭证引用归属不一致",
			})
		}
	}
	slices.SortFunc(issues, compareIssue)
	return issues, nil
}

func (u *UseCase) loadForPlan(ctx context.Context, releaseID uint64) (data_interface.ReleaseRecord, error) {
	if u.cache != nil {
		rec, err := u.cache.Get(ctx, releaseID)
		if err == nil {
			return rec, nil
		}
	}
	rec, err := u.repo.Load(ctx, releaseID)
	if errors.Is(err, data_interface.ErrReleaseNotFound) {
		return data_interface.ReleaseRecord{}, fail(constant.CodeConfigurationUnavailable, "发布版本不可用", "")
	}
	return rec, err
}

func (u *UseCase) checkPayloads(ctx context.Context, doc *domain.ReleaseDocument, plan domain.DeliveryPlan) error {
	var details []domain.Detail
	for _, target := range plan.Targets {
		if ctx.Err() != nil {
			return fail(constant.CodePlanTimeout, "计划计算超时", "")
		}
		adapter, ok := u.adapters.Lookup(target.AdapterType, target.AdapterVersion)
		if !ok {
			details = append(details, domain.Detail{
				FieldPath: "/targets/" + target.RouteTargetID, Code: "unregistered_adapter",
				Message: target.RouteTargetID + ": 适配器未注册",
			})
			continue
		}
		sem, _ := doc.Semantic(target.RouteTargetID)
		if err := adapter.ValidatePayload(ctx, target.PayloadCanonical, sem); err != nil {
			var de *domain.Error
			path := "/targets/" + target.RouteTargetID
			msg := target.RouteTargetID + ": 映射载荷不合法"
			code := "mapping_invalid"
			if errors.As(err, &de) && de != nil {
				if de.Code != "" {
					code = de.Code
				}
				if de.Message != "" {
					msg = target.RouteTargetID + ": " + de.Message
				}
				if de.FieldPath != "" {
					path = de.FieldPath
				}
			}
			details = append(details, domain.Detail{FieldPath: path, Code: code, Message: msg})
		}
	}
	if len(details) == 0 {
		return nil
	}
	if len(details) > constant.MaxErrorDetails {
		details = details[:constant.MaxErrorDetails]
	}
	err, failErr := domain.NewError(constant.CodeMappingInvalid, "计划映射失败", details[0].FieldPath, details)
	if failErr != nil {
		return fail(constant.CodeMappingInvalid, "计划映射失败", "")
	}
	return err
}

func authorizeEvent(principal Principal, env domain.EventEnvelope) error {
	if principal.CallerID != "" && principal.CallerID != env.CallerID {
		return fail(constant.CodeForbidden, "调用方不在授权范围", "caller_id")
	}
	if len(principal.AllowedSources) > 0 && !slices.Contains(principal.AllowedSources, env.SourceBusiness) {
		return fail(constant.CodeForbidden, "来源不在授权范围", "source_business")
	}
	return nil
}

func scopesAllow(principal Principal, callers []string) bool {
	for _, caller := range callers {
		if !slices.Contains(principal.Scopes, caller) {
			return false
		}
	}
	return true
}

func checkKeyReason(key, reason string) error {
	if err := domain.ValidateExternalID("operation_idempotency_key", key); err != nil {
		return fail(constant.CodeInvalidRequest, "操作键不合法", "operation_idempotency_key")
	}
	if len(reason) < 1 || len(reason) > 1024 || !utf8.ValidString(reason) || blank(reason) {
		return fail(constant.CodeInvalidRequest, "原因不合法", "reason")
	}
	return nil
}

func blank(s string) bool {
	for _, r := range s {
		if !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func viewOf(rec data_interface.ReleaseRecord) (ReleaseView, error) {
	version, err := domain.ReadFormatVersion(rec.ContentCanonical)
	if err != nil {
		return ReleaseView{}, fail(constant.CodeConfigurationUnavailable, "发布版本不可用", "")
	}
	return ReleaseView{
		ReleaseID: rec.ReleaseID, VersionNo: rec.VersionNo, Status: rec.Status,
		FormatVersion: version, ContentSHA256: rec.ContentSHA256, Content: rec.ContentCanonical,
		CreatedAt: rec.CreatedAt, PublishedAt: rec.PublishedAt,
	}, nil
}

func invalidContent(issues []domain.Issue, truncated bool) error {
	issues, cut := capIssues(issues)
	if cut {
		truncated = true
	}
	details := make([]domain.Detail, len(issues))
	path := ""
	for i, item := range issues {
		details[i] = domain.Detail{FieldPath: item.FieldPath, Code: item.Code, Message: item.Message}
		if i == 0 {
			path = item.FieldPath
		}
	}
	msg := "配置校验未通过"
	if truncated {
		msg = "配置校验未通过，错误已截断"
	}
	err, failErr := domain.NewError(constant.CodeConfigInvalid, msg, path, details)
	if failErr != nil {
		return fail(constant.CodeConfigInvalid, msg, path)
	}
	return err
}

func capIssues(issues []domain.Issue) ([]domain.Issue, bool) {
	if len(issues) <= constant.MaxErrorDetails {
		return issues, false
	}
	return append([]domain.Issue(nil), issues[:constant.MaxErrorDetails]...), true
}

func compareIssue(a, b domain.Issue) int {
	if n := strings.Compare(a.FieldPath, b.FieldPath); n != 0 {
		return n
	}
	if n := strings.Compare(a.RouteTargetID, b.RouteTargetID); n != 0 {
		return n
	}
	if n := strings.Compare(a.Code, b.Code); n != 0 {
		return n
	}
	return strings.Compare(a.Message, b.Message)
}

func adapterIssue(id string, err error) domain.Issue {
	path := "/targets/" + id + "/semantic_config"
	issue := domain.Issue{Code: "semantic_invalid", FieldPath: path, RouteTargetID: id, Message: "目标语义不合法"}
	var de *domain.Error
	if errors.As(err, &de) && de != nil {
		if de.Code != "" {
			issue.Code = de.Code
		}
		if de.Message != "" {
			issue.Message = de.Message
		}
		if de.FieldPath != "" && de.FieldPath != "/" {
			issue.FieldPath = path + de.FieldPath
		}
	}
	return issue
}

func mapDeadline(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		var de *domain.Error
		if errors.As(err, &de) {
			return err
		}
		return fail(constant.CodePlanTimeout, "计划计算超时", "")
	}
	return err
}

func fail(code, message, path string) error {
	err, _ := domain.NewError(code, message, path, nil)
	return err
}
