package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/enums"
)

// Evidence 是 resolve 核对证据（S07 §7）。
type Evidence struct {
	Type              string   `json:"type"`
	Reference         string   `json:"reference"`
	Summary           string   `json:"summary"`
	CoveredAttemptIDs []uint64 `json:"covered_attempt_ids,omitempty"`
	IdempotencyKey    string   `json:"idempotency_key,omitempty"`
	Scope             string   `json:"scope"`
}

const (
	EvidenceScopeAttempts = "attempts"
	EvidenceScopeBusiness = "business_operation"
)

// OperationFingerprint 覆盖目标任务、操作类型、期望版本、原因、风险确认与证据，按规范 JSON 计算 SHA-256（版本 1）。
func OperationFingerprint(opType string, deliveryID uint64, expectedRowVersion uint32, reason string, riskAck bool, resolution string, ev *Evidence) (string, error) {
	body := map[string]any{
		"operation_type": opType, "delivery_id": strconv.FormatUint(deliveryID, 10),
		"expected_row_version": expectedRowVersion, "reason": reason, "risk_acknowledged": riskAck,
		"resolution": resolution, "fingerprint_version": constant.OperationFingerprintV1,
	}
	if ev != nil {
		ids := make([]string, len(ev.CoveredAttemptIDs))
		for i, id := range ev.CoveredAttemptIDs {
			ids[i] = strconv.FormatUint(id, 10)
		}
		body["evidence"] = map[string]any{"type": ev.Type, "reference": ev.Reference, "summary": ev.Summary,
			"covered_attempt_ids": ids, "idempotency_key": ev.IdempotencyKey, "scope": ev.Scope}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	v, err := canonicaljson.Parse(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonicaljson.AppendCanonical(nil, v))
	return hex.EncodeToString(sum[:]), nil
}

// RetryCheck 是 OPS-R01/R02 前置检查输入。
type RetryCheck struct {
	Task           TaskFacts
	Now            time.Time
	ExpectedRow    uint32
	Constraint     string // S05 无副作用检查的当前约束码
	Covers         bool   // 供应商幂等覆盖全部未决
	RiskPermission bool
	RiskAck        bool
}

// CheckRetry 返回是否需要风险授权，或标准错误码（无副作用）。
func CheckRetry(c RetryCheck) (risk bool, code string) {
	t := c.Task
	switch t.Status {
	case enums.TaskFailed, enums.TaskUnknown:
	default:
		return false, constant.CodeStateConflict
	}
	if t.CurrentAttemptID != nil || t.LeaseToken != "" {
		return false, constant.CodeStateConflict
	}
	if t.RowVersion != c.ExpectedRow {
		return false, constant.CodeVersionConflict
	}
	if c.Constraint != "" {
		if c.Constraint == constant.ErrConfigurationUnavail || c.Constraint == constant.ErrAdapterUnavailable {
			return false, constant.CodeConfigurationUnavailable
		}
		return false, constant.CodeChannelBlocked
	}
	if len(t.Unresolved) == 0 || c.Covers {
		return false, ""
	}
	if c.RiskPermission && c.RiskAck {
		return true, ""
	}
	return false, constant.CodeUnsafeReplay
}

// NarrowPolicy 取原冻结策略与当前允许上限的交集（OPS-13）。退避与强制等待保持原含义；无法满足则拒绝。
func NarrowPolicy(orig FrozenPolicy, maxAttempts uint32, maxTimeout time.Duration) (FrozenPolicy, bool, error) {
	out := orig
	changed := false
	if maxAttempts > 0 && out.MaxAttempts > maxAttempts {
		out.MaxAttempts, changed = maxAttempts, true
	}
	if maxTimeout > 0 && out.RoundTimeout > maxTimeout {
		out.RoundTimeout, changed = maxTimeout, true
	}
	if out.MaxAttempts == 0 || out.RoundTimeout <= out.BackoffInitial {
		return FrozenPolicy{}, false, codeErr(constant.CodeConfigurationUnavailable, "当前允许策略无法满足原协议", "round_policy")
	}
	return out, changed, nil
}

// EncodeFrozenPolicy 输出与 ParseFrozenPolicy 对应的规范 JSON。
func EncodeFrozenPolicy(p FrozenPolicy) []byte {
	raw, _ := json.Marshal(map[string]int64{
		"backoff_cap_ms": p.BackoffCap.Milliseconds(), "backoff_initial_ms": p.BackoffInitial.Milliseconds(),
		"max_attempts": int64(p.MaxAttempts), "round_timeout_seconds": int64(p.RoundTimeout / time.Second),
		"server_wait_cap_seconds": int64(p.ServerWaitCap / time.Second),
	})
	v, _ := canonicaljson.Parse(raw)
	return canonicaljson.AppendCanonical(nil, v)
}

// AttemptRef 是核对证据校验用的尝试归属。
type AttemptRef struct {
	AttemptID uint64
	RoundNo   uint32
}

// ValidateEvidenceShape 检查证据格式（OPS-12 前半）。
func ValidateEvidenceShape(ev Evidence, maxAttempts int) string {
	switch ev.Type {
	case "provider_confirmation", "provider_query", "manual_verification":
	default:
		return constant.CodeEvidenceInvalid
	}
	if ev.Reference == "" || len(ev.Reference) > 191 || strings.TrimSpace(ev.Summary) == "" || len(ev.Summary) > 2048 {
		return constant.CodeEvidenceInvalid
	}
	if LooksSecret(ev.Reference) || LooksSecret(ev.Summary) {
		return constant.CodeEvidenceInvalid
	}
	switch ev.Scope {
	case EvidenceScopeAttempts:
		if len(ev.CoveredAttemptIDs) == 0 || ev.IdempotencyKey != "" || len(ev.CoveredAttemptIDs) > maxAttempts {
			return constant.CodeEvidenceInvalid
		}
		seen := map[uint64]bool{}
		for _, id := range ev.CoveredAttemptIDs {
			if seen[id] {
				return constant.CodeEvidenceInvalid
			}
			seen[id] = true
		}
	case EvidenceScopeBusiness:
		if ev.IdempotencyKey == "" || len(ev.CoveredAttemptIDs) > 0 {
			return constant.CodeEvidenceInvalid
		}
	default:
		return constant.CodeEvidenceInvalid
	}
	return ""
}

// CheckResolve 在锁定任务后重新计算待覆盖集合（OPS-V01、§8）。attempts 为该任务全部历史尝试。
func CheckResolve(t TaskFacts, idempotencyKey string, attempts []AttemptRef, expectedRow uint32, resolution string, ev Evidence) string {
	if t.Status != enums.TaskUnknown || t.CurrentAttemptID != nil || t.LeaseToken != "" {
		return constant.CodeStateConflict
	}
	if t.RowVersion != expectedRow {
		return constant.CodeVersionConflict
	}
	if resolution != string(enums.TaskSucceeded) && resolution != string(enums.TaskFailed) {
		return constant.CodeInvalidRequest
	}
	if ev.Scope == EvidenceScopeBusiness {
		if ev.IdempotencyKey != idempotencyKey {
			return constant.CodeEvidenceInvalid
		}
		return ""
	}
	owned := map[uint64]bool{}
	for _, a := range attempts {
		owned[a.AttemptID] = true
	}
	covered := map[uint64]bool{}
	for _, id := range ev.CoveredAttemptIDs {
		if !owned[id] {
			return constant.CodeEvidenceInvalid
		}
		covered[id] = true
	}
	for _, u := range t.Unresolved {
		if !covered[u.AttemptID] {
			return constant.CodeEvidenceInsufficient
		}
	}
	return ""
}

// LooksSecret 拒绝明显的访问凭证或秘密字段，不静默裁剪。
func LooksSecret(s string) bool {
	l := strings.ToLower(s)
	for _, marker := range []string{"bearer ", "authorization:", "access_token", "refresh_token", "password=", "secret=", "api_key", "apikey"} {
		if strings.Contains(l, marker) {
			return true
		}
	}
	return false
}
