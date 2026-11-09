// Package constant 存放跨模块共享、且没有行为的常量。
package constant

// 权限码。ops:read 不隐含写权限；写操作还要检查 caller_scopes。
const (
	PermEventsWrite         = "events:write"
	PermDataRead            = "data:read"
	PermOpsRead             = "ops:read"
	PermConfigPublish       = "config:publish"
	PermChannelsManage      = "channels:manage"
	PermDeliveriesRetry     = "deliveries:retry"
	PermDeliveriesResolve   = "deliveries:resolve"
	PermDeliveriesRiskRetry = "deliveries:risk-retry"
	PermDLQRepair           = "dlq:repair"
)

// Permissions 是首版全部权限码，顺序稳定，供配置校验使用。
var Permissions = []string{
	PermEventsWrite,
	PermDataRead,
	PermOpsRead,
	PermConfigPublish,
	PermChannelsManage,
	PermDeliveriesRetry,
	PermDeliveriesResolve,
	PermDeliveriesRiskRetry,
	PermDLQRepair,
}

// KnownPermission 判断权限码是否属于首版集合。
func KnownPermission(code string) bool {
	for _, item := range Permissions {
		if item == code {
			return true
		}
	}
	return false
}

const (
	// FingerprintVersion 是首版规范化算法版本。升级算法时递增，不能静默替换已保存的版本。
	FingerprintVersion = 1

	// JSON 结构上限。深度包含 envelope；数字按原始字面量字节计。
	MaxJSONDepth           = 32
	MaxJSONNumberLiteral   = 128
	MaxJSONCoefficientBits = 64 // 十进制系数位数，不是二进制位宽
	MaxJSONExponent        = 308

	// MaxErrorDetails 是 422 明细条数上限。明细不含字段原始值。
	MaxErrorDetails = 100

	// MaxCallerScopes 是单个身份可声明的调用方范围上限。
	MaxCallerScopes = 100

	// Summary 字节上限的配置边界。截断按 UTF-8 字节进行，并另存截断标记。
	SummaryBytesMin = 256
	SummaryBytesMax = 8192

	// GlobalCallerScope 是配置发布等全局管理操作的调用方范围。
	GlobalCallerScope = "__global__"

	// 配置发布操作类型。与 S07 的管理操作共用 management_events。
	OpConfigCreate  = "config_create"
	OpConfigPublish = "config_publish"
)

// 领域错误码。HTTP 映射在 service。未知码不得当成成功。
const (
	CodeInvalidRequest           = "invalid_request"
	CodeForbidden                = "forbidden"
	CodeNotFound                 = "not_found"
	CodeIdempotencyConflict      = "idempotency_conflict"
	CodeVersionConflict          = "version_conflict"
	CodeStateConflict            = "state_conflict"
	CodeConfigInvalid            = "config_invalid"
	CodeConfigurationChanged     = "configuration_changed"
	CodeConfigurationUnavailable = "configuration_unavailable"
	CodeUnsupportedSchema        = "unsupported_schema"
	CodeNoMatchingRoute          = "no_matching_route"
	CodeMappingInvalid           = "mapping_invalid"
	CodePlanTimeout              = "plan_timeout"
	CodeLimitExceeded            = "limit_exceeded"
	CodeDependencyUnavailable    = "dependency_unavailable"
	CodePayloadTooLarge          = "payload_too_large"
	CodeEventDataInvalid         = "event_data_invalid"
	CodeFanoutExceeded           = "fanout_exceeded"
	CodeAdmissionPaused          = "admission_paused"
	CodeAcceptanceUnconfirmed    = "acceptance_unconfirmed"
	CodeOwnershipLost            = "ownership_lost"
	CodePreconditionChanged      = "precondition_changed"
	CodeInternal                 = "internal"
)

// 投递诊断码。与任务状态分开保存，最近错误不替代任务级确定性。
const (
	ErrHTTPTimeout          = "HTTP_TIMEOUT"
	ErrConnection           = "CONNECTION_ERROR"
	ErrResponseRead         = "RESPONSE_READ_ERROR"
	ErrResponseTooLarge     = "RESPONSE_TOO_LARGE"
	ErrProviderRetryable    = "PROVIDER_RETRYABLE_REJECT"
	ErrProviderPermanent    = "PROVIDER_PERMANENT_REJECT"
	ErrAuthWait             = "AUTH_WAIT"
	ErrRateLimitWait        = "RATE_LIMIT_WAIT"
	ErrChannelDisabled      = "CHANNEL_DISABLED"
	ErrCallerForbidden      = "CALLER_FORBIDDEN"
	ErrAdapterUnavailable   = "ADAPTER_UNAVAILABLE"
	ErrConfigurationUnavail = "CONFIGURATION_UNAVAILABLE"
	ErrTargetForbidden      = "TARGET_FORBIDDEN"
	ErrBuildFailed          = "BUILD_FAILED"
	ErrLeaseExpired         = "LEASE_EXPIRED"
	ErrAttemptLimit         = "ATTEMPT_LIMIT_REACHED"
	ErrRoundDeadline        = "ROUND_DEADLINE_REACHED"
	ErrUnsafeResend         = "UNSAFE_RESEND"
	ErrProtocolInvalid      = "PROTOCOL_INVALID_RESPONSE"
	ErrAuthRejected         = "PROVIDER_AUTH_REJECTED"
	ErrRedirectRefused      = "REDIRECT_REFUSED"
	ErrCredentialRevoked    = "CREDENTIAL_REVOKED"
	ErrCredentialReauth     = "CREDENTIAL_REAUTH_REQUIRED"
	ErrAccountMismatch      = "ACCOUNT_MISMATCH"
	ErrRateLimiterDown      = "RATE_LIMITER_UNAVAILABLE"
)
