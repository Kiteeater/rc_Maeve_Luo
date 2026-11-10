# 错误码体系与业务失败分类

错误码描述明确的业务失败原因，使调用方、运维查询和测试能够使用同一套稳定语义。
统一码表位于 [error_catalog.go](../internal/domain/error_catalog.go)，码值位于
[constant.go](../internal/constant/constant.go) 与 [error_reason.go](../internal/constant/error_reason.go)。
码表不依赖 HTTP、数据库或 MQ，也不决定某条通知能否再次发送。

## 1. 响应契约

```json
{
  "error": {
    "code": "idempotency_conflict",
    "reason_code": "event.source_id_content_conflict",
    "category": "event",
    "message": "相同来源事件 ID 已受理，但本次事件内容与原内容不一致",
    "field_path": "source_event_id",
    "request_id": "req-123"
  }
}
```

| 字段 | 职责 |
| --- | --- |
| `code` | 保留现有通用错误码和 HTTP 契约，供已有调用方处理错误大类 |
| `reason_code` | 精确到可独立判断和测试的失败条件；命名为 `<业务域>.<原因>`，已有单义通用码可直接沿用 |
| `category` | 按业务域或执行边界聚合，用于筛选和统计 |
| `message` | 安全且可读的业务说明；细分原因的文本由统一码表固定，客户端不解析消息文本做分支 |
| `field_path` | 可选，指出不满足契约的字段；不携带字段原始值 |
| `request_id` | 请求关联标识，便于关联日志；不是幂等依据 |
| `details` | 可选字段校验明细，保留现有 `code / message / field_path`，最多 100 条 |

`reason_code` 和 `category` 是向后兼容的新增字段。原 `code` 与 HTTP 状态不变；
已有通用码仍可携带安全的上下文说明，新细分码使用码表中的固定消息。
同一个通用码下的不同原因有独立标识，例如事件内容冲突与人工操作参数冲突均返回 409
`idempotency_conflict`，但分别使用 `event.source_id_content_conflict` 和
`operation.idempotency_parameters_conflict`。

## 2. 业务域与定位方式

- `request / authentication / authorization / resource`：请求格式、认证、授权和资源访问；
  参数问题由精确原因加 `field_path` 定位。
- `event / admission / configuration`：事件语义、积压准入、不可变配置及发布引用。
- `operation / tracking`：人工处置、核对证据、并发版本和分页查询。
- `dependency / delivery / internal`：依赖失败、内部执行协调和未预期故障。
- 投递诊断另按 `transport / provider_protocol / provider_rejection / provider_authentication /
  credential / channel / quota / recovery / budget / resend_safety` 分类。

## 3. 通用错误码

下表消息为码表默认说明。现有业务分支可以保留更具体且安全的消息；新场景优先登记细分原因。
`ownership_lost / precondition_changed` 仅用于内部执行协调，不作为公开 HTTP 错误直接返回，
若误传入公开错误处理器，会统一转成 `internal / 500`。

| code | category | HTTP | 默认业务说明 |
| --- | --- | --- | --- |
| `invalid_request` | `request` | 400 | 请求参数不合法 |
| `invalid_cursor` | `tracking` | 400 | 分页游标无效，请重新开始查询 |
| `unauthenticated` | `authentication` | 401 | 未认证 |
| `forbidden` | `authorization` | 403 | 无权执行此操作 |
| `not_found` | `resource` | 404 | 资源不存在 |
| `idempotency_conflict` | `operation` | 409 | 幂等标识对应的请求内容不一致 |
| `version_conflict` | `operation` | 409 | 资源版本已变化，请重新读取 |
| `state_conflict` | `operation` | 409 | 资源当前状态不允许此操作 |
| `payload_too_large` | `request` | 413 | 请求体超过上限 |
| `unsupported_media_type` | `request` | 415 | 仅接受 UTF-8 application/json |
| `rate_limited` | `admission` | 429 | 请求超过接入配额，请稍后重试 |
| `not_implemented` | `request` | 501 | 接口尚未实现 |
| `config_invalid` | `configuration` | 422 | 配置校验未通过，请查看字段明细 |
| `configuration_changed` | `configuration` | 409 | 引用配置已变化，请重新校验 |
| `configuration_unavailable` | `configuration` | 503 | 所需配置不可用 |
| `unsupported_schema` | `event` | 422 | 不支持的事件类型或数据版本 |
| `no_matching_route` | `event` | 422 | 没有匹配的路由 |
| `mapping_invalid` | `event` | 422 | 事件无法生成有效映射载荷，请查看字段明细 |
| `plan_timeout` | `event` | 503 | 分发计划计算超时，请沿用原事件标识重试 |
| `limit_exceeded` | `request` | 422 | 请求超过处理上限 |
| `dependency_unavailable` | `dependency` | 503 | 服务依赖暂不可用 |
| `event_data_invalid` | `event` | 422 | 事件数据不符合已发布的数据结构，请查看字段明细 |
| `fanout_exceeded` | `event` | 422 | 事件匹配的投递目标数超过上限 |
| `admission_paused` | `admission` | 503 | 事件受理暂时暂停，请稍后重试 |
| `acceptance_unconfirmed` | `event` | 503 | 受理结果尚未确认，请沿用原来源事件 ID 和原内容重试 |
| `ownership_lost` | `delivery` | 内部协作 | 当前任务领取权已失效 |
| `precondition_changed` | `delivery` | 内部协作 | 任务执行前置条件已变化 |
| `internal` | `internal` | 500 | 内部错误 |
| `unsafe_replay` | `operation` | 422 | 存在未决结果，供应商幂等无法覆盖；请先核对或完成风险授权 |
| `evidence_insufficient` | `operation` | 422 | 核对证据未覆盖全部未决尝试 |
| `evidence_invalid` | `operation` | 422 | 核对证据不合法 |
| `channel_blocked` | `operation` | 422 | 当前通道或访问约束阻止人工重投 |
| `operation_unconfirmed` | `operation` | 503 | 操作结果无法确认，请用原操作键和原参数重试 |
| `query_timeout` | `tracking` | 503 | 查询超时，请缩小查询范围后重试 |

`configuration_unavailable` 在配置读取、计划计算中默认返回 503；
人工重投和核对接口沿用 S07 契约，返回 422。相同原因的归属不因 HTTP 映射而变化。

## 4. 精确业务原因

每个细分原因都对应独立的失败条件；更改处理结论时新增原因码，不复用旧码表达另一种语义。

| reason_code | code | category | 固定业务说明 |
| --- | --- | --- | --- |
| `request.invalid_json` | `invalid_request` | `request` | 请求必须是完整且合法的 JSON 对象 |
| `request.body_unreadable` | `invalid_request` | `request` | 无法读取请求体 |
| `request.unknown_field` | `invalid_request` | `request` | 请求包含未声明的字段 |
| `request.unknown_query_parameter` | `invalid_request` | `request` | 请求包含不支持的查询参数 |
| `request.duplicate_query_parameter` | `invalid_request` | `request` | 同一查询参数不能重复提供 |
| `request.missing_field` | `invalid_request` | `request` | 缺少必填字段，或必填字段为 null |
| `request.invalid_field_type` | `invalid_request` | `request` | 字段类型不符合接口要求 |
| `request.invalid_field_value` | `invalid_request` | `request` | 字段值不符合接口约束，请检查对应字段 |
| `event.source_ambiguous` | `invalid_request` | `event` | 无法确定业务来源，请明确提供已授权的 source_business |
| `authorization.caller_scope_denied` | `forbidden` | `authorization` | 调用方不在当前身份授权范围内 |
| `authorization.source_denied` | `forbidden` | `authorization` | 业务来源不在当前身份授权范围内 |
| `event.source_id_content_conflict` | `idempotency_conflict` | `event` | 相同来源事件 ID 已受理，但本次事件内容与原内容不一致 |
| `event.backlog_count_exceeded` | `admission_paused` | `admission` | 待发布指令数量超过受理阈值，暂停接收新事件 |
| `event.backlog_age_exceeded` | `admission_paused` | `admission` | 最老待发布指令的等待时间超过受理阈值，暂停接收新事件 |
| `event.backlog_measurement_unavailable` | `dependency_unavailable` | `admission` | 无法取得有效的积压统计，暂停接收新事件 |
| `event.current_release_unavailable` | `dependency_unavailable` | `configuration` | 当前发布配置不可读取，暂时无法生成新事件的分发计划 |
| `event.store_unavailable` | `dependency_unavailable` | `dependency` | 事件存储暂不可用，请沿用原来源事件 ID 和原内容重试 |
| `event.duplicate_plan_target` | `mapping_invalid` | `event` | 分发计划包含重复的路由目标 |
| `event.plan_size_exceeded` | `limit_exceeded` | `event` | 固定分发计划的载荷与目标配置总大小超过上限 |
| `configuration.release_not_found` | `not_found` | `configuration` | 指定的配置发布不存在 |
| `configuration.release_already_published` | `state_conflict` | `configuration` | 该草稿已发布，不能作为新的发布操作再次发布 |
| `configuration.release_content_unavailable` | `configuration_unavailable` | `configuration` | 原配置版本内容不可读取或完整性校验未通过 |
| `configuration.current_release_missing` | `configuration_unavailable` | `configuration` | 当前没有可读取的已发布配置 |
| `configuration.reference_changed` | `configuration_changed` | `configuration` | 草稿引用的通道或凭证版本已变化，请重新校验 |
| `configuration.current_version_changed` | `version_conflict` | `configuration` | 当前发布版本与请求期望版本不一致，请重新读取 |
| `configuration.version_sequence_exhausted` | `limit_exceeded` | `configuration` | 配置发布版本序号已达到上限，需运维处理 |
| `operation.idempotency_parameters_conflict` | `idempotency_conflict` | `operation` | 相同操作幂等键对应的参数与原操作不一致 |
| `operation.idempotency_key_taken` | `idempotency_conflict` | `operation` | 操作幂等键已被占用，当前无法取得可重放的原操作结果 |
| `operation.task_not_retryable` | `state_conflict` | `operation` | 仅 FAILED 或 UNKNOWN 状态的任务允许人工重投 |
| `operation.task_not_unknown` | `state_conflict` | `operation` | 仅 UNKNOWN 状态的任务允许核对结案 |
| `operation.task_has_active_attempt` | `state_conflict` | `operation` | 任务仍有当前尝试或领取者，暂不能人工处置 |
| `operation.task_version_changed` | `version_conflict` | `operation` | 任务行版本与请求期望版本不一致，请重新读取 |
| `operation.original_adapter_unavailable` | `configuration_unavailable` | `operation` | 原任务需要的适配器版本不可用 |
| `operation.frozen_policy_unavailable` | `configuration_unavailable` | `operation` | 原任务冻结的目标配置或轮次策略不可读取 |
| `operation.current_policy_incompatible` | `configuration_unavailable` | `operation` | 当前允许的预算与等待上限无法满足原任务协议 |
| `operation.resolution_invalid` | `invalid_request` | `operation` | 核对结案结果仅允许 SUCCEEDED 或 FAILED |
| `operation.evidence_type_unsupported` | `evidence_invalid` | `operation` | 不支持的核对证据类型 |
| `operation.evidence_reference_invalid` | `evidence_invalid` | `operation` | 证据引用或摘要为空，或长度超过上限 |
| `operation.evidence_contains_secret` | `evidence_invalid` | `operation` | 证据引用或摘要包含禁止保存的凭证信息 |
| `operation.evidence_scope_invalid` | `evidence_invalid` | `operation` | 不支持的核对证据覆盖范围 |
| `operation.evidence_attempts_invalid` | `evidence_invalid` | `operation` | 证据必须明确选择尝试集合或业务幂等标识，且满足数量限制 |
| `operation.evidence_attempt_duplicate` | `evidence_invalid` | `operation` | 证据中的投递尝试 ID 不能重复 |
| `operation.evidence_business_key_mismatch` | `evidence_invalid` | `operation` | 证据中的业务幂等标识与当前任务不一致 |
| `operation.evidence_attempt_not_owned` | `evidence_invalid` | `operation` | 证据包含不属于当前任务的投递尝试 |

## 5. 异步投递诊断码

API 错误表示本次接入或管理请求未能完成；下列诊断码描述任务或某次投递发生了什么。
诊断码保存在任务、尝试或恢复记录中，不直接映射为 API 的 HTTP 状态。
任务查询新增 `last_error_category`，尝试查询新增 `error_category`，均由码表派生，不增加数据库列。
没有错误时分类返回 null；历史未登记的诊断码保留原码值并标记 `unknown`，不误分类。

| error_code | category | 业务说明 |
| --- | --- | --- |
| `HTTP_TIMEOUT` | `transport` | 供应商 HTTP 请求超时，是否已接收需结合尝试事实判断 |
| `CONNECTION_ERROR` | `transport` | 供应商网络连接失败，是否已接收需结合尝试事实判断 |
| `RESPONSE_READ_ERROR` | `transport` | 供应商响应读取失败，不能据此确认业务结果 |
| `RESPONSE_TOO_LARGE` | `provider_protocol` | 供应商响应超过读取上限 |
| `PROVIDER_RETRYABLE_REJECT` | `provider_rejection` | 供应商按协议明确拒绝，本次拒绝允许重试 |
| `PROVIDER_PERMANENT_REJECT` | `provider_rejection` | 供应商按协议明确永久拒绝 |
| `PROVIDER_AUTH_REJECTED` | `provider_authentication` | 供应商拒绝本次鉴权 |
| `PROTOCOL_INVALID_RESPONSE` | `provider_protocol` | 供应商响应不符合约定协议，无法确认接收结果 |
| `REDIRECT_REFUSED` | `provider_protocol` | 供应商返回重定向，受控客户端未跟随 |
| `AUTH_WAIT` | `credential` | 有效凭证暂不可用，等待后重新检查 |
| `CREDENTIAL_REVOKED` | `credential` | 供应商凭证已撤销 |
| `CREDENTIAL_REAUTH_REQUIRED` | `credential` | 供应商凭证需要重新授权 |
| `ACCOUNT_MISMATCH` | `channel` | 凭证或通道的账号归属与原任务不一致 |
| `RATE_LIMIT_WAIT` | `quota` | 通道额度不足，等待下一次可执行时间 |
| `RATE_LIMITER_UNAVAILABLE` | `dependency` | 共享限流器不可用，暂停该通道的新投递 |
| `CHANNEL_DISABLED` | `channel` | 当前通道已禁用 |
| `CALLER_FORBIDDEN` | `authorization` | 当前调用方无权使用原通道 |
| `TARGET_FORBIDDEN` | `channel` | 原任务目标已不在当前允许范围内 |
| `ADAPTER_UNAVAILABLE` | `configuration` | 原任务需要的适配器版本未注册 |
| `CONFIGURATION_UNAVAILABLE` | `configuration` | 原任务目标配置或通道信息不可读取 |
| `BUILD_FAILED` | `delivery` | 本次请求在构造阶段失败，明确未发送 |
| `LEASE_EXPIRED` | `recovery` | 任务领取过期，原尝试结果尚未确认 |
| `ATTEMPT_LIMIT_REACHED` | `budget` | 本轮投递尝试次数已达到上限 |
| `ROUND_DEADLINE_REACHED` | `budget` | 本轮执行或等待截止时间已到 |
| `UNSAFE_RESEND` | `resend_safety` | 未决结果缺少供应商幂等覆盖或有效风险授权，暂停自动重发 |
| `RECOVERY_INCONSISTENT` | `recovery` | 恢复时发现任务事实不一致，需人工核对 |
| `RECOVERY_MANUAL_REQUIRED` | `recovery` | 自动恢复无法安全继续，需人工处理 |
| `DLQ_INVALID_MESSAGE` | `recovery` | 死信消息格式或关联信息无效 |

网络超时在协议解释后仍保留 `HTTP_TIMEOUT`；响应过大与响应读取失败分别使用
`RESPONSE_TOO_LARGE / RESPONSE_READ_ERROR`；非法响应结构才使用 `PROTOCOL_INVALID_RESPONSE`。
共享限流器故障使用 `RATE_LIMITER_UNAVAILABLE`，额度耗尽使用 `RATE_LIMIT_WAIT`。
这些细分不改变原结果确定性：收到部分响应或请求超时都不能凭错误码推断“供应商明确未接收”。

## 6. 处置原则

| 场景 | 调用方或运维处置 |
| --- | --- |
| 缺字段、类型或值错误 | 根据 `reason_code / field_path / details` 修正输入 |
| 同来源事件 ID 内容冲突 | 核对原事件，禁止通过修改同一事实的原始内容覆盖已受理计划 |
| 操作键参数冲突 | 核对原操作；同一操作的重试沿用原键和原参数 |
| 配置引用或任务行版本变化 | 重新读取当前状态；重新校验后发起新的管理操作 |
| 积压保护或依赖不可用 | 按业务接口契约稍后重试，事件 ID 与操作幂等键保持稳定 |
| 受理或操作提交结果未确认 | 用原标识和原参数核对、重试，不能直接判定原操作失败 |
| 证据不完整或引用错误 | 补足当前任务的核对证据，不修改原尝试历史 |
| 凭证撤销、通道禁用或配置版本缺失 | 恢复所需运行条件后，由原任务状态机继续处理 |
| 结果不确定且无法安全重发 | 保留 UNKNOWN，核对供应商结果或执行有审计的风险授权 |
| 未登记错误或内部故障 | API 仅返回 `internal` 和请求关联标识，内部排障核对日志 |

错误码和 `retryable` 均不能单独决定自动投递。状态机继续综合任务级未决证据、
供应商幂等窗口、当前权限和通道约束、预算及截止时间。
`PROVIDER_RETRYABLE_REJECT` 仅说明本次拒绝按协议允许重试，不保证整个任务可以立即重发。

## 7. 分层、消息安全与演进

1. `constant` 保存稳定码值，`domain` 维护码表、分类和安全消息，`service` 映射 HTTP；
   `biz` 和仓储在确认失败条件的位置选择原因，不通过匹配错误文本反推分类。
2. `domain.NewError` 接受通用码或已登记的细分原因，规范化为原 `code` 并保存 `ReasonCode`。
   细分原因不可被调用方临时消息覆盖；字段校验明细仍按原契约保留。
3. 未登记错误、原因与通用码不匹配、公开接口收到内部错误时，统一返回 500
   `internal`，清除原始消息、字段路径和明细，保留 `request_id`。
4. 错误消息与明细不得包含 SQL、堆栈、Token、完整请求响应或未经允许的个人信息。
   其他调用方的不可见资源继续返回 404，不通过原因码暴露是否存在。
5. 新增业务失败先登记原因、分类和消息，再接入实际分支，补充行为测试并更新本文。
   已发布码值保持稳定，禁止把旧码重新分配给不同含义。
6. 当前精细化覆盖请求解析、事件受理与准入、配置发布、人工重投与核对证据、供应商执行诊断。
   字段级配置和 schema 校验继续使用已有 `details[].code`；后续细分其他场景时遵守同一登记规则。

## 8. 验证入口

```sh
go test ./unit_test/domain ./unit_test/service ./unit_test/biz/biz_event ./unit_test/biz/biz_operations ./unit_test/biz/biz_provider ./unit_test/biz/biz_tracking
make test
make vet
make race
```

验证包括同一通用码下的原因区分、字段定位、HTTP 兼容、内部消息隐藏、供应商错误细分、
不确定结果保留及任务查询中的诊断分类。数据库和 MQ 的真实行为仍由集成测试验证。
