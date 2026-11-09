-- +goose Up
-- 将 00001 草案迁移校准为开发规格 v0.2 的首版持久化契约。
--
-- 依据：
--   S00 第 5.1 / 6 / 9 / 10 节，S01 第 6 节，S02 第 7 节，
--   S03 第 2 / 7 节，S04 第 7 节，S05 第 8 节，S07 第 7 节，S08 第 10 节。
--
-- 前提：00001 是尚未实现的草案骨架，业务代码未落地，相关表为空。
-- 若 00001 已在有数据的环境执行，需先按各注释回填存量数据再执行本迁移；
-- 不得通过清库绕过回填。

-- =====================================================================
-- business_events：规范化算法与精确 JSON 字节（S00 第 10 节、S02 第 7 节）
-- =====================================================================
ALTER TABLE business_events
    ADD COLUMN fingerprint_version INT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '指纹算法版本；编码器升级不得静默改变重复提交判断' AFTER fingerprint,
    ADD COLUMN data_canonical MEDIUMBLOB NOT NULL
        COMMENT '规范化 JSON 字节，指纹/映射/请求构造的事实来源；data_snapshot 保留为查询投影' AFTER data_snapshot;

-- =====================================================================
-- delivery_tasks：固定语义、轮次策略、阻塞恢复、风险授权与历史序号
-- =====================================================================
ALTER TABLE delivery_tasks
    ADD COLUMN adapter_type VARCHAR(64) NOT NULL
        COMMENT '固定适配器类型；原任务引用的版本缺失时阻塞，不换最新版本' AFTER operation,
    ADD COLUMN mapping_id VARCHAR(128) NOT NULL
        COMMENT '固定映射引用 ID（S01 映射字典键）' AFTER adapter_type,
    ADD COLUMN fixed_target JSON NOT NULL
        COMMENT '冻结的通道/供应商/账号/操作/适配器/凭证引用与语义目标配置' AFTER mapping_id,
    ADD COLUMN round_policy JSON NOT NULL
        COMMENT '本轮冻结的预算与退避策略（S01 policy）；执行不读取当前默认值' AFTER fixed_target,
    ADD COLUMN payload_canonical MEDIUMBLOB NOT NULL
        COMMENT '精确映射载荷字节；历史重试载荷不变化' AFTER payload,
    ADD COLUMN resume_status VARCHAR(16) NULL
        COMMENT '阻塞恢复类别 PENDING | RETRY_WAIT（S03 DLV-S02）' AFTER blocked_reason,
    ADD COLUMN resume_not_before DATETIME(6) NULL
        COMMENT '阻塞解除后最早允许发送时间' AFTER resume_status,
    ADD COLUMN blocked_since DATETIME(6) NULL
        COMMENT '进入 BLOCKED 的时间；阻塞时长不被重复检查重置' AFTER resume_not_before,
    ADD COLUMN history_seq INT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '任务行上的历史序号分配器：锁定任务后递增写入 delivery_history' AFTER last_result_certainty,
    ADD COLUMN risk_operation_id VARCHAR(64) NULL
        COMMENT '本轮风险重投的一次性授权操作引用（S07 OPS-R03）' AFTER history_seq,
    ADD COLUMN risk_authorized_attempt_id BIGINT UNSIGNED NULL
        COMMENT '风险授权已关联使用的首次尝试；不可跨轮次或重复复用（S03 DLV-E03）' AFTER risk_operation_id,
    MODIFY COLUMN schedule_version INT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '调度版本，初始为 1；建立替代计划或作废当前指令时递增',
    MODIFY COLUMN row_version INT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '行版本，初始为 1；任务业务状态/计划/预算/所有权变化时递增',
    MODIFY COLUMN result_certainty VARCHAR(16) NOT NULL DEFAULT 'NOT_RECEIVED'
        COMMENT '任务级确定性 RECEIVED | NOT_RECEIVED | UNCERTAIN；初始无尝试为 NOT_RECEIVED',
    MODIFY COLUMN round_deadline_at DATETIME(6) NOT NULL
        COMMENT '本轮截止时间；所有新任务必须非空';

-- =====================================================================
-- delivery_attempts：结果枚举长度、证据覆盖与摘要截断标记（S00 第 5.1 / 6 / 9 节）
-- =====================================================================
ALTER TABLE delivery_attempts
    ADD COLUMN request_summary_truncated BOOLEAN NOT NULL DEFAULT FALSE
        COMMENT '请求摘要按字节截断标记' AFTER request_summary,
    ADD COLUMN response_summary_truncated BOOLEAN NOT NULL DEFAULT FALSE
        COMMENT '响应摘要按字节截断标记；response_truncated 表示响应体读取截断' AFTER response_summary,
    ADD COLUMN error_summary_truncated BOOLEAN NOT NULL DEFAULT FALSE
        COMMENT '错误摘要按字节截断标记' AFTER error_reason,
    ADD COLUMN evidence_scope JSON NULL
        COMMENT '证据覆盖范围：未决尝试/操作/幂等键及其范围（S00 第 5.2）' AFTER provider_wait_hint,
    MODIFY COLUMN outcome VARCHAR(32) NULL
        COMMENT 'RECEIVED | RETRYABLE_REJECT | PERMANENT_REJECT | LOCAL_FAILURE | UNCERTAIN；VARCHAR(16) 容不下 RETRYABLE_REJECT',
    MODIFY COLUMN send_state VARCHAR(16) NULL
        COMMENT 'NOT_SENT | HTTP_INVOKED | UNCERTAIN；原 SENT 草案值校准为 HTTP_INVOKED',
    MODIFY COLUMN error_reason VARCHAR(2048) NULL
        COMMENT '脱敏错误摘要；容量覆盖允许的最大字节上限';

-- =====================================================================
-- outbox_messages：发布状态、领取代次、补发与发布索引（S04 第 7 节）
-- =====================================================================
ALTER TABLE outbox_messages
    CHANGE COLUMN published_at last_published_at DATETIME(6) NULL
        COMMENT '最近一次 Broker send 明确成功时间',
    ADD COLUMN publish_revision INT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '发布修订：每次领取/恢复发布资格/同消息再发布时递增；与 token 共同校验' AFTER publish_count,
    ADD COLUMN last_publish_started_at DATETIME(6) NULL
        COMMENT '最近一次发布尝试开始时间' AFTER last_published_at,
    ADD COLUMN repair_count INT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '当前调度版本补发次数，初始 0；S06 冷却与上限依据' AFTER last_publish_started_at,
    ADD COLUMN last_repair_at DATETIME(6) NULL
        COMMENT '最近一次补发时间' AFTER repair_count,
    ADD COLUMN next_repair_at DATETIME(6) NULL
        COMMENT '允许再次补发的最早时间' AFTER last_repair_at,
    MODIFY COLUMN publish_status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
        COMMENT 'PENDING | PUBLISHING | PUBLISHED | FAILED | CANCELLED',
    ADD KEY idx_outbox_publish_next (publish_status, next_publish_at, outbox_id),
    ADD KEY idx_outbox_publish_lease (publish_status, lease_expires_at, outbox_id);

-- =====================================================================
-- channels：操作、行版本、启停默认与唯一键（S05 第 8 节）
-- =====================================================================
ALTER TABLE channels
    ADD COLUMN operation VARCHAR(64) NOT NULL
        COMMENT '供应商业务操作；同账号不同操作必须独立通道，不能被唯一约束合并' AFTER provider_account_id,
    ADD COLUMN row_version INT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '通道行版本，初始 1；每次实际变更加 1，供 CAS 更新' AFTER updated_at,
    MODIFY COLUMN enabled BOOLEAN NOT NULL DEFAULT FALSE
        COMMENT '完成目标许可/凭证/额度配置后显式启用；创建默认 false',
    DROP INDEX uq_channel_account,
    ADD UNIQUE KEY uq_channel_account (provider_id, provider_account_id, operation, adapter_type);

-- =====================================================================
-- management_events：指纹版本与风险/核对证据（S00 第 10 节、S07 第 7 节）
-- =====================================================================
ALTER TABLE management_events
    ADD COLUMN fingerprint_version INT UNSIGNED NULL
        COMMENT '请求指纹算法版本（S02 同类确定性规范化）' AFTER request_fingerprint,
    ADD COLUMN evidence JSON NULL
        COMMENT '风险确认与核对证据摘要；脱敏，不含原始响应/Token/个人信息' AFTER reason;

-- +goose Down
ALTER TABLE management_events
    DROP COLUMN evidence,
    DROP COLUMN fingerprint_version;

ALTER TABLE channels
    DROP INDEX uq_channel_account,
    MODIFY COLUMN enabled BOOLEAN NOT NULL DEFAULT TRUE,
    DROP COLUMN row_version,
    DROP COLUMN operation,
    ADD UNIQUE KEY uq_channel_account (provider_id, provider_account_id, adapter_type);

ALTER TABLE outbox_messages
    DROP INDEX idx_outbox_publish_lease,
    DROP INDEX idx_outbox_publish_next,
    MODIFY COLUMN publish_status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
        COMMENT 'PENDING | PUBLISHED | FAILED',
    DROP COLUMN next_repair_at,
    DROP COLUMN last_repair_at,
    DROP COLUMN repair_count,
    DROP COLUMN last_publish_started_at,
    DROP COLUMN publish_revision,
    CHANGE COLUMN last_published_at published_at DATETIME(6) NULL;

ALTER TABLE delivery_attempts
    DROP COLUMN evidence_scope,
    DROP COLUMN error_summary_truncated,
    DROP COLUMN response_summary_truncated,
    DROP COLUMN request_summary_truncated,
    MODIFY COLUMN error_reason VARCHAR(1024) NULL,
    MODIFY COLUMN send_state VARCHAR(16) NULL
        COMMENT 'NOT_SENT | SENT | UNCERTAIN',
    MODIFY COLUMN outcome VARCHAR(16) NULL
        COMMENT 'RECEIVED | RETRYABLE_REJECT | PERMANENT_REJECT | LOCAL_FAILURE | UNCERTAIN';

ALTER TABLE delivery_tasks
    MODIFY COLUMN round_deadline_at DATETIME(6) NULL,
    MODIFY COLUMN result_certainty VARCHAR(16) NULL,
    MODIFY COLUMN row_version INT UNSIGNED NOT NULL DEFAULT 0,
    MODIFY COLUMN schedule_version INT UNSIGNED NOT NULL DEFAULT 0,
    DROP COLUMN risk_authorized_attempt_id,
    DROP COLUMN risk_operation_id,
    DROP COLUMN history_seq,
    DROP COLUMN blocked_since,
    DROP COLUMN resume_not_before,
    DROP COLUMN resume_status,
    DROP COLUMN payload_canonical,
    DROP COLUMN round_policy,
    DROP COLUMN fixed_target,
    DROP COLUMN mapping_id,
    DROP COLUMN adapter_type;

ALTER TABLE business_events
    DROP COLUMN data_canonical,
    DROP COLUMN fingerprint_version;
