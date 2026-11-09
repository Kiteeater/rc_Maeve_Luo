-- +goose Up
-- S00 第 10 节和第 12 节里 00002 尚未覆盖的列、枚举宽度和摘要容量。
-- 不改写 00001/00002。凭证轮换表留给需要 refresh token 的供应商（S05）。
-- 枚举列至少 VARCHAR(32)。摘要改为 TEXT，才能放下 8192 字节上限；截断标记仍是独立布尔列。

UPDATE delivery_attempts
    SET send_state = 'HTTP_INVOKED'
    WHERE send_state = 'SENT';

ALTER TABLE routing_releases
    MODIFY COLUMN status VARCHAR(32) NOT NULL DEFAULT 'draft'
        COMMENT 'draft | published | superseded',
    ADD COLUMN content_canonical MEDIUMBLOB NULL
        COMMENT '发布内容的规范化 JSON 字节；S01 写入，空库迁移时允许为空' AFTER content,
    ADD COLUMN content_sha256 CHAR(64) NULL
        COMMENT 'content_canonical 的 SHA-256' AFTER content_canonical,
    ADD COLUMN fingerprint_version INT UNSIGNED NULL
        COMMENT '发布内容指纹算法版本' AFTER content_sha256;

ALTER TABLE delivery_tasks
    MODIFY COLUMN status VARCHAR(32) NOT NULL DEFAULT 'PENDING'
        COMMENT 'PENDING | PROCESSING | RETRY_WAIT | BLOCKED | SUCCEEDED | FAILED | UNKNOWN',
    MODIFY COLUMN result_certainty VARCHAR(32) NOT NULL DEFAULT 'NOT_RECEIVED'
        COMMENT 'RECEIVED | NOT_RECEIVED | UNCERTAIN',
    MODIFY COLUMN last_result_certainty VARCHAR(32) NULL
        COMMENT '最近一次尝试的确定性',
    MODIFY COLUMN resume_status VARCHAR(32) NULL
        COMMENT '阻塞恢复类别 PENDING | RETRY_WAIT',
    MODIFY COLUMN last_error_reason TEXT NULL
        COMMENT '脱敏错误摘要，容量覆盖 8192 字节上限',
    ADD COLUMN last_error_truncated BOOLEAN NOT NULL DEFAULT FALSE
        COMMENT '任务级错误摘要截断标记' AFTER last_error_reason,
    ADD COLUMN unresolved_evidence JSON NULL
        COMMENT '任务级未决证据关联；不能只靠最近一次结果推断' AFTER last_result_certainty;

ALTER TABLE delivery_attempts
    MODIFY COLUMN status VARCHAR(32) NOT NULL DEFAULT 'PROCESSING'
        COMMENT 'PROCESSING | FINISHED',
    MODIFY COLUMN send_state VARCHAR(32) NULL
        COMMENT 'NOT_SENT | HTTP_INVOKED | UNCERTAIN',
    MODIFY COLUMN result_certainty VARCHAR(32) NULL
        COMMENT 'RECEIVED | NOT_RECEIVED | UNCERTAIN',
    MODIFY COLUMN request_summary TEXT NULL
        COMMENT '脱敏请求摘要，容量覆盖 8192 字节上限',
    MODIFY COLUMN response_summary TEXT NULL
        COMMENT '脱敏响应摘要，容量覆盖 8192 字节上限',
    MODIFY COLUMN error_reason TEXT NULL
        COMMENT '脱敏错误摘要，容量覆盖 8192 字节上限',
    ADD COLUMN diagnostic_code VARCHAR(64) NULL
        COMMENT '可对外的诊断码，不含原始响应或凭证' AFTER error_code;

ALTER TABLE delivery_history
    MODIFY COLUMN from_status VARCHAR(32) NULL,
    MODIFY COLUMN to_status VARCHAR(32) NULL,
    MODIFY COLUMN reason TEXT NULL
        COMMENT '脱敏原因，容量覆盖摘要上限';

ALTER TABLE outbox_messages
    MODIFY COLUMN message_type VARCHAR(32) NOT NULL
        COMMENT 'immediate | delayed',
    MODIFY COLUMN publish_status VARCHAR(32) NOT NULL DEFAULT 'PENDING'
        COMMENT 'PENDING | PUBLISHING | PUBLISHED | FAILED | CANCELLED',
    MODIFY COLUMN last_error TEXT NULL
        COMMENT '脱敏发布错误摘要';

ALTER TABLE channels
    MODIFY COLUMN access_constraints JSON NULL
        COMMENT 'S05 目标许可与网络访问约束；本迁移只保留列并标明归属',
    MODIFY COLUMN runtime_policy JSON NULL
        COMMENT 'S05 限流与运行策略；本迁移只保留列并标明归属';

ALTER TABLE management_events
    ADD COLUMN expected_row_version INT UNSIGNED NULL
        COMMENT '操作时期望的任务行版本；保留 expected_version 兼容旧草案' AFTER expected_version,
    ADD COLUMN expected_schedule_version INT UNSIGNED NULL
        COMMENT '操作时期望的调度版本' AFTER expected_row_version;

-- +goose Down
ALTER TABLE management_events
    DROP COLUMN expected_schedule_version,
    DROP COLUMN expected_row_version;

ALTER TABLE channels
    MODIFY COLUMN access_constraints JSON NULL,
    MODIFY COLUMN runtime_policy JSON NULL;

ALTER TABLE outbox_messages
    MODIFY COLUMN last_error VARCHAR(512) NULL,
    MODIFY COLUMN publish_status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
        COMMENT 'PENDING | PUBLISHING | PUBLISHED | FAILED | CANCELLED',
    MODIFY COLUMN message_type VARCHAR(16) NOT NULL
        COMMENT 'immediate | delayed';

ALTER TABLE delivery_history
    MODIFY COLUMN reason VARCHAR(1024) NULL,
    MODIFY COLUMN to_status VARCHAR(16) NULL,
    MODIFY COLUMN from_status VARCHAR(16) NULL;

ALTER TABLE delivery_attempts
    DROP COLUMN diagnostic_code,
    MODIFY COLUMN error_reason VARCHAR(2048) NULL
        COMMENT '脱敏错误摘要；容量覆盖允许的最大字节上限',
    MODIFY COLUMN response_summary VARCHAR(2048) NULL,
    MODIFY COLUMN request_summary VARCHAR(2048) NULL,
    MODIFY COLUMN result_certainty VARCHAR(16) NULL,
    MODIFY COLUMN send_state VARCHAR(32) NULL;

UPDATE delivery_attempts
    SET send_state = 'SENT'
    WHERE send_state = 'HTTP_INVOKED';

ALTER TABLE delivery_attempts
    MODIFY COLUMN send_state VARCHAR(16) NULL
        COMMENT 'NOT_SENT | SENT | UNCERTAIN',
    MODIFY COLUMN status VARCHAR(16) NOT NULL DEFAULT 'PROCESSING'
        COMMENT 'PROCESSING | FINISHED';

ALTER TABLE delivery_tasks
    DROP COLUMN unresolved_evidence,
    DROP COLUMN last_error_truncated,
    MODIFY COLUMN last_error_reason VARCHAR(1024) NULL,
    MODIFY COLUMN resume_status VARCHAR(16) NULL
        COMMENT '阻塞恢复类别 PENDING | RETRY_WAIT（S03 DLV-S02）',
    MODIFY COLUMN last_result_certainty VARCHAR(16) NULL,
    MODIFY COLUMN result_certainty VARCHAR(16) NOT NULL DEFAULT 'NOT_RECEIVED'
        COMMENT '任务级确定性 RECEIVED | NOT_RECEIVED | UNCERTAIN；初始无尝试为 NOT_RECEIVED',
    MODIFY COLUMN status VARCHAR(16) NOT NULL DEFAULT 'PENDING';

ALTER TABLE routing_releases
    DROP COLUMN fingerprint_version,
    DROP COLUMN content_sha256,
    DROP COLUMN content_canonical,
    MODIFY COLUMN status VARCHAR(16) NOT NULL DEFAULT 'draft'
        COMMENT 'draft | published | superseded';
