-- +goose Up
-- 初始数据模型：事件、投递任务、Outbox、尝试、时间线、配置发布、通道与管理操作。
-- 依据 TECH_DESIGN.md 第 11 节；状态与错误码取值、详细字段以评审后的契约为准。
-- 约定：内部主键为 BIGINT UNSIGNED 自增；业务/外部标识为字符串；
--       所有持久化时间使用 UTC DATETIME(6)；JSON 用于快照与摘要。

-- 路由发布快照（不可变；另维护当前发布指针）
CREATE TABLE routing_releases (
    release_id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    version_no   INT UNSIGNED    NOT NULL,                 -- 业务版本号
    content      JSON            NOT NULL,                 -- 路由规则、映射及语义相关配置
    status       VARCHAR(16)     NOT NULL DEFAULT 'draft', -- draft | published | superseded
    published_at DATETIME(6)     NULL,
    created_at   DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (release_id),
    UNIQUE KEY uq_release_version (version_no),
    KEY idx_release_status (status, published_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 当前发布指针（单行）
CREATE TABLE routing_current (
    singleton  TINYINT         NOT NULL DEFAULT 1,
    release_id BIGINT UNSIGNED NOT NULL,
    updated_at DATETIME(6)     NOT NULL,
    PRIMARY KEY (singleton),
    KEY idx_current_release (release_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 通道：供应商账号与凭证引用、当前启停与访问约束、运行策略
CREATE TABLE channels (
    channel_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    provider_id         VARCHAR(64)     NOT NULL,
    provider_account_id VARCHAR(64)     NOT NULL,
    credential_ref      VARCHAR(191)    NOT NULL,          -- 凭证引用，指向凭证存储
    adapter_type        VARCHAR(64)     NOT NULL,
    adapter_version     VARCHAR(32)     NOT NULL,
    enabled             BOOLEAN         NOT NULL DEFAULT TRUE,
    access_constraints  JSON            NULL,
    runtime_policy      JSON            NULL,
    created_at          DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (channel_id),
    UNIQUE KEY uq_channel_account (provider_id, provider_account_id, adapter_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 业务事件
CREATE TABLE business_events (
    event_id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    caller_id             VARCHAR(64)     NOT NULL,
    source_business       VARCHAR(64)     NOT NULL,
    source_event_id       VARCHAR(191)    NOT NULL,        -- 调用方范围内唯一，区分大小写
    event_type            VARCHAR(128)    NOT NULL,
    schema_version        VARCHAR(32)     NOT NULL,
    occurred_at           DATETIME(6)     NOT NULL,        -- 事件发生时间（业务事实）
    business_object_type  VARCHAR(64)     NOT NULL,
    business_object_id    VARCHAR(191)    NOT NULL,
    data_snapshot         JSON            NOT NULL,        -- 发生时的数据快照
    fingerprint           CHAR(64)        NOT NULL,        -- 事件指纹（不含动态凭证/发送时间）
    routing_release_id    BIGINT UNSIGNED NOT NULL,
    accepted_at           DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (event_id),
    UNIQUE KEY uq_event_source (caller_id, source_event_id),
    KEY idx_event_object (caller_id, source_business, business_object_type, business_object_id, accepted_at, event_id),
    CONSTRAINT fk_event_release FOREIGN KEY (routing_release_id) REFERENCES routing_releases (release_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 投递任务（每个事件 × 每个路由目标一项，最小追踪与恢复单位）
CREATE TABLE delivery_tasks (
    delivery_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id             BIGINT UNSIGNED NOT NULL,
    route_target_id      VARCHAR(128)    NOT NULL,         -- 路由目标标识
    channel_id           BIGINT UNSIGNED NOT NULL,
    provider_id          VARCHAR(64)     NOT NULL,
    provider_account_id  VARCHAR(64)     NOT NULL,
    operation            VARCHAR(64)     NOT NULL,
    payload              JSON            NOT NULL,         -- 固定映射载荷
    mapping_version      VARCHAR(32)     NOT NULL,
    adapter_version      VARCHAR(32)     NOT NULL,
    idempotency_key      VARCHAR(191)    NOT NULL,         -- 供应商业务幂等标识（稳定）
    status               VARCHAR(16)     NOT NULL DEFAULT 'PENDING',
    round_no             INT UNSIGNED    NOT NULL DEFAULT 1,
    round_attempt_count  INT UNSIGNED    NOT NULL DEFAULT 0,
    total_attempt_count  INT UNSIGNED    NOT NULL DEFAULT 0,
    max_attempts         INT UNSIGNED    NOT NULL,
    round_deadline_at    DATETIME(6)     NULL,
    scheduled_at         DATETIME(6)     NULL,             -- 当前可执行计划时间
    next_retry_at        DATETIME(6)     NULL,             -- 仅 RETRY_WAIT 非空
    next_check_at        DATETIME(6)     NULL,             -- 阻塞条件检查时间
    schedule_version     INT UNSIGNED    NOT NULL DEFAULT 0,
    row_version          INT UNSIGNED    NOT NULL DEFAULT 0,
    current_attempt_id   BIGINT UNSIGNED NULL,
    lease_token          VARCHAR(64)     NULL,
    lease_expires_at     DATETIME(6)     NULL,
    result_certainty     VARCHAR(16)     NULL,
    last_attempt_id      BIGINT UNSIGNED NULL,
    last_error_code      VARCHAR(64)     NULL,
    last_error_reason    VARCHAR(1024)   NULL,
    last_result_certainty VARCHAR(16)    NULL,
    blocked_reason       VARCHAR(512)    NULL,
    round_finished_at    DATETIME(6)     NULL,
    created_at           DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at           DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (delivery_id),
    UNIQUE KEY uq_task_target (event_id, route_target_id),
    KEY idx_task_event (event_id, delivery_id),
    KEY idx_task_provider_status (provider_id, status, updated_at, delivery_id),
    KEY idx_task_scan_scheduled (status, scheduled_at),
    KEY idx_task_scan_lease (status, lease_expires_at),
    KEY idx_task_scan_check (status, next_check_at),
    KEY idx_task_scan_deadline (status, round_deadline_at),
    CONSTRAINT fk_task_event FOREIGN KEY (event_id) REFERENCES business_events (event_id),
    CONSTRAINT fk_task_channel FOREIGN KEY (channel_id) REFERENCES channels (channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- Outbox 消息（某任务、轮次、调度版本的一条投递指令）
CREATE TABLE outbox_messages (
    outbox_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    message_id       VARCHAR(64)     NOT NULL,             -- 稳定消息标识
    delivery_id      BIGINT UNSIGNED NOT NULL,
    round_no         INT UNSIGNED    NOT NULL,
    schedule_version INT UNSIGNED    NOT NULL,
    message_type     VARCHAR(16)     NOT NULL,             -- immediate | delayed
    scheduled_at     DATETIME(6)     NOT NULL,
    publish_status   VARCHAR(16)     NOT NULL DEFAULT 'PENDING', -- PENDING | PUBLISHED | FAILED
    publish_count    INT UNSIGNED    NOT NULL DEFAULT 0,
    next_publish_at  DATETIME(6)     NULL,
    lease_token      VARCHAR(64)     NULL,
    lease_expires_at DATETIME(6)     NULL,
    last_error       VARCHAR(512)    NULL,
    resend_record    JSON            NULL,
    published_at     DATETIME(6)     NULL,
    created_at       DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at       DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (outbox_id),
    UNIQUE KEY uq_outbox_message (message_id),
    UNIQUE KEY uq_outbox_schedule (delivery_id, round_no, schedule_version),
    KEY idx_outbox_publish (publish_status, scheduled_at),
    CONSTRAINT fk_outbox_delivery FOREIGN KEY (delivery_id) REFERENCES delivery_tasks (delivery_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 投递尝试（一次实际业务请求尝试）
CREATE TABLE delivery_attempts (
    attempt_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    delivery_id         BIGINT UNSIGNED NOT NULL,
    round_no            INT UNSIGNED    NOT NULL,
    attempt_no          INT UNSIGNED    NOT NULL,
    outbox_id           BIGINT UNSIGNED NULL,
    trace_id            VARCHAR(64)     NULL,
    worker_id           VARCHAR(64)     NULL,
    deployment_version  VARCHAR(64)     NULL,
    status              VARCHAR(16)     NOT NULL DEFAULT 'PROCESSING', -- PROCESSING | FINISHED
    send_state          VARCHAR(16)     NULL,                -- NOT_SENT | SENT | UNCERTAIN
    method              VARCHAR(16)     NULL,
    target_path         VARCHAR(512)    NULL,                -- 脱敏路径
    request_size_bytes  INT UNSIGNED    NULL,
    request_summary     VARCHAR(2048)   NULL,
    http_status         SMALLINT UNSIGNED NULL,
    provider_code       VARCHAR(64)     NULL,
    provider_request_id VARCHAR(191)    NULL,
    response_size_bytes INT UNSIGNED    NULL,
    response_summary    VARCHAR(2048)   NULL,
    response_truncated  BOOLEAN         NULL,
    outcome             VARCHAR(16)     NULL,                -- RECEIVED | RETRYABLE_REJECT | PERMANENT_REJECT | LOCAL_FAILURE | UNCERTAIN
    result_certainty    VARCHAR(16)     NULL,
    error_code          VARCHAR(64)     NULL,
    error_reason        VARCHAR(1024)   NULL,
    retryable           BOOLEAN         NULL,
    provider_wait_hint  VARCHAR(64)     NULL,
    started_at          DATETIME(6)     NOT NULL,
    request_started_at  DATETIME(6)     NULL,
    response_received_at DATETIME(6)    NULL,
    finished_at         DATETIME(6)     NULL,
    duration_ms         BIGINT UNSIGNED NULL,
    attempt_duration_ms BIGINT UNSIGNED NULL,
    timing_incomplete   BOOLEAN         NULL,
    created_at          DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (attempt_id),
    UNIQUE KEY uq_attempt_seq (delivery_id, round_no, attempt_no),
    KEY idx_attempt_delivery (delivery_id, attempt_id),
    CONSTRAINT fk_attempt_delivery FOREIGN KEY (delivery_id) REFERENCES delivery_tasks (delivery_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 任务全过程时间线（追加记录）
CREATE TABLE delivery_history (
    history_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    delivery_id      BIGINT UNSIGNED NOT NULL,
    history_seq      INT UNSIGNED    NOT NULL,
    write_id         VARCHAR(64)     NOT NULL,              -- 写入标识，事务重试不重复追加
    phase            VARCHAR(64)     NOT NULL,
    from_status      VARCHAR(16)     NULL,
    to_status        VARCHAR(16)     NULL,
    reason           VARCHAR(1024)   NULL,
    round_no         INT UNSIGNED    NULL,
    schedule_version INT UNSIGNED    NULL,
    attempt_id       BIGINT UNSIGNED NULL,
    outbox_id        BIGINT UNSIGNED NULL,
    operation_id     VARCHAR(64)     NULL,
    actor            VARCHAR(64)     NULL,
    evidence         JSON            NULL,                  -- 脱敏补充证据
    occurred_at      DATETIME(6)     NULL,                  -- 实际发生时间
    recorded_at      DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (history_id),
    UNIQUE KEY uq_history_write (write_id),
    UNIQUE KEY uq_history_seq (delivery_id, history_seq),
    KEY idx_history_delivery (delivery_id, history_seq),
    CONSTRAINT fk_history_delivery FOREIGN KEY (delivery_id) REFERENCES delivery_tasks (delivery_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 管理操作（配置发布、人工重投、结果核对及死信处理）
CREATE TABLE management_events (
    management_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    operation_type      VARCHAR(64)     NOT NULL,           -- config_publish | manual_retry | resolve | dlq_repair
    operation_id        VARCHAR(64)     NOT NULL,
    idempotency_key     VARCHAR(191)    NOT NULL,
    caller_scope        VARCHAR(64)     NOT NULL,
    request_fingerprint CHAR(64)        NULL,
    expected_version    INT UNSIGNED    NULL,
    actor               VARCHAR(64)     NOT NULL,
    reason              VARCHAR(1024)   NULL,
    target_type         VARCHAR(32)     NULL,
    target_id           VARCHAR(191)    NULL,
    result              JSON            NULL,
    created_at          DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (management_id),
    UNIQUE KEY uq_mgmt_idempotency (caller_scope, operation_type, idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- +goose Down
DROP TABLE IF EXISTS management_events;
DROP TABLE IF EXISTS delivery_history;
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS outbox_messages;
DROP TABLE IF EXISTS delivery_tasks;
DROP TABLE IF EXISTS business_events;
DROP TABLE IF EXISTS channels;
DROP TABLE IF EXISTS routing_current;
DROP TABLE IF EXISTS routing_releases;
