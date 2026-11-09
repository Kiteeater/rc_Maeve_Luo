-- +goose Up
-- S02 积压准入按未完成发布状态统计数量和最早创建时间。
-- 不含已发布和已作废的旧调度指令。不改 00001–00004。

ALTER TABLE outbox_messages
    ADD KEY idx_outbox_backlog (publish_status, created_at, outbox_id);

-- +goose Down
ALTER TABLE outbox_messages
    DROP INDEX idx_outbox_backlog;
