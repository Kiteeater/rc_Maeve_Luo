-- +goose Up
-- S08 §10：事件默认时间分页 (caller_id, accepted_at, event_id)。任务 (event_id, delivery_id)、
-- 尝试 (delivery_id, round_no, attempt_no) 与历史 (delivery_id, history_seq) 已由 00001 提供。
ALTER TABLE business_events
    ADD KEY idx_event_caller_accepted (caller_id, accepted_at, event_id);

-- +goose Down
ALTER TABLE business_events
    DROP INDEX idx_event_caller_accepted;
