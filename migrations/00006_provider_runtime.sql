-- +goose Up
-- S05 §9：需要 Refresh Token 的凭证在受保护存储中维护刷新状态机。不含秘密。不改 00001–00005。
ALTER TABLE credential_refs
    ADD COLUMN state VARCHAR(32) NOT NULL DEFAULT 'ACTIVE'
        COMMENT 'ACTIVE | REFRESHING | REAUTH_REQUIRED | REVOKED' AFTER version,
    ADD COLUMN refresh_owner VARCHAR(128) NULL
        COMMENT '持有刷新权的实例；只经 CAS 写入' AFTER state,
    ADD COLUMN refresh_deadline DATETIME(6) NULL
        COMMENT '刷新所有权到期时间（UTC）' AFTER refresh_owner;

-- +goose Down
ALTER TABLE credential_refs
    DROP COLUMN refresh_deadline,
    DROP COLUMN refresh_owner,
    DROP COLUMN state;
