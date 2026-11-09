-- +goose Up
-- S01 发布指针与凭证引用版本。不改 00001–00003。
-- routing_current 预置 singleton=1，release_id 允许 NULL，首次发布与后续发布锁同一行。
-- credential_refs 只保存逻辑引用、账号归属和版本，不含秘密。完整凭证存储与刷新由 S05 完成。

ALTER TABLE routing_current
    MODIFY COLUMN release_id BIGINT UNSIGNED NULL
        COMMENT '当前发布；未发布时为 NULL';

INSERT INTO routing_current (singleton, release_id, updated_at)
VALUES (1, NULL, UTC_TIMESTAMP(6))
ON DUPLICATE KEY UPDATE singleton = singleton;

ALTER TABLE routing_releases
    ADD COLUMN reference_snapshot JSON NULL
        COMMENT '草稿创建时冻结的通道与凭证引用版本，发布事务内复核' AFTER fingerprint_version;

CREATE TABLE credential_refs (
    credential_ref      VARCHAR(191)    NOT NULL,
    provider_id         VARCHAR(64)     NOT NULL,
    provider_account_id VARCHAR(64)     NOT NULL,
    version             INT UNSIGNED    NOT NULL,
    updated_at          DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (credential_ref)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin
  COMMENT='逻辑凭证引用版本。不含秘密。完整凭证存储由 S05 完成';

-- +goose Down
DROP TABLE IF EXISTS credential_refs;

ALTER TABLE routing_releases
    DROP COLUMN reference_snapshot;

DELETE FROM routing_current WHERE release_id IS NULL;

ALTER TABLE routing_current
    MODIFY COLUMN release_id BIGINT UNSIGNED NOT NULL;
