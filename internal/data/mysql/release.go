package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// ReleaseStore 实现发布草稿、当前指针 CAS 和按版本读取。
// 发布事务先锁 routing_current 的 singleton 行，再复核引用，最后写指针和操作结果。
type ReleaseStore struct {
	db   *sql.DB
	exec *Executor
}

func NewReleaseStore(db *sql.DB, exec *Executor) *ReleaseStore {
	return &ReleaseStore{db: db, exec: exec}
}

func (s *ReleaseStore) CreateDraft(ctx context.Context, in data_interface.CreateDraftInput) (data_interface.CreateDraftResult, error) {
	var out data_interface.CreateDraftResult
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := lockCurrent(ctx, tx); err != nil {
			return err
		}
		if replay, ok, err := readOperation(ctx, tx, constant.OpConfigCreate, in.IdempotencyKey, in.Fingerprint); err != nil || ok {
			if err != nil {
				return err
			}
			decoded, err := decodeCreate(replay)
			if err != nil {
				return err
			}
			out = decoded
			return nil
		}
		set, err := freezeReferences(ctx, tx, in.Bindings)
		if err != nil {
			return err
		}
		snapshot, err := data_interface.MarshalReferences(set)
		if err != nil {
			return err
		}
		var maxVer sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version_no), 0) FROM routing_releases`).Scan(&maxVer); err != nil {
			return err
		}
		next := maxVer.Int64 + 1
		if next <= 0 || next > int64(^uint32(0)) {
			return domainErr(constant.CodeLimitExceeded, "发布序号溢出")
		}
		opID, err := newOperationID()
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			INSERT INTO routing_releases (
				version_no, content, content_canonical, content_sha256, fingerprint_version,
				reference_snapshot, status, created_at)
			VALUES (?, CAST(? AS JSON), ?, ?, ?, CAST(? AS JSON), 'draft', UTC_TIMESTAMP(6))`,
			next, string(in.ContentCanonical), in.ContentCanonical, in.ContentSHA256, constant.FingerprintVersion, string(snapshot))
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil || id <= 0 {
			return err
		}
		out = data_interface.CreateDraftResult{
			OperationID:   opID,
			ReleaseID:     uint64(id),
			VersionNo:     uint32(next),
			Status:        "draft",
			ContentSHA256: in.ContentSHA256,
		}
		body, err := json.Marshal(out)
		if err != nil {
			return err
		}
		return insertOperation(ctx, tx, constant.OpConfigCreate, opID, in.IdempotencyKey, in.Fingerprint, in.ActorID, in.Reason, uint64(id), body)
	})
	if err != nil && isUnique(err) {
		if decoded, ok, rerr := s.loadCreateReplay(ctx, in.IdempotencyKey, in.Fingerprint); rerr != nil {
			return data_interface.CreateDraftResult{}, mapTxErr(rerr)
		} else if ok {
			return decoded, nil
		}
	}
	return out, mapTxErr(err)
}

func (s *ReleaseStore) Publish(ctx context.Context, in data_interface.PublishInput) (data_interface.PublishResult, error) {
	var out data_interface.PublishResult
	err := s.exec.RunTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		current, err := lockCurrentID(ctx, tx)
		if err != nil {
			return err
		}
		if replay, ok, err := readOperation(ctx, tx, constant.OpConfigPublish, in.IdempotencyKey, in.Fingerprint); err != nil || ok {
			if err != nil {
				return err
			}
			decoded, err := decodePublish(replay)
			if err != nil {
				return err
			}
			out = decoded
			return nil
		}
		var (
			versionNo uint32
			status    string
			canon     []byte
			shaHex    string
			fpVer     sql.NullInt64
			snapshot  []byte
		)
		err = tx.QueryRowContext(ctx, `
			SELECT version_no, status, content_canonical, content_sha256, fingerprint_version, reference_snapshot
			FROM routing_releases WHERE release_id = ? FOR UPDATE`, in.ReleaseID).
			Scan(&versionNo, &status, &canon, &shaHex, &fpVer, &snapshot)
		if errors.Is(err, sql.ErrNoRows) {
			return data_interface.ErrReleaseNotFound
		}
		if err != nil {
			return err
		}
		if status != "draft" {
			return domainErr(constant.CodeStateConflict, "草稿不能再次发布")
		}
		if !fpVer.Valid || int(fpVer.Int64) != constant.FingerprintVersion || len(canon) == 0 || domainSHA(canon) != shaHex {
			return domainErr(constant.CodeConfigurationUnavailable, "发布内容不可用")
		}
		stored, err := data_interface.UnmarshalReferences(snapshot)
		if err != nil {
			return domainErr(constant.CodeConfigurationUnavailable, "发布内容不可用")
		}
		live, err := loadLiveReferences(ctx, tx, stored)
		if err != nil {
			return err
		}
		if !data_interface.SameReferences(stored, live) {
			return domainErr(constant.CodeConfigurationChanged, "引用版本已变化，请重新校验")
		}
		if !samePointer(current, in.Expected) {
			return domainErr(constant.CodeVersionConflict, "当前发布版本已变化")
		}
		if current != nil {
			res, err := tx.ExecContext(ctx, `
				UPDATE routing_releases SET status = 'superseded'
				WHERE release_id = ? AND status = 'published'`, *current)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return domainErr(constant.CodeInternal, "内部错误")
			}
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE routing_releases
			SET status = 'published', published_at = UTC_TIMESTAMP(6)
			WHERE release_id = ? AND status = 'draft'`, in.ReleaseID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return domainErr(constant.CodeStateConflict, "草稿不能再次发布")
		}
		res, err = tx.ExecContext(ctx, `
			UPDATE routing_current SET release_id = ?, updated_at = UTC_TIMESTAMP(6) WHERE singleton = 1`, in.ReleaseID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return domainErr(constant.CodeInternal, "内部错误")
		}
		var published time.Time
		if err := tx.QueryRowContext(ctx, `SELECT published_at FROM routing_releases WHERE release_id = ?`, in.ReleaseID).Scan(&published); err != nil {
			return err
		}
		opID, err := newOperationID()
		if err != nil {
			return err
		}
		out = data_interface.PublishResult{
			OperationID: opID,
			ReleaseID:   in.ReleaseID,
			VersionNo:   versionNo,
			Previous:    current,
			PublishedAt: asUTC(published),
		}
		body, err := encodePublish(out)
		if err != nil {
			return err
		}
		return insertOperation(ctx, tx, constant.OpConfigPublish, opID, in.IdempotencyKey, in.Fingerprint, in.ActorID, in.Reason, in.ReleaseID, body)
	})
	if err != nil && isUnique(err) {
		if decoded, ok, rerr := s.loadPublishReplay(ctx, in.IdempotencyKey, in.Fingerprint); rerr != nil {
			return data_interface.PublishResult{}, mapTxErr(rerr)
		} else if ok {
			return decoded, nil
		}
	}
	return out, mapTxErr(err)
}

func (s *ReleaseStore) Load(ctx context.Context, releaseID uint64) (data_interface.ReleaseRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT release_id, version_no, status, content_canonical, content_sha256, fingerprint_version,
		       reference_snapshot, created_at, published_at
		FROM routing_releases WHERE release_id = ?`, releaseID)
	return scanRelease(row)
}

func (s *ReleaseStore) LoadCurrentID(ctx context.Context) (uint64, error) {
	var id sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT release_id FROM routing_current WHERE singleton = 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !id.Valid) {
		return 0, data_interface.ErrNoCurrent
	}
	if err != nil {
		return 0, domainErr(constant.CodeInternal, "内部错误")
	}
	if id.Int64 <= 0 {
		return 0, data_interface.ErrNoCurrent
	}
	return uint64(id.Int64), nil
}

func (s *ReleaseStore) GetChannel(ctx context.Context, id uint64) (data_interface.ChannelPin, error) {
	pin, err := scanChannel(s.db.QueryRowContext(ctx, channelSelect+` WHERE channel_id = ?`, id), id)
	if errors.Is(err, sql.ErrNoRows) {
		return data_interface.ChannelPin{}, data_interface.ErrChannelNotFound
	}
	if err != nil {
		return data_interface.ChannelPin{}, domainErr(constant.CodeInternal, "内部错误")
	}
	return pin, nil
}

func (s *ReleaseStore) GetCredential(ctx context.Context, ref string) (data_interface.CredentialPin, error) {
	pin, err := scanCredential(s.db.QueryRowContext(ctx, credentialSelect+` WHERE credential_ref = ?`, ref), ref)
	if errors.Is(err, sql.ErrNoRows) {
		return data_interface.CredentialPin{}, data_interface.ErrCredentialNotFound
	}
	if err != nil {
		return data_interface.CredentialPin{}, domainErr(constant.CodeInternal, "内部错误")
	}
	return pin, nil
}

const channelSelect = `
	SELECT provider_id, provider_account_id, operation, adapter_type, adapter_version, credential_ref, row_version
	FROM channels`

const credentialSelect = `
	SELECT provider_id, provider_account_id, version FROM credential_refs`

func lockCurrent(ctx context.Context, tx *sql.Tx) error {
	var id sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT release_id FROM routing_current WHERE singleton = 1 FOR UPDATE`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return domainErr(constant.CodeConfigurationUnavailable, "当前发布指针不存在")
	}
	return err
}

func lockCurrentID(ctx context.Context, tx *sql.Tx) (*uint64, error) {
	var id sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT release_id FROM routing_current WHERE singleton = 1 FOR UPDATE`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domainErr(constant.CodeConfigurationUnavailable, "当前发布指针不存在")
	}
	if err != nil {
		return nil, err
	}
	if !id.Valid {
		return nil, nil
	}
	if id.Int64 <= 0 {
		return nil, domainErr(constant.CodeInternal, "内部错误")
	}
	v := uint64(id.Int64)
	return &v, nil
}

func readOperation(ctx context.Context, tx *sql.Tx, opType, key, fingerprint string) ([]byte, bool, error) {
	var storedFP string
	var result []byte
	err := tx.QueryRowContext(ctx, `
		SELECT request_fingerprint, result FROM management_events
		WHERE caller_scope = ? AND operation_type = ? AND idempotency_key = ?`,
		constant.GlobalCallerScope, opType, key).Scan(&storedFP, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if storedFP != fingerprint {
		return nil, false, domainErr(constant.CodeIdempotencyConflict, "相同操作键的参数不一致")
	}
	return result, true, nil
}

func insertOperation(ctx context.Context, tx *sql.Tx, opType, opID, key, fingerprint, actor, reason string, releaseID uint64, result []byte) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO management_events (
			operation_type, operation_id, idempotency_key, caller_scope, request_fingerprint,
			actor, reason, target_type, target_id, result, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'routing_release', ?, ?, UTC_TIMESTAMP(6))`,
		opType, opID, key, constant.GlobalCallerScope, fingerprint, actor, reason, strconv.FormatUint(releaseID, 10), result)
	return err
}

func freezeReferences(ctx context.Context, tx *sql.Tx, bindings []domain.TargetBinding) (data_interface.ReferenceSet, error) {
	var set data_interface.ReferenceSet
	seenCh := map[uint64]struct{}{}
	seenRef := map[string]struct{}{}
	ids := make([]uint64, 0, len(bindings))
	for _, binding := range bindings {
		if _, ok := seenCh[binding.ChannelID]; ok {
			continue
		}
		seenCh[binding.ChannelID] = struct{}{}
		ids = append(ids, binding.ChannelID)
	}
	sortU64(ids)
	for _, id := range ids {
		pin, err := scanChannel(tx.QueryRowContext(ctx, channelSelect+` WHERE channel_id = ? FOR SHARE`, id), id)
		if errors.Is(err, sql.ErrNoRows) {
			return set, domainErr(constant.CodeConfigurationChanged, "引用版本已变化，请重新校验")
		}
		if err != nil {
			return set, err
		}
		for _, binding := range bindings {
			if binding.ChannelID != id {
				continue
			}
			if pin.ProviderID != binding.ProviderID || pin.ProviderAccountID != binding.ProviderAccountID ||
				pin.Operation != binding.Operation || pin.AdapterType != binding.AdapterType ||
				pin.AdapterVersion != binding.AdapterVersion || pin.CredentialRef != binding.CredentialRef {
				return set, domainErr(constant.CodeConfigurationChanged, "引用版本已变化，请重新校验")
			}
		}
		set.Channels = append(set.Channels, pin)
	}
	refs := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		if _, ok := seenRef[binding.CredentialRef]; ok {
			continue
		}
		seenRef[binding.CredentialRef] = struct{}{}
		refs = append(refs, binding.CredentialRef)
	}
	sortStrings(refs)
	for _, ref := range refs {
		pin, err := scanCredential(tx.QueryRowContext(ctx, credentialSelect+` WHERE credential_ref = ? FOR SHARE`, ref), ref)
		if errors.Is(err, sql.ErrNoRows) {
			return set, domainErr(constant.CodeConfigurationChanged, "引用版本已变化，请重新校验")
		}
		if err != nil {
			return set, err
		}
		for _, binding := range bindings {
			if binding.CredentialRef != ref {
				continue
			}
			if pin.ProviderID != binding.ProviderID || pin.ProviderAccountID != binding.ProviderAccountID {
				return set, domainErr(constant.CodeConfigurationChanged, "引用版本已变化，请重新校验")
			}
		}
		set.Credentials = append(set.Credentials, pin)
	}
	return set, nil
}

func loadLiveReferences(ctx context.Context, tx *sql.Tx, stored data_interface.ReferenceSet) (data_interface.ReferenceSet, error) {
	var live data_interface.ReferenceSet
	ids := make([]uint64, len(stored.Channels))
	for i, item := range stored.Channels {
		ids[i] = item.ChannelID
	}
	sortU64(ids)
	for _, id := range ids {
		pin, err := scanChannel(tx.QueryRowContext(ctx, channelSelect+` WHERE channel_id = ? FOR SHARE`, id), id)
		if errors.Is(err, sql.ErrNoRows) {
			return live, domainErr(constant.CodeConfigurationChanged, "引用版本已变化，请重新校验")
		}
		if err != nil {
			return live, err
		}
		live.Channels = append(live.Channels, pin)
	}
	refs := make([]string, len(stored.Credentials))
	for i, item := range stored.Credentials {
		refs[i] = item.Ref
	}
	sortStrings(refs)
	for _, ref := range refs {
		pin, err := scanCredential(tx.QueryRowContext(ctx, credentialSelect+` WHERE credential_ref = ? FOR SHARE`, ref), ref)
		if errors.Is(err, sql.ErrNoRows) {
			return live, domainErr(constant.CodeConfigurationChanged, "引用版本已变化，请重新校验")
		}
		if err != nil {
			return live, err
		}
		live.Credentials = append(live.Credentials, pin)
	}
	return live, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanChannel(row rowScanner, id uint64) (data_interface.ChannelPin, error) {
	var pin data_interface.ChannelPin
	pin.ChannelID = id
	err := row.Scan(&pin.ProviderID, &pin.ProviderAccountID, &pin.Operation, &pin.AdapterType, &pin.AdapterVersion, &pin.CredentialRef, &pin.RowVersion)
	return pin, err
}

func scanCredential(row rowScanner, ref string) (data_interface.CredentialPin, error) {
	var pin data_interface.CredentialPin
	pin.Ref = ref
	err := row.Scan(&pin.ProviderID, &pin.ProviderAccountID, &pin.Version)
	return pin, err
}

func scanRelease(row rowScanner) (data_interface.ReleaseRecord, error) {
	var rec data_interface.ReleaseRecord
	var canon, snapshot []byte
	var sha string
	var fp sql.NullInt64
	var published sql.NullTime
	err := row.Scan(&rec.ReleaseID, &rec.VersionNo, &rec.Status, &canon, &sha, &fp, &snapshot, &rec.CreatedAt, &published)
	if errors.Is(err, sql.ErrNoRows) {
		return data_interface.ReleaseRecord{}, data_interface.ErrReleaseNotFound
	}
	if err != nil {
		return data_interface.ReleaseRecord{}, domainErr(constant.CodeInternal, "内部错误")
	}
	if len(canon) == 0 || !fp.Valid || int(fp.Int64) != constant.FingerprintVersion || domainSHA(canon) != sha {
		return data_interface.ReleaseRecord{}, domainErr(constant.CodeConfigurationUnavailable, "发布内容不可用")
	}
	rec.ContentCanonical = canon
	rec.ContentSHA256 = sha
	rec.FingerprintVersion = int(fp.Int64)
	rec.ReferenceSnapshot = snapshot
	rec.CreatedAt = asUTC(rec.CreatedAt)
	if published.Valid {
		t := asUTC(published.Time)
		rec.PublishedAt = &t
	}
	return rec, nil
}

func domainSHA(canonical []byte) string {
	return canonicaljson.Fingerprint(canonical)
}

// asUTC 把 MySQL DATETIME 的墙钟读成 UTC。驱动在未指定 loc 时会标成本地时区。
func asUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

func (s *ReleaseStore) loadCreateReplay(ctx context.Context, key, fingerprint string) (data_interface.CreateDraftResult, bool, error) {
	raw, ok, err := s.readCommitted(ctx, constant.OpConfigCreate, key, fingerprint)
	if err != nil || !ok {
		return data_interface.CreateDraftResult{}, false, err
	}
	decoded, err := decodeCreate(raw)
	if err != nil {
		return data_interface.CreateDraftResult{}, false, err
	}
	return decoded, true, nil
}

func (s *ReleaseStore) loadPublishReplay(ctx context.Context, key, fingerprint string) (data_interface.PublishResult, bool, error) {
	raw, ok, err := s.readCommitted(ctx, constant.OpConfigPublish, key, fingerprint)
	if err != nil || !ok {
		return data_interface.PublishResult{}, false, err
	}
	decoded, err := decodePublish(raw)
	if err != nil {
		return data_interface.PublishResult{}, false, err
	}
	return decoded, true, nil
}

func (s *ReleaseStore) readCommitted(ctx context.Context, opType, key, fingerprint string) ([]byte, bool, error) {
	var storedFP string
	var result []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT request_fingerprint, result FROM management_events
		WHERE caller_scope = ? AND operation_type = ? AND idempotency_key = ?`,
		constant.GlobalCallerScope, opType, key).Scan(&storedFP, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if storedFP != fingerprint {
		return nil, false, domainErr(constant.CodeIdempotencyConflict, "相同操作键的参数不一致")
	}
	return result, true, nil
}

func samePointer(current, expected *uint64) bool {
	if current == nil || expected == nil {
		return current == nil && expected == nil
	}
	return *current == *expected
}

func newOperationID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

type storedPublish struct {
	OperationID string  `json:"operation_id"`
	ReleaseID   string  `json:"release_id"`
	VersionNo   uint32  `json:"version_no"`
	Previous    *string `json:"previous_release_id"`
	PublishedAt string  `json:"published_at"`
}

func encodePublish(out data_interface.PublishResult) ([]byte, error) {
	id, err := domain.FormatInternalID(out.ReleaseID)
	if err != nil {
		return nil, err
	}
	body := storedPublish{
		OperationID: out.OperationID,
		ReleaseID:   id,
		VersionNo:   out.VersionNo,
		PublishedAt: domain.FormatUTCMicros(out.PublishedAt),
	}
	if out.Previous != nil {
		prev, err := domain.FormatInternalID(*out.Previous)
		if err != nil {
			return nil, err
		}
		body.Previous = &prev
	}
	return json.Marshal(body)
}

func decodePublish(raw []byte) (data_interface.PublishResult, error) {
	var body storedPublish
	if err := json.Unmarshal(raw, &body); err != nil {
		return data_interface.PublishResult{}, domainErr(constant.CodeInternal, "内部错误")
	}
	id, err := domain.ParseInternalID("release_id", body.ReleaseID)
	if err != nil {
		return data_interface.PublishResult{}, domainErr(constant.CodeInternal, "内部错误")
	}
	at, err := domain.ParseRFC3339("published_at", body.PublishedAt)
	if err != nil {
		return data_interface.PublishResult{}, domainErr(constant.CodeInternal, "内部错误")
	}
	out := data_interface.PublishResult{OperationID: body.OperationID, ReleaseID: id, VersionNo: body.VersionNo, PublishedAt: at}
	if body.Previous != nil {
		prev, err := domain.ParseInternalID("previous_release_id", *body.Previous)
		if err != nil {
			return data_interface.PublishResult{}, domainErr(constant.CodeInternal, "内部错误")
		}
		out.Previous = &prev
	}
	return out, nil
}

func decodeCreate(raw []byte) (data_interface.CreateDraftResult, error) {
	var body data_interface.CreateDraftResult
	if err := json.Unmarshal(raw, &body); err != nil {
		return data_interface.CreateDraftResult{}, domainErr(constant.CodeInternal, "内部错误")
	}
	if body.ReleaseID == 0 || body.OperationID == "" {
		return data_interface.CreateDraftResult{}, domainErr(constant.CodeInternal, "内部错误")
	}
	return body, nil
}

func domainErr(code, message string) error {
	err, _ := domain.NewError(code, message, "", nil)
	return err
}

func mapTxErr(err error) error {
	if err == nil {
		return nil
	}
	var de *domain.Error
	if errors.As(err, &de) {
		return de
	}
	if errors.Is(err, data_interface.ErrReleaseNotFound) || errors.Is(err, data_interface.ErrNoCurrent) {
		return err
	}
	if Classify(err) == FailureCommitUncertain {
		return domainErr(constant.CodeDependencyUnavailable, "提交结果不确定")
	}
	return domainErr(constant.CodeInternal, "内部错误")
}

func sortU64(ids []uint64) {
	for i := 1; i < len(ids); i++ {
		j := i
		for j > 0 && ids[j] < ids[j-1] {
			ids[j], ids[j-1] = ids[j-1], ids[j]
			j--
		}
	}
}

func sortStrings(items []string) {
	for i := 1; i < len(items); i++ {
		j := i
		for j > 0 && items[j] < items[j-1] {
			items[j], items[j-1] = items[j-1], items[j]
			j--
		}
	}
}
