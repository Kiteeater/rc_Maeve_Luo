package mysql

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
)

// 锁顺序：事件（需要时，由调用方先锁）→ 按 ID 升序的任务 → 尝试/Outbox/历史/操作记录。
// Outbox 单独领取事务不再反向锁任务。history_seq 的递增不要求增加业务 row_version。

// LockTasks 按 delivery_id 升序锁定任务行。调用方需要事件锁时必须先取得事件锁。
func LockTasks(ctx context.Context, tx *sql.Tx, ids []uint64) error {
	ordered := append([]uint64(nil), ids...)
	slices.Sort(ordered)
	for _, id := range ordered {
		var got uint64
		err := tx.QueryRowContext(ctx,
			`SELECT delivery_id FROM delivery_tasks WHERE delivery_id = ? FOR UPDATE`, id).Scan(&got)
		if err != nil {
			return err
		}
	}
	return nil
}

// NextHistorySeq 锁定任务行并递增 history_seq。溢出时报错，不回绕。
// 具体历史写入由后续事务操作完成。
func NextHistorySeq(ctx context.Context, tx *sql.Tx, deliveryID uint64) (uint32, error) {
	var seq uint32
	err := tx.QueryRowContext(ctx,
		`SELECT history_seq FROM delivery_tasks WHERE delivery_id = ? FOR UPDATE`, deliveryID).Scan(&seq)
	if err != nil {
		return 0, err
	}
	if uint64(seq) == math.MaxUint32 {
		return 0, errors.New("mysql: history_seq overflow")
	}
	seq++
	if _, err = tx.ExecContext(ctx,
		`UPDATE delivery_tasks SET history_seq = ? WHERE delivery_id = ?`, seq, deliveryID); err != nil {
		return 0, err
	}
	return seq, nil
}
