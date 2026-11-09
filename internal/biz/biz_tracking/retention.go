package biz_tracking

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// RetentionConfig 是 S08 §9 清理参数。
type RetentionConfig struct {
	ResolvedAfter   time.Duration
	BatchEvents     int
	MaxRowsPerEvent int
}

// RetentionCounters 是清理结果计数；TooLarge 用于告警。
type RetentionCounters struct {
	Deleted, Skipped, TooLarge, Failed atomic.Int64
}

// Cleaner 按单事件独立事务协调清理（TRK-R01/R02）。
type Cleaner struct {
	Store    data_interface.RetentionStore
	Config   RetentionConfig
	Counters *RetentionCounters
	Now      func() time.Time
	Log      *slog.Logger
}

// ScanOnce 发现候选并逐事件复核删除；超限事件整体保留并告警，不分批删除。
func (c *Cleaner) ScanOnce(ctx context.Context) error {
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	cutoff := now.Add(-c.Config.ResolvedAfter)
	cands, err := c.Store.ListRetentionCandidates(ctx, cutoff, c.Config.BatchEvents)
	if err != nil {
		return err
	}
	for _, cand := range cands {
		res, err := c.Store.DeleteResolvedEvent(ctx, cand, cutoff, c.Config.MaxRowsPerEvent)
		switch {
		case err != nil:
			c.Counters.Failed.Add(1)
		case res == domain.RetentionDelete:
			c.Counters.Deleted.Add(1)
		case res == domain.RetentionTooLarge:
			c.Counters.TooLarge.Add(1)
			if c.Log != nil {
				c.Log.Warn("retention_event_too_large", "event_id", cand.EventID)
			}
		default:
			c.Counters.Skipped.Add(1)
		}
	}
	return nil
}
