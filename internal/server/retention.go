package server

import (
	"context"
	"errors"
	"time"
)

// RetentionRunner 是一轮协调清理。
type RetentionRunner interface {
	ScanOnce(ctx context.Context) error
}

// Retention 是 S08 保留清理后台角色：每轮结束后等待间隔，不堆叠扫描。
type Retention struct {
	Runner   RetentionRunner
	Interval time.Duration
}

func (r *Retention) Name() string { return "retention-cleaner" }

func (r *Retention) Run(ctx context.Context) error {
	if r == nil || r.Runner == nil || r.Interval <= 0 {
		return errors.New("server: retention cleaner is incomplete")
	}
	for {
		_ = r.Runner.ScanOnce(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.Interval):
		}
	}
}
