package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"notifier/internal/biz/biz_recovery"
)

// RecoveryRunner 是一轮恢复扫描。
type RecoveryRunner interface {
	ScanOnce(ctx context.Context) (biz_recovery.ScanResult, error)
}

// Recovery 是后台扫描角色：上一批结束后才启动下一轮，不堆叠同实例扫描；基础设施失败按退避。
type Recovery struct {
	Runner       RecoveryRunner
	Interval     time.Duration
	ErrorBackoff time.Duration
	Log          *slog.Logger
}

func (r *Recovery) Name() string { return "delivery-recovery" }

func (r *Recovery) Run(ctx context.Context) error {
	if r == nil || r.Runner == nil || r.Interval <= 0 {
		return errors.New("server: recovery scanner is incomplete")
	}
	for {
		_, err := r.Runner.ScanOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		wait := r.Interval
		if err != nil {
			if r.Log != nil {
				r.Log.Info("recovery scan failed")
			}
			if r.ErrorBackoff > 0 {
				wait = r.ErrorBackoff
			}
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}
