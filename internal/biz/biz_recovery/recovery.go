// Package biz_recovery 以 MySQL 未完成任务为基准恢复处理进度（S06）：丢失/停滞指令、过期领取、阻塞、截止与死信。
// 不重跑路由或映射，不调用供应商业务 HTTP，不以 Redis 锁或扫描游标代替任务实际修复。
package biz_recovery

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// Config 是 S06 §7 参数。
type Config struct {
	BatchSize      int
	Parallelism    int
	TaskTimeout    time.Duration
	Grace          time.Duration
	RepairCooldown time.Duration
	MaxRepairs     uint32
	BlockedCheck   time.Duration
	DLQAutoRepairs uint32
	Actor          string
}

// Counters 是恢复指标与告警计数。
type Counters struct {
	Applied        atomic.Int64
	Stale          atomic.Int64
	Failed         atomic.Int64
	Expired        atomic.Int64
	Repaired       atomic.Int64
	Inconsistent   atomic.Int64
	ManualRequired atomic.Int64
	DLQDisposed    atomic.Int64
}

// Recoverer 是扫描与单任务处置用例。
type Recoverer struct {
	Store       data_interface.RecoveryStore
	Constraints data_interface.ConstraintChecker
	Config      Config
	Counters    *Counters
}

// ScanResult 汇总一轮扫描。
type ScanResult struct {
	Seen    int
	Applied int
	Failed  int
}

// ScanOnce 按类别独立限额扫描；单个候选失败不阻止其他候选，失败候选保留资格在下轮重新发现（RCV-10、RCV-17）。
func (r *Recoverer) ScanOnce(ctx context.Context) (ScanResult, error) {
	var res ScanResult
	var listErr error
	for _, kind := range domain.RecoveryKinds {
		cands, err := r.Store.ListRecoveryCandidates(ctx, kind, r.Config.BatchSize)
		if err != nil {
			listErr = errors.Join(listErr, err)
			continue
		}
		res.Seen += len(cands)
		applied, failed := r.processAll(ctx, cands)
		res.Applied += applied
		res.Failed += failed
	}
	return res, listErr
}

func (r *Recoverer) processAll(ctx context.Context, cands []data_interface.RecoveryCandidate) (int, int) {
	par := r.Config.Parallelism
	if par <= 0 {
		par = 1
	}
	sem := make(chan struct{}, par)
	var wg sync.WaitGroup
	var applied, failed atomic.Int64
	for _, c := range cands {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			defer func() { <-sem }()
			tctx, cancel := context.WithTimeout(ctx, r.taskTimeout())
			defer cancel()
			out, err := r.Recover(tctx, id)
			switch {
			case err != nil:
				failed.Add(1)
				r.Counters.Failed.Add(1)
			case out == data_interface.RecoveryApplied:
				applied.Add(1)
			}
		}(c.DeliveryID)
	}
	wg.Wait()
	return int(applied.Load()), int(failed.Load())
}

// Recover 处置单个任务：读取快照 → 判断 → 带期望版本与稳定修复键的 RecoverTask。
func (r *Recoverer) Recover(ctx context.Context, deliveryID uint64) (data_interface.RecoveryOutcome, error) {
	snap, err := r.Store.LoadRecoverySnapshot(ctx, deliveryID)
	if err != nil {
		return "", err
	}
	task := snap.Task
	facts := task.Facts()
	dec := domain.DecideRecovery(domain.RecoveryFacts{
		Task: facts, AttemptFinished: snap.AttemptFinished, Outbox: snap.Outbox, Now: snap.Now,
		Grace: r.Config.Grace, MaxRepairs: r.Config.MaxRepairs,
	})
	in := data_interface.RecoverInput{
		DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
		ExpectedRowVersion: task.RowVersion, Action: dec.Action, Actor: r.actor(), Reason: dec.Reason,
	}
	switch dec.Action {
	case domain.RecoveryNoop:
		return "", nil
	case domain.RecoveryEscalate:
		if dec.Code == constant.ErrRecoveryInconsistent {
			r.Counters.Inconsistent.Add(1)
		} else {
			r.Counters.ManualRequired.Add(1)
		}
		return "", nil
	case domain.RecoveryExpire:
		plan, err := r.expirePlan(ctx, task, snap.Now)
		if err != nil {
			return "", err
		}
		in.AttemptID = *task.CurrentAttemptID
		in.Plan = &plan
		in.RepairKey = repairKey(task, dec.Action, in.AttemptID)
	case domain.RecoveryClose:
		plan := domain.CloseRoundPlan(facts, snap.Now)
		in.Plan = &plan
		in.RepairKey = repairKey(task, dec.Action, 0)
	case domain.RecoveryRecheck:
		cr, err := r.Constraints.CheckConstraints(ctx, task, snap.Now)
		if err != nil {
			return "", err
		}
		if cr.Code != "" && domain.IsBlockingConstraint(cr.Code) {
			next := domain.NextBlockedCheck(facts, snap.Now, r.Config.BlockedCheck)
			in.NextCheckAt = &next
			in.RepairKey = repairKey(task, dec.Action, uint64(task.RowVersion))
			break
		}
		allowed := cr.Code == "" && (len(task.Unresolved) == 0 || cr.Covers || domain.RiskAuthAvailable(facts))
		plan, err := domain.ResumePlan(facts, snap.Now, allowed)
		if err != nil {
			return "", err
		}
		in.Plan = &plan
		in.RepairKey = repairKey(task, dec.Action, 0)
	case domain.RecoveryRepair:
		in.Repair = dec.Repair
		in.RepairCount = snap.Outbox.RepairCount
		in.Cooldown = r.Config.RepairCooldown
		in.MaxRepairs = r.Config.MaxRepairs
		in.RepairKey = repairKey(task, dec.Action, uint64(snap.Outbox.RepairCount))
	default:
		return "", fmt.Errorf("biz_recovery: unknown action %s", dec.Action)
	}
	out, err := r.Store.RecoverTask(ctx, in)
	if err != nil {
		// 提交确认不确定：不盲目重建计划；候选保留，下一轮按修复键核对（RCV-15）。
		return "", err
	}
	r.count(dec.Action, out)
	return out, nil
}

// expirePlan 在事务外读取 S05 安全信息；当前尝试本身也计入未决结果。仓储锁内复核版本与数据库时间。
func (r *Recoverer) expirePlan(ctx context.Context, task data_interface.TaskRecord, now time.Time) (domain.Plan, error) {
	policy, err := domain.ParseFrozenPolicy(task.RoundPolicy)
	if err != nil {
		// 无法取得策略不猜测可重发：最低恢复能力，结束为 UNKNOWN（RCV-L03）。
		policy = domain.FrozenPolicy{MaxAttempts: 1}
	}
	probe := task
	probe.Unresolved = append(append([]data_interface.UnresolvedRef(nil), task.Unresolved...),
		data_interface.UnresolvedRef{AttemptID: *task.CurrentAttemptID, RoundNo: task.RoundNo})
	cr, cerr := r.Constraints.CheckConstraints(ctx, probe, now)
	if cerr != nil {
		cr = data_interface.ConstraintResult{}
	}
	return domain.ExpirePlan(task.Facts(), domain.ExpireFacts{
		Now: now, Policy: policy, AttemptID: *task.CurrentAttemptID, Covers: cr.Covers && err == nil,
		Constraint: cr.Code, JitterNumer: stableJitter(repairKey(task, domain.RecoveryExpire, *task.CurrentAttemptID)),
		BlockedCheck: r.Config.BlockedCheck,
	})
}

// HandleDLQ 处置一条死信：只有处置已保存后调用方才确认 DLQ 消费进度（RCV-D01）。
func (r *Recoverer) HandleDLQ(ctx context.Context, messageID string, cmd domain.DeliveryCommand, cmdOK bool) (domain.DLQAction, error) {
	var snap data_interface.RecoverySnapshot
	var err error
	if cmdOK {
		snap, err = r.Store.LoadRecoverySnapshot(ctx, cmd.DeliveryID)
		if err != nil {
			cmdOK = false
		}
	}
	action := domain.DisposeDLQ(cmdOK, cmd, snap.Task.Facts(), snap.Outbox.RepairCount, r.Config.DLQAutoRepairs)
	if action == domain.DLQInvalid {
		r.Counters.ManualRequired.Add(1)
		return action, nil // 不合法或关联缺失：无任务可写，保留死信依据
	}
	if action == domain.DLQRepair {
		task := snap.Task
		if _, err := r.Store.RecoverTask(ctx, data_interface.RecoverInput{
			DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
			ExpectedRowVersion: task.RowVersion, Action: domain.RecoveryRepair, Repair: domain.RepairRepublish,
			RepairCount: snap.Outbox.RepairCount, Cooldown: r.Config.RepairCooldown, MaxRepairs: r.Config.MaxRepairs,
			RepairKey: "dlq:" + messageID + ":" + fmt.Sprint(snap.Outbox.RepairCount), Actor: r.actor(), Reason: "死信按当前指令恢复",
		}); err != nil {
			return action, err
		}
	}
	if action == domain.DLQEscalate {
		r.Counters.ManualRequired.Add(1)
	}
	if _, err := r.Store.RecordDLQ(ctx, data_interface.DLQRecord{
		DeliveryID: cmd.DeliveryID, MessageID: messageID, Action: action, Reason: string(action), Actor: r.actor(),
	}); err != nil {
		return action, err
	}
	r.Counters.DLQDisposed.Add(1)
	return action, nil
}

func (r *Recoverer) count(action domain.RecoveryAction, out data_interface.RecoveryOutcome) {
	switch out {
	case data_interface.RecoveryStale:
		r.Counters.Stale.Add(1)
		return
	case data_interface.RecoveryAlreadyApplied:
		return
	}
	r.Counters.Applied.Add(1)
	switch action {
	case domain.RecoveryExpire:
		r.Counters.Expired.Add(1)
	case domain.RecoveryRepair:
		r.Counters.Repaired.Add(1)
	}
}

func (r *Recoverer) taskTimeout() time.Duration {
	if r.Config.TaskTimeout > 0 {
		return r.Config.TaskTimeout
	}
	return 5 * time.Second
}

func (r *Recoverer) actor() string {
	if r.Config.Actor != "" {
		return r.Config.Actor
	}
	return "recovery"
}

// repairKey 由 (delivery_id, round_no, schedule_version, recovery_kind, attempt_id 或 repair_count) 组成，扫描重试沿用同一键。
func repairKey(task data_interface.TaskRecord, action domain.RecoveryAction, discriminator uint64) string {
	return fmt.Sprintf("rcv:%d:%d:%d:%s:%d", task.DeliveryID, task.RoundNo, task.ScheduleVersion, action, discriminator)
}

// stableJitter 由修复键确定，事务重试和扫描重试不另抽抖动。
func stableJitter(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return 5000 + h.Sum32()%5001
}
