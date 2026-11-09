package biz_delivery_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// mem 是投递仓储的内存替身。条件更新用行版本串行化，不连接 MySQL。
type mem struct {
	mu              sync.Mutex
	tasks           map[uint64]data_interface.TaskRecord
	now             func() time.Time
	failHistory     bool
	uncertainFinish bool
	uncertainInvoke bool
	readUnconfirmed bool
	nextAttempt     uint64
	nextOutbox      uint64
	beforeStart     func()
}

func newMem(now func() time.Time, tasks ...data_interface.TaskRecord) *mem {
	m := &mem{tasks: map[uint64]data_interface.TaskRecord{}, now: now, nextAttempt: 1, nextOutbox: 1000}
	for _, task := range tasks {
		m.tasks[task.DeliveryID] = task
	}
	return m
}

func (m *mem) LoadTask(_ context.Context, id uint64) (data_interface.TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[id]
	if !ok {
		return data_interface.TaskRecord{}, domainErr(constant.CodeNotFound, "任务不存在")
	}
	return cloneTask(task), nil
}

func (m *mem) ReadDisposition(ctx context.Context, id, attemptID uint64) (data_interface.TaskRecord, error) {
	if m.readUnconfirmed {
		return data_interface.TaskRecord{}, data_interface.ErrUnconfirmed
	}
	task, err := m.LoadTask(ctx, id)
	if err != nil {
		return data_interface.TaskRecord{}, err
	}
	if attemptID == 0 {
		return task, nil
	}
	for _, item := range task.Attempts {
		if item.AttemptID == attemptID {
			return task, nil
		}
	}
	return data_interface.TaskRecord{}, data_interface.ErrUnconfirmed
}

func (m *mem) StartAttempt(_ context.Context, in data_interface.StartAttemptInput) (data_interface.StartAttemptResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.beforeStart != nil {
		m.beforeStart()
	}
	task, ok := m.tasks[in.DeliveryID]
	if !ok {
		return data_interface.StartAttemptResult{}, domainErr(constant.CodeNotFound, "任务不存在")
	}
	now := m.now().UTC()
	if task.EventID != in.EventID || task.RoundNo != in.RoundNo || task.ScheduleVersion != in.ScheduleVersion || task.RowVersion != in.ExpectedRowVersion || task.OutboxID != in.OutboxID ||
		(task.Status != enums.TaskPending && task.Status != enums.TaskRetryWait) || task.CurrentAttemptID != nil || task.LeaseToken != "" {
		return data_interface.StartAttemptResult{}, data_interface.ErrOwnershipLost
	}
	if task.ChannelVersion != in.ExpectedChannelVersion || task.ReleaseID != in.ExpectedReleaseID || (!in.SafetyNotAfter.IsZero() && !now.Before(in.SafetyNotAfter)) {
		return data_interface.StartAttemptResult{}, data_interface.ErrPreconditionChanged
	}
	if task.ScheduledAt == nil || now.Before(*task.ScheduledAt) || !now.Before(task.RoundDeadlineAt) || task.RoundAttemptCount >= task.MaxAttempts {
		return data_interface.StartAttemptResult{}, domainErr(constant.CodeStateConflict, "任务当前不能领取")
	}
	if in.UseRiskAuthorization && (task.RiskOperationID == "" || task.RiskAttemptID != nil || task.RoundAttemptCount != 0) {
		return data_interface.StartAttemptResult{}, domainErr(constant.CodeStateConflict, "风险授权不可用")
	}
	task.RoundAttemptCount++
	task.TotalAttemptCount++
	task.RowVersion++
	attemptID := m.nextAttempt
	m.nextAttempt++
	expires := now.Add(in.LeaseTimeout)
	token := token()
	attemptNo := task.RoundAttemptCount
	task.Attempts = append(task.Attempts, data_interface.AttemptRecord{
		AttemptID: attemptID, DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, AttemptNo: attemptNo,
		OutboxID: in.OutboxID, WorkerID: in.WorkerID, Status: "PROCESSING", SendState: enums.SendUncertain, StartedAt: now,
	})
	task.Status = enums.TaskProcessing
	task.CurrentAttemptID = &attemptID
	task.LeaseToken = token
	task.LeaseExpiresAt = &expires
	task.ScheduledAt = nil
	task.NextRetryAt = nil
	task.NextCheckAt = nil
	if in.UseRiskAuthorization {
		task.RiskAttemptID = &attemptID
	}
	m.pushHistory(&task, enums.PhaseAttemptStarted, enums.TaskPending, enums.TaskProcessing, "尝试已领取", attemptID, task.OutboxID)
	if err := domain.CheckTaskTimes(timesOf(task)); err != nil {
		return data_interface.StartAttemptResult{}, err
	}
	m.tasks[task.DeliveryID] = task
	return data_interface.StartAttemptResult{
		Ownership: domain.Ownership{
			DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
			AttemptID: attemptID, LeaseToken: token, LeaseExpiresAt: expires,
		},
		AttemptNo: attemptNo, StartedAt: now, RowVersion: task.RowVersion,
		RoundAttemptCount: task.RoundAttemptCount, TotalAttemptCount: task.TotalAttemptCount, ChannelVersion: task.ChannelVersion,
	}, nil
}

func (m *mem) RecordInvocation(_ context.Context, in data_interface.RecordInvocationInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[in.Ownership.DeliveryID]
	if !ok || !ownerOK(task, in.Ownership, m.now()) {
		return data_interface.ErrOwnershipLost
	}
	for i := range task.Attempts {
		if task.Attempts[i].AttemptID == in.Ownership.AttemptID && task.Attempts[i].Status == "PROCESSING" {
			task.Attempts[i].SendState = enums.SendHTTPInvoked
			started := in.RequestStartedAt.UTC()
			task.Attempts[i].RequestStartedAt = &started
			task.Attempts[i].RequestSummary = in.RequestSummary
			m.tasks[task.DeliveryID] = task
			if m.uncertainInvoke {
				return data_interface.ErrCommitUncertain
			}
			return nil
		}
	}
	return data_interface.ErrOwnershipLost
}

func (m *mem) FinishAttemptAndPlan(_ context.Context, in data_interface.FinishInput) (data_interface.TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[in.Ownership.DeliveryID]
	if !ok {
		return data_interface.TaskRecord{}, domainErr(constant.CodeNotFound, "任务不存在")
	}
	for _, item := range task.Attempts {
		if item.AttemptID == in.Ownership.AttemptID && item.Status == "FINISHED" {
			return cloneTask(task), nil
		}
	}
	if !ownerOK(task, in.Ownership, m.now()) {
		return data_interface.TaskRecord{}, data_interface.ErrOwnershipLost
	}
	if m.failHistory {
		return data_interface.TaskRecord{}, domainErr(constant.CodeDependencyUnavailable, "历史写入失败")
	}
	if err := domain.CheckTaskTimes(planTimes(in.Plan)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	now := m.now().UTC()
	for i := range task.Attempts {
		if task.Attempts[i].AttemptID == in.Ownership.AttemptID {
			task.Attempts[i].Status = "FINISHED"
			task.Attempts[i].SendState = in.SendState
			task.Attempts[i].Outcome = in.Outcome
			task.Attempts[i].ResultCertainty = in.ResultCertainty
			task.Attempts[i].ErrorCode = in.ErrorCode
			task.Attempts[i].ErrorReason = in.ErrorReason
			task.Attempts[i].HTTPDuration = in.HTTPDuration
			task.Attempts[i].AttemptDuration = in.AttemptDuration
			task.Attempts[i].FinishedAt = &now
		}
	}
	apply(&task, in.Plan, &in.Ownership.AttemptID, now)
	m.pushHistory(&task, enums.PhaseAttemptFinished, enums.TaskProcessing, in.Plan.Status, in.Plan.Reason, in.Ownership.AttemptID, task.OutboxID)
	if phase := extraPhase(in.Plan); phase != "" {
		m.pushHistory(&task, phase, enums.TaskProcessing, in.Plan.Status, in.Plan.Reason, in.Ownership.AttemptID, task.OutboxID)
	}
	if in.Plan.BumpSchedule {
		m.supersede(&task, in.Plan, now)
	}
	if err := domain.CheckTaskTimes(timesOf(task)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	m.tasks[task.DeliveryID] = task
	if m.uncertainFinish {
		return cloneTask(task), data_interface.ErrCommitUncertain
	}
	return cloneTask(task), nil
}

func (m *mem) WaitOrBlock(_ context.Context, in data_interface.WaitOrBlockInput) (data_interface.TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[in.DeliveryID]
	if !ok {
		return data_interface.TaskRecord{}, domainErr(constant.CodeNotFound, "任务不存在")
	}
	if task.ScheduleVersion != in.ScheduleVersion || task.RowVersion != in.ExpectedRowVersion || task.CurrentAttemptID != nil ||
		(task.Status != enums.TaskPending && task.Status != enums.TaskRetryWait) {
		return data_interface.TaskRecord{}, data_interface.ErrOwnershipLost
	}
	if m.failHistory {
		return data_interface.TaskRecord{}, domainErr(constant.CodeDependencyUnavailable, "历史写入失败")
	}
	if err := domain.CheckTaskTimes(planTimes(in.Plan)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	from := task.Status
	now := m.now().UTC()
	apply(&task, in.Plan, nil, now)
	phase := extraPhase(in.Plan)
	if phase == "" {
		phase = enums.PhaseRoundClosed
	}
	m.pushHistory(&task, phase, from, in.Plan.Status, in.Plan.Reason, 0, task.OutboxID)
	if in.Plan.BumpSchedule {
		m.supersede(&task, in.Plan, now)
	}
	if err := domain.CheckTaskTimes(timesOf(task)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	m.tasks[task.DeliveryID] = task
	if m.uncertainFinish {
		return cloneTask(task), data_interface.ErrCommitUncertain
	}
	return cloneTask(task), nil
}

func (m *mem) ResumeBlocked(_ context.Context, in data_interface.ResumeInput) (data_interface.TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[in.DeliveryID]
	if !ok {
		return data_interface.TaskRecord{}, domainErr(constant.CodeNotFound, "任务不存在")
	}
	if task.Status != enums.TaskBlocked || task.RowVersion != in.ExpectedRowVersion || task.ScheduleVersion != in.ScheduleVersion || task.RoundNo != in.RoundNo {
		return data_interface.TaskRecord{}, domainErr(constant.CodeStateConflict, "阻塞恢复条件不匹配")
	}
	if err := domain.CheckTaskTimes(planTimes(in.Plan)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	now := m.now().UTC()
	apply(&task, in.Plan, nil, now)
	phase := enums.PhaseUnblocked
	if in.Plan.Status == enums.TaskFailed || in.Plan.Status == enums.TaskUnknown {
		phase = enums.PhaseRoundClosed
	}
	m.pushHistory(&task, phase, enums.TaskBlocked, in.Plan.Status, in.Plan.Reason, 0, 0)
	m.supersede(&task, in.Plan, now)
	if err := domain.CheckTaskTimes(timesOf(task)); err != nil {
		return data_interface.TaskRecord{}, err
	}
	m.tasks[task.DeliveryID] = task
	return cloneTask(task), nil
}

func (m *mem) AppendLateEvidence(_ context.Context, in data_interface.LateEvidenceInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[in.DeliveryID]
	if !ok {
		return domainErr(constant.CodeNotFound, "任务不存在")
	}
	status := task.Status
	attemptSeen := task.CurrentAttemptID
	m.pushHistory(&task, enums.PhaseLateResponse, status, status, in.Reason, in.AttemptID, task.OutboxID)
	if task.Status != status || task.CurrentAttemptID != attemptSeen {
		return domainErr(constant.CodeInternal, "迟到证据改写了任务")
	}
	m.tasks[task.DeliveryID] = task
	return nil
}

func (m *mem) supersede(task *data_interface.TaskRecord, plan domain.Plan, now time.Time) {
	if plan.Status != enums.TaskPending && plan.Status != enums.TaskRetryWait {
		task.OutboxID = 0
		return
	}
	m.nextOutbox++
	task.OutboxID = m.nextOutbox
	_ = now
}

func (m *mem) pushHistory(task *data_interface.TaskRecord, phase enums.HistoryPhase, from, to enums.TaskStatus, reason string, attemptID, outboxID uint64) {
	task.HistorySeq++
	task.History = append(task.History, data_interface.HistoryRecord{
		Seq: task.HistorySeq, Phase: phase, From: from, To: to, Reason: reason,
		RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion, AttemptID: attemptID, OutboxID: outboxID,
		OccurredAt: m.now().UTC(),
	})
}

func apply(task *data_interface.TaskRecord, plan domain.Plan, attemptID *uint64, now time.Time) {
	task.Status = plan.Status
	task.ResultCertainty = plan.ResultCertainty
	if plan.LastCertainty != "" {
		task.LastCertainty = plan.LastCertainty
	}
	task.ScheduledAt = copyTime(plan.ScheduledAt)
	task.NextRetryAt = copyTime(plan.NextRetryAt)
	task.NextCheckAt = copyTime(plan.NextCheckAt)
	task.RoundFinishedAt = copyTime(plan.RoundFinishedAt)
	task.ResumeStatus = plan.ResumeStatus
	task.ResumeNotBefore = copyTime(plan.ResumeNotBefore)
	task.CurrentAttemptID = nil
	task.LeaseToken = ""
	task.LeaseExpiresAt = nil
	if attemptID != nil {
		task.LastAttemptID = attemptID
	}
	if plan.ClearLastError {
		task.LastErrorCode, task.LastErrorReason = "", ""
	} else if plan.ErrorCode != "" {
		task.LastErrorCode = plan.ErrorCode
		task.LastErrorReason = plan.Reason
	}
	if plan.Status == enums.TaskBlocked {
		task.BlockedReason = plan.Reason
		if task.BlockedSince == nil {
			task.BlockedSince = copyTime(&now)
		}
	} else {
		task.BlockedReason = ""
		task.BlockedSince = nil
		task.ResumeStatus = ""
		task.ResumeNotBefore = nil
	}
	if plan.Status == enums.TaskSucceeded || plan.Unresolved != nil {
		task.Unresolved = nil
		for _, item := range plan.Unresolved {
			task.Unresolved = append(task.Unresolved, data_interface.UnresolvedRef{AttemptID: item.AttemptID, RoundNo: item.RoundNo})
		}
	}
	task.RowVersion++
	if plan.BumpSchedule {
		task.ScheduleVersion++
	}
}

func ownerOK(task data_interface.TaskRecord, own domain.Ownership, now time.Time) bool {
	return task.Status == enums.TaskProcessing && task.RoundNo == own.RoundNo && task.ScheduleVersion == own.ScheduleVersion &&
		task.CurrentAttemptID != nil && *task.CurrentAttemptID == own.AttemptID && task.LeaseToken == own.LeaseToken &&
		task.LeaseExpiresAt != nil && now.Before(*task.LeaseExpiresAt)
}

func timesOf(task data_interface.TaskRecord) domain.TaskTimes {
	return domain.TaskTimes{
		Status: task.Status, ScheduledAt: task.ScheduledAt, NextRetryAt: task.NextRetryAt, NextCheckAt: task.NextCheckAt,
		RoundFinishedAt: task.RoundFinishedAt, CurrentAttemptID: task.CurrentAttemptID, LeaseToken: task.LeaseToken,
		LeaseExpiresAt: task.LeaseExpiresAt, BlockedReason: task.BlockedReason,
	}
}

func planTimes(plan domain.Plan) domain.TaskTimes {
	times := domain.TaskTimes{
		Status: plan.Status, ScheduledAt: plan.ScheduledAt, NextRetryAt: plan.NextRetryAt, NextCheckAt: plan.NextCheckAt,
		RoundFinishedAt: plan.RoundFinishedAt,
	}
	if plan.Status == enums.TaskBlocked {
		times.BlockedReason = plan.Reason
		if times.BlockedReason == "" {
			times.BlockedReason = "blocked"
		}
	}
	return times
}

func extraPhase(plan domain.Plan) enums.HistoryPhase {
	switch plan.Status {
	case enums.TaskRetryWait:
		if plan.Kind == enums.ActionWait {
			return enums.PhaseWaitPlanned
		}
		return enums.PhaseRetryPlanned
	case enums.TaskBlocked:
		return enums.PhaseBlocked
	default:
		return ""
	}
}

func cloneTask(task data_interface.TaskRecord) data_interface.TaskRecord {
	task.Attempts = append([]data_interface.AttemptRecord(nil), task.Attempts...)
	task.History = append([]data_interface.HistoryRecord(nil), task.History...)
	task.Unresolved = append([]data_interface.UnresolvedRef(nil), task.Unresolved...)
	task.ScheduledAt = copyTime(task.ScheduledAt)
	task.NextRetryAt = copyTime(task.NextRetryAt)
	task.NextCheckAt = copyTime(task.NextCheckAt)
	task.LeaseExpiresAt = copyTime(task.LeaseExpiresAt)
	task.RoundFinishedAt = copyTime(task.RoundFinishedAt)
	task.ResumeNotBefore = copyTime(task.ResumeNotBefore)
	task.BlockedSince = copyTime(task.BlockedSince)
	if task.CurrentAttemptID != nil {
		id := *task.CurrentAttemptID
		task.CurrentAttemptID = &id
	}
	if task.LastAttemptID != nil {
		id := *task.LastAttemptID
		task.LastAttemptID = &id
	}
	if task.RiskAttemptID != nil {
		id := *task.RiskAttemptID
		task.RiskAttemptID = &id
	}
	return task
}

func copyTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := t.UTC()
	return &v
}

func token() string {
	var buf [32]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

func domainErr(code, message string) error {
	err, _ := domain.NewError(code, message, "", nil)
	return err
}
