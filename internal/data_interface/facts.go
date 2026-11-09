package data_interface

import "notifier/internal/domain"

// Facts 是决策用任务视图（S03 定义，S06 复用）。
func (task TaskRecord) Facts() domain.TaskFacts {
	return domain.TaskFacts{
		DeliveryID: task.DeliveryID, EventID: task.EventID, OutboxID: task.OutboxID, Status: task.Status,
		ResultCertainty: task.ResultCertainty, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
		RowVersion: task.RowVersion, RoundAttemptCount: task.RoundAttemptCount, TotalAttemptCount: task.TotalAttemptCount,
		MaxAttempts: task.MaxAttempts, RoundDeadlineAt: task.RoundDeadlineAt, ScheduledAt: task.ScheduledAt,
		NextRetryAt: task.NextRetryAt, NextCheckAt: task.NextCheckAt, RoundFinishedAt: task.RoundFinishedAt,
		CurrentAttemptID: task.CurrentAttemptID, LeaseToken: task.LeaseToken, LeaseExpiresAt: task.LeaseExpiresAt,
		BlockedReason: task.BlockedReason, ResumeStatus: task.ResumeStatus, ResumeNotBefore: task.ResumeNotBefore,
		BlockedSince: task.BlockedSince, RiskOperationID: task.RiskOperationID, RiskAttemptID: task.RiskAttemptID,
		Unresolved: ToDomainUnresolved(task.Unresolved),
	}
}

func ToDomainUnresolved(items []UnresolvedRef) []domain.UnresolvedRef {
	out := make([]domain.UnresolvedRef, len(items))
	for i, item := range items {
		out[i] = domain.UnresolvedRef{AttemptID: item.AttemptID, RoundNo: item.RoundNo}
	}
	return out
}
