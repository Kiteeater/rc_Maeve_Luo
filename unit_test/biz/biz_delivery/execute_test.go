package biz_delivery_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"notifier/internal/biz/biz_delivery"
	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

func TestDLV01_IndependentTargets(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	okTask := task(1, pol(8, 5000, 5000, 86400), now)
	noTask := task(2, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, okTask, noTask)
	rt := &rt{view: receivedView(), prep: readyPrep(true)}
	rt.send = func(context.Context, data_interface.TaskRecord) (data_interface.SendResponse, error) {
		return data_interface.SendResponse{HTTPStatus: 200, BodySummary: "ok"}, nil
	}
	uc := newUC(store, rt, &now)
	if _, _, err := uc.Execute(context.Background(), commandOf(okTask)); err != nil {
		t.Fatal(err)
	}
	rt.view = data_interface.Interpreted{Outcome: enums.OutcomeRetryableReject, Retryable: true, ErrorCode: constant.ErrProviderRetryable}
	if _, got, err := uc.Execute(context.Background(), commandOf(noTask)); err != nil {
		t.Fatal(err)
	} else if got.Status != enums.TaskRetryWait || got.RoundAttemptCount != 1 {
		t.Fatalf("rejected = %s count %d", got.Status, got.RoundAttemptCount)
	}
	before := rt.sends.Load()
	if _, got, err := uc.Execute(context.Background(), commandOf(okTask)); err != nil {
		t.Fatal(err)
	} else if got.Status != enums.TaskSucceeded || got.RoundAttemptCount != 1 || got.ScheduledAt != nil || got.NextRetryAt != nil {
		t.Fatalf("success target = %+v", got.Status)
	}
	if _, _, err := uc.Execute(context.Background(), commandOf(noTask)); err != nil {
		t.Fatal(err)
	}
	if rt.sends.Load() != before {
		t.Fatalf("redelivery sent again: %d -> %d", before, rt.sends.Load())
	}
}

func TestDLV02_ConcurrentInstructionCreatesOneAttempt(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	rt := &rt{view: receivedView(), prep: readyPrep(true), send: okSend}
	uc := newUC(store, rt, &now)
	cmd := commandOf(item)
	var wg sync.WaitGroup
	var sendsOK atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			step, _, err := uc.Execute(context.Background(), cmd)
			if err == nil && step.ACK {
				sendsOK.Add(1)
			}
		}()
	}
	wg.Wait()
	got, err := store.LoadTask(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.RoundAttemptCount != 1 || len(got.Attempts) != 1 || rt.sends.Load() != 1 {
		t.Fatalf("attempts %d sends %d", got.RoundAttemptCount, rt.sends.Load())
	}
}

func TestDLV03_WaitDoesNotConsumeAttempts(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	clock := now
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return clock }, item)
	n := 0
	rt := &rt{prep: func(in data_interface.PrepareRequest) data_interface.PrepareResponse {
		n++
		if n < 3 {
			return data_interface.PrepareResponse{Kind: domain.PrepareWait, ErrorCode: constant.ErrRateLimitWait, RateLimitUntil: clock.Add(time.Minute), ChannelVersion: in.Task.ChannelVersion, ReleaseID: in.Task.ReleaseID, IdempotencyCovers: true}
		}
		return readyPrep(true)(in)
	}, view: receivedView(), send: okSend}
	uc := newUC(store, rt, &clock)
	for i := 0; i < 2; i++ {
		current, _ := store.LoadTask(context.Background(), 1)
		step, got, err := uc.Execute(context.Background(), commandOf(current))
		if err != nil || !step.ACK || got.RoundAttemptCount != 0 || got.Status != enums.TaskRetryWait {
			t.Fatalf("wait %d: %+v %v count %d", i, step, err, got.RoundAttemptCount)
		}
		clock = *got.ScheduledAt
	}
	current, _ := store.LoadTask(context.Background(), 1)
	_, got, err := uc.Execute(context.Background(), commandOf(current))
	if err != nil || got.RoundAttemptCount != 1 || len(got.Attempts) != 1 {
		t.Fatalf("after wait = count %d err %v", got.RoundAttemptCount, err)
	}
}

func TestDLV05_BuildFailureKeepsAttempt(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	rt := &rt{prep: readyPrep(true), buildErr: errBuild, view: receivedView(), send: okSend}
	uc := newUC(store, rt, &now)
	step, got, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil || !step.ACK {
		t.Fatal(err, step)
	}
	if got.RoundAttemptCount != 1 || rt.sends.Load() != 0 || len(got.Attempts) != 1 {
		t.Fatalf("count %d sends %d", got.RoundAttemptCount, rt.sends.Load())
	}
	if got.Attempts[0].SendState != enums.SendNotSent || got.Attempts[0].HTTPDuration != nil || got.Attempts[0].Outcome != enums.OutcomeLocalFailure {
		t.Fatalf("attempt = %+v", got.Attempts[0])
	}
}

func TestDLV06_AckFailureThenRedeliveryDoesNotSend(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	rt := &rt{prep: readyPrep(true), view: receivedView(), send: okSend}
	uc := newUC(store, rt, &now)
	cmd := commandOf(item)
	step, _, err := uc.Execute(context.Background(), cmd)
	if err != nil || !step.ACK || rt.sends.Load() != 1 {
		t.Fatal(err, step, rt.sends.Load())
	}
	step, got, err := uc.Execute(context.Background(), cmd)
	if err != nil || !step.ACK || rt.sends.Load() != 1 || got.Status != enums.TaskSucceeded {
		t.Fatalf("redelivery sends %d status %s err %v", rt.sends.Load(), got.Status, err)
	}
}

func TestDLV07_UnconfirmedFinishDoesNotAckOrResend(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	store.uncertainFinish = true
	store.readUnconfirmed = true
	rt := &rt{prep: readyPrep(true), view: receivedView(), send: okSend}
	uc := newUC(store, rt, &now)
	step, _, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil {
		t.Fatal(err)
	}
	if step.ACK || !step.Unconfirmed || rt.sends.Load() != 1 {
		t.Fatalf("step %+v sends %d", step, rt.sends.Load())
	}
}

func TestDLV08_LateResultDoesNotOverride(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	oldID := uint64(7)
	exp := now.Add(time.Minute)
	item.Status = enums.TaskProcessing
	item.CurrentAttemptID = &oldID
	item.LeaseToken = "stale"
	item.LeaseExpiresAt = &exp
	item.ScheduledAt = nil
	item.RoundAttemptCount = 1
	item.Attempts = []data_interface.AttemptRecord{{AttemptID: oldID, Status: "PROCESSING", SendState: enums.SendHTTPInvoked}}
	store := newMem(func() time.Time { return now }, item)
	next := uint64(99)
	store.mu.Lock()
	cur := store.tasks[1]
	cur.CurrentAttemptID = &next
	cur.LeaseToken = "new"
	store.tasks[1] = cur
	store.mu.Unlock()
	uc := newUC(store, &rt{prep: readyPrep(true), view: receivedView(), send: okSend}, &now)
	old := domain.Ownership{DeliveryID: 1, RoundNo: 1, ScheduleVersion: 1, AttemptID: oldID, LeaseToken: "stale", LeaseExpiresAt: exp}
	step, got, err := uc.ApplyResult(context.Background(), old, biz_delivery.ResultInput{
		SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived, CoversBusinessKey: true,
		RoundAttemptCount: 1, RoundNo: 1,
	})
	if err != nil || !step.ACK {
		t.Fatal(err, step)
	}
	if got.CurrentAttemptID == nil || *got.CurrentAttemptID != 99 || got.Status != enums.TaskProcessing {
		t.Fatalf("task moved: %+v", got)
	}
	if !hasPhase(got, enums.PhaseLateResponse) {
		t.Fatal("late evidence missing")
	}
}

func TestDLV09_EarlyBlockAndTerminalSchedules(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	item.ScheduledAt = &later
	store := newMem(func() time.Time { return now }, item)
	box := &sched{}
	rt := &rt{prep: func(in data_interface.PrepareRequest) data_interface.PrepareResponse {
		return data_interface.PrepareResponse{Kind: domain.PrepareBlock, ErrorCode: constant.ErrChannelDisabled, Reason: "disabled", ChannelVersion: in.Task.ChannelVersion, ReleaseID: in.Task.ReleaseID}
	}}
	uc := newUC(store, rt, &now)
	uc.Schedule = box
	step, got, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil || !step.ACK || box.n != 1 || got.RoundAttemptCount != 0 {
		t.Fatalf("early %+v n %d err %v", step, box.n, err)
	}
	item.ScheduledAt = &now
	store.mu.Lock()
	store.tasks[1] = item
	store.mu.Unlock()
	_, blocked, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil || blocked.Status != enums.TaskBlocked || blocked.NextCheckAt == nil || blocked.ScheduledAt != nil || blocked.NextRetryAt != nil || blocked.BlockedReason == "" {
		t.Fatalf("blocked = %+v %v", blocked.Status, err)
	}
	done := task(2, pol(8, 5000, 5000, 86400), now)
	store.mu.Lock()
	store.tasks[2] = done
	store.mu.Unlock()
	rt.prep = readyPrep(true)
	rt.view = receivedView()
	rt.send = okSend
	_, terminal, err := uc.Execute(context.Background(), commandOf(done))
	if err != nil || terminal.Status != enums.TaskSucceeded || terminal.ScheduledAt != nil || terminal.NextRetryAt != nil || terminal.NextCheckAt != nil || terminal.LeaseToken != "" || terminal.RoundFinishedAt == nil {
		t.Fatalf("terminal = %+v err %v", terminal, err)
	}
}

func TestDLV13_NotSentConstraintBlocksInOneDisposition(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	calls := 0
	rt := &rt{prep: func(in data_interface.PrepareRequest) data_interface.PrepareResponse {
		calls++
		if calls == 1 {
			return readyPrep(true)(in)
		}
		return data_interface.PrepareResponse{Kind: domain.PrepareBlock, ErrorCode: constant.ErrChannelDisabled, Reason: "disabled", ChannelVersion: in.Task.ChannelVersion, ReleaseID: in.Task.ReleaseID}
	}, send: okSend}
	uc := newUC(store, rt, &now)
	step, got, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil || !step.ACK {
		t.Fatal(err, step)
	}
	if got.Status != enums.TaskBlocked || got.RoundAttemptCount != 1 || got.LeaseToken != "" || got.CurrentAttemptID != nil || got.ScheduledAt != nil || got.NextRetryAt != nil || got.NextCheckAt == nil || got.ResumeStatus != string(enums.TaskRetryWait) || got.ResumeNotBefore == nil || got.ScheduleVersion != 2 {
		t.Fatalf("blocked = %+v", got)
	}
	if rt.sends.Load() != 0 || got.Attempts[0].SendState != enums.SendNotSent || got.Attempts[0].Outcome != enums.OutcomeLocalFailure || got.Attempts[0].HTTPDuration != nil || got.Attempts[0].ResultCertainty != enums.CertaintyNotReceived {
		t.Fatalf("attempt %+v sends %d", got.Attempts[0], rt.sends.Load())
	}
	if !hasPhase(got, enums.PhaseAttemptFinished) || !hasPhase(got, enums.PhaseBlocked) {
		t.Fatal("history missing")
	}
}

func TestDLV14_ResumeOnceFromBlocked(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	earliest := now.Add(30 * time.Minute)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	item.Status = enums.TaskBlocked
	item.BlockedReason = "disabled"
	item.BlockedSince = &now
	item.NextCheckAt = &now
	item.ScheduledAt = nil
	item.ResumeStatus = string(enums.TaskRetryWait)
	item.ResumeNotBefore = &earliest
	item.ScheduleVersion = 2
	item.RowVersion = 3
	item.RoundAttemptCount = 1
	item.TotalAttemptCount = 4
	store := newMem(func() time.Time { return now }, item)
	rt := &rt{prep: readyPrep(true), send: okSend}
	uc := newUC(store, rt, &now)
	var wg sync.WaitGroup
	var wins atomic.Int32
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, got, err := uc.Resume(context.Background(), 1)
			if err == nil && got.Status == enums.TaskRetryWait {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	got, err := store.LoadTask(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if wins.Load() < 1 || len(got.History) != 1 || got.Status != enums.TaskRetryWait || got.ScheduleVersion != 3 || got.RoundAttemptCount != 1 || got.RoundNo != 1 || got.ScheduledAt == nil || got.ScheduledAt.Before(earliest) || got.LeaseToken != "" {
		t.Fatalf("wins %d task %+v", wins.Load(), got)
	}
	_, again, err := uc.Resume(context.Background(), 1)
	if err != nil || again.ScheduleVersion != 3 || len(again.History) != 1 {
		t.Fatalf("second resume %+v hist %d err %v", again.ScheduleVersion, len(again.History), err)
	}
	stale := commandOf(item)
	step, _, err := uc.Execute(context.Background(), stale)
	if err != nil || !step.ACK || rt.sends.Load() != 0 {
		t.Fatalf("old command sent %d %+v %v", rt.sends.Load(), step, err)
	}
}

func TestDLV16_HistoryFailureRollsBackAndUnconfirmedIsNotAcked(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	store.failHistory = true
	rt := &rt{prep: readyPrep(true), view: receivedView(), send: okSend}
	uc := newUC(store, rt, &now)
	step, _, err := uc.Execute(context.Background(), commandOf(item))
	if err == nil || step.ACK || rt.sends.Load() != 1 {
		t.Fatalf("rollback step %+v err %v sends %d", step, err, rt.sends.Load())
	}
	got, _ := store.LoadTask(context.Background(), 1)
	if got.Status != enums.TaskProcessing || got.Attempts[0].Status != "PROCESSING" || hasPhase(got, enums.PhaseAttemptFinished) {
		t.Fatalf("rolled back badly %+v", got)
	}
	store.failHistory = false
	store.uncertainFinish = true
	store.readUnconfirmed = true
	hist := len(got.History)
	step, _, err = uc.ApplyResult(context.Background(), domain.Ownership{
		DeliveryID: 1, RoundNo: 1, ScheduleVersion: got.ScheduleVersion, AttemptID: *got.CurrentAttemptID,
		LeaseToken: got.LeaseToken, LeaseExpiresAt: *got.LeaseExpiresAt,
	}, biz_delivery.ResultInput{SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived, CoversBusinessKey: true, RoundAttemptCount: 1, RoundNo: 1})
	if err != nil || step.ACK || !step.Unconfirmed {
		t.Fatal(err, step)
	}
	again, _ := store.LoadTask(context.Background(), 1)
	if len(again.History) != hist+1 || !hasPhase(again, enums.PhaseAttemptFinished) {
		t.Fatalf("history %d -> %d", hist, len(again.History))
	}
	step, _, err = uc.ApplyResult(context.Background(), domain.Ownership{
		DeliveryID: 1, RoundNo: 1, ScheduleVersion: 1, AttemptID: *got.CurrentAttemptID, LeaseToken: got.LeaseToken, LeaseExpiresAt: *got.LeaseExpiresAt,
	}, biz_delivery.ResultInput{SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived, CoversBusinessKey: true, RoundAttemptCount: 1, RoundNo: 1})
	_ = step
	_ = err
	final, _ := store.LoadTask(context.Background(), 1)
	if len(final.History) != len(again.History) || rt.sends.Load() != 1 {
		t.Fatalf("duplicated history %d sends %d", len(final.History), rt.sends.Load())
	}
}

func TestDLV17_InvokedCallIsNotRewrittenAsNotSent(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	rt := &rt{prep: readyPrep(true), view: data_interface.Interpreted{Outcome: enums.OutcomeUncertain, ErrorCode: constant.ErrConnection}, send: func(context.Context, data_interface.TaskRecord) (data_interface.SendResponse, error) {
		return data_interface.SendResponse{ErrorCode: constant.ErrConnection, TransportErr: "reset"}, nil
	}}
	uc := newUC(store, rt, &now)
	_, got, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Attempts) != 1 || got.Attempts[0].SendState == enums.SendNotSent || got.Attempts[0].SendState != enums.SendHTTPInvoked {
		t.Fatalf("attempt %+v", got.Attempts[0])
	}
	if got.ResultCertainty != enums.CertaintyUncertain {
		t.Fatalf("certainty %s", got.ResultCertainty)
	}
}

func TestDLV18_BlockBeforeFirstAttemptKeepsRiskAuth(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	item.RiskOperationID = "op-risk"
	item.Unresolved = []data_interface.UnresolvedRef{{AttemptID: 3, RoundNo: 1}}
	item.ResultCertainty = enums.CertaintyUncertain
	store := newMem(func() time.Time { return now }, item)
	n := 0
	rt := &rt{prep: func(in data_interface.PrepareRequest) data_interface.PrepareResponse {
		n++
		if n == 1 {
			return data_interface.PrepareResponse{Kind: domain.PrepareBlock, ErrorCode: constant.ErrChannelDisabled, Reason: "disabled", ChannelVersion: in.Task.ChannelVersion, ReleaseID: in.Task.ReleaseID}
		}
		return readyPrep(false)(in)
	}, view: data_interface.Interpreted{Outcome: enums.OutcomeUncertain}, send: okSend}
	uc := newUC(store, rt, &now)
	_, blocked, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil || blocked.Status != enums.TaskBlocked || blocked.RoundAttemptCount != 0 || blocked.RiskAttemptID != nil {
		t.Fatalf("blocked %+v %v", blocked, err)
	}
	_, resumed, err := uc.Resume(context.Background(), 1)
	if err != nil || resumed.RoundAttemptCount != 0 || resumed.RiskAttemptID != nil || resumed.RoundNo != 1 {
		t.Fatalf("resumed %+v %v", resumed, err)
	}
	_, started, err := uc.Execute(context.Background(), commandOf(resumed))
	if err != nil || started.RoundAttemptCount != 1 || started.RiskAttemptID == nil {
		t.Fatalf("started %+v %v", started, err)
	}
}

func TestDLV19_StaleReadyAndShortLeaseDoNotSend(t *testing.T) {
	t.Run("version", func(t *testing.T) {
		now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		item := task(1, pol(8, 5000, 5000, 86400), now)
		store := newMem(func() time.Time { return now }, item)
		store.beforeStart = func() {
			cur := store.tasks[1]
			if cur.ChannelVersion == 1 {
				cur.ChannelVersion = 2
				store.tasks[1] = cur
			}
		}
		var built []uint32
		rt := &rt{prep: func(in data_interface.PrepareRequest) data_interface.PrepareResponse {
			out := readyPrep(true)(in)
			out.ChannelVersion = in.Task.ChannelVersion
			return out
		}, view: receivedView(), send: okSend, onBuild: func(prep data_interface.PrepareResponse) { built = append(built, prep.ChannelVersion) }}
		uc := newUC(store, rt, &now)
		_, got, err := uc.Execute(context.Background(), commandOf(item))
		if err != nil || rt.sends.Load() != 1 || len(built) != 1 || built[0] != 2 || got.ChannelVersion != 2 {
			t.Fatalf("sends %d built %v err %v", rt.sends.Load(), built, err)
		}
	})
	t.Run("lease", func(t *testing.T) {
		now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		item := task(1, pol(8, 5000, 5000, 86400), now)
		store := newMem(func() time.Time { return now }, item)
		rt := &rt{prep: readyPrep(true), send: okSend, view: receivedView()}
		uc := newUC(store, rt, &now)
		uc.Config.LeaseTimeout = time.Second
		uc.Config.ResultReserve = 5 * time.Second
		_, got, err := uc.Execute(context.Background(), commandOf(item))
		if err != nil || rt.sends.Load() != 0 || got.RoundAttemptCount != 1 || got.Attempts[0].SendState != enums.SendNotSent || got.Attempts[0].HTTPDuration != nil {
			t.Fatalf("%+v sends %d err %v", got.Attempts, rt.sends.Load(), err)
		}
	})
}

func TestDLV20_UnconfirmedInvocationDoesNotSend(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	store.uncertainInvoke = true
	rt := &rt{prep: readyPrep(true), view: receivedView(), send: okSend}
	uc := newUC(store, rt, &now)
	step, _, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil || step.ACK || rt.sends.Load() != 0 {
		t.Fatalf("%+v sends %d err %v", step, rt.sends.Load(), err)
	}
	got, _ := store.LoadTask(context.Background(), 1)
	if got.Attempts[0].SendState != enums.SendHTTPInvoked || !domain.InvocationIsNotReceipt(got.Attempts[0].SendState) || got.ResultCertainty == enums.CertaintyReceived {
		t.Fatalf("attempt %+v certainty %s", got.Attempts[0], got.ResultCertainty)
	}
}

func TestDLV21_ExecutionUsesFrozenPolicy(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(4, 8000, 60000, 86400), now)
	item.MaxAttempts = 4
	store := newMem(func() time.Time { return now }, item)
	numer := uint32(5000)
	calls := 0
	rt := &rt{prep: readyPrep(true), view: data_interface.Interpreted{Outcome: enums.OutcomeRetryableReject, Retryable: true, ErrorCode: constant.ErrProviderRetryable}, send: okSend}
	uc := newUC(store, rt, &now)
	uc.Jitter = func() (uint32, error) {
		calls++
		return numer, nil
	}
	_, got, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil {
		t.Fatal(err)
	}
	// 一次已计次尝试：8000ms * 5000/10000 = 4s。冻结的 max_attempts=4 仍安排重试，不改用启动默认。
	if calls != 1 || got.Status != enums.TaskRetryWait || got.ScheduledAt == nil || !got.ScheduledAt.Equal(now.Add(4*time.Second)) || got.MaxAttempts != 4 {
		t.Fatalf("status %s at %v calls %d max %d", got.Status, got.ScheduledAt, calls, got.MaxAttempts)
	}
	numer = 10000
	_, again, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil || again.ScheduledAt == nil || !again.ScheduledAt.Equal(*got.ScheduledAt) || calls != 1 || rt.sends.Load() != 1 {
		t.Fatalf("recomputed at %v calls %d sends %d err %v", again.ScheduledAt, calls, rt.sends.Load(), err)
	}
}

func TestC02_RecoveredTaskRejectsOldResult(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	old := domain.Ownership{DeliveryID: 1, RoundNo: 1, ScheduleVersion: 1, AttemptID: 1, LeaseToken: "old", LeaseExpiresAt: now.Add(-time.Second)}
	exp := now.Add(time.Minute)
	current := uint64(2)
	store.mu.Lock()
	cur := store.tasks[1]
	cur.Status = enums.TaskProcessing
	cur.CurrentAttemptID = &current
	cur.LeaseToken = "new"
	cur.LeaseExpiresAt = &exp
	cur.ScheduledAt = nil
	cur.RoundAttemptCount = 2
	store.tasks[1] = cur
	store.mu.Unlock()
	uc := newUC(store, &rt{prep: readyPrep(true)}, &now)
	step, got, err := uc.ApplyResult(context.Background(), old, biz_delivery.ResultInput{
		SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived, CoversBusinessKey: true, RoundAttemptCount: 1, RoundNo: 1,
	})
	if err != nil || !step.ACK {
		t.Fatal(err, step)
	}
	if got.RoundAttemptCount != 2 || got.CurrentAttemptID == nil || *got.CurrentAttemptID != 2 || !hasPhase(got, enums.PhaseLateResponse) {
		t.Fatalf("advanced %+v", got)
	}
}

func TestC03_ConfirmedReadDoesNotDuplicate(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	item := task(1, pol(8, 5000, 5000, 86400), now)
	store := newMem(func() time.Time { return now }, item)
	store.uncertainFinish = true
	rt := &rt{prep: readyPrep(true), view: receivedView(), send: okSend}
	uc := newUC(store, rt, &now)
	step, first, err := uc.Execute(context.Background(), commandOf(item))
	if err != nil || !step.ACK || rt.sends.Load() != 1 {
		t.Fatal(err, step, rt.sends.Load())
	}
	hist := len(first.History)
	attempts := len(first.Attempts)
	own := domain.Ownership{DeliveryID: 1, RoundNo: 1, ScheduleVersion: 1, AttemptID: first.Attempts[0].AttemptID, LeaseToken: "gone", LeaseExpiresAt: now.Add(time.Minute)}
	step, second, err := uc.ApplyResult(context.Background(), own, biz_delivery.ResultInput{
		SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived, CoversBusinessKey: true, RoundAttemptCount: 1, RoundNo: 1,
	})
	if err != nil || !step.ACK || len(second.History) != hist || len(second.Attempts) != attempts || rt.sends.Load() != 1 {
		t.Fatalf("hist %d/%d sends %d err %v", hist, len(second.History), rt.sends.Load(), err)
	}
}

type rt struct {
	prep     func(data_interface.PrepareRequest) data_interface.PrepareResponse
	send     func(context.Context, data_interface.TaskRecord) (data_interface.SendResponse, error)
	view     data_interface.Interpreted
	buildErr error
	onBuild  func(data_interface.PrepareResponse)
	sends    atomic.Int32
}

func (r *rt) Prepare(_ context.Context, in data_interface.PrepareRequest) (data_interface.PrepareResponse, error) {
	if r.prep != nil {
		return r.prep(in), nil
	}
	return readyPrep(true)(in), nil
}

func (r *rt) Build(_ context.Context, task data_interface.TaskRecord, _ domain.Ownership, prep data_interface.PrepareResponse) (data_interface.BuiltRequest, error) {
	if r.onBuild != nil {
		r.onBuild(prep)
	}
	if r.buildErr != nil {
		return data_interface.BuiltRequest{}, r.buildErr
	}
	return data_interface.BuiltRequest{Method: "POST", TargetPath: "/n", Summary: string(task.PayloadCanonical), Size: 2}, nil
}

func (r *rt) SendOnce(ctx context.Context, _ data_interface.BuiltRequest) (data_interface.SendResponse, error) {
	r.sends.Add(1)
	if r.send != nil {
		return r.send(ctx, data_interface.TaskRecord{})
	}
	return data_interface.SendResponse{HTTPStatus: 200}, nil
}

func (r *rt) Interpret(context.Context, data_interface.TaskRecord, data_interface.BuiltRequest, data_interface.SendResponse) (data_interface.Interpreted, error) {
	return r.view, nil
}

type sched struct {
	n  int
	at time.Time
}

func (s *sched) ScheduleOriginal(_ context.Context, _ domain.DeliveryCommand, notBefore time.Time) error {
	s.n++
	s.at = notBefore
	return nil
}

func newUC(store *mem, runtime data_interface.ProviderRuntime, now *time.Time) *biz_delivery.UseCase {
	return &biz_delivery.UseCase{
		Store: store, Runtime: runtime, Schedule: &sched{},
		Now:        func() time.Time { return *now },
		Jitter:     func() (uint32, error) { return 10000, nil },
		WaitJitter: func() (time.Duration, error) { return 0, nil },
		Config: biz_delivery.Config{
			HTTPTimeout: 10 * time.Second, BuildTimeout: time.Second, LeaseTimeout: 30 * time.Second,
			ResultReserve: 5 * time.Second, CredentialWait: 5 * time.Second, BlockedCheck: time.Minute,
			SummaryMax: 2048, WorkerID: "w", Deployment: "test",
		},
	}
}

func task(id uint64, policy string, now time.Time) data_interface.TaskRecord {
	deadline := now.Add(24 * time.Hour)
	sched := now
	return data_interface.TaskRecord{
		DeliveryID: id, EventID: 10, CallerID: "caller", RouteTargetID: "target",
		ChannelID: 1, ChannelVersion: 1, ReleaseID: 7,
		ProviderID: "prov", ProviderAccountID: "acct", Operation: "notify",
		AdapterType: "mock-json", AdapterVersion: "1", MappingID: "map", MappingVersion: "1",
		FixedTarget: []byte(`{"channel_id":"1"}`), PayloadCanonical: []byte(`{"ok":true}`),
		RoundPolicy: []byte(policy), IdempotencyKey: "n1_local_1",
		Status: enums.TaskPending, ResultCertainty: enums.CertaintyNotReceived,
		RoundNo: 1, ScheduleVersion: 1, RowVersion: 1, MaxAttempts: 8,
		RoundDeadlineAt: deadline, ScheduledAt: &sched, OutboxID: 50 + id,
	}
}

func commandOf(task data_interface.TaskRecord) domain.DeliveryCommand {
	messageID, err := domain.OutboxMessageID(task.OutboxID)
	if err != nil {
		panic(err)
	}
	return domain.DeliveryCommand{
		ProtocolVersion: 1, OutboxID: task.OutboxID, MessageID: messageID, EventID: task.EventID,
		DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
	}
}

func pol(max, initial, cap, timeout int) string {
	return `{"backoff_cap_ms":` + itoa(cap) + `,"backoff_initial_ms":` + itoa(initial) + `,"max_attempts":` + itoa(max) + `,"round_timeout_seconds":` + itoa(timeout) + `,"server_wait_cap_seconds":3600}`
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func readyPrep(covers bool) func(data_interface.PrepareRequest) data_interface.PrepareResponse {
	return func(in data_interface.PrepareRequest) data_interface.PrepareResponse {
		return data_interface.PrepareResponse{
			Kind: domain.PrepareReady, ChannelVersion: in.Task.ChannelVersion, ReleaseID: in.Task.ReleaseID,
			IdempotencyCovers: covers, SafeUntil: in.Now.Add(time.Hour),
		}
	}
}

func receivedView() data_interface.Interpreted {
	return data_interface.Interpreted{Outcome: enums.OutcomeReceived, Certainty: enums.CertaintyReceived, CoversBusinessKey: true}
}

func okSend(context.Context, data_interface.TaskRecord) (data_interface.SendResponse, error) {
	return data_interface.SendResponse{HTTPStatus: 200, BodySummary: "ok"}, nil
}

func hasPhase(task data_interface.TaskRecord, phase enums.HistoryPhase) bool {
	for _, item := range task.History {
		if item.Phase == phase {
			return true
		}
	}
	return false
}

var errBuild = errString("build")

type errString string

func (e errString) Error() string { return string(e) }
