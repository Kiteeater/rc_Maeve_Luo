//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"notifier/internal/biz/biz_delivery"
	"notifier/internal/constant"
	"notifier/internal/data/mysql"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

func TestDLV02_ConcurrentStartAttempt(t *testing.T) {
	db := s02DB(t)
	task, store := s03Task(t, db, "dlv-02")
	in := s03Start(task)
	var wg sync.WaitGroup
	var wins atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.StartAttempt(context.Background(), in)
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, data_interface.ErrOwnershipLost) {
				t.Errorf("start: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("wins %d", wins.Load())
	}
	assertSQLCount(t, db, 1, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id = %d`, task.DeliveryID))
	assertSQLCount(t, db, 1, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_tasks WHERE delivery_id = %d AND status = 'PROCESSING' AND round_attempt_count = 1`, task.DeliveryID))
}

func TestDLV06_RedeliveryAfterAckLoss(t *testing.T) {
	db := s02DB(t)
	task, store := s03Task(t, db, "dlv-06")
	rt := &dlvRuntime{outcome: enums.OutcomeReceived}
	uc := s03UC(store, rt)
	cmd := s03Command(t, task)
	step, _, err := uc.Execute(context.Background(), cmd)
	if err != nil || !step.ACK || rt.sends.Load() != 1 {
		t.Fatalf("first %+v sends %d err %v", step, rt.sends.Load(), err)
	}
	step, got, err := uc.Execute(context.Background(), cmd)
	if err != nil || !step.ACK || rt.sends.Load() != 1 || got.Status != enums.TaskSucceeded {
		t.Fatalf("redelivery %+v status %s sends %d err %v", step, got.Status, rt.sends.Load(), err)
	}
}

func TestDLV07_FinishCommitUncertain(t *testing.T) {
	db := s02DB(t)
	task, store := s03Task(t, db, "dlv-07")
	started, err := store.StartAttempt(context.Background(), s03Start(task))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RecordInvocation(context.Background(), data_interface.RecordInvocationInput{
		Ownership: started.Ownership, RequestStartedAt: time.Now().UTC(), Method: "POST", TargetPath: "/mock", RequestSummary: "ok",
	}); err != nil {
		t.Fatal(err)
	}
	seen, err := store.ReadDisposition(context.Background(), task.DeliveryID, started.Ownership.AttemptID)
	if err != nil || seen.Attempts[0].Status == "FINISHED" || seen.Attempts[0].SendState != enums.SendHTTPInvoked || seen.ResultCertainty == enums.CertaintyReceived {
		t.Fatalf("unconfirmed read %+v %v", seen.Attempts, err)
	}
	finished := time.Now().UTC().Truncate(time.Microsecond)
	if _, err = store.FinishAttemptAndPlan(context.Background(), s03Finish(started.Ownership, receivedPlan(finished))); err != nil {
		t.Fatal(err)
	}
	again, err := store.ReadDisposition(context.Background(), task.DeliveryID, started.Ownership.AttemptID)
	if err != nil || again.Attempts[0].Status != "FINISHED" || again.Status != enums.TaskSucceeded {
		t.Fatalf("confirmed %+v %v", again.Status, err)
	}
	hist := len(again.History)
	if _, err = store.FinishAttemptAndPlan(context.Background(), s03Finish(started.Ownership, receivedPlan(finished))); err != nil {
		t.Fatal(err)
	}
	final, err := store.LoadTask(context.Background(), task.DeliveryID)
	if err != nil || len(final.History) != hist || len(final.Attempts) != 1 {
		t.Fatalf("duplicated hist %d attempts %d err %v", len(final.History), len(final.Attempts), err)
	}
}

func TestDLV08_LateEvidenceConditional(t *testing.T) {
	db := s02DB(t)
	task, store := s03Task(t, db, "dlv-08")
	started, err := store.StartAttempt(context.Background(), s03Start(task))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE delivery_tasks SET lease_token = 'replaced' WHERE delivery_id = ?`, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
	uc := s03UC(store, &dlvRuntime{outcome: enums.OutcomeReceived})
	step, got, err := uc.ApplyResult(context.Background(), started.Ownership, biz_delivery.ResultInput{
		SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived, CoversBusinessKey: true,
		RoundAttemptCount: started.RoundAttemptCount, RoundNo: started.Ownership.RoundNo,
	})
	if err != nil || !step.ACK || got.Status != enums.TaskProcessing || got.CurrentAttemptID == nil || *got.CurrentAttemptID != started.Ownership.AttemptID {
		t.Fatalf("advanced %+v %+v %v", step, got.Status, err)
	}
	assertSQLCount(t, db, 1, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_history WHERE delivery_id = %d AND phase = 'LATE_RESPONSE'`, task.DeliveryID))
	assertSQLCount(t, db, 1, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id = %d AND status = 'PROCESSING'`, task.DeliveryID))
}

func TestDLV14_ResumeRace(t *testing.T) {
	db := s02DB(t)
	task, store := s03Task(t, db, "dlv-14")
	check := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	_, err := store.WaitOrBlock(context.Background(), data_interface.WaitOrBlockInput{
		DeliveryID: task.DeliveryID, EventID: task.EventID, RoundNo: task.RoundNo,
		ScheduleVersion: task.ScheduleVersion, ExpectedRowVersion: task.RowVersion, OutboxID: task.OutboxID,
		Plan: domain.Plan{
			Kind: enums.ActionBlock, Status: enums.TaskBlocked, Reason: "disabled", ErrorCode: constant.ErrChannelDisabled,
			ResultCertainty: enums.CertaintyNotReceived, NextCheckAt: &check,
			ResumeStatus: string(enums.TaskRetryWait), ResumeNotBefore: &check, BumpSchedule: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := store.LoadTask(context.Background(), task.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	uc := s03UC(store, &dlvRuntime{outcome: enums.OutcomeReceived})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = uc.Resume(context.Background(), task.DeliveryID)
		}()
	}
	wg.Wait()
	got, err := store.LoadTask(context.Background(), task.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != enums.TaskRetryWait || got.ScheduleVersion != blocked.ScheduleVersion+1 || got.RoundAttemptCount != 0 || got.ScheduledAt == nil || got.ScheduledAt.Before(check) {
		t.Fatalf("resumed %+v", got.Status)
	}
	assertSQLCount(t, db, 1, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_history WHERE delivery_id = %d AND phase = 'UNBLOCKED'`, task.DeliveryID))
	step, _, err := uc.Execute(context.Background(), s03Command(t, task))
	if err != nil || !step.ACK {
		t.Fatalf("old command %+v %v", step, err)
	}
	assertSQLCount(t, db, 0, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id = %d`, task.DeliveryID))
}

func TestDLV16_FinishHistoryRollback(t *testing.T) {
	db := s02DB(t)
	task, store := s03Task(t, db, "dlv-16")
	started, err := store.StartAttempt(context.Background(), s03Start(task))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`
		INSERT INTO delivery_history (delivery_id, history_seq, write_id, phase, from_status, to_status, reason, round_no, schedule_version)
		SELECT delivery_id, history_seq + 1, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'LATE_RESPONSE', 'PROCESSING', 'PROCESSING', 'blocker', 1, 1
		FROM delivery_tasks WHERE delivery_id = ?`, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
	_, err = store.FinishAttemptAndPlan(context.Background(), s03Finish(started.Ownership, receivedPlan(time.Now().UTC())))
	if err == nil {
		t.Fatal("history collision must roll the finish back")
	}
	got, err := store.LoadTask(context.Background(), task.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != enums.TaskProcessing || got.Attempts[0].Status != "PROCESSING" {
		t.Fatalf("rolled %+v", got.Status)
	}
	assertSQLCount(t, db, 0, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_history WHERE delivery_id = %d AND phase = 'ATTEMPT_FINISHED'`, task.DeliveryID))
}

func TestC02_LateResultDoesNotAdvance(t *testing.T) {
	db := s02DB(t)
	task, store := s03Task(t, db, "c-02")
	started, err := store.StartAttempt(context.Background(), s03Start(task))
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Exec(`
		INSERT INTO delivery_attempts (delivery_id, round_no, attempt_no, status, send_state, started_at)
		VALUES (?, 1, 2, 'PROCESSING', 'UNCERTAIN', UTC_TIMESTAMP(6))`, task.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	next, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`
		UPDATE delivery_tasks
		SET current_attempt_id = ?, lease_token = 'recovered', lease_expires_at = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 1 MINUTE),
			round_attempt_count = 2, total_attempt_count = 2
		WHERE delivery_id = ?`, next, task.DeliveryID); err != nil {
		t.Fatal(err)
	}
	uc := s03UC(store, &dlvRuntime{outcome: enums.OutcomeReceived})
	step, got, err := uc.ApplyResult(context.Background(), started.Ownership, biz_delivery.ResultInput{
		SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived, CoversBusinessKey: true, RoundAttemptCount: 1, RoundNo: 1,
	})
	if err != nil || !step.ACK || got.RoundAttemptCount != 2 || got.CurrentAttemptID == nil || *got.CurrentAttemptID != uint64(next) {
		t.Fatalf("advanced count %d %+v %v", got.RoundAttemptCount, step, err)
	}
	assertSQLCount(t, db, 1, fmt.Sprintf(`SELECT COUNT(*) FROM delivery_history WHERE delivery_id = %d AND phase = 'LATE_RESPONSE'`, task.DeliveryID))
}

func TestC03_ReadAfterUncertainCommit(t *testing.T) {
	db := s02DB(t)
	task, store := s03Task(t, db, "c-03")
	rt := &dlvRuntime{outcome: enums.OutcomeReceived}
	uc := s03UC(store, rt)
	cmd := s03Command(t, task)
	step, first, err := uc.Execute(context.Background(), cmd)
	if err != nil || !step.ACK || first.Status != enums.TaskSucceeded {
		t.Fatalf("finish %+v %s %v", step, first.Status, err)
	}
	seen, err := store.ReadDisposition(context.Background(), task.DeliveryID, *first.LastAttemptID)
	if err != nil || seen.Attempts[0].Status != "FINISHED" || seen.Status != enums.TaskSucceeded {
		t.Fatalf("read %+v %v", seen.Status, err)
	}
	hist := len(seen.History)
	step, second, err := uc.ApplyResult(context.Background(), domain.Ownership{
		DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
		AttemptID: *first.LastAttemptID, LeaseToken: "lost", LeaseExpiresAt: time.Now().Add(time.Minute),
	}, biz_delivery.ResultInput{
		SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived, CoversBusinessKey: true, RoundAttemptCount: 1, RoundNo: 1,
	})
	if err != nil || !step.ACK || len(second.History) != hist || rt.sends.Load() != 1 {
		t.Fatalf("duplicate hist %d sends %d err %v", len(second.History), rt.sends.Load(), err)
	}
}

func s03Task(t *testing.T, db *sql.DB, sourceID string) (data_interface.TaskRecord, *mysql.DeliveryStore) {
	t.Helper()
	releaseID, hash := insertRelease(t, db, 1, []byte(`{"v":1}`))
	accepted := s02UC(t, db, &fixedPlan{id: releaseID, hash: hash, plan: oneTarget(releaseID, "ad-registration")}, openEventLimits())
	out, err := accepted.Submit(context.Background(), evtPrincipal(), evtJSON("user-service", sourceID, "2026-11-09T09:30:00Z", `{"user_id":"u-1"}`))
	if err != nil || len(out.Deliveries) != 1 {
		t.Fatalf("accept %+v %v", out, err)
	}
	store := mysql.NewDeliveryStore(db, mysql.NewExecutor(db, 2*time.Second))
	task, err := store.LoadTask(context.Background(), out.Deliveries[0].DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	return task, store
}

func s03UC(store data_interface.DeliveryStore, rt data_interface.ProviderRuntime) *biz_delivery.UseCase {
	return &biz_delivery.UseCase{
		Store: store, Runtime: rt, Schedule: dlvSched{}, Now: time.Now,
		Jitter: func() (uint32, error) { return 10000, nil }, WaitJitter: func() (time.Duration, error) { return 0, nil },
		Config: biz_delivery.Config{
			HTTPTimeout: 10 * time.Second, BuildTimeout: time.Second, LeaseTimeout: 30 * time.Second,
			ResultReserve: 5 * time.Second, CredentialWait: 5 * time.Second, BlockedCheck: time.Minute,
			SummaryMax: 2048, WorkerID: "notifier", Deployment: "local",
		},
	}
}

func s03Start(task data_interface.TaskRecord) data_interface.StartAttemptInput {
	return data_interface.StartAttemptInput{
		DeliveryID: task.DeliveryID, EventID: task.EventID, RoundNo: task.RoundNo,
		ScheduleVersion: task.ScheduleVersion, ExpectedRowVersion: task.RowVersion, OutboxID: task.OutboxID,
		WorkerID: "notifier", DeploymentVersion: "local", ExpectedChannelVersion: task.ChannelVersion,
		ExpectedReleaseID: task.ReleaseID, LeaseTimeout: 30 * time.Second,
	}
}

func s03Command(t *testing.T, task data_interface.TaskRecord) domain.DeliveryCommand {
	t.Helper()
	messageID, err := domain.OutboxMessageID(task.OutboxID)
	if err != nil {
		t.Fatal(err)
	}
	return domain.DeliveryCommand{
		ProtocolVersion: 1, OutboxID: task.OutboxID, MessageID: messageID, EventID: task.EventID,
		DeliveryID: task.DeliveryID, RoundNo: task.RoundNo, ScheduleVersion: task.ScheduleVersion,
	}
}

func s03Finish(own domain.Ownership, plan domain.Plan) data_interface.FinishInput {
	dur := time.Millisecond
	return data_interface.FinishInput{
		Ownership: own, SendState: enums.SendHTTPInvoked, Outcome: enums.OutcomeReceived,
		ResultCertainty: enums.CertaintyReceived, Plan: plan, AttemptDuration: &dur,
	}
}

func receivedPlan(now time.Time) domain.Plan {
	finished := now.UTC().Truncate(time.Microsecond)
	return domain.Plan{
		Kind: enums.ActionSucceed, Status: enums.TaskSucceeded, Reason: "received",
		ResultCertainty: enums.CertaintyReceived, LastCertainty: enums.CertaintyReceived,
		RoundFinishedAt: &finished, BumpSchedule: true, ClearLastError: true,
		Unresolved: []domain.UnresolvedRef{},
	}
}

type dlvRuntime struct {
	outcome enums.Outcome
	sends   atomic.Int32
}

func (r *dlvRuntime) Prepare(_ context.Context, in data_interface.PrepareRequest) (data_interface.PrepareResponse, error) {
	return data_interface.PrepareResponse{
		Kind: domain.PrepareReady, ChannelVersion: in.Task.ChannelVersion, ReleaseID: in.Task.ReleaseID,
		IdempotencyCovers: true, SafeUntil: in.Now.Add(time.Minute),
	}, nil
}

func (r *dlvRuntime) Build(context.Context, data_interface.TaskRecord, domain.Ownership, data_interface.PrepareResponse) (data_interface.BuiltRequest, error) {
	return data_interface.BuiltRequest{Method: "POST", TargetPath: "/mock", Summary: "ok", Size: 2, Body: []byte(`{}`)}, nil
}

func (r *dlvRuntime) SendOnce(context.Context, data_interface.BuiltRequest) (data_interface.SendResponse, error) {
	r.sends.Add(1)
	return data_interface.SendResponse{HTTPStatus: 200, BodySummary: "ok"}, nil
}

func (r *dlvRuntime) Interpret(context.Context, data_interface.TaskRecord, data_interface.BuiltRequest, data_interface.SendResponse) (data_interface.Interpreted, error) {
	outcome := r.outcome
	if outcome == "" {
		outcome = enums.OutcomeReceived
	}
	return data_interface.Interpreted{Outcome: outcome, CoversBusinessKey: true}, nil
}

type dlvSched struct{}

func (dlvSched) ScheduleOriginal(context.Context, domain.DeliveryCommand, time.Time) error {
	return nil
}
