package server_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"notifier/internal/domain"
	"notifier/internal/server"
)

func TestDLV11_ShutdownDoesNotInventResultOrAck(t *testing.T) {
	started := make(chan struct{})
	var acks atomic.Int32
	var finished atomic.Int32
	src := &scriptSource{acks: &acks, release: started}
	run := runnerFunc(func(ctx context.Context, _ domain.DeliveryCommand) (domain.Step, error) {
		finished.Add(1)
		<-ctx.Done()
		return domain.Step{}, ctx.Err()
	})
	worker := &server.Worker{
		Parallel: 1, Batch: 1, ReceiveTimeout: 50 * time.Millisecond, AckTimeout: 20 * time.Millisecond,
		Drain: 30 * time.Millisecond, Runner: run, Source: src,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = worker.Run(ctx)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not take the in-flight instruction")
	}
	time.Sleep(40 * time.Millisecond)
	if src.receives.Load() != 1 {
		t.Fatalf("receive calls = %d, want no pull without a free slot", src.receives.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not drain")
	}
	if acks.Load() != 0 || finished.Load() != 1 {
		t.Fatalf("acks %d finished %d", acks.Load(), finished.Load())
	}
}

type runnerFunc func(context.Context, domain.DeliveryCommand) (domain.Step, error)

func (f runnerFunc) ExecuteInstruction(ctx context.Context, cmd domain.DeliveryCommand) (domain.Step, error) {
	return f(ctx, cmd)
}

type scriptSource struct {
	acks     *atomic.Int32
	receives atomic.Int32
	release  chan struct{}
	once     sync.Once
}

func (s *scriptSource) Receive(ctx context.Context, limit int) ([]server.Inbound, error) {
	s.receives.Add(1)
	if limit < 1 {
		return nil, nil
	}
	var sent bool
	s.once.Do(func() { sent = true })
	if !sent {
		<-ctx.Done()
		return nil, nil
	}
	close(s.release)
	return []server.Inbound{{
		Receipt: "r1",
		Command: domain.DeliveryCommand{ProtocolVersion: 1, OutboxID: 1, MessageID: "o1_1", EventID: 1, DeliveryID: 1, RoundNo: 1, ScheduleVersion: 1},
	}}, nil
}

func (s *scriptSource) Ack(context.Context, string) error {
	s.acks.Add(1)
	return nil
}
