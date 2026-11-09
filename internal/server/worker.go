package server

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"notifier/internal/conf"
	"notifier/internal/domain"
)

// Runner 执行一条已验证的指令。receipt 留在入口，不传入业务编排。
type Runner interface {
	ExecuteInstruction(ctx context.Context, cmd domain.DeliveryCommand) (domain.Step, error)
}

// Inbound 是一条待执行指令。Receipt 只用于 ACK。
type Inbound struct {
	Command domain.DeliveryCommand
	Receipt string
}

// Source 是 S04 的拉取与 ACK 端口。真实消费由 S04 完成。
type Source interface {
	Receive(ctx context.Context, limit int) ([]Inbound, error)
	Ack(ctx context.Context, receipt string) error
}

// IdleSource 在 S04 接入前不拉取消息。取消或超时时返回空结果，不写业务错误。
type IdleSource struct{}

func (IdleSource) Receive(ctx context.Context, limit int) ([]Inbound, error) {
	if limit < 1 {
		return nil, nil
	}
	<-ctx.Done()
	return nil, nil
}

func (IdleSource) Ack(context.Context, string) error { return nil }

// Worker 按空闲槽位拉取指令，并在持久化确认后 ACK。
type Worker struct {
	Parallel       int
	Batch          int
	ReceiveTimeout time.Duration
	AckTimeout     time.Duration
	Drain          time.Duration
	Runner         Runner
	Source         Source
	Log            *slog.Logger
}

// NewWorker 用启动配置构造后台角色。排空时间短于进程退出预算，避免入口先报超时。
func NewWorker(cfg conf.Delivery, http conf.HTTP, run Runner, src Source) *Worker {
	drain := http.ShutdownTimeout
	if http.ShutdownTimeout > time.Second {
		drain = http.ShutdownTimeout - time.Second
	}
	return &Worker{
		Parallel: cfg.Parallelism, Batch: cfg.ReceiveBatch, ReceiveTimeout: cfg.ReceiveTimeout,
		AckTimeout: cfg.AckTimeout, Drain: drain, Runner: run, Source: src,
	}
}

func (w *Worker) Name() string { return "delivery" }

// Run 在 ctx 取消后停止拉取。在途调用只保留到 Drain，超时后取消，不虚构结果，也不提前 ACK。
func (w *Worker) Run(ctx context.Context) error {
	if w == nil || w.Runner == nil || w.Source == nil || w.Parallel < 1 || w.Batch < 1 {
		return errors.New("server: delivery worker is incomplete")
	}
	slots := make(chan struct{}, w.Parallel)
	release := make(chan struct{}, w.Parallel)
	var wg sync.WaitGroup
	workCtx, workCancel := context.WithCancel(context.Background())
	defer workCancel()

	for {
		if ctx.Err() != nil {
			break
		}
		free := w.Parallel - len(slots)
		if free <= 0 {
			select {
			case <-ctx.Done():
			case <-release:
			}
			continue
		}
		limit := w.Batch
		if limit > free {
			limit = free
		}
		recvCtx, cancel := context.WithTimeout(ctx, w.ReceiveTimeout)
		msgs, err := w.Source.Receive(recvCtx, limit)
		cancel()
		if ctx.Err() != nil {
			break
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			if w.Log != nil {
				w.Log.Info("delivery receive failed")
			}
			continue
		}
		for _, msg := range msgs {
			if err := domain.ValidateDeliveryCommand(msg.Command); err != nil {
				continue
			}
			slots <- struct{}{}
			wg.Add(1)
			go func(msg Inbound) {
				defer wg.Done()
				defer func() {
					<-slots
					select {
					case release <- struct{}{}:
					default:
					}
				}()
				itemCtx, itemCancel := context.WithCancel(workCtx)
				defer itemCancel()
				step, runErr := w.Runner.ExecuteInstruction(itemCtx, msg.Command)
				if runErr != nil || !step.ACK || step.Unconfirmed {
					return
				}
				ackCtx, ackCancel := context.WithTimeout(context.Background(), w.AckTimeout)
				ackErr := w.Source.Ack(ackCtx, msg.Receipt)
				ackCancel()
				if ackErr != nil && w.Log != nil {
					w.Log.Info("delivery ack failed", "delivery_id", msg.Command.DeliveryID)
				}
			}(msg)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(w.Drain)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		workCancel()
		<-done
		return nil
	}
}
