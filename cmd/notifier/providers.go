package main

import (
	"context"
	"database/sql"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/biz/biz_config"
	"notifier/internal/biz/biz_delivery"
	"notifier/internal/biz/biz_event"
	"notifier/internal/biz/biz_outbox"
	"notifier/internal/biz/biz_provider"
	"notifier/internal/biz/biz_recovery"
	"notifier/internal/conf"
	"notifier/internal/data/mysql"
	"notifier/internal/data/redis"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/server"
	"notifier/internal/service"
)

func newExecutor(db *sql.DB, cfg conf.MySQL) *mysql.Executor {
	return mysql.NewExecutor(db, cfg.OperationTimeout)
}

func newReleaseCache(client *goredis.Client, cfg conf.Config) *redis.ReleaseCache {
	return redis.NewReleaseCache(client, cfg.CacheTTL)
}

func newConfigUseCase(
	repo data_interface.ReleaseRepository,
	channels data_interface.ChannelReader,
	creds data_interface.CredentialReader,
	adapters data_interface.AdapterRegistry,
	cache data_interface.ReleaseCache,
	cfg conf.Config,
) *biz_config.UseCase {
	bounds := domain.Bounds{
		MaxBytes:         cfg.MaxBytes,
		MaxRoutes:        cfg.MaxRoutes,
		MaxTargets:       cfg.MaxTargets,
		MaxConditions:    cfg.MaxConditions,
		MaxMappingFields: cfg.MaxMappingFields,
		MaxPathBytes:     cfg.MaxPathBytes,
		MaxEventBytes:    cfg.MaxEventBytes,
		MaxFanout:        cfg.MaxFanout,
		MaxPayloadBytes:  256 << 10,
		ServerWaitCap:    cfg.ServerWaitCap,
	}
	return biz_config.NewUseCase(repo, channels, creds, adapters, cache, bounds, cfg.CalculationTimeout)
}

func newReleaseService(uc *biz_config.UseCase, cfg conf.Config) *service.ReleaseService {
	return service.NewReleaseService(uc, cfg.MaxBytes)
}

type releasePlanner struct {
	uc *biz_config.UseCase
}

func (p releasePlanner) Calculate(ctx context.Context, principal biz_event.Principal, releaseID uint64, env domain.EventEnvelope) (domain.DeliveryPlan, error) {
	return p.uc.Calculate(ctx, biz_config.Principal{
		ActorID: principal.ActorID, CallerID: principal.CallerID,
		AllowedSources: principal.AllowedSources, Scopes: principal.Scopes,
	}, releaseID, env)
}

func newEventUseCase(store *mysql.EventStore, releases *mysql.ReleaseStore, planner *biz_config.UseCase, cfg conf.Event) *biz_event.UseCase {
	return biz_event.NewUseCase(store, store, releases, releasePlanner{planner}, biz_event.Limits{
		MaxBody: cfg.MaxBodyBytes, MaxFanout: cfg.MaxFanout, MaxPlanBytes: cfg.MaxPlanBytes,
		MaxPending: int64(cfg.MaxPendingOutbox), MaxOldest: cfg.MaxOldestOutboxAge,
		SampleInterval: cfg.BacklogSampleInterval, SampleMaxAge: cfg.BacklogSampleMaxAge,
		Namespace: cfg.IdempotencyNamespace,
	}, nil)
}

func newEventService(uc *biz_event.UseCase, cfg conf.Event) *service.EventService {
	return service.NewEventService(uc, cfg.MaxBodyBytes)
}

func newDeliveryUseCase(store data_interface.DeliveryStore, rt data_interface.ProviderRuntime, sched data_interface.ScheduleOriginal, cfg conf.Delivery, limits conf.Limits) *biz_delivery.UseCase {
	return &biz_delivery.UseCase{
		Store: store, Runtime: rt, Schedule: sched,
		Config: biz_delivery.Config{
			HTTPTimeout: cfg.HTTPTimeout, BuildTimeout: cfg.BuildTimeout, LeaseTimeout: cfg.LeaseTimeout,
			ResultReserve: cfg.ResultReserve, CredentialWait: cfg.CredentialWait, BlockedCheck: cfg.BlockedCheck,
			SummaryMax: limits.SummaryBytes, WorkerID: "notifier", Deployment: "local",
		},
	}
}

type deliveryRunner struct{ uc *biz_delivery.UseCase }

func (r deliveryRunner) ExecuteInstruction(ctx context.Context, cmd domain.DeliveryCommand) (domain.Step, error) {
	return r.uc.ExecuteInstruction(ctx, cmd)
}

func newDeliveryRunner(uc *biz_delivery.UseCase) server.Runner { return deliveryRunner{uc} }

func newOutboxCounters() *biz_outbox.Counters { return &biz_outbox.Counters{} }

func newRescheduler(store data_interface.OutboxStore, cfg conf.Outbox) *biz_outbox.Rescheduler {
	return &biz_outbox.Rescheduler{Store: store, MaxDelay: cfg.MaxDelay, ClockSafety: cfg.ClockSafety}
}

func newPublisher(store data_interface.OutboxStore, sender data_interface.CommandSender, cfg conf.Outbox, mq conf.RocketMQ, counters *biz_outbox.Counters) *biz_outbox.Publisher {
	return &biz_outbox.Publisher{
		Store: store, Sender: sender, Counters: counters,
		Config: biz_outbox.Config{
			BatchSize: cfg.BatchSize, Parallelism: cfg.Parallelism, Lease: cfg.LeaseTimeout,
			SendTimeout: cfg.SendTimeout, BackoffInitial: cfg.BackoffInitial, BackoffCap: cfg.BackoffCap,
			MaxDelay: cfg.MaxDelay, ClockSafety: cfg.ClockSafety,
			TopicImmediate: mq.TopicImmediate, TopicDelayed: mq.TopicDelayed, AlertAfter: 5,
		},
	}
}

func newDecoder(store data_interface.OutboxStore, counters *biz_outbox.Counters) *biz_outbox.Decoder {
	return &biz_outbox.Decoder{Store: store, Counters: counters}
}

func newMQSource(consumer data_interface.CommandConsumer, dec *biz_outbox.Decoder, cfg conf.Delivery) *server.MQSource {
	return &server.MQSource{Consumer: consumer, Decoder: dec, Invisible: cfg.MQInvisible}
}

func newPublisherRole(p *biz_outbox.Publisher, cfg conf.Outbox) *server.Publisher {
	return &server.Publisher{Runner: p, Interval: cfg.PollInterval, Batch: cfg.BatchSize}
}

func newRoles(worker *server.Worker, pub *server.Publisher, rec *server.Recovery) *server.Group {
	return server.NewGroup(pub, worker, rec)
}

// newRecoverer 组装 S06 恢复用例。
func newRecoverer(store data_interface.RecoveryStore, cc data_interface.ConstraintChecker, r conf.Recovery, o conf.Outbox, d conf.Delivery) *biz_recovery.Recoverer {
	return &biz_recovery.Recoverer{
		Store: store, Constraints: cc, Counters: &biz_recovery.Counters{},
		Config: biz_recovery.Config{
			BatchSize: r.BatchSize, Parallelism: r.Parallelism, TaskTimeout: r.TaskTimeout, Grace: o.GracePeriod,
			RepairCooldown: r.RepairCooldown, MaxRepairs: uint32(r.MaxRepairs), BlockedCheck: d.BlockedCheck,
			DLQAutoRepairs: uint32(r.DLQAutoRepairs),
		},
	}
}

func newRecoveryRole(rec *biz_recovery.Recoverer, r conf.Recovery) *server.Recovery {
	return &server.Recovery{Runner: rec, Interval: r.ScanInterval, ErrorBackoff: r.ErrorBackoff}
}

// newProviderRuntime 组装 S05 供应商执行边界。
func newProviderRuntime(reg data_interface.AdapterRegistry, ch data_interface.ChannelStateReader, lim data_interface.RateLimiter,
	creds data_interface.CredentialResolver, http data_interface.HTTPCaller, d conf.Delivery, p conf.Provider, limits conf.Limits) *biz_provider.Runtime {
	return &biz_provider.Runtime{
		Registry: reg, Channels: ch, Limiter: lim, Creds: creds, HTTP: http,
		Config: biz_provider.Config{
			HTTPBudget: d.HTTPTimeout, TokenSkew: p.TokenSkew, MaxRequestBytes: p.MaxRequestBytes,
			SummaryMax: limits.SummaryBytes, LimiterRetry: time.Second,
		},
	}
}
