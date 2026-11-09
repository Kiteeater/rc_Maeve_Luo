package main

import (
	"context"
	"database/sql"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/biz/biz_config"
	"notifier/internal/biz/biz_delivery"
	"notifier/internal/biz/biz_event"
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

func newIdleSource() *server.IdleSource { return &server.IdleSource{} }

func newRoles(worker *server.Worker) *server.Group { return server.NewGroup(worker) }
