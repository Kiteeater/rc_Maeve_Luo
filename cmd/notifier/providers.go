package main

import (
	"database/sql"

	goredis "github.com/redis/go-redis/v9"

	"notifier/internal/biz/biz_config"
	"notifier/internal/conf"
	"notifier/internal/data/mysql"
	"notifier/internal/data/redis"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
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
