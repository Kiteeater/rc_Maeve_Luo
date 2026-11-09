//go:build wireinject

package main

import (
	"context"

	"github.com/google/wire"

	"notifier/internal/biz/biz_outbox"
	"notifier/internal/conf"
	"notifier/internal/data/mysql"
	"notifier/internal/data/provider"
	"notifier/internal/data/redis"
	"notifier/internal/data/rocketmq"
	"notifier/internal/data_interface"
	"notifier/internal/server"
	"notifier/internal/service"
)

// initializeApp 组装启动配置、身份、基础设施和 HTTP 入口。
// 身份文件先于数据库拨号；连接失败时清理函数由 Wire 逆序调用。
func initializeApp(ctx context.Context) (*application, func(), error) {
	wire.Build(
		conf.Load,
		wire.FieldsOf(new(conf.GlobalConfig), "HTTP", "MySQL", "Redis", "RocketMQ", "Outbox", "Log", "Auth", "Config", "Event", "Delivery", "Limits"),
		loadIdentities,
		mysql.Open,
		redis.Open,
		rocketmq.Open,
		newExecutor,
		mysql.NewReleaseStore,
		newReleaseCache,
		provider.NewMockRegistry,
		wire.Bind(new(data_interface.ReleaseRepository), new(*mysql.ReleaseStore)),
		wire.Bind(new(data_interface.ChannelReader), new(*mysql.ReleaseStore)),
		wire.Bind(new(data_interface.CredentialReader), new(*mysql.ReleaseStore)),
		wire.Bind(new(data_interface.ReleaseCache), new(*redis.ReleaseCache)),
		wire.Bind(new(data_interface.AdapterRegistry), new(*provider.Registry)),
		newConfigUseCase,
		newReleaseService,
		mysql.NewEventStore,
		newEventUseCase,
		newEventService,
		newReadiness,
		service.NewHealthService,
		server.NewHTTPServer,
		mysql.NewDeliveryStore,
		provider.NewUnavailableRuntime,
		mysql.NewOutboxStore,
		wire.Bind(new(data_interface.OutboxStore), new(*mysql.OutboxStore)),
		wire.Bind(new(data_interface.CommandSender), new(*rocketmq.Client)),
		rocketmq.OpenConsumer,
		wire.Bind(new(data_interface.CommandConsumer), new(*rocketmq.Consumer)),
		newOutboxCounters,
		newRescheduler,
		newPublisher,
		newDecoder,
		newMQSource,
		newPublisherRole,
		wire.Bind(new(data_interface.DeliveryStore), new(*mysql.DeliveryStore)),
		wire.Bind(new(data_interface.ProviderRuntime), new(*provider.UnavailableRuntime)),
		wire.Bind(new(data_interface.ScheduleOriginal), new(*biz_outbox.Rescheduler)),
		newDeliveryUseCase,
		newDeliveryRunner,
		wire.Bind(new(server.Source), new(*server.MQSource)),
		server.NewWorker,
		newRoles,
		newApplication,
	)
	return nil, nil, nil
}
