//go:build wireinject

package main

import (
	"context"

	"github.com/google/wire"

	"notifier/internal/conf"
	"notifier/internal/server"
	"notifier/internal/service"
)

// initializeApp 是唯一的依赖组装入口。基础设施接入后替换 Readiness 值。
func initializeApp(ctx context.Context) (*application, error) {
	wire.Build(
		conf.Load,
		wire.FieldsOf(new(conf.GlobalConfig), "HTTP", "Log"),
		wire.Value(service.Readiness(nil)),
		service.NewHealthService,
		server.NewHTTPServer,
		newApplication,
	)
	return nil, nil
}
