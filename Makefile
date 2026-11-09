# API 通知服务构建与运行入口。
# 依赖：Go 1.26.2、Docker（compose 场景）、Goose（迁移，经 go run 拉取）。

BIN_DIR  := bin
NOTIFIER := $(BIN_DIR)/notifier
MOCK     := $(BIN_DIR)/mockvendor

GO       ?= go
GOOSE    := $(GO) run github.com/pressly/goose/v3/cmd/goose
MYSQL_DSN ?= notifier:notifier@tcp(127.0.0.1:3306)/notifier?parseTime=true&charset=utf8mb4

.PHONY: build run mockvendor test vet race fmt tidy deps wire \
        migrate-up migrate-down migrate-status migrate-create \
        topics compose-up compose-down compose-logs clean

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -o $(NOTIFIER) ./cmd/notifier
	$(GO) build -trimpath -o $(MOCK) ./cmd/mockvendor

run: build
	./$(NOTIFIER)

mockvendor: build
	./$(MOCK)

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

race:
	$(GO) test -race ./...

fmt:
	gofmt -w cmd internal unit_test

wire:
	$(GO) tool wire ./cmd/notifier

tidy:
	$(GO) mod tidy

# 版本与 compose 镜像的对应关系见 TECH_DESIGN.md 第 2 节。不要改回 @latest。
deps:
	$(GO) get github.com/go-sql-driver/mysql@v1.9.3
	$(GO) get github.com/redis/go-redis/v9@v9.17.2
	$(GO) get github.com/pressly/goose/v3@v3.26.0
	$(GO) get github.com/apache/rocketmq-clients/golang/v5@v5.1.4
	$(GO) mod tidy

topics:
	sh scripts/init-topics.sh

migrate-up:
	$(GOOSE) -dir migrations mysql "$(MYSQL_DSN)" up

migrate-down:
	$(GOOSE) -dir migrations mysql "$(MYSQL_DSN)" down

migrate-status:
	$(GOOSE) -dir migrations mysql "$(MYSQL_DSN)" status

migrate-create:
	$(GOOSE) -dir migrations mysql "$(MYSQL_DSN)" create $(name) sql

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down

compose-logs:
	docker compose logs -f

clean:
	rm -rf $(BIN_DIR)
