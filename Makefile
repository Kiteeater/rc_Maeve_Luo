# API 通知服务构建与运行入口。
# 依赖：Go 1.26.2、Docker（compose 场景）、Goose（迁移，经 go run 拉取）。

BIN_DIR  := bin
NOTIFIER := $(BIN_DIR)/notifier
MOCK     := $(BIN_DIR)/mockvendor

GO       ?= go
GOOSE    := $(GO) run github.com/pressly/goose/v3/cmd/goose@latest
MYSQL_DSN ?= notifier:notifier@tcp(127.0.0.1:3306)/notifier?parseTime=true&charset=utf8mb4

.PHONY: build run mockvendor test vet race fmt tidy deps \
        migrate-up migrate-down migrate-status migrate-create \
        compose-up compose-down compose-logs clean

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
	gofmt -w cmd internal

tidy:
	$(GO) mod tidy

# 首次实现业务时按技术方案第 2 节引入依赖。
# RocketMQ 客户端锁定当前稳定版本，其余取各自最新稳定版后统一 go mod tidy。
deps:
	$(GO) get github.com/go-sql-driver/mysql@latest
	$(GO) get github.com/redis/go-redis/v9@latest
	$(GO) get github.com/pressly/goose/v3@latest
	$(GO) get github.com/apache/rocketmq-clients/golang/v5@v5.1.4
	$(GO) mod tidy

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
