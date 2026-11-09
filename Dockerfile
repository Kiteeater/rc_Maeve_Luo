# 通知服务构建与运行镜像。
# 依赖固定版本见 go.mod（实现阶段引入外部依赖后随之更新）。
FROM golang:1.26.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifier ./cmd/notifier

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/notifier /notifier
ENTRYPOINT ["/notifier"]
