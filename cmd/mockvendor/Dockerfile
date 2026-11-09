# 独立模拟供应商镜像，用于集成验证与故障演练。
FROM golang:1.26.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mockvendor ./cmd/mockvendor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mockvendor /mockvendor
ENTRYPOINT ["/mockvendor"]
