//go:build tools

// Package tools 锁定不进入服务二进制、但构建和迁移要使用的命令版本。
package tools

import _ "github.com/pressly/goose/v3/cmd/goose"
