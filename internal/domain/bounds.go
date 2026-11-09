package domain

import "time"

// Bounds 是 S01 第 8 节的发布与计划计算上限。启动配置注入，计算时不再读取环境变量。
type Bounds struct {
	MaxBytes         int
	MaxRoutes        int
	MaxTargets       int
	MaxConditions    int
	MaxMappingFields int
	MaxPathBytes     int
	MaxEventBytes    int
	MaxFanout        int
	MaxPayloadBytes  int
	ServerWaitCap    time.Duration
}

// DefaultBounds 返回规格中的默认上限。
func DefaultBounds() Bounds {
	return Bounds{
		MaxBytes:         1 << 20,
		MaxRoutes:        500,
		MaxTargets:       500,
		MaxConditions:    20,
		MaxMappingFields: 200,
		MaxPathBytes:     256,
		MaxEventBytes:    256 << 10,
		MaxFanout:        50,
		MaxPayloadBytes:  256 << 10,
		ServerWaitCap:    time.Hour,
	}
}
