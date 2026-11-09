package conf

import (
	"fmt"
	"time"
)

const (
	httpRequestMin  = time.Second
	httpRequestMax  = 30 * time.Second
	mysqlOpMin      = 100 * time.Millisecond
	mysqlOpMax      = 5 * time.Second
	httpShutdownMin = 5 * time.Second
	httpShutdownMax = 120 * time.Second
	headerBytesMin  = 1 * 1024
	headerBytesMax  = 64 * 1024
	summaryBytesMin = 256
	summaryBytesMax = 8192
)

// validate 检查 S00 第 9 节的取值边界，以及第 7 节的时间关系。
//
// HTTP 截止必须早于领取到期，结果事务余量按 MYSQL_OPERATION_TIMEOUT 计：
// DELIVERY_HTTP_TIMEOUT + MYSQL_OPERATION_TIMEOUT < DELIVERY_LEASE_TIMEOUT。
// MQ 不可见期限覆盖这一次本地处理预算：
// DELIVERY_MQ_INVISIBLE >= DELIVERY_LEASE_TIMEOUT + MYSQL_OPERATION_TIMEOUT。
// S03 的构造超时、结果预留、ACK 和凭证准备预算尚未进入本配置，届时再收紧。
func validate(c *GlobalConfig) error {
	if err := durationBetween("HTTP_REQUEST_TIMEOUT", c.HTTP.RequestTimeout, httpRequestMin, httpRequestMax); err != nil {
		return err
	}
	if err := durationBetween("MYSQL_OPERATION_TIMEOUT", c.MySQL.OperationTimeout, mysqlOpMin, mysqlOpMax); err != nil {
		return err
	}
	if err := durationBetween("HTTP_SHUTDOWN_TIMEOUT", c.HTTP.ShutdownTimeout, httpShutdownMin, httpShutdownMax); err != nil {
		return err
	}
	if c.HTTP.MaxHeaderBytes < headerBytesMin || c.HTTP.MaxHeaderBytes > headerBytesMax {
		return fmt.Errorf("conf: HTTP_MAX_HEADER_BYTES out of range")
	}
	if c.Limits.SummaryBytes < summaryBytesMin || c.Limits.SummaryBytes > summaryBytesMax {
		return fmt.Errorf("conf: SUMMARY_MAX_BYTES out of range")
	}
	if c.Delivery.HTTPTimeout <= 0 {
		return fmt.Errorf("conf: DELIVERY_HTTP_TIMEOUT out of range")
	}
	if c.Delivery.LeaseTimeout <= 0 {
		return fmt.Errorf("conf: DELIVERY_LEASE_TIMEOUT out of range")
	}
	if c.Delivery.MQInvisible <= 0 {
		return fmt.Errorf("conf: DELIVERY_MQ_INVISIBLE out of range")
	}
	if c.Delivery.HTTPTimeout+c.MySQL.OperationTimeout >= c.Delivery.LeaseTimeout {
		return fmt.Errorf("conf: DELIVERY_HTTP_TIMEOUT + MYSQL_OPERATION_TIMEOUT must be < DELIVERY_LEASE_TIMEOUT")
	}
	if c.Delivery.MQInvisible < c.Delivery.LeaseTimeout+c.MySQL.OperationTimeout {
		return fmt.Errorf("conf: DELIVERY_MQ_INVISIBLE must be >= DELIVERY_LEASE_TIMEOUT + MYSQL_OPERATION_TIMEOUT")
	}
	if err := intBetween("CONFIG_MAX_BYTES", c.Config.MaxBytes, 64*1024, 4*1024*1024); err != nil {
		return err
	}
	if err := intBetween("CONFIG_MAX_ROUTES", c.Config.MaxRoutes, 1, 2000); err != nil {
		return err
	}
	if err := intBetween("CONFIG_MAX_TARGETS", c.Config.MaxTargets, 1, 2000); err != nil {
		return err
	}
	if err := intBetween("CONFIG_MAX_CONDITIONS", c.Config.MaxConditions, 0, 100); err != nil {
		return err
	}
	if err := intBetween("CONFIG_MAX_MAPPING_FIELDS", c.Config.MaxMappingFields, 1, 1000); err != nil {
		return err
	}
	if err := intBetween("CONFIG_MAX_PATH_BYTES", c.Config.MaxPathBytes, 16, 1024); err != nil {
		return err
	}
	if err := intBetween("CONFIG_MAX_EVENT_BYTES", c.Config.MaxEventBytes, 1024, 1<<20); err != nil {
		return err
	}
	if err := intBetween("CONFIG_MAX_FANOUT", c.Config.MaxFanout, 1, 50); err != nil {
		return err
	}
	if err := durationBetween("CONFIG_CALCULATION_TIMEOUT", c.Config.CalculationTimeout, 50*time.Millisecond, 2*time.Second); err != nil {
		return err
	}
	if err := durationBetween("CONFIG_CACHE_TTL", c.Config.CacheTTL, time.Minute, 24*time.Hour); err != nil {
		return err
	}
	if err := intBetween("EVENT_MAX_BODY_BYTES", c.Event.MaxBodyBytes, 1<<10, 1<<20); err != nil {
		return err
	}
	if err := intBetween("EVENT_MAX_FANOUT", c.Event.MaxFanout, 1, 200); err != nil {
		return err
	}
	if err := intBetween("EVENT_MAX_PLAN_BYTES", c.Event.MaxPlanBytes, 256<<10, 8<<20); err != nil {
		return err
	}
	if err := intBetween("EVENT_MAX_PENDING_OUTBOX", c.Event.MaxPendingOutbox, 100, 1000000); err != nil {
		return err
	}
	if err := durationBetween("EVENT_MAX_OLDEST_OUTBOX_AGE", c.Event.MaxOldestOutboxAge, time.Minute, 24*time.Hour); err != nil {
		return err
	}
	if err := durationBetween("EVENT_BACKLOG_SAMPLE_INTERVAL", c.Event.BacklogSampleInterval, 100*time.Millisecond, 5*time.Second); err != nil {
		return err
	}
	if err := durationBetween("EVENT_BACKLOG_SAMPLE_MAX_AGE", c.Event.BacklogSampleMaxAge, c.Event.BacklogSampleInterval, 30*time.Second); err != nil {
		return err
	}
	if c.Event.BacklogSampleMaxAge < c.Event.BacklogSampleInterval {
		return fmt.Errorf("conf: EVENT_BACKLOG_SAMPLE_MAX_AGE must be >= EVENT_BACKLOG_SAMPLE_INTERVAL")
	}
	if !asciiAlnum(c.Event.IdempotencyNamespace, 1, 24) {
		return fmt.Errorf("conf: DELIVERY_IDEMPOTENCY_NAMESPACE out of range")
	}
	return nil
}

func asciiAlnum(value string, minLen, maxLen int) bool {
	if len(value) < minLen || len(value) > maxLen {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < '0' || (c > '9' && c < 'A') || (c > 'Z' && c < 'a') || c > 'z' {
			return false
		}
	}
	return true
}

func intBetween(name string, value, min, max int) error {
	if value < min || value > max {
		return fmt.Errorf("conf: %s out of range", name)
	}
	return nil
}

func durationBetween(name string, value, min, max time.Duration) error {
	if value < min || value > max {
		return fmt.Errorf("conf: %s out of range", name)
	}
	return nil
}
