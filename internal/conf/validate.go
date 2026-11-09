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

// validate 检查 S00 第 9 节的取值边界，以及 S03 落地后的时间关系。
//
// 批次 0 的关系仍然成立：
// DELIVERY_HTTP_TIMEOUT + MYSQL_OPERATION_TIMEOUT < DELIVERY_LEASE_TIMEOUT，
// DELIVERY_MQ_INVISIBLE >= DELIVERY_LEASE_TIMEOUT + MYSQL_OPERATION_TIMEOUT。
// S03 在此之上要求：
// BUILD_TIMEOUT + HTTP_TIMEOUT + RESULT_RESERVE < LEASE_TIMEOUT，
// RESULT_RESERVE > MYSQL_OPERATION_TIMEOUT，
// MQ_INVISIBLE >= CREDENTIAL_PREPARE + LEASE_TIMEOUT + ACK_TIMEOUT + 5s。
// CREDENTIAL_PREPARE 在 S05 接入真实凭证预算前，用 DELIVERY_CREDENTIAL_PREPARE 表示同一段准备时间。
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
	d := c.Delivery
	if err := intBetween("DELIVERY_PARALLELISM", d.Parallelism, 1, 100); err != nil {
		return err
	}
	if err := intBetween("DELIVERY_RECEIVE_BATCH", d.ReceiveBatch, 1, 32); err != nil {
		return err
	}
	if d.ReceiveBatch > d.Parallelism {
		return fmt.Errorf("conf: DELIVERY_RECEIVE_BATCH must be <= DELIVERY_PARALLELISM")
	}
	if err := durationBetween("DELIVERY_HTTP_TIMEOUT", d.HTTPTimeout, 100*time.Millisecond, 60*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_BUILD_TIMEOUT", d.BuildTimeout, 10*time.Millisecond, 5*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_LEASE_TIMEOUT", d.LeaseTimeout, 5*time.Second, 300*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_RESULT_RESERVE", d.ResultReserve, time.Millisecond, 60*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_MQ_INVISIBLE", d.MQInvisible, 10*time.Second, 600*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_ACK_TIMEOUT", d.AckTimeout, 100*time.Millisecond, 10*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_CREDENTIAL_WAIT", d.CredentialWait, time.Second, 60*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_CREDENTIAL_PREPARE", d.CredentialPrepare, 100*time.Millisecond, 10*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_RECEIVE_TIMEOUT", d.ReceiveTimeout, 100*time.Millisecond, 20*time.Second); err != nil {
		return err
	}
	if err := durationBetween("DELIVERY_MQ_MAX_EXTENSION", d.MQMaxExtension, d.MQInvisible, 24*time.Hour); err != nil {
		return err
	}
	if err := intBetween("DELIVERY_MAX_ATTEMPTS", d.MaxAttempts, 1, 32); err != nil {
		return err
	}
	if err := intBetween("DELIVERY_MAX_ALLOWED_ATTEMPTS", d.MaxAllowedAttempts, 1, 32); err != nil {
		return err
	}
	if d.MaxAttempts > d.MaxAllowedAttempts {
		return fmt.Errorf("conf: DELIVERY_MAX_ATTEMPTS must be <= DELIVERY_MAX_ALLOWED_ATTEMPTS")
	}
	if err := durationBetween("DELIVERY_MAX_ALLOWED_ROUND_TIMEOUT", d.MaxAllowedRoundTimeout, time.Hour, 168*time.Hour); err != nil {
		return err
	}
	if d.RoundDeadline > d.MaxAllowedRoundTimeout {
		return fmt.Errorf("conf: DELIVERY_ROUND_DEADLINE must be <= DELIVERY_MAX_ALLOWED_ROUND_TIMEOUT")
	}
	if d.HTTPTimeout+c.MySQL.OperationTimeout >= d.LeaseTimeout {
		return fmt.Errorf("conf: DELIVERY_HTTP_TIMEOUT + MYSQL_OPERATION_TIMEOUT must be < DELIVERY_LEASE_TIMEOUT")
	}
	if d.MQInvisible < d.LeaseTimeout+c.MySQL.OperationTimeout {
		return fmt.Errorf("conf: DELIVERY_MQ_INVISIBLE must be >= DELIVERY_LEASE_TIMEOUT + MYSQL_OPERATION_TIMEOUT")
	}
	if d.ResultReserve <= c.MySQL.OperationTimeout {
		return fmt.Errorf("conf: DELIVERY_RESULT_RESERVE must be > MYSQL_OPERATION_TIMEOUT")
	}
	if d.BuildTimeout+d.HTTPTimeout+d.ResultReserve >= d.LeaseTimeout {
		return fmt.Errorf("conf: DELIVERY_BUILD_TIMEOUT + DELIVERY_HTTP_TIMEOUT + DELIVERY_RESULT_RESERVE must be < DELIVERY_LEASE_TIMEOUT")
	}
	if d.MQInvisible < d.CredentialPrepare+d.LeaseTimeout+d.AckTimeout+5*time.Second {
		return fmt.Errorf("conf: DELIVERY_MQ_INVISIBLE must be >= DELIVERY_CREDENTIAL_PREPARE + DELIVERY_LEASE_TIMEOUT + DELIVERY_ACK_TIMEOUT + 5s")
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
	return validateOutbox(c)
}

// validateOutbox 校验 S04 第 8 节的发布参数及其关系。
func validateOutbox(c *GlobalConfig) error {
	o := c.Outbox
	if err := durationBetween("OUTBOX_POLL_INTERVAL", o.PollInterval, 100*time.Millisecond, 10*time.Second); err != nil {
		return err
	}
	if err := intBetween("OUTBOX_BATCH_SIZE", o.BatchSize, 1, 500); err != nil {
		return err
	}
	if err := intBetween("OUTBOX_PARALLELISM", o.Parallelism, 1, 32); err != nil {
		return err
	}
	if err := durationBetween("OUTBOX_SEND_TIMEOUT", o.SendTimeout, 100*time.Millisecond, 10*time.Second); err != nil {
		return err
	}
	if err := durationBetween("OUTBOX_LEASE_TIMEOUT", o.LeaseTimeout, 5*time.Second, 120*time.Second); err != nil {
		return err
	}
	if o.LeaseTimeout <= o.SendTimeout+c.MySQL.OperationTimeout+5*time.Second {
		return fmt.Errorf("conf: OUTBOX_LEASE_TIMEOUT must be > OUTBOX_SEND_TIMEOUT + MYSQL_OPERATION_TIMEOUT + 5s")
	}
	if err := durationBetween("OUTBOX_BACKOFF_INITIAL", o.BackoffInitial, 100*time.Millisecond, 10*time.Second); err != nil {
		return err
	}
	if err := durationBetween("OUTBOX_BACKOFF_CAP", o.BackoffCap, o.BackoffInitial, 10*time.Minute); err != nil {
		return err
	}
	if err := intBetween("ROCKETMQ_MAX_DELIVERY_ATTEMPTS", o.MaxDeliveries, 2, 64); err != nil {
		return err
	}
	if err := durationBetween("ROCKETMQ_CLOCK_SAFETY", o.ClockSafety, 0, 5*time.Second); err != nil {
		return err
	}
	if o.MaxDelay <= o.ClockSafety {
		return fmt.Errorf("conf: ROCKETMQ_MAX_DELAY must be > ROCKETMQ_CLOCK_SAFETY")
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
