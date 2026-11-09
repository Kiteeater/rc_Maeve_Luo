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
	return nil
}

func durationBetween(name string, value, min, max time.Duration) error {
	if value < min || value > max {
		return fmt.Errorf("conf: %s out of range", name)
	}
	return nil
}
