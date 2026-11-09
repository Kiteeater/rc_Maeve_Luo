package domain

import (
	"fmt"
	"time"
)

// ParseRFC3339 解析带时区的 RFC3339 时间。
// 小数最多 6 位，年份 1000..9999，结果为 UTC。超过微秒精度时拒绝，不截断。
func ParseRFC3339(field, value string) (time.Time, error) {
	if err := validateRFC3339Text(value); err != nil {
		return time.Time{}, fmt.Errorf("domain: %s: %w", field, err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("domain: %s: invalid time", field)
	}
	year := parsed.UTC().Year()
	if year < 1000 || year > 9999 {
		return time.Time{}, fmt.Errorf("domain: %s: invalid time", field)
	}
	return parsed.UTC(), nil
}

func validateRFC3339Text(value string) error {
	dot := -1
	for i := 0; i < len(value); i++ {
		if value[i] == '.' {
			dot = i
			break
		}
	}
	if dot < 0 {
		return nil
	}
	frac := 0
	for i := dot + 1; i < len(value); i++ {
		c := value[i]
		if c < '0' || c > '9' {
			break
		}
		frac++
	}
	if frac == 0 || frac > 6 {
		return fmt.Errorf("invalid time")
	}
	return nil
}

// FormatUTCMicros 写成指纹使用的 UTC 微秒文本，固定 6 位小数。
func FormatUTCMicros(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}
