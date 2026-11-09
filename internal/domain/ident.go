package domain

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode"
	"unicode/utf8"
)

// 标识规则见 S00 第 9 节。比较区分大小写，不裁剪、不改大小写、不做 Unicode 归一化。

const (
	maxASCIIIdent64  = 64
	maxASCIIIdent128 = 128
	maxASCIIIdent32  = 32
	maxExternalID    = 191
	maxCount         = math.MaxUint32
)

// ValidateASCIIIdent 校验 `[A-Za-z0-9][A-Za-z0-9._:-]*`，长度按字节计。
func ValidateASCIIIdent(field, value string, maxLen int) error {
	if value == "" || len(value) > maxLen {
		return fmt.Errorf("domain: %s: invalid identifier", field)
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		ok := c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
		if i > 0 {
			ok = ok || c == '.' || c == '_' || c == ':' || c == '-'
		}
		if !ok {
			return fmt.Errorf("domain: %s: invalid identifier", field)
		}
	}
	return nil
}

func ValidateCallerID(value string) error {
	return ValidateASCIIIdent("caller_id", value, maxASCIIIdent64)
}

func ValidateSourceBusiness(value string) error {
	return ValidateASCIIIdent("source_business", value, maxASCIIIdent64)
}

func ValidateProviderID(value string) error {
	return ValidateASCIIIdent("provider_id", value, maxASCIIIdent64)
}

func ValidateProviderAccountID(value string) error {
	return ValidateASCIIIdent("provider_account_id", value, maxASCIIIdent64)
}

func ValidateOperation(value string) error {
	return ValidateASCIIIdent("operation", value, maxASCIIIdent64)
}

func ValidateBusinessObjectType(value string) error {
	return ValidateASCIIIdent("business_object_type", value, maxASCIIIdent64)
}

func ValidateActorID(value string) error {
	return ValidateASCIIIdent("actor_id", value, maxASCIIIdent64)
}

func ValidateEventType(value string) error {
	return ValidateASCIIIdent("event_type", value, maxASCIIIdent128)
}

func ValidateRouteTargetID(value string) error {
	return ValidateASCIIIdent("route_target_id", value, maxASCIIIdent128)
}

func ValidateSchemaVersion(value string) error {
	return ValidateASCIIIdent("schema_version", value, maxASCIIIdent32)
}

func ValidateAdapterVersion(value string) error {
	return ValidateASCIIIdent("adapter_version", value, maxASCIIIdent32)
}

func ValidateMappingVersion(value string) error {
	return ValidateASCIIIdent("mapping_version", value, maxASCIIIdent32)
}

// ValidateExternalID 校验来源事件、业务对象和操作幂等键。
// 保留原文，禁止控制字符和纯空白。
func ValidateExternalID(field, value string) error {
	if value == "" || len(value) > maxExternalID || !utf8.ValidString(value) {
		return fmt.Errorf("domain: %s: invalid identifier", field)
	}
	blank := true
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("domain: %s: invalid identifier", field)
		}
		if !unicode.IsSpace(r) {
			blank = false
		}
	}
	if blank {
		return fmt.Errorf("domain: %s: invalid identifier", field)
	}
	return nil
}

// ParseInternalID 解析 HTTP/MQ 中的内部主键十进制字符串。
// 范围 1..uint64 max，禁止符号、前导零和浮点形式。
func ParseInternalID(field, value string) (uint64, error) {
	if value == "" || value[0] == '0' || value[0] == '+' || value[0] == '-' {
		return 0, fmt.Errorf("domain: %s: invalid identifier", field)
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, fmt.Errorf("domain: %s: invalid identifier", field)
		}
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("domain: %s: invalid identifier", field)
	}
	return n, nil
}

// FormatInternalID 把内部主键写成不带前导零的十进制字符串。
func FormatInternalID(id uint64) (string, error) {
	if id == 0 {
		return "", errors.New("domain: internal id must be positive")
	}
	return strconv.FormatUint(id, 10), nil
}

// CheckCount 校验轮次、版本、次数和历史序号的首版上限。溢出不回绕。
func CheckCount(field string, n uint64) error {
	if n > maxCount {
		return fmt.Errorf("domain: %s: count overflow", field)
	}
	return nil
}
