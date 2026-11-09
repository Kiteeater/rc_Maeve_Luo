package domain

import "unicode/utf8"

// TruncateUTF8 按字节截断，不切断多字节字符。返回值和是否发生截断。
// 截断标记由调用方单独持久化。摘要上限的配置边界见 constant.SummaryBytesMin/Max。
func TruncateUTF8(s string, maxBytes int) (string, bool) {
	if maxBytes < 0 {
		maxBytes = 0
	}
	if len(s) <= maxBytes {
		return s, false
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut], true
}
