package domain

import "strings"

const (
	maxPointerSegments = 16
	maxShapeDepth      = 16
	maxEnumItems       = 100
	maxInItems         = 100
	maxStringRunesCap  = 65536
	defaultStringRunes = 4096
	maxArrayItemsCap   = 1000
	defaultArrayItems  = 100
	maxCollectedIssues = 1000
	maxEventFieldCount = 10000
)

// DecodePointer 解析配置里的 JSON Pointer。
// 禁止空根路径、数字数组索引、非法转义，并受路径字节数和段数限制。
func DecodePointer(path string, maxBytes int) ([]string, error) {
	if path == "" || len(path) > maxBytes || path[0] != '/' {
		return nil, errPointer
	}
	parts := strings.Split(path[1:], "/")
	if len(parts) == 0 || len(parts) > maxPointerSegments {
		return nil, errPointer
	}
	out := make([]string, len(parts))
	for i, part := range parts {
		seg, err := unescapePointer(part)
		if err != nil || seg == "" || isArrayIndex(seg) {
			return nil, errPointer
		}
		out[i] = seg
	}
	return out, nil
}

var errPointer = errString("invalid pointer")

type errString string

func (e errString) Error() string { return string(e) }

func unescapePointer(seg string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(seg); i++ {
		if seg[i] != '~' {
			b.WriteByte(seg[i])
			continue
		}
		if i+1 >= len(seg) {
			return "", errPointer
		}
		switch seg[i+1] {
		case '0':
			b.WriteByte('~')
		case '1':
			b.WriteByte('/')
		default:
			return "", errPointer
		}
		i++
	}
	return b.String(), nil
}

func isArrayIndex(s string) bool {
	if s == "-" || s == "0" {
		return true
	}
	if s[0] < '1' || s[0] > '9' {
		return false
	}
	for i := 1; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func pointerToken(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	return strings.ReplaceAll(s, "/", "~1")
}

func joinPath(path, key string) string {
	return path + "/" + pointerToken(key)
}
