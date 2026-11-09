package canonicaljson

import (
	"strconv"
	"strings"
)

// Parse 解析一个完整 JSON 值。规则与 ParseObject 相同。
func Parse(raw []byte) (Value, error) {
	p := parser{b: raw}
	p.skipWS()
	if p.i >= len(p.b) {
		return Value{}, errAt("unexpected end", p.i)
	}
	// 根对象的深度与 ParseObject 一致：容器本身算第 1 层。
	v, err := p.parseValue(0)
	if err != nil {
		return Value{}, err
	}
	p.skipWS()
	if p.i != len(p.b) {
		return Value{}, errAt("trailing data", p.i)
	}
	return v, nil
}

// Null、Bool、String、Number、Object、Array 构造精确值。
// Object 和 Array 复制切片，调用方之后改原切片不影响值。
func Null() Value { return Value{kind: KindNull} }

func Bool(b bool) Value { return Value{kind: KindBool, b: b} }

func String(s string) Value { return Value{kind: KindString, str: s} }

func Number(canonical string) Value { return Value{kind: KindNumber, num: canonical} }

func Object(fields []Field) Value {
	return Value{kind: KindObject, obj: append([]Field(nil), fields...)}
}

func Array(items []Value) Value {
	return Value{kind: KindArray, arr: append([]Value(nil), items...)}
}

// Uint 把非负整数规范成 JSON 数字。
func Uint(n uint64) (Value, error) {
	return Parse([]byte(strconv.FormatUint(n, 10)))
}

// Member 按键查找对象字段。
func (v Value) Member(key string) (Value, bool) {
	fields, ok := v.Object()
	if !ok {
		return Value{}, false
	}
	for _, field := range fields {
		if field.Key == key {
			return field.Value, true
		}
	}
	return Value{}, false
}

// Equal 用规范化字节比较。数字 1 与 1.0 相等，不同类型不相等。
func Equal(a, b Value) bool {
	if a.kind != b.kind {
		return false
	}
	return string(AppendCanonical(nil, a)) == string(AppendCanonical(nil, b))
}

// IsInteger 判断规范化数字是否为整数。1.0 规范成 1e0，视为整数。
func (v Value) IsInteger() bool {
	if v.kind != KindNumber {
		return false
	}
	if v.num == "0" {
		return true
	}
	s := v.num
	if s[0] == '-' {
		s = s[1:]
	}
	e := strings.IndexByte(s, 'e')
	if e <= 0 {
		return false
	}
	exp, err := strconv.Atoi(s[e+1:])
	return err == nil && exp >= 0
}

// Int64 读取整数。非整数或超出 int64 时返回 false。
func (v Value) Int64() (int64, bool) {
	if !v.IsInteger() {
		return 0, false
	}
	if v.num == "0" {
		return 0, true
	}
	neg := v.num[0] == '-'
	s := v.num
	if neg {
		s = s[1:]
	}
	e := strings.IndexByte(s, 'e')
	exp, err := strconv.Atoi(s[e+1:])
	if err != nil || exp < 0 {
		return 0, false
	}
	coef := s[:e]
	if len(coef)+exp > 18 {
		return 0, false
	}
	var n int64
	for _, c := range coef {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	for i := 0; i < exp; i++ {
		if n > (1<<63-1)/10 {
			return 0, false
		}
		n *= 10
	}
	if neg {
		n = -n
	}
	return n, true
}
