// Package canonicaljson 解析并规范化精确 JSON。
//
// 数字保持十进制，不经过 float64。对象键按 Unicode 码点排序。
// 指纹算法版本见 constant.FingerprintVersion；本包只对已规范化字节计算 SHA-256。
package canonicaljson

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"notifier/internal/constant"
)

const (
	maxDepth         = constant.MaxJSONDepth
	maxNumberLiteral = constant.MaxJSONNumberLiteral
	maxCoefficient   = constant.MaxJSONCoefficientBits
	maxExponent      = constant.MaxJSONExponent
)

// Error 是可定位的 JSON 语法或范围错误。
type Error struct {
	Reason string
	Offset int
}

func (e *Error) Error() string {
	return fmt.Sprintf("canonicaljson: %s at byte %d", e.Reason, e.Offset)
}

func errAt(reason string, offset int) error {
	return &Error{Reason: reason, Offset: offset}
}

// Kind 是 JSON 值类别。
type Kind int

const (
	KindNull Kind = iota
	KindBool
	KindNumber
	KindString
	KindArray
	KindObject
)

// Field 是对象的一个成员。编码时按键排序，不保留原文顺序。
type Field struct {
	Key   string
	Value Value
}

// Value 是精确 JSON 值。数字以规范化十进制文本保存。
type Value struct {
	kind Kind
	b    bool
	num  string
	str  string
	arr  []Value
	obj  []Field
}

func (v Value) Kind() Kind { return v.kind }

func (v Value) Bool() (bool, bool) {
	if v.kind != KindBool {
		return false, false
	}
	return v.b, true
}

func (v Value) Text() (string, bool) {
	if v.kind != KindString {
		return "", false
	}
	return v.str, true
}

// NumberCanonical 返回规范化数字文本，例如 1e0、123e-4。零为 "0"。
func (v Value) NumberCanonical() (string, bool) {
	if v.kind != KindNumber {
		return "", false
	}
	return v.num, true
}

func (v Value) Array() ([]Value, bool) {
	if v.kind != KindArray {
		return nil, false
	}
	return v.arr, true
}

func (v Value) Object() ([]Field, bool) {
	if v.kind != KindObject {
		return nil, false
	}
	return v.obj, true
}

// ParseObject 解析一个完整 JSON 对象。拒绝重复键、尾随数据、非法 UTF-8、
// 不成对的代理项、超过 32 层的容器、超过 128 字节的数字字面量，
// 以及规范化后系数超过 64 位或指数超出 -308..308 的数字。
func ParseObject(raw []byte) (Value, error) {
	p := parser{b: raw}
	p.skipWS()
	if p.i >= len(p.b) || p.b[p.i] != '{' {
		return Value{}, errAt("expected object", p.i)
	}
	v, err := p.parseObject(1)
	if err != nil {
		return Value{}, err
	}
	p.skipWS()
	if p.i != len(p.b) {
		return Value{}, errAt("trailing data", p.i)
	}
	return v, nil
}

// Canonical 解析对象并返回规范化字节。
func Canonical(raw []byte) ([]byte, error) {
	v, err := ParseObject(raw)
	if err != nil {
		return nil, err
	}
	return AppendCanonical(nil, v), nil
}

// AppendCanonical 把值写成规范化 JSON。
func AppendCanonical(dst []byte, v Value) []byte {
	switch v.kind {
	case KindNull:
		return append(dst, "null"...)
	case KindBool:
		if v.b {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case KindNumber:
		return append(dst, v.num...)
	case KindString:
		return appendString(dst, v.str)
	case KindArray:
		dst = append(dst, '[')
		for i, item := range v.arr {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = AppendCanonical(dst, item)
		}
		return append(dst, ']')
	case KindObject:
		fields := append([]Field(nil), v.obj...)
		slices.SortFunc(fields, func(a, b Field) int {
			return strings.Compare(a.Key, b.Key)
		})
		dst = append(dst, '{')
		for i, field := range fields {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendString(dst, field.Key)
			dst = append(dst, ':')
			dst = AppendCanonical(dst, field.Value)
		}
		return append(dst, '}')
	default:
		return append(dst, "null"...)
	}
}

// Fingerprint 是规范化字节的 SHA-256 小写十六进制。版本号单独持久化。
func Fingerprint(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for _, r := range s {
		switch r {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		default:
			if r < 0x20 {
				dst = fmt.Appendf(dst, `\u%04x`, r)
			} else {
				dst = utf8.AppendRune(dst, r)
			}
		}
	}
	return append(dst, '"')
}

type parser struct {
	b []byte
	i int
}

func (p *parser) skipWS() {
	for p.i < len(p.b) {
		switch p.b[p.i] {
		case ' ', '\n', '\r', '\t':
			p.i++
		default:
			return
		}
	}
}

func (p *parser) parseObject(depth int) (Value, error) {
	if depth > maxDepth {
		return Value{}, errAt("max depth", p.i)
	}
	p.i++ // '{'
	p.skipWS()
	fields := make([]Field, 0)
	seen := make(map[string]struct{})
	if p.i < len(p.b) && p.b[p.i] == '}' {
		p.i++
		return Value{kind: KindObject, obj: fields}, nil
	}
	for {
		p.skipWS()
		if p.i >= len(p.b) || p.b[p.i] != '"' {
			return Value{}, errAt("expected object key", p.i)
		}
		key, err := p.parseString()
		if err != nil {
			return Value{}, err
		}
		if _, ok := seen[key]; ok {
			return Value{}, errAt("duplicate key", p.i)
		}
		seen[key] = struct{}{}
		p.skipWS()
		if p.i >= len(p.b) || p.b[p.i] != ':' {
			return Value{}, errAt("expected colon", p.i)
		}
		p.i++
		val, err := p.parseValue(depth)
		if err != nil {
			return Value{}, err
		}
		fields = append(fields, Field{Key: key, Value: val})
		p.skipWS()
		if p.i >= len(p.b) {
			return Value{}, errAt("unexpected end", p.i)
		}
		switch p.b[p.i] {
		case ',':
			p.i++
			continue
		case '}':
			p.i++
			return Value{kind: KindObject, obj: fields}, nil
		default:
			return Value{}, errAt("expected comma or object end", p.i)
		}
	}
}

func (p *parser) parseArray(depth int) (Value, error) {
	if depth > maxDepth {
		return Value{}, errAt("max depth", p.i)
	}
	p.i++ // '['
	p.skipWS()
	items := make([]Value, 0)
	if p.i < len(p.b) && p.b[p.i] == ']' {
		p.i++
		return Value{kind: KindArray, arr: items}, nil
	}
	for {
		val, err := p.parseValue(depth)
		if err != nil {
			return Value{}, err
		}
		items = append(items, val)
		p.skipWS()
		if p.i >= len(p.b) {
			return Value{}, errAt("unexpected end", p.i)
		}
		switch p.b[p.i] {
		case ',':
			p.i++
			continue
		case ']':
			p.i++
			return Value{kind: KindArray, arr: items}, nil
		default:
			return Value{}, errAt("expected comma or array end", p.i)
		}
	}
}

func (p *parser) parseValue(depth int) (Value, error) {
	p.skipWS()
	if p.i >= len(p.b) {
		return Value{}, errAt("unexpected end", p.i)
	}
	switch p.b[p.i] {
	case '{':
		return p.parseObject(depth + 1)
	case '[':
		return p.parseArray(depth + 1)
	case '"':
		s, err := p.parseString()
		if err != nil {
			return Value{}, err
		}
		return Value{kind: KindString, str: s}, nil
	case 't':
		if err := p.consumeLiteral("true"); err != nil {
			return Value{}, err
		}
		return Value{kind: KindBool, b: true}, nil
	case 'f':
		if err := p.consumeLiteral("false"); err != nil {
			return Value{}, err
		}
		return Value{kind: KindBool, b: false}, nil
	case 'n':
		if err := p.consumeLiteral("null"); err != nil {
			return Value{}, err
		}
		return Value{kind: KindNull}, nil
	default:
		if p.b[p.i] == '-' || (p.b[p.i] >= '0' && p.b[p.i] <= '9') {
			return p.parseNumber()
		}
		return Value{}, errAt("invalid value", p.i)
	}
}

func (p *parser) consumeLiteral(lit string) error {
	if p.i+len(lit) > len(p.b) || string(p.b[p.i:p.i+len(lit)]) != lit {
		return errAt("invalid literal", p.i)
	}
	p.i += len(lit)
	return nil
}

func (p *parser) parseString() (string, error) {
	start := p.i
	p.i++ // '"'
	var b strings.Builder
	for p.i < len(p.b) {
		c := p.b[p.i]
		if c == '"' {
			p.i++
			return b.String(), nil
		}
		if c == '\\' {
			r, err := p.parseEscape()
			if err != nil {
				return "", err
			}
			b.WriteRune(r)
			continue
		}
		if c < 0x20 {
			return "", errAt("raw control character", p.i)
		}
		r, size := utf8.DecodeRune(p.b[p.i:])
		if r == utf8.RuneError && size == 1 {
			return "", errAt("invalid utf-8", p.i)
		}
		b.WriteRune(r)
		p.i += size
	}
	return "", errAt("unterminated string", start)
}

func (p *parser) parseEscape() (rune, error) {
	if p.i >= len(p.b) || p.b[p.i] != '\\' {
		return 0, errAt("invalid escape", p.i)
	}
	p.i++
	if p.i >= len(p.b) {
		return 0, errAt("invalid escape", p.i)
	}
	switch p.b[p.i] {
	case '"', '\\', '/':
		r := rune(p.b[p.i])
		p.i++
		return r, nil
	case 'b':
		p.i++
		return '\b', nil
	case 'f':
		p.i++
		return '\f', nil
	case 'n':
		p.i++
		return '\n', nil
	case 'r':
		p.i++
		return '\r', nil
	case 't':
		p.i++
		return '\t', nil
	case 'u':
		p.i++
		r, err := p.parseHex4()
		if err != nil {
			return 0, err
		}
		if r < 0xD800 || r > 0xDFFF {
			return r, nil
		}
		if r > 0xDBFF {
			return 0, errAt("lone surrogate", p.i)
		}
		if p.i+1 >= len(p.b) || p.b[p.i] != '\\' || p.b[p.i+1] != 'u' {
			return 0, errAt("lone surrogate", p.i)
		}
		p.i += 2
		lo, err := p.parseHex4()
		if err != nil {
			return 0, err
		}
		if lo < 0xDC00 || lo > 0xDFFF {
			return 0, errAt("lone surrogate", p.i)
		}
		return 0x10000 + (r-0xD800)<<10 + (lo - 0xDC00), nil
	default:
		return 0, errAt("invalid escape", p.i)
	}
}

func (p *parser) parseHex4() (rune, error) {
	if p.i+4 > len(p.b) {
		return 0, errAt("invalid unicode escape", p.i)
	}
	var r rune
	for n := 0; n < 4; n++ {
		c := p.b[p.i]
		var d rune
		switch {
		case c >= '0' && c <= '9':
			d = rune(c - '0')
		case c >= 'a' && c <= 'f':
			d = rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			d = rune(c-'A') + 10
		default:
			return 0, errAt("invalid unicode escape", p.i)
		}
		r = r<<4 | d
		p.i++
	}
	return r, nil
}

func (p *parser) parseNumber() (Value, error) {
	start := p.i
	if p.b[p.i] == '-' {
		p.i++
	}
	if p.i >= len(p.b) || p.b[p.i] < '0' || p.b[p.i] > '9' {
		return Value{}, errAt("invalid number", start)
	}
	if p.b[p.i] == '0' {
		p.i++
	} else {
		for p.i < len(p.b) && p.b[p.i] >= '0' && p.b[p.i] <= '9' {
			p.i++
		}
	}
	intEnd := p.i
	fracStart := -1
	if p.i < len(p.b) && p.b[p.i] == '.' {
		p.i++
		fracStart = p.i
		if p.i >= len(p.b) || p.b[p.i] < '0' || p.b[p.i] > '9' {
			return Value{}, errAt("invalid number", start)
		}
		for p.i < len(p.b) && p.b[p.i] >= '0' && p.b[p.i] <= '9' {
			p.i++
		}
	}
	fracEnd := p.i
	exp := 0
	if p.i < len(p.b) && (p.b[p.i] == 'e' || p.b[p.i] == 'E') {
		p.i++
		sign := 1
		if p.i < len(p.b) && (p.b[p.i] == '+' || p.b[p.i] == '-') {
			if p.b[p.i] == '-' {
				sign = -1
			}
			p.i++
		}
		if p.i >= len(p.b) || p.b[p.i] < '0' || p.b[p.i] > '9' {
			return Value{}, errAt("invalid number", start)
		}
		expOverflow := false
		for p.i < len(p.b) && p.b[p.i] >= '0' && p.b[p.i] <= '9' {
			if !expOverflow {
				exp = exp*10 + int(p.b[p.i]-'0')
				if exp > 1_000_000 {
					expOverflow = true
				}
			}
			p.i++
		}
		if expOverflow {
			// 系数全零时仍规范成 0；非零数字的指数必然越界。用哨兵留给 canonicalNumber。
			exp = maxExponent + 1
		}
		exp *= sign
	}
	if p.i-start > maxNumberLiteral {
		return Value{}, errAt("number literal too long", start)
	}
	// 前导零只允许单独的 0。整数部分在负号之后。
	intStart := start
	neg := p.b[start] == '-'
	if neg {
		intStart++
	}
	if intEnd-intStart > 1 && p.b[intStart] == '0' {
		return Value{}, errAt("invalid number", start)
	}
	intDigits := string(p.b[intStart:intEnd])
	fracDigits := ""
	if fracStart >= 0 {
		fracDigits = string(p.b[fracStart:fracEnd])
	}
	num, err := canonicalNumber(neg, intDigits, fracDigits, exp)
	if err != nil {
		return Value{}, err
	}
	return Value{kind: KindNumber, num: num}, nil
}

func canonicalNumber(neg bool, intDigits, fracDigits string, exp int) (string, error) {
	coef := strings.TrimLeft(intDigits+fracDigits, "0")
	exp -= len(fracDigits)
	if coef == "" {
		return "0", nil
	}
	trimmed := strings.TrimRight(coef, "0")
	exp += len(coef) - len(trimmed)
	coef = trimmed
	if len(coef) > maxCoefficient {
		return "", errAt("coefficient too long", 0)
	}
	if exp < -maxExponent || exp > maxExponent {
		return "", errAt("exponent out of range", 0)
	}
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString(coef)
	b.WriteByte('e')
	b.WriteString(strconv.Itoa(exp))
	return b.String(), nil
}
