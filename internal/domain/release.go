package domain

import (
	"slices"
	"strings"
	"time"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
)

// Issue 是配置静态校验的一条错误。消息不含字段原始值。
type Issue struct {
	Code          string
	FieldPath     string
	RouteTargetID string
	Message       string
}

// ContentResult 是静态校验结果。存在 Issues 时 Doc 为空，不进入发布。
type ContentResult struct {
	Doc       *ReleaseDocument
	Callers   []string
	Issues    []Issue
	Truncated bool
}

// TargetBinding 是发布事务要复核的目标归属。语义正文是规范化字节。
type TargetBinding struct {
	RouteTargetID     string
	ChannelID         uint64
	ProviderID        string
	ProviderAccountID string
	Operation         string
	AdapterType       string
	AdapterVersion    string
	CredentialRef     string
	MappingID         string
	MappingVersion    string
	Semantic          []byte
}

// ReleaseDocument 是一份通过静态校验的发布内容。字段顺序不影响后续计算。
type ReleaseDocument struct {
	FormatVersion   int
	Canonical       []byte
	SHA256          string
	PolicyCanonical []byte
	callers         []string
	bindings        []TargetBinding
	schemas         []schemaDef
	routes          []routeDef
	targets         map[string]targetDef
	mappings        map[string]mappingDef
}

// CallerIDs 返回路由中出现的调用方，供发布权限核对。
func (d *ReleaseDocument) CallerIDs() []string {
	if d == nil {
		return nil
	}
	return append([]string(nil), d.callers...)
}

// Semantic 返回目标上已冻结的 semantic_config 规范化字节。
func (d *ReleaseDocument) Semantic(routeTargetID string) ([]byte, bool) {
	if d == nil {
		return nil, false
	}
	target, ok := d.targets[routeTargetID]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), target.SemanticCanonical...), true
}

// Bindings 返回目标归属，顺序按 route_target_id 固定。
func (d *ReleaseDocument) Bindings() []TargetBinding {
	if d == nil {
		return nil
	}
	return append([]TargetBinding(nil), d.bindings...)
}

type collector struct {
	items    []Issue
	overflow bool
}

func (c *collector) add(code, path, target, msg string) {
	if len(c.items) >= maxCollectedIssues {
		c.overflow = true
		return
	}
	c.items = append(c.items, Issue{Code: code, FieldPath: path, RouteTargetID: target, Message: msg})
}

func (c *collector) result() ([]Issue, bool) {
	slices.SortFunc(c.items, func(a, b Issue) int {
		if n := strings.Compare(a.FieldPath, b.FieldPath); n != 0 {
			return n
		}
		if n := strings.Compare(a.RouteTargetID, b.RouteTargetID); n != 0 {
			return n
		}
		if n := strings.Compare(a.Code, b.Code); n != 0 {
			return n
		}
		return strings.Compare(a.Message, b.Message)
	})
	truncated := c.overflow || len(c.items) > constant.MaxErrorDetails
	if len(c.items) > constant.MaxErrorDetails {
		c.items = append([]Issue(nil), c.items[:constant.MaxErrorDetails]...)
	}
	return c.items, truncated
}

type bag struct {
	m    map[string]canonicaljson.Value
	path string
	c    *collector
}

func newBag(v canonicaljson.Value, path string, c *collector) (bag, bool) {
	fields, ok := v.Object()
	if !ok {
		c.add("type_mismatch", loc(path), "", "必须是对象")
		return bag{}, false
	}
	m := make(map[string]canonicaljson.Value, len(fields))
	for _, field := range fields {
		m[field.Key] = field.Value
	}
	return bag{m: m, path: path, c: c}, true
}

func (b bag) allow(keys ...string) {
	allow := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		allow[key] = struct{}{}
	}
	for key := range b.m {
		if _, ok := allow[key]; ok {
			continue
		}
		code := "unknown_field"
		if alt, bad := forbiddenKey(key); bad {
			code = alt
		}
		b.c.add(code, joinPath(b.path, key), "", "不支持的字段")
	}
}

func forbiddenKey(key string) (string, bool) {
	switch key {
	case "script", "function", "pattern", "eval":
		return "script_forbidden", true
	case "$ref", "$schema", "allOf", "anyOf", "oneOf", "not":
		return "unsupported_keyword", true
	default:
		return "", false
	}
}

func (b bag) want(key string) (canonicaljson.Value, bool) {
	v, ok := b.m[key]
	if !ok {
		b.c.add("missing_field", joinPath(b.path, key), "", "缺少字段")
		return canonicaljson.Value{}, false
	}
	return v, true
}

func (b bag) text(key string, required bool) (string, bool) {
	v, ok := b.m[key]
	if !ok {
		if required {
			b.c.add("missing_field", joinPath(b.path, key), "", "缺少字段")
		}
		return "", false
	}
	s, ok := v.Text()
	if !ok {
		b.c.add("type_mismatch", joinPath(b.path, key), "", "必须是字符串")
		return "", false
	}
	return s, true
}

func (b bag) boolean(key string, def bool) bool {
	v, ok := b.m[key]
	if !ok {
		return def
	}
	flag, ok := v.Bool()
	if !ok {
		b.c.add("type_mismatch", joinPath(b.path, key), "", "必须是布尔值")
		return def
	}
	return flag
}

func (b bag) integer(key string) (int64, bool) {
	v, ok := b.want(key)
	if !ok {
		return 0, false
	}
	n, ok := v.Int64()
	if !ok {
		b.c.add("type_mismatch", joinPath(b.path, key), "", "必须是整数")
		return 0, false
	}
	return n, true
}

func loc(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

// ValidateContent 做本地静态校验，不访问通道、凭证或网络。
// JSON 语法错误通过 error 返回；内容不合法放在 Issues，调用方据此拒绝发布。
func ValidateContent(raw []byte, bounds Bounds) (ContentResult, error) {
	if len(raw) > bounds.MaxBytes {
		return ContentResult{Issues: []Issue{{
			Code: constant.CodeLimitExceeded, FieldPath: "/", Message: "配置超过大小上限",
		}}}, nil
	}
	value, err := canonicaljson.ParseObject(raw)
	if err != nil {
		return ContentResult{}, err
	}
	canon := canonicaljson.AppendCanonical(nil, value)
	if len(canon) > bounds.MaxBytes {
		return ContentResult{Issues: []Issue{{
			Code: constant.CodeLimitExceeded, FieldPath: "/", Message: "配置超过大小上限",
		}}}, nil
	}
	c := &collector{}
	doc := parseDocument(value, bounds, c)
	issues, truncated := c.result()
	if len(issues) > 0 {
		return ContentResult{Callers: doc.callers, Issues: issues, Truncated: truncated}, nil
	}
	doc.Canonical = canon
	doc.SHA256 = canonicaljson.Fingerprint(canon)
	return ContentResult{Doc: doc, Callers: append([]string(nil), doc.callers...)}, nil
}

// ReadFormatVersion 只读取已保存内容的格式版本，不用当前默认值补齐缺项。
func ReadFormatVersion(raw []byte) (int, error) {
	value, err := canonicaljson.ParseObject(raw)
	if err != nil {
		return 0, err
	}
	field, ok := value.Member("format_version")
	if !ok {
		return 0, errPointer
	}
	n, ok := field.Int64()
	if !ok || n != 1 {
		return 0, errPointer
	}
	return int(n), nil
}

func parseDocument(root canonicaljson.Value, bounds Bounds, c *collector) *ReleaseDocument {
	doc := &ReleaseDocument{targets: map[string]targetDef{}, mappings: map[string]mappingDef{}}
	b, ok := newBag(root, "", c)
	if !ok {
		return doc
	}
	b.allow("format_version", "event_schemas", "routes", "targets", "mappings", "policy")
	if v, ok := b.want("format_version"); ok {
		n, ok := v.Int64()
		if !ok || n != 1 {
			c.add("unsupported_version", "/format_version", "", "格式版本必须为 1")
		} else {
			doc.FormatVersion = 1
		}
	}
	if v, ok := b.want("event_schemas"); ok {
		doc.schemas = parseSchemas(v, c)
	}
	if v, ok := b.want("routes"); ok {
		doc.routes = parseRoutes(v, bounds, doc, c)
	}
	if v, ok := b.want("targets"); ok {
		parseTargets(v, bounds, doc, c)
	}
	if v, ok := b.want("mappings"); ok {
		parseMappings(v, bounds, doc, c)
	}
	if v, ok := b.want("policy"); ok {
		doc.PolicyCanonical = parsePolicy(v, bounds, c)
	}
	link(doc, c)
	return doc
}

type schemaDef struct {
	EventType     string
	SchemaVersion string
	Shape         shapeDef
}

type shapeDef struct {
	Type       string
	Nullable   bool
	Properties map[string]shapeDef
	Required   []string
	Additional bool
	Items      *shapeDef
	MaxItems   int
	MaxLength  int
	Enum       []canonicaljson.Value
}

func parseSchemas(v canonicaljson.Value, c *collector) []schemaDef {
	items, ok := v.Array()
	if !ok || len(items) == 0 {
		c.add("missing_field", "/event_schemas", "", "至少要有一个事件 schema")
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]schemaDef, 0, len(items))
	for i, item := range items {
		path := joinPath("/event_schemas", itoa(i))
		b, ok := newBag(item, path, c)
		if !ok {
			continue
		}
		b.allow("event_type", "schema_version", "shape")
		eventType, eOK := b.text("event_type", true)
		version, vOK := b.text("schema_version", true)
		if eOK && !validIdent(c, joinPath(path, "event_type"), eventType, maxASCIIIdent128) {
			eOK = false
		}
		if vOK && !validIdent(c, joinPath(path, "schema_version"), version, maxASCIIIdent32) {
			vOK = false
		}
		shapeV, sOK := b.want("shape")
		var shape shapeDef
		if sOK {
			shape = parseShape(shapeV, joinPath(path, "shape"), 1, c)
		}
		if eOK && vOK {
			key := eventType + "\x00" + version
			if _, dup := seen[key]; dup {
				c.add("duplicate", path, "", "事件类型与版本重复")
			} else {
				seen[key] = struct{}{}
				out = append(out, schemaDef{EventType: eventType, SchemaVersion: version, Shape: shape})
			}
		}
	}
	return out
}

func parseShape(v canonicaljson.Value, path string, depth int, c *collector) shapeDef {
	if depth > maxShapeDepth {
		c.add(constant.CodeLimitExceeded, path, "", "schema 层级超过上限")
		return shapeDef{}
	}
	b, ok := newBag(v, path, c)
	if !ok {
		return shapeDef{}
	}
	typ, ok := b.text("type", true)
	if !ok {
		return shapeDef{}
	}
	sh := shapeDef{Type: typ, Nullable: false}
	switch typ {
	case "object":
		b.allow("type", "properties", "required", "additional_properties", "nullable")
		sh.Nullable = b.boolean("nullable", false)
		sh.Additional = b.boolean("additional_properties", false)
		props, ok := b.want("properties")
		sh.Properties = map[string]shapeDef{}
		if ok {
			pb, ok := newBag(props, joinPath(path, "properties"), c)
			if ok {
				for key, child := range pb.m {
					if key == "" {
						c.add("invalid_identifier", joinPath(path, "properties"), "", "属性名不能为空")
						continue
					}
					sh.Properties[key] = parseShape(child, joinPath(joinPath(path, "properties"), key), depth+1, c)
				}
			}
		}
		if req, ok := b.m["required"]; ok {
			sh.Required = parseRequired(req, sh.Properties, joinPath(path, "required"), c)
		}
	case "array":
		b.allow("type", "items", "max_items", "nullable")
		sh.Nullable = b.boolean("nullable", false)
		items, ok := b.want("items")
		if ok {
			child := parseShape(items, joinPath(path, "items"), depth+1, c)
			sh.Items = &child
		}
		sh.MaxItems = defaultArrayItems
		if raw, ok := b.m["max_items"]; ok {
			n, ok := raw.Int64()
			if !ok || n < 0 || n > maxArrayItemsCap {
				c.add(constant.CodeLimitExceeded, joinPath(path, "max_items"), "", "max_items 超出上限")
			} else {
				sh.MaxItems = int(n)
			}
		}
	case "string", "number", "integer", "boolean", "null":
		b.allow("type", "enum", "max_length", "nullable")
		if typ != "string" {
			if _, ok := b.m["max_length"]; ok {
				c.add("unknown_field", joinPath(path, "max_length"), "", "不支持的字段")
			}
		}
		sh.Nullable = b.boolean("nullable", false)
		if typ == "string" {
			sh.MaxLength = defaultStringRunes
			if raw, ok := b.m["max_length"]; ok {
				n, ok := raw.Int64()
				if !ok || n < 0 || n > maxStringRunesCap {
					c.add(constant.CodeLimitExceeded, joinPath(path, "max_length"), "", "max_length 超出上限")
				} else {
					sh.MaxLength = int(n)
				}
			}
		}
		if raw, ok := b.m["enum"]; ok {
			sh.Enum = parseEnum(raw, typ, joinPath(path, "enum"), c)
		}
	default:
		c.add("unsupported_keyword", joinPath(path, "type"), "", "不支持的 schema 类型")
	}
	return sh
}

func parseRequired(v canonicaljson.Value, props map[string]shapeDef, path string, c *collector) []string {
	items, ok := v.Array()
	if !ok {
		c.add("type_mismatch", path, "", "required 必须是字符串数组")
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for i, item := range items {
		text, ok := item.Text()
		if !ok {
			c.add("type_mismatch", joinPath(path, itoa(i)), "", "required 必须是字符串")
			continue
		}
		if _, ok := props[text]; !ok {
			c.add("missing_field", joinPath(path, itoa(i)), "", "required 字段不在 properties 中")
			continue
		}
		if _, dup := seen[text]; dup {
			c.add("duplicate", joinPath(path, itoa(i)), "", "required 重复")
			continue
		}
		seen[text] = struct{}{}
		out = append(out, text)
	}
	return out
}

func parseEnum(v canonicaljson.Value, typ, path string, c *collector) []canonicaljson.Value {
	items, ok := v.Array()
	if !ok {
		c.add("type_mismatch", path, "", "enum 必须是数组")
		return nil
	}
	if len(items) == 0 || len(items) > maxEnumItems {
		c.add(constant.CodeLimitExceeded, path, "", "enum 数量超出上限")
		return nil
	}
	out := make([]canonicaljson.Value, 0, len(items))
	for i, item := range items {
		if !valueMatchesType(item, typ) {
			c.add("type_mismatch", joinPath(path, itoa(i)), "", "enum 成员类型不一致")
			continue
		}
		dup := false
		for _, prev := range out {
			if canonicaljson.Equal(prev, item) {
				dup = true
				break
			}
		}
		if dup {
			c.add("duplicate", joinPath(path, itoa(i)), "", "enum 成员重复")
			continue
		}
		out = append(out, item)
	}
	return out
}

func valueMatchesType(v canonicaljson.Value, typ string) bool {
	switch typ {
	case "string":
		_, ok := v.Text()
		return ok
	case "number":
		return v.Kind() == canonicaljson.KindNumber
	case "integer":
		return v.IsInteger()
	case "boolean":
		_, ok := v.Bool()
		return ok
	case "null":
		return v.Kind() == canonicaljson.KindNull
	case "object":
		return v.Kind() == canonicaljson.KindObject
	case "array":
		return v.Kind() == canonicaljson.KindArray
	default:
		return false
	}
}

type routeDef struct {
	ID             string
	CallerID       string
	SourceBusiness string
	EventType      string
	SchemaVersion  string
	Conditions     []conditionDef
	Targets        []string
}

type conditionDef struct {
	Segments []string
	Op       string
	Value    canonicaljson.Value
	In       []canonicaljson.Value
}

func parseRoutes(v canonicaljson.Value, bounds Bounds, doc *ReleaseDocument, c *collector) []routeDef {
	items, ok := v.Array()
	if !ok {
		c.add("type_mismatch", "/routes", "", "routes 必须是数组")
		return nil
	}
	if len(items) > bounds.MaxRoutes {
		c.add(constant.CodeLimitExceeded, "/routes", "", "路由数量超过上限")
	}
	limit := len(items)
	if limit > bounds.MaxRoutes {
		limit = bounds.MaxRoutes
	}
	seen := map[string]struct{}{}
	out := make([]routeDef, 0, limit)
	for i := 0; i < limit; i++ {
		path := joinPath("/routes", itoa(i))
		b, ok := newBag(items[i], path, c)
		if !ok {
			continue
		}
		b.allow("route_id", "caller_id", "source_business", "event_type", "schema_version", "conditions", "targets")
		route := routeDef{}
		if id, ok := b.text("route_id", true); ok && validIdent(c, joinPath(path, "route_id"), id, maxASCIIIdent128) {
			if _, dup := seen[id]; dup {
				c.add("duplicate", joinPath(path, "route_id"), "", "route_id 重复")
			} else {
				seen[id] = struct{}{}
				route.ID = id
			}
		}
		if caller, ok := b.text("caller_id", true); ok && validIdent(c, joinPath(path, "caller_id"), caller, maxASCIIIdent64) {
			route.CallerID = caller
			doc.callers = append(doc.callers, caller)
		}
		if src, ok := b.text("source_business", true); ok && validIdent(c, joinPath(path, "source_business"), src, maxASCIIIdent64) {
			route.SourceBusiness = src
		}
		if eventType, ok := b.text("event_type", true); ok && validIdent(c, joinPath(path, "event_type"), eventType, maxASCIIIdent128) {
			route.EventType = eventType
		}
		if version, ok := b.text("schema_version", true); ok && validIdent(c, joinPath(path, "schema_version"), version, maxASCIIIdent32) {
			route.SchemaVersion = version
		}
		cond, ok := b.want("conditions")
		if ok {
			route.Conditions = parseConditions(cond, bounds, joinPath(path, "conditions"), c)
		}
		targets, ok := b.want("targets")
		if ok {
			route.Targets = parseRouteTargets(targets, joinPath(path, "targets"), c)
		}
		out = append(out, route)
	}
	return out
}

func parseConditions(v canonicaljson.Value, bounds Bounds, path string, c *collector) []conditionDef {
	items, ok := v.Array()
	if !ok {
		c.add("type_mismatch", path, "", "conditions 必须是数组")
		return nil
	}
	if len(items) > bounds.MaxConditions {
		c.add(constant.CodeLimitExceeded, path, "", "条件数量超过上限")
	}
	limit := len(items)
	if limit > bounds.MaxConditions {
		limit = bounds.MaxConditions
	}
	out := make([]conditionDef, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, parseCondition(items[i], bounds, joinPath(path, itoa(i)), c))
	}
	return out
}

func parseCondition(v canonicaljson.Value, bounds Bounds, path string, c *collector) conditionDef {
	b, ok := newBag(v, path, c)
	if !ok {
		return conditionDef{}
	}
	b.allow("path", "op", "value")
	op, _ := b.text("op", true)
	rawPath, _ := b.text("path", true)
	cond := conditionDef{Op: op}
	if rawPath != "" {
		segs, err := DecodePointer(rawPath, bounds.MaxPathBytes)
		if err != nil {
			c.add("invalid_pointer", joinPath(path, "path"), "", "JSON Pointer 不合法")
		} else {
			cond.Segments = segs
		}
	}
	switch op {
	case "exists":
		if _, ok := b.m["value"]; ok {
			c.add("unknown_field", joinPath(path, "value"), "", "exists 不能带 value")
		}
	case "eq":
		val, ok := b.want("value")
		if ok && !scalar(val) {
			c.add("type_mismatch", joinPath(path, "value"), "", "eq 的值必须是标量")
		} else if ok {
			cond.Value = val
		}
	case "in":
		val, ok := b.want("value")
		if !ok {
			break
		}
		items, ok := val.Array()
		if !ok || len(items) == 0 || len(items) > maxInItems {
			c.add(constant.CodeLimitExceeded, joinPath(path, "value"), "", "in 必须是非空标量数组且不超过 100 项")
			break
		}
		for i, item := range items {
			if !scalar(item) {
				c.add("type_mismatch", joinPath(joinPath(path, "value"), itoa(i)), "", "in 的成员必须是标量")
				continue
			}
			cond.In = append(cond.In, item)
		}
	default:
		if op == "" {
			break
		}
		code := "unsupported_op"
		if op == "script" || op == "eval" || op == "regex" || op == "function" {
			code = "script_forbidden"
		}
		c.add(code, joinPath(path, "op"), "", "不支持的条件运算")
	}
	return cond
}

func scalar(v canonicaljson.Value) bool {
	switch v.Kind() {
	case canonicaljson.KindNull, canonicaljson.KindBool, canonicaljson.KindNumber, canonicaljson.KindString:
		return true
	default:
		return false
	}
}

func parseRouteTargets(v canonicaljson.Value, path string, c *collector) []string {
	items, ok := v.Array()
	if !ok || len(items) == 0 || len(items) > 50 {
		c.add(constant.CodeLimitExceeded, path, "", "每条路由的目标数量必须在 1 到 50")
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for i, item := range items {
		text, ok := item.Text()
		if !ok {
			c.add("type_mismatch", joinPath(path, itoa(i)), "", "目标引用必须是字符串")
			continue
		}
		if !validIdent(c, joinPath(path, itoa(i)), text, maxASCIIIdent128) {
			continue
		}
		if _, dup := seen[text]; dup {
			c.add("duplicate", joinPath(path, itoa(i)), text, "同一路由内目标重复")
			continue
		}
		seen[text] = struct{}{}
		out = append(out, text)
	}
	return out
}

type targetDef struct {
	ID                string
	ChannelID         string
	ChannelNum        uint64
	ProviderID        string
	ProviderAccountID string
	Operation         string
	AdapterType       string
	AdapterVersion    string
	CredentialRef     string
	MappingID         string
	MappingVersion    string
	Semantic          canonicaljson.Value
	SemanticCanonical []byte
}

func parseTargets(v canonicaljson.Value, bounds Bounds, doc *ReleaseDocument, c *collector) {
	b, ok := newBag(v, "/targets", c)
	if !ok {
		return
	}
	if len(b.m) > bounds.MaxTargets {
		c.add(constant.CodeLimitExceeded, "/targets", "", "目标数量超过上限")
	}
	keys := make([]string, 0, len(b.m))
	for key := range b.m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	kept := 0
	for _, key := range keys {
		if kept >= bounds.MaxTargets {
			break
		}
		kept++
		path := joinPath("/targets", key)
		if !validIdent(c, path, key, maxASCIIIdent128) {
			continue
		}
		tb, ok := newBag(b.m[key], path, c)
		if !ok {
			continue
		}
		tb.allow("channel_id", "provider_id", "provider_account_id", "operation", "adapter_type", "adapter_version", "credential_ref", "mapping_id", "mapping_version", "semantic_config")
		target := targetDef{ID: key}
		if id, ok := tb.text("channel_id", true); ok {
			n, err := ParseInternalID("channel_id", id)
			if err != nil {
				c.add("invalid_identifier", joinPath(path, "channel_id"), key, "标识不合法")
			} else {
				target.ChannelID = id
				target.ChannelNum = n
			}
		}
		target.ProviderID = identField(tb, c, "provider_id", key, maxASCIIIdent64)
		target.ProviderAccountID = identField(tb, c, "provider_account_id", key, maxASCIIIdent64)
		target.Operation = identField(tb, c, "operation", key, maxASCIIIdent64)
		target.AdapterType = identField(tb, c, "adapter_type", key, maxASCIIIdent64)
		target.AdapterVersion = identField(tb, c, "adapter_version", key, maxASCIIIdent32)
		if ref, ok := tb.text("credential_ref", true); ok {
			if err := ValidateExternalID("credential_ref", ref); err != nil {
				c.add("invalid_identifier", joinPath(path, "credential_ref"), key, "标识不合法")
			} else {
				target.CredentialRef = ref
			}
		}
		target.MappingID = identField(tb, c, "mapping_id", key, maxASCIIIdent128)
		target.MappingVersion = identField(tb, c, "mapping_version", key, maxASCIIIdent32)
		sem, ok := tb.want("semantic_config")
		if ok {
			if sem.Kind() != canonicaljson.KindObject {
				c.add("type_mismatch", joinPath(path, "semantic_config"), key, "必须是对象")
			} else {
				walkForbidden(sem, joinPath(path, "semantic_config"), key, c)
				target.Semantic = sem
				target.SemanticCanonical = canonicaljson.AppendCanonical(nil, sem)
			}
		}
		doc.targets[key] = target
	}
}

func identField(b bag, c *collector, key, target string, maxLen int) string {
	text, ok := b.text(key, true)
	if !ok {
		return ""
	}
	if !validIdent(c, joinPath(b.path, key), text, maxLen) {
		return ""
	}
	return text
}

func walkForbidden(v canonicaljson.Value, path, target string, c *collector) {
	switch v.Kind() {
	case canonicaljson.KindObject:
		fields, _ := v.Object()
		for _, field := range fields {
			if code, bad := forbiddenKey(field.Key); bad {
				c.add(code, joinPath(path, field.Key), target, "不支持的字段")
			}
			walkForbidden(field.Value, joinPath(path, field.Key), target, c)
		}
	case canonicaljson.KindArray:
		items, _ := v.Array()
		for i, item := range items {
			walkForbidden(item, joinPath(path, itoa(i)), target, c)
		}
	}
}

type mappingDef struct {
	Version string
	Fields  []fieldDef
}

type fieldDef struct {
	To          string
	ToSeg       []string
	FromSeg     []string
	HasFrom     bool
	Constant    canonicaljson.Value
	HasConstant bool
	Default     canonicaljson.Value
	HasDefault  bool
	Required    bool
	Nullable    bool
	Type        string
	SourceType  string
	Enum        []enumPair
}

type enumPair struct {
	From canonicaljson.Value
	To   canonicaljson.Value
}

func parseMappings(v canonicaljson.Value, bounds Bounds, doc *ReleaseDocument, c *collector) {
	b, ok := newBag(v, "/mappings", c)
	if !ok {
		return
	}
	keys := make([]string, 0, len(b.m))
	for key := range b.m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		path := joinPath("/mappings", key)
		if !validIdent(c, path, key, maxASCIIIdent128) {
			continue
		}
		mb, ok := newBag(b.m[key], path, c)
		if !ok {
			continue
		}
		mb.allow("version", "fields")
		mapping := mappingDef{}
		if ver, ok := mb.text("version", true); ok && validIdent(c, joinPath(path, "version"), ver, maxASCIIIdent32) {
			mapping.Version = ver
		}
		fields, ok := mb.want("fields")
		if ok {
			mapping.Fields = parseFields(fields, bounds, joinPath(path, "fields"), c)
		}
		doc.mappings[key] = mapping
	}
}

func parseFields(v canonicaljson.Value, bounds Bounds, path string, c *collector) []fieldDef {
	items, ok := v.Array()
	if !ok {
		c.add("type_mismatch", path, "", "fields 必须是数组")
		return nil
	}
	if len(items) == 0 || len(items) > bounds.MaxMappingFields {
		c.add(constant.CodeLimitExceeded, path, "", "映射字段数量超出上限")
	}
	limit := len(items)
	if limit > bounds.MaxMappingFields {
		limit = bounds.MaxMappingFields
	}
	out := make([]fieldDef, 0, limit)
	var toPaths [][]string
	for i := 0; i < limit; i++ {
		field, segs := parseField(items[i], bounds, joinPath(path, itoa(i)), c)
		out = append(out, field)
		if len(segs) > 0 {
			toPaths = append(toPaths, segs)
		}
	}
	for i := 0; i < len(toPaths); i++ {
		for j := i + 1; j < len(toPaths); j++ {
			if pathConflict(toPaths[i], toPaths[j]) {
				c.add("conflict", path, "", "输出路径重复或父子冲突")
			}
		}
	}
	return out
}

func pathConflict(a, b []string) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func parseField(v canonicaljson.Value, bounds Bounds, path string, c *collector) (fieldDef, []string) {
	b, ok := newBag(v, path, c)
	if !ok {
		return fieldDef{}, nil
	}
	b.allow("to", "from", "constant", "default", "required", "nullable", "type", "source_type", "enum_map")
	field := fieldDef{Required: b.boolean("required", false), Nullable: b.boolean("nullable", false)}
	rawTo, toOK := b.text("to", true)
	if toOK {
		segs, err := DecodePointer(rawTo, bounds.MaxPathBytes)
		if err != nil {
			c.add("invalid_pointer", joinPath(path, "to"), "", "JSON Pointer 不合法")
		} else {
			field.To = rawTo
			field.ToSeg = segs
		}
	}
	_, hasFrom := b.m["from"]
	_, hasConst := b.m["constant"]
	if hasFrom == hasConst {
		c.add("conflict", path, "", "from 与 constant 必须恰有一个")
	}
	if hasFrom {
		rawFrom, ok := b.text("from", true)
		if ok {
			segs, err := DecodePointer(rawFrom, bounds.MaxPathBytes)
			if err != nil {
				c.add("invalid_pointer", joinPath(path, "from"), "", "JSON Pointer 不合法")
			} else {
				field.HasFrom = true
				field.FromSeg = segs
			}
		}
	}
	if hasConst {
		field.HasConstant = true
		field.Constant = b.m["constant"]
		if _, ok := b.m["default"]; ok {
			c.add("unknown_field", joinPath(path, "default"), "", "constant 不能带 default")
		}
		if _, ok := b.m["enum_map"]; ok {
			c.add("unknown_field", joinPath(path, "enum_map"), "", "constant 不能带 enum_map")
		}
		if _, ok := b.m["source_type"]; ok {
			c.add("unknown_field", joinPath(path, "source_type"), "", "constant 不能带 source_type")
		}
	}
	if raw, ok := b.m["default"]; ok && hasFrom {
		field.HasDefault = true
		field.Default = raw
	}
	typ, ok := b.text("type", true)
	if ok && !knownJSONType(typ) {
		c.add("unsupported_keyword", joinPath(path, "type"), "", "不支持的 JSON 类型")
	} else if ok {
		field.Type = typ
	}
	if raw, ok := b.m["source_type"]; ok && hasFrom {
		text, ok := raw.Text()
		if !ok || !knownJSONType(text) {
			c.add("unsupported_keyword", joinPath(path, "source_type"), "", "不支持的 JSON 类型")
		} else {
			field.SourceType = text
		}
	}
	if raw, ok := b.m["enum_map"]; ok && hasFrom {
		if field.SourceType == "" {
			c.add("missing_field", joinPath(path, "source_type"), "", "使用 enum_map 时必须声明 source_type")
		}
		field.Enum = parseEnumMap(raw, field.SourceType, field.Type, joinPath(path, "enum_map"), c)
	}
	if field.HasConstant && field.Type != "" && !valueMatchesType(field.Constant, field.Type) {
		if !(field.Constant.Kind() == canonicaljson.KindNull && field.Nullable) {
			c.add("type_mismatch", joinPath(path, "constant"), "", "constant 与 type 不一致")
		}
	}
	if field.HasDefault && field.Type != "" {
		checkDefault(field, joinPath(path, "default"), c)
	}
	return field, field.ToSeg
}

func checkDefault(field fieldDef, path string, c *collector) {
	if len(field.Enum) > 0 {
		if field.SourceType != "" && !valueMatchesType(field.Default, field.SourceType) {
			c.add("type_mismatch", path, "", "default 与 source_type 不一致")
			return
		}
		found := false
		for _, pair := range field.Enum {
			if canonicaljson.Equal(pair.From, field.Default) {
				found = true
				break
			}
		}
		if !found {
			c.add("enum_miss", path, "", "default 不在枚举来源中")
		}
		return
	}
	expect := field.Type
	if field.SourceType != "" {
		expect = field.SourceType
	}
	if expect != "" && !valueMatchesType(field.Default, expect) {
		c.add("type_mismatch", path, "", "default 与类型不一致")
	}
}

func parseEnumMap(v canonicaljson.Value, sourceType, outType, path string, c *collector) []enumPair {
	items, ok := v.Array()
	if !ok || len(items) == 0 || len(items) > maxEnumItems {
		c.add(constant.CodeLimitExceeded, path, "", "enum_map 数量超出上限")
		return nil
	}
	out := make([]enumPair, 0, len(items))
	for i, item := range items {
		ip := joinPath(path, itoa(i))
		b, ok := newBag(item, ip, c)
		if !ok {
			continue
		}
		b.allow("from", "to")
		from, fOK := b.want("from")
		to, tOK := b.want("to")
		if !fOK || !tOK {
			continue
		}
		if !scalar(from) {
			c.add("type_mismatch", joinPath(ip, "from"), "", "enum_map 来源必须是标量")
			continue
		}
		if sourceType != "" && !valueMatchesType(from, sourceType) {
			c.add("type_mismatch", joinPath(ip, "from"), "", "enum_map 来源类型不一致")
			continue
		}
		if outType != "" && !valueMatchesType(to, outType) {
			c.add("type_mismatch", joinPath(ip, "to"), "", "enum_map 目标类型不一致")
			continue
		}
		dup := false
		for _, prev := range out {
			if canonicaljson.Equal(prev.From, from) {
				dup = true
				break
			}
		}
		if dup {
			c.add("duplicate", joinPath(ip, "from"), "", "枚举来源重复")
			continue
		}
		out = append(out, enumPair{From: from, To: to})
	}
	return out
}

func knownJSONType(typ string) bool {
	switch typ {
	case "string", "number", "integer", "boolean", "null", "object", "array":
		return true
	default:
		return false
	}
}

func parsePolicy(v canonicaljson.Value, bounds Bounds, c *collector) []byte {
	b, ok := newBag(v, "/policy", c)
	if !ok {
		return nil
	}
	b.allow("max_attempts", "round_timeout_seconds", "backoff_initial_ms", "backoff_cap_ms", "server_wait_cap_seconds")
	attempts, aOK := b.integer("max_attempts")
	round, rOK := b.integer("round_timeout_seconds")
	initial, iOK := b.integer("backoff_initial_ms")
	capMs, cOK := b.integer("backoff_cap_ms")
	wait, wOK := b.integer("server_wait_cap_seconds")
	if aOK && (attempts < 1 || attempts > 32) {
		c.add(constant.CodeLimitExceeded, "/policy/max_attempts", "", "超出允许范围")
	}
	if rOK && (round < 60 || round > 604800) {
		c.add(constant.CodeLimitExceeded, "/policy/round_timeout_seconds", "", "超出允许范围")
	}
	if iOK && (initial < 100 || initial > 60000) {
		c.add(constant.CodeLimitExceeded, "/policy/backoff_initial_ms", "", "超出允许范围")
	}
	if cOK && iOK && (capMs < initial || capMs > 3600000) {
		c.add(constant.CodeLimitExceeded, "/policy/backoff_cap_ms", "", "超出允许范围")
	}
	if wOK && (wait < 1 || wait > 3600 || time.Duration(wait)*time.Second > bounds.ServerWaitCap) {
		c.add(constant.CodeLimitExceeded, "/policy/server_wait_cap_seconds", "", "超出允许范围")
	}
	return canonicaljson.AppendCanonical(nil, v)
}

func link(doc *ReleaseDocument, c *collector) {
	schemas := map[string]struct{}{}
	for _, schema := range doc.schemas {
		schemas[schema.EventType+"\x00"+schema.SchemaVersion] = struct{}{}
	}
	for i, route := range doc.routes {
		path := joinPath("/routes", itoa(i))
		if route.EventType != "" && route.SchemaVersion != "" {
			if _, ok := schemas[route.EventType+"\x00"+route.SchemaVersion]; !ok {
				c.add("missing_field", path, "", "路由引用了不存在的事件 schema")
			}
		}
		for j, id := range route.Targets {
			if _, ok := doc.targets[id]; !ok {
				c.add("missing_field", joinPath(joinPath(path, "targets"), itoa(j)), id, "目标不存在")
			}
		}
	}
	ids := make([]string, 0, len(doc.targets))
	for id := range doc.targets {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		target := doc.targets[id]
		mapping, ok := doc.mappings[target.MappingID]
		if target.MappingID != "" && !ok {
			c.add("missing_field", joinPath(joinPath("/targets", id), "mapping_id"), id, "映射不存在")
			continue
		}
		if ok && target.MappingVersion != "" && mapping.Version != "" && mapping.Version != target.MappingVersion {
			c.add("conflict", joinPath(joinPath("/targets", id), "mapping_version"), id, "映射版本不一致")
		}
		if target.ChannelNum != 0 && target.ProviderID != "" && target.CredentialRef != "" && len(target.SemanticCanonical) > 0 {
			doc.bindings = append(doc.bindings, TargetBinding{
				RouteTargetID:     id,
				ChannelID:         target.ChannelNum,
				ProviderID:        target.ProviderID,
				ProviderAccountID: target.ProviderAccountID,
				Operation:         target.Operation,
				AdapterType:       target.AdapterType,
				AdapterVersion:    target.AdapterVersion,
				CredentialRef:     target.CredentialRef,
				MappingID:         target.MappingID,
				MappingVersion:    target.MappingVersion,
				Semantic:          append([]byte(nil), target.SemanticCanonical...),
			})
		}
	}
}

func validIdent(c *collector, path, value string, maxLen int) bool {
	if err := ValidateASCIIIdent(path, value, maxLen); err != nil {
		c.add("invalid_identifier", path, "", "标识不合法")
		return false
	}
	return true
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
