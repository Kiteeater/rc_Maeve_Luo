package domain

import (
	"context"
	"slices"
	"unicode/utf8"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
)

// BuildPlan 按固定发布计算投递计划。不写库，也不读取当前指针。
// 任一目标映射或数据版本失败时不返回部分计划。
func BuildPlan(ctx context.Context, doc *ReleaseDocument, releaseID uint64, versionNo uint32, env EventEnvelope, bounds Bounds) (DeliveryPlan, error) {
	if err := ctx.Err(); err != nil {
		return DeliveryPlan{}, planTimeout()
	}
	if doc == nil || releaseID == 0 {
		return DeliveryPlan{}, codeErr(constant.CodeConfigurationUnavailable, "发布版本不可用", "/")
	}
	if len(env.DataCanonical) > bounds.MaxEventBytes {
		return DeliveryPlan{}, codeErr(constant.CodeLimitExceeded, "事件数据超过上限", "/")
	}
	data, err := canonicaljson.Parse(env.DataCanonical)
	if err != nil {
		return DeliveryPlan{}, codeErr(constant.CodeInvalidRequest, "事件数据不是合法 JSON", "/")
	}
	// 大小已在入口检查。字段数先于 schema 解释；层级由 schema 遍历自己封顶。
	if fieldCount(data) > maxEventFieldCount {
		return DeliveryPlan{}, codeErr(constant.CodeLimitExceeded, "事件数据字段数超过上限", "/")
	}
	schema, ok := findSchema(doc, env.EventType, env.SchemaVersion)
	if !ok {
		return DeliveryPlan{}, codeErr(constant.CodeUnsupportedSchema, "不支持的事件类型或版本", "/event_type")
	}
	var shapeIssues []Issue
	applyShape(schema.Shape, data, "", 1, &shapeIssues)
	if len(shapeIssues) > 0 {
		return DeliveryPlan{}, issuesError(constant.CodeUnsupportedSchema, "事件数据不符合已发布的 schema", shapeIssues)
	}
	if err := ctx.Err(); err != nil {
		return DeliveryPlan{}, planTimeout()
	}
	matched := matchRoutes(doc, env, data)
	if len(matched) == 0 {
		return DeliveryPlan{}, codeErr(constant.CodeNoMatchingRoute, "没有匹配的路由", "")
	}
	ids := unionTargets(matched)
	if len(ids) > bounds.MaxFanout {
		return DeliveryPlan{}, codeErr(constant.CodeLimitExceeded, "扇出超过上限", "/targets")
	}
	releaseRef, err := FormatInternalID(releaseID)
	if err != nil {
		return DeliveryPlan{}, codeErr(constant.CodeInternal, "内部错误", "")
	}
	var mapIssues []Issue
	targets := make([]TargetPlan, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return DeliveryPlan{}, planTimeout()
		}
		target := doc.targets[id]
		mapping := doc.mappings[target.MappingID]
		payload, issues := applyMapping(data, mapping, id, bounds)
		if len(issues) > 0 {
			mapIssues = append(mapIssues, issues...)
			continue
		}
		fixed := fixedTarget(target)
		targets = append(targets, TargetPlan{
			RouteTargetID:     id,
			ProviderID:        target.ProviderID,
			ProviderAccountID: target.ProviderAccountID,
			Operation:         target.Operation,
			MappingID:         target.MappingID,
			MappingVersion:    target.MappingVersion,
			AdapterType:       target.AdapterType,
			AdapterVersion:    target.AdapterVersion,
			PayloadCanonical:  payload,
			FixedTarget:       fixed,
		})
	}
	if len(mapIssues) > 0 {
		return DeliveryPlan{}, issuesError(constant.CodeMappingInvalid, "发布 "+releaseRef+" 映射失败", mapIssues)
	}
	return DeliveryPlan{
		ReleaseRef:    releaseRef,
		VersionNo:     versionNo,
		FormatVersion: doc.FormatVersion,
		Targets:       targets,
		RoundPolicy:   append([]byte(nil), doc.PolicyCanonical...),
	}, nil
}

func findSchema(doc *ReleaseDocument, eventType, version string) (schemaDef, bool) {
	for _, schema := range doc.schemas {
		if schema.EventType == eventType && schema.SchemaVersion == version {
			return schema, true
		}
	}
	return schemaDef{}, false
}

func matchRoutes(doc *ReleaseDocument, env EventEnvelope, data canonicaljson.Value) []routeDef {
	var out []routeDef
	for _, route := range doc.routes {
		if route.CallerID != env.CallerID || route.SourceBusiness != env.SourceBusiness {
			continue
		}
		if route.EventType != env.EventType || route.SchemaVersion != env.SchemaVersion {
			continue
		}
		if !matchConditions(route.Conditions, data) {
			continue
		}
		out = append(out, route)
	}
	return out
}

func matchConditions(conditions []conditionDef, data canonicaljson.Value) bool {
	for _, cond := range conditions {
		value, exists := lookup(data, cond.Segments)
		switch cond.Op {
		case "exists":
			if !exists {
				return false
			}
		case "eq":
			if !exists || !canonicaljson.Equal(value, cond.Value) {
				return false
			}
		case "in":
			if !exists || !inList(value, cond.In) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func inList(value canonicaljson.Value, items []canonicaljson.Value) bool {
	for _, item := range items {
		if canonicaljson.Equal(value, item) {
			return true
		}
	}
	return false
}

// lookup 从事件 data 根按对象字段下钻。非对象继续下钻视为缺失。显式 null 视为存在。
func lookup(root canonicaljson.Value, segs []string) (canonicaljson.Value, bool) {
	cur := root
	for _, seg := range segs {
		if cur.Kind() != canonicaljson.KindObject {
			return canonicaljson.Value{}, false
		}
		next, ok := cur.Member(seg)
		if !ok {
			return canonicaljson.Value{}, false
		}
		cur = next
	}
	return cur, true
}

func unionTargets(routes []routeDef) []string {
	seen := map[string]struct{}{}
	var ids []string
	for _, route := range routes {
		for _, id := range route.Targets {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func applyMapping(data canonicaljson.Value, mapping mappingDef, target string, bounds Bounds) ([]byte, []Issue) {
	root := &tree{}
	var issues []Issue
	for _, field := range mapping.Fields {
		value, omit, issue := evalField(data, field)
		if issue != nil {
			issue.RouteTargetID = target
			issues = append(issues, *issue)
			continue
		}
		if omit {
			continue
		}
		if err := root.insert(field.ToSeg, value); err != nil {
			issues = append(issues, Issue{
				Code: "conflict", FieldPath: field.To, RouteTargetID: target, Message: "输出路径冲突",
			})
		}
	}
	if len(issues) > 0 {
		return nil, issues
	}
	payload := canonicaljson.AppendCanonical(nil, root.value())
	if len(payload) > bounds.MaxPayloadBytes {
		return nil, []Issue{{
			Code: constant.CodeLimitExceeded, FieldPath: "/", RouteTargetID: target, Message: "映射载荷超过上限",
		}}
	}
	return payload, nil
}

func evalField(data canonicaljson.Value, field fieldDef) (canonicaljson.Value, bool, *Issue) {
	var value canonicaljson.Value
	present := false
	if field.HasConstant {
		value = field.Constant
		present = true
	} else if field.HasFrom {
		value, present = lookup(data, field.FromSeg)
		if !present && field.HasDefault {
			value = field.Default
			present = true
		}
	}
	if !present {
		if field.Required {
			return canonicaljson.Value{}, false, &Issue{Code: "required", FieldPath: field.To, Message: "缺少必填字段"}
		}
		return canonicaljson.Value{}, true, nil
	}
	if value.Kind() == canonicaljson.KindNull {
		if field.Nullable || field.Type == "null" {
			return value, false, nil
		}
		return canonicaljson.Value{}, false, &Issue{Code: "null_forbidden", FieldPath: field.To, Message: "该字段不允许 null"}
	}
	if field.SourceType != "" && !valueMatchesType(value, field.SourceType) {
		return canonicaljson.Value{}, false, &Issue{Code: "type_mismatch", FieldPath: field.To, Message: "来源类型不匹配"}
	}
	if len(field.Enum) > 0 {
		found := false
		for _, pair := range field.Enum {
			if canonicaljson.Equal(pair.From, value) {
				value = pair.To
				found = true
				break
			}
		}
		if !found {
			return canonicaljson.Value{}, false, &Issue{Code: "enum_miss", FieldPath: field.To, Message: "枚举没有对应值"}
		}
	}
	if field.Type != "" && !valueMatchesType(value, field.Type) {
		return canonicaljson.Value{}, false, &Issue{Code: "type_mismatch", FieldPath: field.To, Message: "输出类型不匹配"}
	}
	return value, false, nil
}

type tree struct {
	child map[string]*tree
	set   bool
	val   canonicaljson.Value
}

func (n *tree) insert(segs []string, val canonicaljson.Value) error {
	cur := n
	for i, seg := range segs {
		if cur.set {
			return errPointer
		}
		if cur.child == nil {
			cur.child = map[string]*tree{}
		}
		next := cur.child[seg]
		if next == nil {
			next = &tree{}
			cur.child[seg] = next
		}
		if i == len(segs)-1 {
			if next.set || len(next.child) > 0 {
				return errPointer
			}
			next.set = true
			next.val = val
			return nil
		}
		if next.set {
			return errPointer
		}
		cur = next
	}
	return errPointer
}

func (n *tree) value() canonicaljson.Value {
	if n == nil || n.set {
		if n == nil {
			return canonicaljson.Object(nil)
		}
		return n.val
	}
	fields := make([]canonicaljson.Field, 0, len(n.child))
	for key, child := range n.child {
		fields = append(fields, canonicaljson.Field{Key: key, Value: child.value()})
	}
	return canonicaljson.Object(fields)
}

func fixedTarget(target targetDef) []byte {
	obj := canonicaljson.Object([]canonicaljson.Field{
		{Key: "adapter_type", Value: canonicaljson.String(target.AdapterType)},
		{Key: "adapter_version", Value: canonicaljson.String(target.AdapterVersion)},
		{Key: "channel_id", Value: canonicaljson.String(target.ChannelID)},
		{Key: "credential_ref", Value: canonicaljson.String(target.CredentialRef)},
		{Key: "mapping_id", Value: canonicaljson.String(target.MappingID)},
		{Key: "mapping_version", Value: canonicaljson.String(target.MappingVersion)},
		{Key: "operation", Value: canonicaljson.String(target.Operation)},
		{Key: "provider_account_id", Value: canonicaljson.String(target.ProviderAccountID)},
		{Key: "provider_id", Value: canonicaljson.String(target.ProviderID)},
		{Key: "route_target_id", Value: canonicaljson.String(target.ID)},
		{Key: "semantic_config", Value: target.Semantic},
	})
	return canonicaljson.AppendCanonical(nil, obj)
}

func applyShape(sh shapeDef, v canonicaljson.Value, path string, depth int, out *[]Issue) {
	if depth > maxShapeDepth {
		*out = append(*out, Issue{Code: constant.CodeLimitExceeded, FieldPath: loc(path), Message: "层级超过上限"})
		return
	}
	if v.Kind() == canonicaljson.KindNull {
		if sh.Nullable || sh.Type == "null" {
			return
		}
		*out = append(*out, Issue{Code: "null_forbidden", FieldPath: loc(path), Message: "不允许 null"})
		return
	}
	switch sh.Type {
	case "object":
		fields, ok := v.Object()
		if !ok {
			*out = append(*out, Issue{Code: "type_mismatch", FieldPath: loc(path), Message: "类型不匹配"})
			return
		}
		present := make(map[string]canonicaljson.Value, len(fields))
		for _, field := range fields {
			present[field.Key] = field.Value
		}
		if !sh.Additional {
			for key := range present {
				if _, ok := sh.Properties[key]; !ok {
					*out = append(*out, Issue{Code: "unknown_field", FieldPath: joinPath(path, key), Message: "存在未声明字段"})
				}
			}
		}
		for _, key := range sh.Required {
			if _, ok := present[key]; !ok {
				*out = append(*out, Issue{Code: "required", FieldPath: joinPath(path, key), Message: "缺少必填字段"})
			}
		}
		for key, child := range sh.Properties {
			val, ok := present[key]
			if !ok {
				continue
			}
			applyShape(child, val, joinPath(path, key), depth+1, out)
		}
	case "array":
		items, ok := v.Array()
		if !ok {
			*out = append(*out, Issue{Code: "type_mismatch", FieldPath: loc(path), Message: "类型不匹配"})
			return
		}
		maxItems := sh.MaxItems
		if maxItems == 0 {
			maxItems = defaultArrayItems
		}
		if len(items) > maxItems {
			*out = append(*out, Issue{Code: constant.CodeLimitExceeded, FieldPath: loc(path), Message: "数组长度超过上限"})
			return
		}
		if sh.Items == nil {
			*out = append(*out, Issue{Code: "missing_field", FieldPath: loc(path), Message: "数组缺少 items"})
			return
		}
		for i, item := range items {
			applyShape(*sh.Items, item, joinPath(path, itoa(i)), depth+1, out)
		}
	case "string":
		text, ok := v.Text()
		if !ok {
			*out = append(*out, Issue{Code: "type_mismatch", FieldPath: loc(path), Message: "类型不匹配"})
			return
		}
		maxLen := sh.MaxLength
		if maxLen == 0 {
			maxLen = defaultStringRunes
		}
		if utf8.RuneCountInString(text) > maxLen {
			*out = append(*out, Issue{Code: constant.CodeLimitExceeded, FieldPath: loc(path), Message: "字符串超过上限"})
			return
		}
		if len(sh.Enum) > 0 && !enumHas(sh.Enum, v) {
			*out = append(*out, Issue{Code: "enum_miss", FieldPath: loc(path), Message: "不在枚举中"})
		}
	case "number", "integer", "boolean":
		if !valueMatchesType(v, sh.Type) {
			*out = append(*out, Issue{Code: "type_mismatch", FieldPath: loc(path), Message: "类型不匹配"})
			return
		}
		if len(sh.Enum) > 0 && !enumHas(sh.Enum, v) {
			*out = append(*out, Issue{Code: "enum_miss", FieldPath: loc(path), Message: "不在枚举中"})
		}
	case "null":
		*out = append(*out, Issue{Code: "type_mismatch", FieldPath: loc(path), Message: "类型不匹配"})
	default:
		*out = append(*out, Issue{Code: "unsupported_keyword", FieldPath: loc(path), Message: "不支持的 schema 类型"})
	}
}

func enumHas(items []canonicaljson.Value, v canonicaljson.Value) bool {
	for _, item := range items {
		if canonicaljson.Equal(item, v) {
			return true
		}
	}
	return false
}

func fieldCount(v canonicaljson.Value) int {
	switch v.Kind() {
	case canonicaljson.KindObject:
		fields, _ := v.Object()
		n := len(fields)
		for _, field := range fields {
			n += fieldCount(field.Value)
		}
		return n
	case canonicaljson.KindArray:
		items, _ := v.Array()
		n := 0
		for _, item := range items {
			n += fieldCount(item)
		}
		return n
	default:
		return 0
	}
}

func codeErr(code, message, path string) error {
	err, _ := NewError(code, message, path, nil)
	return err
}

func planTimeout() error {
	return codeErr(constant.CodePlanTimeout, "计划计算超时", "")
}

func issuesError(code, message string, issues []Issue) error {
	slices.SortFunc(issues, func(a, b Issue) int {
		if n := a.RouteTargetID; n != b.RouteTargetID {
			if a.RouteTargetID < b.RouteTargetID {
				return -1
			}
			return 1
		}
		if a.FieldPath < b.FieldPath {
			return -1
		}
		if a.FieldPath > b.FieldPath {
			return 1
		}
		if a.Code < b.Code {
			return -1
		}
		if a.Code > b.Code {
			return 1
		}
		return 0
	})
	if len(issues) > constant.MaxErrorDetails {
		issues = issues[:constant.MaxErrorDetails]
	}
	details := make([]Detail, len(issues))
	for i, item := range issues {
		msg := item.Message
		if item.RouteTargetID != "" {
			msg = item.RouteTargetID + ": " + msg
		}
		details[i] = Detail{FieldPath: item.FieldPath, Code: item.Code, Message: msg}
	}
	path := ""
	if len(details) > 0 {
		path = details[0].FieldPath
	}
	err, fail := NewError(code, message, path, details)
	if fail != nil {
		err, _ = NewError(code, message, path, nil)
	}
	return err
}
