package domain

import (
	"fmt"

	"notifier/internal/constant"
)

// Detail 是 422 校验明细。不得放入字段原始值。
type Detail struct {
	FieldPath string
	Code      string
	Message   string
}

// Error 是跨模块的稳定领域错误。biz 只返回这类错误，不返回驱动错误。
type Error struct {
	Code       string
	ReasonCode string
	Message    string
	FieldPath  string
	Details    []Detail
}

func (e *Error) Error() string {
	code := e.Code
	if e.ReasonCode != "" && e.ReasonCode != e.Code {
		code += "/" + e.ReasonCode
	}
	if e.FieldPath != "" {
		return fmt.Sprintf("domain: %s: %s (%s)", code, e.Message, e.FieldPath)
	}
	return fmt.Sprintf("domain: %s: %s", code, e.Message)
}

// NewError 接受通用码或精确原因码；精确原因的消息由码表固定。明细超限时拒绝，避免静默丢失。
func NewError(code, message, fieldPath string, details []Detail) (*Error, error) {
	if len(details) > constant.MaxErrorDetails {
		return nil, fmt.Errorf("domain: details exceed %d", constant.MaxErrorDetails)
	}
	copied := append([]Detail(nil), details...)
	reasonCode := code
	if definition, ok := LookupError(reasonCode); ok {
		if code != definition.Code || message == "" {
			message = definition.Message
		}
		code = definition.Code
	}
	return &Error{Code: code, ReasonCode: reasonCode, Message: message, FieldPath: fieldPath, Details: copied}, nil
}
