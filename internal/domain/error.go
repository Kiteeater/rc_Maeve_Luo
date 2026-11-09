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
	Code      string
	Message   string
	FieldPath string
	Details   []Detail
}

func (e *Error) Error() string {
	if e.FieldPath != "" {
		return fmt.Sprintf("domain: %s: %s (%s)", e.Code, e.Message, e.FieldPath)
	}
	return fmt.Sprintf("domain: %s: %s", e.Code, e.Message)
}

// NewError 构造领域错误。明细超过 100 项时拒绝，避免静默丢掉校验结果。
func NewError(code, message, fieldPath string, details []Detail) (*Error, error) {
	if len(details) > constant.MaxErrorDetails {
		return nil, fmt.Errorf("domain: details exceed %d", constant.MaxErrorDetails)
	}
	copied := append([]Detail(nil), details...)
	return &Error{Code: code, Message: message, FieldPath: fieldPath, Details: copied}, nil
}
