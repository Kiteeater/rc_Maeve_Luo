package provider

import (
	"context"
	"errors"

	"notifier/internal/constant"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

// UnavailableRuntime 不访问网络。真实凭证、限流和受控 HTTP 由 S05 完成。
type UnavailableRuntime struct{}

func NewUnavailableRuntime() *UnavailableRuntime { return &UnavailableRuntime{} }

func (UnavailableRuntime) Prepare(context.Context, data_interface.PrepareRequest) (data_interface.PrepareResponse, error) {
	return data_interface.PrepareResponse{
		Kind: domain.PrepareBlock, ErrorCode: constant.ErrAdapterUnavailable, Reason: "供应商运行时由 S05 完成",
	}, nil
}

func (UnavailableRuntime) Build(context.Context, data_interface.TaskRecord, domain.Ownership, data_interface.PrepareResponse) (data_interface.BuiltRequest, error) {
	return data_interface.BuiltRequest{}, errors.New("provider: build unavailable")
}

func (UnavailableRuntime) SendOnce(context.Context, data_interface.BuiltRequest) (data_interface.SendResponse, error) {
	return data_interface.SendResponse{}, errors.New("provider: send unavailable")
}

func (UnavailableRuntime) Interpret(context.Context, data_interface.TaskRecord, data_interface.BuiltRequest, data_interface.SendResponse) (data_interface.Interpreted, error) {
	return data_interface.Interpreted{Outcome: enums.OutcomeUncertain, Certainty: enums.CertaintyUncertain, ErrorCode: constant.ErrAdapterUnavailable}, nil
}
