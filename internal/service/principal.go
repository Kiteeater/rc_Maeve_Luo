package service

import "context"

// Principal 是传输层交给用例的调用身份。凭证不在其中。
type Principal struct {
	ActorID        string
	CallerID       string
	AllowedSources []string
	Scopes         []string
	Permissions    []string
}

type principalKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
