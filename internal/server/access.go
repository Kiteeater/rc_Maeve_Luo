package server

import (
	"net/http"

	"notifier/internal/auth"
	"notifier/internal/domain"
	"notifier/internal/service"
)

func requirePermission(set *auth.Set, permission string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := auth.BearerToken(r.Header.Get("Authorization"))
		if err != nil || set == nil {
			service.WriteError(w, r, authErr(service.CodeUnauthenticated, "未认证"))
			return
		}
		ctx, err := set.Authenticate(token)
		if err != nil {
			service.WriteError(w, r, authErr(service.CodeUnauthenticated, "未认证"))
			return
		}
		if !ctx.Allows(permission) {
			service.WriteError(w, r, authErr(service.CodeForbidden, "无权限"))
			return
		}
		principal := service.Principal{
			ActorID: ctx.ActorID, CallerID: ctx.CallerID,
			AllowedSources: ctx.AllowedSources, Scopes: ctx.CallerScopes,
		}
		next.ServeHTTP(w, r.WithContext(service.WithPrincipal(r.Context(), principal)))
	})
}

func authErr(code, message string) error {
	err, _ := domain.NewError(code, message, "", nil)
	return err
}
