package auth_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"notifier/internal/auth"
	"notifier/internal/constant"
)

func TestAuth_BadToken(t *testing.T) {
	set := mustSet(t, sample(true))
	if _, err := set.Authenticate("wrong-token"); err != auth.ErrUnauthenticated {
		t.Fatalf("bad token = %v", err)
	}
	if _, err := auth.BearerToken("Token abc"); err != auth.ErrUnauthenticated {
		t.Fatal("non-Bearer scheme must be unauthenticated")
	}
}

func TestAuth_DisabledIdentity(t *testing.T) {
	enabled := sample(true)
	enabled.TokenSHA256 = hashOf("enabled-token")
	disabled := sample(false)
	disabled.ActorID = "disabled-actor"
	disabled.TokenSHA256 = hashOf("disabled-token")
	set := mustSet(t, enabled, disabled)
	if _, err := set.Authenticate("disabled-token"); err != auth.ErrDisabled {
		t.Fatalf("disabled identity = %v, want ErrDisabled", err)
	}
	ctx, err := set.Authenticate("enabled-token")
	if err != nil || ctx.ActorID != enabled.ActorID {
		t.Fatalf("enabled identity = %+v, %v", ctx, err)
	}
}

func TestAuth_DuplicateTokenSHA256(t *testing.T) {
	a := sample(true)
	b := sample(true)
	b.ActorID = "other-actor"
	_, err := auth.NewStatic([]auth.Identity{a, b})
	if err == nil || strings.Contains(err.Error(), a.TokenSHA256) {
		t.Fatalf("duplicate token_sha256 must fail without the hash, got %v", err)
	}
}

func TestAuth_UnknownPermission(t *testing.T) {
	item := sample(true)
	item.Permissions = []string{"events:write", "not-a-permission"}
	if _, err := auth.NewStatic([]auth.Identity{item}); err == nil {
		t.Fatal("unknown permission must fail")
	}
	if !constant.KnownPermission(constant.PermEventsWrite) || constant.KnownPermission("not-a-permission") {
		t.Fatal("permission set is wrong")
	}
}

func TestAuth_EmptyCallerScopes(t *testing.T) {
	item := sample(true)
	item.CallerScopes = nil
	if _, err := auth.NewStatic([]auth.Identity{item}); err == nil {
		t.Fatal("empty caller_scopes must fail")
	}
}

func TestAuth_LoadExampleFile(t *testing.T) {
	path := filepath.Join("..", "..", "config", "identities.example.json")
	set, err := auth.LoadFile(path)
	if err != nil || !set.Available() {
		t.Fatalf("example identities: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) == "" || strings.Contains(string(raw), "local-dev-only-not-a-secret") {
		t.Fatal("example file must store the hash, not the raw token")
	}
}

func mustSet(t *testing.T, items ...auth.Identity) *auth.Set {
	t.Helper()
	set, err := auth.NewStatic(items)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func sample(enabled bool) auth.Identity {
	return auth.Identity{
		ActorID:        "local-dev",
		CallerID:       "local",
		TokenSHA256:    hashOf("local-token"),
		AllowedSources: []string{"demo"},
		CallerScopes:   []string{"local"},
		Permissions:    []string{constant.PermEventsWrite},
		Enabled:        enabled,
	}
}

func hashOf(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
