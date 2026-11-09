package canonicaljson_test

import (
	"strings"
	"testing"

	"notifier/internal/canonicaljson"
	"notifier/internal/constant"
)

func TestC06_RejectMalformedJSON(t *testing.T) {
	t.Run("duplicate key", func(t *testing.T) {
		_, err := canonicaljson.Canonical([]byte(`{"a":1,"a":2}`))
		if err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("duplicate key must be rejected, got %v", err)
		}
	})
	t.Run("lone high surrogate", func(t *testing.T) {
		_, err := canonicaljson.Canonical([]byte(`{"s":"\uDEAD"}`))
		if err == nil {
			t.Fatal("lone surrogate must be rejected")
		}
	})
	t.Run("lone low surrogate", func(t *testing.T) {
		_, err := canonicaljson.Canonical([]byte(`{"s":"\uD800"}`))
		if err == nil {
			t.Fatal("unpaired surrogate must be rejected")
		}
	})
	t.Run("depth 33", func(t *testing.T) {
		_, err := canonicaljson.Canonical([]byte(nested(33)))
		if err == nil {
			t.Fatal("depth 33 must be rejected")
		}
	})
	t.Run("number literal 129", func(t *testing.T) {
		_, err := canonicaljson.Canonical([]byte(`{"n":1` + strings.Repeat("0", 128) + `}`))
		if err == nil {
			t.Fatal("129-byte number literal must be rejected")
		}
	})
	t.Run("exponent 309", func(t *testing.T) {
		if _, err := canonicaljson.Canonical([]byte(`{"n":1e309}`)); err == nil {
			t.Fatal("1e309 must be rejected")
		}
		if _, err := canonicaljson.Canonical([]byte(`{"n":1e-309}`)); err == nil {
			t.Fatal("1e-309 must be rejected")
		}
	})
	t.Run("coefficient 65", func(t *testing.T) {
		_, err := canonicaljson.Canonical([]byte(`{"n":` + strings.Repeat("9", 65) + `}`))
		if err == nil {
			t.Fatal("65-digit coefficient must be rejected")
		}
	})
	t.Run("trailing data", func(t *testing.T) {
		_, err := canonicaljson.Canonical([]byte(`{"a":1}{"b":2}`))
		if err == nil {
			t.Fatal("trailing data must be rejected")
		}
	})
	t.Run("invalid utf-8", func(t *testing.T) {
		raw := []byte(`{"s":"`)
		raw = append(raw, 0xff, '"', '}')
		if _, err := canonicaljson.Canonical(raw); err == nil {
			t.Fatal("invalid utf-8 must be rejected")
		}
	})
}

func TestC06_AcceptBoundaryJSON(t *testing.T) {
	if _, err := canonicaljson.Canonical([]byte(nested(32))); err != nil {
		t.Fatalf("depth 32 must be accepted: %v", err)
	}
	literal := "1" + strings.Repeat("0", constant.MaxJSONNumberLiteral-1)
	got, err := canonicaljson.Canonical([]byte(`{"n":` + literal + `}`))
	if err != nil {
		t.Fatalf("128-byte number literal must be accepted: %v", err)
	}
	if strings.Contains(gotString(got), ".") || !strings.Contains(string(got), "1e127") {
		t.Fatalf("canonical = %s", got)
	}
	if _, err := canonicaljson.Canonical([]byte(`{"n":1e308}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := canonicaljson.Canonical([]byte(`{"n":1e-308}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := canonicaljson.Canonical([]byte(`{"n":` + strings.Repeat("9", 64) + `}`)); err != nil {
		t.Fatal(err)
	}
	got, err = canonicaljson.Canonical([]byte(`{"n":0e1000001}`))
	if err != nil || string(got) != `{"n":0}` {
		t.Fatalf("zero with a huge exponent = %s, %v", got, err)
	}
}

func TestCanonicalSamplesAndExactInteger(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{`{"b":1.0,"a":"中"}`, `{"a":"中","b":1e0}`},
		{`{"v":-0.0,"n":0.01230}`, `{"n":123e-4,"v":0}`},
		{`{"v":"\u0061","list":[true,null]}`, `{"list":[true,null],"v":"a"}`},
		{`{"n":9007199254740993}`, `{"n":9007199254740993e0}`},
		{`{"n":123.00}`, `{"n":123e0}`},
		{`{"s":"\uD83D\uDE00"}`, "{\"s\":\"\U0001F600\"}"},
	}
	for _, tc := range cases {
		got, err := canonicaljson.Canonical([]byte(tc.in))
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if string(got) != tc.want {
			t.Fatalf("%s canonical = %s, want %s", tc.in, got, tc.want)
		}
		again, err := canonicaljson.Canonical(got)
		if err != nil || string(again) != string(got) {
			t.Fatalf("canonical bytes must round-trip, got %s %v", again, err)
		}
	}
	raw, err := canonicaljson.Canonical([]byte(`{"n":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "9007199254740992") || strings.Contains(string(raw), "9.007") {
		t.Fatalf("integer was converted through binary floating point: %s", raw)
	}
	fp := canonicaljson.Fingerprint(raw)
	if len(fp) != 64 || canonicaljson.Fingerprint(raw) != fp {
		t.Fatalf("fingerprint = %s", fp)
	}
	if constant.FingerprintVersion != 1 {
		t.Fatal("fingerprint version must stay 1 until the algorithm changes")
	}
}

func nested(depth int) string {
	var b strings.Builder
	for i := 0; i < depth; i++ {
		b.WriteString(`{"a":`)
	}
	b.WriteByte('1')
	for i := 0; i < depth; i++ {
		b.WriteByte('}')
	}
	return b.String()
}

func gotString(b []byte) string { return string(b) }
