package domain_test

import (
	"strings"
	"testing"
	"time"

	"notifier/internal/constant"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

func TestIdentifiers(t *testing.T) {
	if err := domain.ValidateCallerID("A1._:-ok"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", ".nope", "has space", strings.Repeat("a", 65)} {
		if err := domain.ValidateCallerID(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
	if err := domain.ValidateEventType(strings.Repeat("e", 128)); err != nil {
		t.Fatal(err)
	}
	if err := domain.ValidateSchemaVersion(strings.Repeat("v", 33)); err == nil {
		t.Fatal("version longer than 32 must fail")
	}
	if err := domain.ValidateExternalID("source_event_id", "事件 "+strings.Repeat("a", 180)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "   ", "line\nbreak", strings.Repeat("a", 192)} {
		if err := domain.ValidateExternalID("source_event_id", bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
	id, err := domain.ParseInternalID("delivery_id", "18446744073709551615")
	if err != nil || id != ^uint64(0) {
		t.Fatalf("max uint64 = %d %v", id, err)
	}
	for _, bad := range []string{"0", "01", "+1", "-1", "1.5", "18446744073709551616"} {
		if _, err := domain.ParseInternalID("delivery_id", bad); err == nil {
			t.Fatalf("%s must be rejected", bad)
		}
	}
	if err := domain.CheckCount("history_seq", 4294967295); err != nil {
		t.Fatal(err)
	}
	if err := domain.CheckCount("history_seq", 4294967296); err == nil {
		t.Fatal("count must not wrap")
	}
}

func TestRFC3339UTCEquivalence(t *testing.T) {
	utc, err := domain.ParseRFC3339("occurred_at", "2020-01-02T03:04:05.123456Z")
	if err != nil {
		t.Fatal(err)
	}
	shifted, err := domain.ParseRFC3339("occurred_at", "2020-01-02T11:04:05.123456+08:00")
	if err != nil {
		t.Fatal(err)
	}
	if !utc.Equal(shifted) || utc.Location() != time.UTC {
		t.Fatalf("timezones diverged: %s vs %s", utc, shifted)
	}
	if domain.FormatUTCMicros(shifted) != "2020-01-02T03:04:05.123456Z" {
		t.Fatalf("fingerprint time = %s", domain.FormatUTCMicros(shifted))
	}
	if _, err := domain.ParseRFC3339("occurred_at", "1000-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		"2020-01-02T03:04:05.1234567Z",
		"0999-01-01T00:00:00Z",
		"2020-01-02T03:04:05.",
		"not-a-time",
	} {
		if _, err := domain.ParseRFC3339("occurred_at", bad); err == nil {
			t.Fatalf("%s must be rejected", bad)
		}
	}
}

func TestWriteIDStableTuple(t *testing.T) {
	a, err := domain.WriteID(9, enums.PhaseAttemptFinished, 3, "op-1", 4, 2)
	if err != nil || len(a) != 64 {
		t.Fatalf("write id = %s %v", a, err)
	}
	again, err := domain.WriteID(9, enums.PhaseAttemptFinished, 3, "op-1", 4, 2)
	if err != nil || again != a {
		t.Fatal("same tuple must keep the same write id")
	}
	other, err := domain.WriteID(9, enums.PhaseLateResponse, 3, "op-1", 4, 2)
	if err != nil || other == a {
		t.Fatal("different actions must not share a write id")
	}
	if _, err := domain.WriteID(9, "NOT_A_PHASE", 3, "op-1", 4, 2); err == nil {
		t.Fatal("unknown phase must fail")
	}
	for _, phase := range enums.HistoryPhases {
		if len(phase) > 32 {
			t.Fatalf("phase %s exceeds VARCHAR(32)", phase)
		}
	}
}

func TestSummaryAndErrorDetails(t *testing.T) {
	got, cut := domain.TruncateUTF8("你a", 3)
	if got != "你" || !cut {
		t.Fatalf("truncate = %q %v", got, cut)
	}
	if _, cut = domain.TruncateUTF8("你", 3); cut {
		t.Fatal("exact rune boundary is not a truncation")
	}
	details := make([]domain.Detail, constant.MaxErrorDetails+1)
	if _, err := domain.NewError("invalid_request", "too many", "", details); err == nil {
		t.Fatal("details above 100 must be rejected")
	}
}
