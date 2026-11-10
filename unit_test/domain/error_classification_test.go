package domain_test

import (
	"testing"

	"notifier/internal/constant"
	"notifier/internal/domain"
	"notifier/internal/enums"
)

func TestManualRetryFailureReasonsAreDistinct(t *testing.T) {
	attemptID := uint64(10)
	cases := []struct {
		name   string
		check  domain.RetryCheck
		reason string
		code   string
	}{
		{"succeeded", domain.RetryCheck{Task: domain.TaskFacts{Status: enums.TaskSucceeded}}, constant.ReasonTaskNotRetryable, constant.CodeStateConflict},
		{"active_attempt", domain.RetryCheck{Task: domain.TaskFacts{Status: enums.TaskFailed, CurrentAttemptID: &attemptID}}, constant.ReasonTaskLeaseActive, constant.CodeStateConflict},
		{"version", domain.RetryCheck{Task: domain.TaskFacts{Status: enums.TaskFailed, RowVersion: 2}, ExpectedRow: 1}, constant.ReasonTaskVersion, constant.CodeVersionConflict},
		{"adapter", domain.RetryCheck{Task: domain.TaskFacts{Status: enums.TaskFailed}, Constraint: constant.ErrAdapterUnavailable}, constant.ReasonAdapterMissing, constant.CodeConfigurationUnavailable},
		{"frozen_configuration", domain.RetryCheck{Task: domain.TaskFacts{Status: enums.TaskFailed}, Constraint: constant.ErrConfigurationUnavail}, constant.ReasonFrozenPolicy, constant.CodeConfigurationUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, reason := domain.CheckRetry(tc.check)
			if reason != tc.reason {
				t.Fatalf("reason = %q, want %q", reason, tc.reason)
			}
			err, createErr := domain.NewError(reason, "当前不能重投", "", nil)
			if createErr != nil || err.Code != tc.code || err.ReasonCode != tc.reason || err.Message == "当前不能重投" {
				t.Fatalf("classification = %+v %v", err, createErr)
			}
		})
	}
}

func TestEvidenceFailureReasonsAreDistinct(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*domain.Evidence)
		reason string
	}{
		{"type", func(ev *domain.Evidence) { ev.Type = "unknown" }, constant.ReasonEvidenceType},
		{"reference", func(ev *domain.Evidence) { ev.Reference = "" }, constant.ReasonEvidenceReference},
		{"secret", func(ev *domain.Evidence) { ev.Summary = "Authorization: Bearer example" }, constant.ReasonEvidenceSecret},
		{"scope", func(ev *domain.Evidence) { ev.Scope = "unknown" }, constant.ReasonEvidenceScope},
		{"missing_attempts", func(ev *domain.Evidence) { ev.CoveredAttemptIDs = nil }, constant.ReasonEvidenceAttempts},
		{"duplicate_attempt", func(ev *domain.Evidence) { ev.CoveredAttemptIDs = []uint64{1, 1} }, constant.ReasonEvidenceDuplicate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := domain.Evidence{Type: "provider_query", Reference: "query-1", Summary: "confirmed", Scope: domain.EvidenceScopeAttempts, CoveredAttemptIDs: []uint64{1}}
			tc.mutate(&ev)
			if reason := domain.ValidateEvidenceShape(ev, 10); reason != tc.reason {
				t.Fatalf("reason = %q, want %q", reason, tc.reason)
			}
		})
	}
}

func TestSupplierResponseFailureReasonsKeepUncertainty(t *testing.T) {
	cases := []struct {
		name  string
		facts domain.TransportFacts
		code  string
	}{
		{"too_large", domain.TransportFacts{Invoked: true, Status: 200, TooLarge: true}, constant.ErrResponseTooLarge},
		{"read_failure", domain.TransportFacts{Invoked: true, Status: 200}, constant.ErrResponseRead},
		{"invalid_protocol", domain.TransportFacts{Invoked: true, Status: 200, ReadComplete: true, Body: []byte(`invalid`)}, constant.ErrProtocolInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := domain.InterpretMockJSON(tc.facts)
			if result.ErrorCode != tc.code || result.Certainty != enums.CertaintyUncertain || result.Outcome != enums.OutcomeUncertain {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}
