package sessionmanager

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Original validation field names/types survive transport instead of disappearing
// into a security permit. This does not prove the external authority validated it.
func TestContextValidationFactsRemainDistinctFromPermit(t *testing.T) {
	raw := []byte(`{"validation":{"contract_version":"context-validation-v1","ctx_id":"CTX-261029-abc123","coverage_manifest_sha256":"manifest","payload_sha256":"payload","validated_at":"2026-10-29T00:00:00Z","assay":{"sampled_count":48,"answered_count":47,"contradictions":[{"fact_id":"fact","kind":"literal","family":"source"}]},"claim_gates":{"source_refs":12,"pointers_resolved":11},"failure":{"code":"refused","message":"failed","failed_at":"2026-10-29T00:00:00Z"},"usage":{"input_tokens":32,"output_tokens":4,"cost_usd":0.1}},"validation_audit":{"cheap_audit_count":3,"deep_audit_digest":"deep"}}`)
	var record ContextRecord
	if e := json.Unmarshal(raw, &record); e != nil {
		t.Fatal(e)
	}
	v := record.Validation
	if v.CtxID != "CTX-261029-abc123" || v.Assay == nil || v.Assay.AnsweredCount != 47 || len(v.Assay.Contradictions) != 1 || v.ClaimGates.PointersResolved != 11 || v.Failure == nil || v.Usage.InputTokens != 32 || record.ValidationAudit.DeepAuditDigest != "deep" || !v.ValidatedAt.Equal(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("validation facts lost: %+v", record)
	}
	encoded, e := json.Marshal(record)
	if e != nil {
		t.Fatal(e)
	}
	var again ContextRecord
	if e = json.Unmarshal(encoded, &again); e != nil || again.Validation.Assay.AnsweredCount != 47 {
		t.Fatalf("validation roundtrip failed: %v", e)
	}
	var permit ContextPermit
	if e = json.Unmarshal(raw, &permit); e != nil || permit.Allowed {
		t.Fatal("validation facts became a security permit")
	}
}

// The original renderer retains read/not-read/excluded counts, explicit list
// remainders and unknown-coverage warning; a digest is not fabricated coverage.
func TestContextBoundaryBannerRetainsOriginalWarning(t *testing.T) {
	b := &ContextBoundaryBanner{ContractVersion: "context-boundary-banner-v1", CtxID: "CTX-261029-abc123", ManifestRef: "manifest-ref", ValidationAge: "1h", CoveredReads: []string{"read.go"}, CoveredReadCount: 14, CoveredReadsRemainder: 13, EnumeratedNotRead: []string{"unread.go"}, EnumeratedNotReadCount: 1, ExcludedCount: 2, UnknownRemainder: true}
	text := b.Render()
	for _, want := range []string{"read (14): read.go, +13 more", "seen but not read (1): unread.go", "excluded (2): none", "everything not named above is UNKNOWN", "Read resource manifest-ref"} {
		if !strings.Contains(text, want) {
			t.Fatalf("boundary lost %q: %s", want, text)
		}
	}
	if (*ContextBoundaryBanner)(nil).Render() != "" {
		t.Fatal("nil boundary fabricated coverage")
	}
}
