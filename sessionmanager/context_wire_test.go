package sessionmanager

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// Canonical original wire keys survive strict decoding into the real shared
// record and nested facts. This is wire compatibility, not registry admission.
func TestContextOriginalWireRecordClosure(t *testing.T) {
	raw := []byte(`{"contract_version":"context-v1","id":"CTX-261029-abc123","revision":2,"provider":"codex","kind":"project","binding":{"scope":"element","element_id":"element","producer_role":"developer"},"lineage":{"primary_ctx_id":"CTX-261029-base12","produced_by_run_id":"RUN-fixture"},"state":"ready","freshness":{"commit_sha":"commit","path_specs":["src/**"],"ingested_paths":["src/a.go"],"base_live_tokens":12},"coverage_boundary_state":"known","integrity":{"transcript_sha256":"transcript","verified_at":"2026-10-29T00:00:00Z"},"builder_profile":"scout","secret_class":"project","classification":{"identity_scanner_version":"scanner","evidence_digest":"scan"},"builder_enforcement":{"produced_by_run_id":"RUN-fixture","enforced_scope_digest":"scope"},"validation":{"ctx_id":"CTX-261029-abc123","attestation_sha256":"verdict"},"validation_audit":{"cheap_audit_count":1},"actualization":{"from_ctx_id":"CTX-261029-base12","frontier_model_required":false,"metrics":{"diff_bytes":123,"projected_prompt_tokens":30}},"payload":{"state":"present","resource_ref":"payload","withdrawal":{"attempts":2}},"reclamation":{"archived_included":true,"files":3},"pinned":true}`)
	var r ContextRecord
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(&r); e != nil {
		t.Fatal(e)
	}
	if r.ContractVersion != "context-v1" || r.Kind != "project" || r.Lineage.PrimaryCtxID != "CTX-261029-base12" || r.Freshness.BaseLiveTokens != 12 || r.BuilderEnforcement.RunID != "RUN-fixture" || r.BuilderEnforcement.EnforcedScope != "scope" || r.Actualization.Metrics.DiffBytes != 123 || r.Reclamation.Files != 3 {
		t.Fatalf("original fields dropped: %+v", r)
	}
	out, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"contract_version", "primary_ctx_id", "commit_sha", "base_live_tokens", "produced_by_run_id", "enforced_scope_digest", "resource_ref", "archived_included", "frontier_model_required"} {
		if !bytes.Contains(out, []byte(`"`+key+`"`)) {
			t.Fatalf("original wire key %s lost: %s", key, out)
		}
	}
	if bytes.Contains(out, []byte("RequiresConfinement")) {
		t.Fatal("internal runtime claim became body authority")
	}
	var again ContextRecord
	d = json.NewDecoder(bytes.NewReader(out))
	d.DisallowUnknownFields()
	if e = d.Decode(&again); e != nil || again.Actualization.Metrics.DiffBytes != 123 {
		t.Fatalf("canonical roundtrip: %v", e)
	}
	for _, bad := range []string{`{"contract_version":false}`, `{"unknown_authority":true}`} {
		d = json.NewDecoder(strings.NewReader(bad))
		d.DisallowUnknownFields()
		if e = d.Decode(&again); e == nil {
			t.Fatalf("wrong/unknown wire shape accepted: %s", bad)
		}
	}
}

// Original coverage and retention output tags are preserved through nested
// collection/plan DTOs. Runtime namespace does not become a caller-supplied key.
func TestContextOriginalManifestAndRetentionWire(t *testing.T) {
	raw := []byte(`{"contract_version":"coverage-v1","ctx_id":"CTX-261029-abc123","project_id":"project","provider":"codex","observed":[{"path":"src/a.go","access":"read","content_sha256":"hash","first_tool_event_id":"evt","ranges":[{"byte_offset":4,"byte_length":8,"total_bytes":12,"tool_event_id":"evt"}]}],"unknown_remainder":true,"manifest_sha256":"manifest"}`)
	var m CoverageManifest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(&m); e != nil {
		t.Fatal(e)
	}
	if m.ProjectID != "project" || len(m.Observed) != 1 || m.Observed[0].Ranges[0].ByteLength != 8 {
		t.Fatalf("coverage data lost: %+v", m)
	}
	m.Namespace = "runtime-only"
	out, e := json.Marshal(m)
	if e != nil || bytes.Contains(out, []byte("runtime-only")) {
		t.Fatalf("runtime namespace leaked: %v %s", e, out)
	}
	report := RetentionCollection{Retired: []*ContextRecord{{ID: "CTX-261029-abc123", Kind: "project"}}, Delete: []ContextRetentionCollection{{CtxID: "CTX-261029-abc123", ExpectedRevision: 2, IncludeArchived: true}}, Expiring: []ContextRetentionOutlook{{CtxID: "CTX-261029-abc123", Kind: "project", Remaining: 12}}, Skipped: []ContextRetentionSkip{{CtxID: "CTX-261029-other1", Reason: "leased"}}}
	out, e = json.Marshal(report)
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{`"retired"`, `"delete"`, `"ctx_id"`, `"expected_revision"`, `"include_archived"`, `"remaining_seconds"`, `"kind":"project"`, `"skipped"`} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("retention wire %s lost: %s", want, out)
		}
	}
	if bytes.Contains(out, []byte("ExpectedRevision")) || bytes.Contains(out, []byte("Retired")) {
		t.Fatal("PascalCase wire drift")
	}
}
