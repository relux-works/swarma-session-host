package sessioncontext

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func legacyCatalogFixture(t *testing.T, provider string) (*materializationStore, *materializationRecord, []byte) {
	t.Helper()
	l := testLayout(t, "legacy-catalog")
	s := newMaterializationStore(l.MaterializationsDir, l.MaterializationQuarantine)
	r := &materializationRecord{Contract: MaterializationContractVersion, CtxID: "CTX-260729-c04101", Provider: "codex", BindingScope: "element", BindingElementID: "TASK-260729-bbbbbb", TargetElementID: "TASK-260729-bbbbbb", TargetRole: "developer", State: MaterializationReady, Source: MaterializationSourceBuilt, PayloadSHA256: strings.Repeat("a", 64), CreatedAt: materializationTestClock, UpdatedAt: materializationTestClock, LastUsedAt: materializationTestClock, ProviderHost: testMaterializationHost()}
	b, e := os.ReadFile(filepath.Join("..", "sessionmanager", "testdata", "legacy-records", provider+"-initial.json"))
	if e != nil {
		t.Fatal(e)
	}
	var fixture map[string]json.RawMessage
	if e = json.Unmarshal(b, &fixture); e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	var wire map[string]json.RawMessage
	if e = json.Unmarshal(raw, &wire); e != nil {
		t.Fatal(e)
	}
	wire["provider"] = json.RawMessage(`"` + provider + `"`)
	// Derive the supported goal-less historical neighbor from the actual full
	// fixture, retaining every other native/private field. The untouched active
	// source is exercised separately below and must still refuse.
	var host map[string]json.RawMessage
	_ = json.Unmarshal(fixture["provider_host"], &host)
	var opaque map[string]json.RawMessage
	_ = json.Unmarshal(host["opaque"], &opaque)
	for _, key := range []string{"owner_run", "goal_token_budget", "pending_goal_directive"} {
		delete(opaque, key)
	}
	host["opaque"], _ = json.Marshal(opaque)
	wire["provider_host"], _ = json.Marshal(host)
	wire["unknown_catalog_fact"] = json.RawMessage(`{"keep":"original"}`)
	raw, e = json.MarshalIndent(wire, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	raw = append(raw, '\n')
	path := filepath.Join(s.root, r.CtxID, materializationRecordFileName)
	if e = writePrivateFile(path, raw); e != nil {
		t.Fatal(e)
	}
	return s, r, raw
}

// The real shared historical converter reads the actual old provider subrecord;
// unchanged bytes and unrelated private facts survive a reopen and state write.
func TestMaterializationLegacyRealCodecReopenAndPrivatePreservation(t *testing.T) {
	for _, p := range []string{"codex", "claude"} {
		t.Run(p, func(t *testing.T) {
			s, r, raw := legacyCatalogFixture(t, p)
			rows, e := s.load()
			if e != nil || len(rows) != 1 {
				t.Fatalf("actual legacy materialization was not decoded: rows=%d err=%v", len(rows), e)
			}
			if !hostPresent(rows[0].ProviderHost) || rows[0].ProviderHost.Provider != p {
				t.Fatal("real legacy handle lost")
			}
			if e = s.save(rows[0]); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(s.root, r.CtxID, materializationRecordFileName)
			same, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(raw, same) {
				t.Fatal("unchanged original legacy bytes not preserved")
			}
			rows[0].State = MaterializationLeased
			rows[0].ActiveRunID = "RUN-legacy"
			rows[0].LeaseAcquiredAt = materializationTestClock
			if e = s.save(rows[0]); e != nil {
				t.Fatal(e)
			}
			after, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var a, b map[string]json.RawMessage
			_ = json.Unmarshal(raw, &a)
			_ = json.Unmarshal(after, &b)
			if !bytes.Equal(a["unknown_catalog_fact"], b["unknown_catalog_fact"]) {
				t.Fatal("catalog unknown private facts lost")
			}
			var x, y any
			_ = json.Unmarshal(a["provider_host"], &x)
			_ = json.Unmarshal(b["provider_host"], &y)
			xb, _ := json.Marshal(x)
			yb, _ := json.Marshal(y)
			if !bytes.Equal(xb, yb) {
				t.Fatal("provider private subrecord changed during lease write")
			}
			loaded, e := s.load()
			if e != nil || len(loaded) != 1 || loaded[0].ActiveRunID != "RUN-legacy" {
				t.Fatalf("legacy lease not durable: %v", e)
			}
			loaded[0].State = MaterializationReady
			loaded[0].ActiveRunID = ""
			// Clearing an omitted field must remove the old durable lease, not overlay it.
			loaded[0].LeaseAcquiredAt = time.Time{}
			if e = s.save(loaded[0]); e != nil {
				t.Fatal(e)
			}
			cleared, e := s.load()
			if e != nil || cleared[0].ActiveRunID != "" || !cleared[0].LeaseAcquiredAt.IsZero() {
				t.Fatalf("released legacy lease resurrected: %v", e)
			}
		})
	}
}

// Unsupported old native state refuses before any write; its complete original
// image remains available for quarantine/recovery rather than becoming empty.
func TestMaterializationLegacyUnsupportedRemainsUnchanged(t *testing.T) {
	s, r, raw := legacyCatalogFixture(t, "codex")
	bad := bytes.Replace(raw, []byte(`"pid": 101`), []byte(`"pid": 0`), 1)
	path := filepath.Join(s.root, r.CtxID, materializationRecordFileName)
	if e := os.WriteFile(path, bad, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := s.load(); e == nil {
		t.Fatal("unsupported historical handle became a supported empty snapshot")
	}
	after, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(after, bad) {
		t.Fatal("refused old handle was modified")
	}

}

// Actual unmodified original active private state is not reclassified as cold.
func TestMaterializationLegacyActiveOriginalRemainsClosed(t *testing.T) {
	for _, p := range []string{"codex", "claude"} {
		t.Run(p, func(t *testing.T) {
			s, r, raw := legacyCatalogFixture(t, p)
			input, e := os.ReadFile(filepath.Join("..", "sessionmanager", "testdata", "legacy-records", p+"-initial.json"))
			if e != nil {
				t.Fatal(e)
			}
			var source, wire map[string]json.RawMessage
			_ = json.Unmarshal(input, &source)
			_ = json.Unmarshal(raw, &wire)
			wire["provider_host"] = source["provider_host"]
			raw, e = json.Marshal(wire)
			if e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(s.root, r.CtxID, materializationRecordFileName)
			if e = writePrivateFile(path, raw); e != nil {
				t.Fatal(e)
			}
			if _, e = s.load(); e == nil || !strings.Contains(e.Error(), "actual runtime supplier") {
				t.Fatalf("active source state became supported: %v", e)
			}
			got, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(raw, got) {
				t.Fatal("active source state modified on refusal")
			}
		})
	}
}
