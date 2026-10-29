package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func legacyProviderFixture(t *testing.T, name string) []byte {
	t.Helper()
	in := legacyCodecInput(t, "testdata/legacy-records/"+name+"-initial.json")
	var record map[string]json.RawMessage
	if e := json.Unmarshal(in.Bytes, &record); e != nil {
		t.Fatal(e)
	}
	return record["provider_host"]
}

// Real historical provider_host subrecords preserve complete native execution
// payloads and unknown bytes. This converter never admits unsupported active
// accounting state or authorizes materialization/Restore.
func TestLegacyProviderHostConverter(t *testing.T) {
	ctx := context.Background()
	c := NewLegacyRecordCodec()
	for _, path := range legacyCodecFixtures(t) {
		original := legacyCodecInput(t, path)
		record, e := c.DecodeLegacy(ctx, original)
		if e != nil {
			t.Fatal(e)
		}
		cold, e := c.EncodeProviderHost(ctx, nil, record.ProviderHost)
		if e != nil {
			t.Fatalf("cold provider %s: %v", path, e)
		}
		decoded, e := c.DecodeProviderHost(ctx, cold)
		if e != nil || decoded.Provider != record.Provider {
			t.Fatalf("cold converter %s: %v", path, e)
		}
		got, e := c.EncodeProviderHost(ctx, cold, decoded)
		if e != nil || !bytes.Equal(got, cold) {
			t.Fatalf("cold converter wire %s: %v", path, e)
		}
	}
	for _, name := range []string{"muse-pty", "muse-pty-provenance"} {
		t.Run(name, func(t *testing.T) {
			raw := legacyProviderFixture(t, name)
			rec, e := c.DecodeProviderHost(ctx, raw)
			if e != nil {
				t.Fatal(e)
			}
			got, e := c.EncodeProviderHost(ctx, raw, rec)
			if e != nil || !bytes.Equal(raw, got) {
				t.Fatalf("unchanged provider wire lost: %v", e)
			}
			changed := rec
			changed.Provider = "codex"
			if _, e = c.EncodeProviderHost(ctx, raw, changed); e == nil {
				t.Fatal("changed provider accepted")
			}
		})
	}
	for _, name := range []string{"claude", "codex", "muse-native", "muse-text"} {
		if _, e := c.DecodeProviderHost(ctx, legacyProviderFixture(t, name)); !errors.Is(e, ErrHostCapabilityUnavailable) {
			t.Fatalf("%s unsupported active state not refused: %v", name, e)
		}
	}
	if _, e := c.DecodeProviderHost(ctx, []byte(`{"provider":"muse","opaque":null}`)); e == nil {
		t.Fatal("null native snapshot admitted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := c.DecodeProviderHost(cancelled, legacyProviderFixture(t, "muse-pty")); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled conversion: %v", e)
	}
}

// Native hydration reads the original typed accounting group and merges only
// that group; text-child accounting, execution facts and future fields survive.
// Data extraction alone grants neither a lease nor native restore capability.
func TestLegacyMuseAccountingHydrationPreservesOtherState(t *testing.T) {
	ctx := context.Background()
	c := NewLegacyRecordCodec()
	raw := legacyProviderFixture(t, "muse-native")
	var envelope map[string]json.RawMessage
	_ = json.Unmarshal(raw, &envelope)
	var opaque map[string]json.RawMessage
	_ = json.Unmarshal(envelope["opaque"], &opaque)
	opaque["futureNativeState"] = json.RawMessage(`{"retained":true}`)
	envelope["opaque"], _ = json.Marshal(opaque)
	raw, _ = json.Marshal(envelope)
	accounting, e := c.DecodeMuseAccounting(ctx, raw)
	if e != nil {
		t.Fatal(e)
	}
	if accounting.SessionID != "native-muse" || accounting.GoalTransport != "native" || accounting.GoalRef == nil || len(accounting.GoalLedger) == 0 {
		t.Fatalf("actual accounting fields dropped: %+v", accounting)
	}
	same, e := c.EncodeMuseAccounting(ctx, raw, accounting)
	if e != nil || !bytes.Equal(same, raw) {
		t.Fatalf("unchanged accounting lost bytes: %v", e)
	}
	accounting.TokenUsageTotal++
	updated, e := c.EncodeMuseAccounting(ctx, raw, accounting)
	if e != nil {
		t.Fatal(e)
	}
	var nextEnvelope map[string]json.RawMessage
	_ = json.Unmarshal(updated, &nextEnvelope)
	var next map[string]json.RawMessage
	_ = json.Unmarshal(nextEnvelope["opaque"], &next)
	for _, k := range []string{"text", "futureNativeState", "versionFallbackEvent", "modelId"} {
		if !bytes.Equal(compactLegacyJSON(opaque[k]), compactLegacyJSON(next[k])) {
			t.Fatalf("accounting merge lost %s", k)
		}
	}
	got, e := c.DecodeMuseAccounting(ctx, updated)
	if e != nil || got.TokenUsageTotal != accounting.TokenUsageTotal {
		t.Fatalf("accounting not updated: %v", e)
	}
	if _, e = c.DecodeProviderHost(ctx, updated); !errors.Is(e, ErrHostCapabilityUnavailable) {
		t.Fatalf("hydration bypassed native supplier admission: %v", e)
	}
	accounting.SessionID = "another-native-session"
	if _, e = c.EncodeMuseAccounting(ctx, raw, accounting); !errors.Is(e, ErrLegacyViewStale) {
		t.Fatalf("identity replacement admitted: %v", e)
	}
	for _, name := range []string{"muse-text", "muse-pty", "codex"} {
		if _, e = c.DecodeMuseAccounting(ctx, legacyProviderFixture(t, name)); e == nil {
			t.Fatalf("%s accepted as native accounting", name)
		}
	}
}
