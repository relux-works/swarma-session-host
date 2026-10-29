package musepty

import (
	"context"
	"encoding/json"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Existing JSON corpus cases are projected through the real typed core inputs.
// Malformed JSON has no typed representation and becomes an absent input;
// strict wire syntax/provenance rejection is covered by DecodeMuseSessionLaunch.
func museTestLaunchSpec(raw json.RawMessage) sm.LaunchSpec {
	var launch MuseSessionLaunch
	if len(raw) == 0 || json.Unmarshal(raw, &launch) != nil {
		return sm.LaunchSpec{}
	}
	return sm.LaunchSpec{Muse: &launch}
}
func museTestCoreSnapshot(raw json.RawMessage) sm.RunnerSnapshot {
	var base musePTYSnapshot
	if json.Unmarshal(raw, &base) != nil {
		return sm.RunnerSnapshot{Kind: "invalid-legacy-wire"}
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return sm.RunnerSnapshot{Kind: "invalid-legacy-wire"}
	}
	if _, ok := obj["profile"]; ok {
		var p musePTYProvenanceSnapshot
		if json.Unmarshal(raw, &p) != nil {
			return sm.RunnerSnapshot{}
		}
		return sm.RunnerSnapshot{Kind: "muse-pty-provenance", MusePTYProvenance: &sm.MusePTYProvenanceSnapshot{Base: museSnapshotToCore(p.musePTYSnapshot), Profile: p.Profile, ProfileLock: p.ProfileLock, ManagedHome: p.ManagedHome, FragmentSHA256: p.FragmentSHA256}}
	}
	return sm.RunnerSnapshot{Kind: "muse-pty", MusePTY: ptrMuseCoreSnapshot(base)}
}
func museTestSnapshot(h interface {
	Snapshot() (sm.RunnerSnapshot, error)
}) (json.RawMessage, error) {
	s, err := h.Snapshot()
	if err != nil {
		return nil, err
	}
	base, err := museRecordSnapshot(sm.ProviderHostRecord{Provider: "muse", Snapshot: s})
	if err != nil {
		return nil, err
	}
	if s.Kind == "muse-pty-provenance" {
		p := s.MusePTYProvenance
		return json.Marshal(musePTYProvenanceSnapshot{musePTYSnapshot: base, Profile: p.Profile, ProfileLock: p.ProfileLock, ManagedHome: p.ManagedHome, FragmentSHA256: p.FragmentSHA256})
	}
	return json.Marshal(base)
}

// Every recovery/provenance field survives the typed boundary and snapshots
// do not share mutable warning pointers with live or detached host state.
func TestMusePTYTypedSnapshotPreservesRecoveryAndIsolation(t *testing.T) {
	base := musePTYSnapshot{Contract: musePTYHostSnapshotContract, SessionID: "native-session", LogPath: "/fixture/log", PID: 42, VersionFallback: true, VersionFallbackFrom: "/fixture/newest",
		VersionFallbackEvent: &musePTYFallbackWarning{Code: "fallback", State: "ready", Newest: musePTYFrozenTupleJSON{"new", "new-sha", "/new"}, FailureAttempt: "attempt", FailureStage: "exec", FailureCode: "refused", LastGood: musePTYFrozenTupleJSON{"old", "old-sha", "/old"}, LastGoodSequence: 7, Mode: "consent", Consent: true},
		VersionDeferralEvent: &musePTYDeferralWarning{Code: "deferral", State: "ready", Target: musePTYFrozenTupleJSON{"target", "target-sha", "/target"}, Newest: musePTYFrozenTupleJSON{"new", "new-sha", "/new"}, Attempt: "deferred", Mode: "consent", Consent: true}}
	if got := museSnapshotFromCore(museSnapshotToCore(base)); !reflect.DeepEqual(got, base) {
		t.Fatalf("snapshot fields lost: %#v", got)
	}
	h := &musePTYProvenanceHost{musePTYHost: &musePTYHost{snapshot: base}, profile: "profile", profileLock: "lock", managedHome: "/managed", fragmentSHA256: "fragment"}
	s, err := h.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewMusePTYHostFactory().Restore(context.Background(), sm.ProviderHostRecord{Provider: "muse", Snapshot: s})
	if err != nil {
		t.Fatal(err)
	}
	before, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, s) {
		t.Fatal("restored snapshot lost provenance or recovery")
	}
	s.MusePTYProvenance.Base.VersionFallbackEvent.Code = "mutated"
	again, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if again.MusePTYProvenance.Base.VersionFallbackEvent.Code != "fallback" || h.snapshot.VersionFallbackEvent.Code != "fallback" {
		t.Fatal("snapshot aliases caller memory")
	}
	before.MusePTYProvenance.Base.VersionDeferralEvent.State = "mutated"
	again, _ = restored.Snapshot()
	if again.MusePTYProvenance.Base.VersionDeferralEvent.State != "ready" {
		t.Fatal("detached snapshot aliases caller memory")
	}
}

// Foreign, empty, wrong-kind and ambiguous union payloads cannot be restored.
// A matching base snapshot is the nearby positive control; no process starts.
func TestMusePTYTypedSnapshotRejectsAmbiguousOrForeign(t *testing.T) {
	good := sm.RunnerSnapshot{Kind: "muse-pty", MusePTY: &sm.MusePTYSnapshot{Contract: musePTYHostSnapshotContract, SessionID: "native"}}
	factory := NewMusePTYHostFactory()
	if _, err := factory.Restore(context.Background(), sm.ProviderHostRecord{Provider: "muse", Snapshot: good}); err != nil {
		t.Fatal(err)
	}
	foreign := good
	foreign.Codex = &sm.CodexRunnerSnapshot{}
	ambiguous := good
	ambiguous.MusePTYProvenance = &sm.MusePTYProvenanceSnapshot{}
	wrong := good
	wrong.Kind = "muse-pty-provenance"
	for _, s := range []sm.RunnerSnapshot{{}, foreign, ambiguous, wrong} {
		if _, err := factory.Restore(context.Background(), sm.ProviderHostRecord{Provider: "muse", Snapshot: s}); err != sm.ErrProviderHostNotFound {
			t.Fatalf("invalid typed snapshot accepted: %v", err)
		}
	}
	if _, err := factory.Restore(context.Background(), sm.ProviderHostRecord{Provider: "claude", Snapshot: good}); err != sm.ErrProviderHostNotFound {
		t.Fatal("foreign provider admitted")
	}
}

// An empty prior still means cold start, as an absent original opaque payload did.
func TestMusePTYTypedEmptyPriorRetainsColdStart(t *testing.T) {
	home := museFactoryPeerHome(t, "idle")
	h := museFactoryPeerStart(t, home, museFactoryPeerLaunchBytes(t, home), &sm.ProviderHostRecord{Provider: "muse"})
	if h.NativeIdentity() == "" {
		t.Fatal("empty prior lost cold launch")
	}
}

// Typed payloads still pass the original integrity/provenance decoder before
// any native session starts. The nearby existing factory test launches valid input.
func TestMusePTYTypedFactoryRejectsAlteredProvenanceBeforeStart(t *testing.T) {
	for _, name := range []string{"digest", "profile", "model"} {
		t.Run(name, func(t *testing.T) {
			home := museFactoryPeerHome(t, "idle")
			launch := museFactoryPeerLaunch(t, home)
			want := ""
			switch name {
			case "digest":
				launch.FragmentSHA256 = strings.Repeat("0", 64)
				want = "fragment does not match the recorded sha256"
			case "profile":
				launch.Profile = "other"
				want = "provenance does not match its fragment"
			case "model":
				launch.Model = ""
				want = "requires a model"
			}
			host, err := NewMusePTYHostFactory().Launch(context.Background(), sm.ProviderLaunchRequest{Launch: sm.LaunchSpec{Muse: launch}})
			if host != nil {
				_ = host.Stop(context.Background())
				t.Fatal("invalid typed launch started a host")
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("typed integrity refusal lost: %v want %s", err, want)
			}
			logs, err := filepath.Glob(filepath.Join(home, "data", "sessions", "*", "*", "*", "*", "session.jsonl"))
			if err != nil || len(logs) != 0 {
				t.Fatal("invalid typed launch created a native session")
			}
		})
	}
}
