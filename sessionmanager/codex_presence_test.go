package sessionmanager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Only live rollouts count as present. Archived, absent, unreadable and unprobed
// stores never become an admitted presence report; no native command runs.
func TestCodexPayloadPresenceLiveArchivedAndUnknown(t *testing.T) {
	home := t.TempDir()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	thread := "thread-1"
	check := func(want string) {
		t.Helper()
		r := probeCodexPayloadPresence("opaque-context", home, thread, at)
		if r.Presence != want || !r.CheckedAt.Equal(at) || r.CtxID != "opaque-context" {
			t.Fatalf("presence=%#v want %s", r, want)
		}
	}
	check("vanished")
	archive := filepath.Join(home, "archived_sessions")
	if err := os.MkdirAll(archive, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archive, "rollout-"+thread+".jsonl"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	check("vanished")
	if !strings.Contains(probeCodexPayloadPresence("opaque-context", home, thread, at).Detail, "archived") {
		t.Fatal("archive detail dropped")
	}
	live := filepath.Join(home, "sessions")
	if err := os.Mkdir(live, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(live, "rollout-"+thread+".jsonl"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	check("present")
	if r := probeCodexPayloadPresence("opaque-context", "", thread, at); r.Presence != "unverifiable" {
		t.Fatalf("unknown accepted: %#v", r)
	}
}

// No canonical policy is an unsupported capability, even with a malformed
// source. Neither fork nor assay can perform file/process/RPC work first.
func TestCodexContextCapabilityRefusesBeforeEffects(t *testing.T) {
	f := NewCodexHostFactory(Layout{})
	if _, err := f.materializeContextFork(context.Background(), nil, "", ""); !errors.Is(err, ErrHostCapabilityUnavailable) {
		t.Fatal(err)
	}
	if _, err := f.RunContextAssay(context.Background(), ProviderHostRecord{}, CodexContextAssayRequest{}); !errors.Is(err, ErrHostCapabilityUnavailable) {
		t.Fatal(err)
	}
}

// Original assay request validation refuses empty prompts and malformed schema
// before native work; valid requests are exercised by the relocated assay roots.
func TestCodexContextAssayRejectsMalformedRequestsBeforeIO(t *testing.T) {
	f := NewCodexHostFactory(Layout{}, codexPolicyProbe{})
	for _, q := range []CodexContextAssayRequest{{OutputSchema: []byte("{}")}, {Prompt: "valid", OutputSchema: []byte("broken")}} {
		_, err := f.RunContextAssay(context.Background(), ProviderHostRecord{}, q)
		if err == nil || (!strings.Contains(err.Error(), "prompt is required") && !strings.Contains(err.Error(), "output schema must be valid JSON")) {
			t.Fatalf("request validation lost: %v", err)
		}
	}
}
