package hostdefaults

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The selected file supplies the real timeout/provider defaults; absence is
// compatible only for an optional default path, never an explicit missing file.
func TestFileConfigLoadsActualSettingsAndAbsenceDefaults(t *testing.T) {
	ctx := context.Background()
	name := filepath.Join(t.TempDir(), "config.json")
	cfg := FileConfig{Path: name, Optional: true, Version: "test"}
	got, err := cfg.Snapshot(ctx)
	if err != nil || got.GoalAckTimeout != 30*time.Second || got.LocalPolicy.PayloadMode != "off" {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	cfg.Optional = false
	if _, err := cfg.Snapshot(ctx); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("explicit missing = %v", err)
	}
	data := `{"session_manager":{"providers":{"claude":{"goal_ack_timeout":"125ms"},"codex":{"sleep_tool":false,"goal_token_budget":123,"auto_continue":{"enabled":true,"delay_seconds":7,"max_nudges_per_turn":3,"prompt":"continue"}}}},"contexts":{"clearance":{"dev":{"allowed_secret_classes":["restricted","unrestricted"]}},"builder_allowlist":["src/**"],"payloads":{"mode":"unrestricted"}}}`
	if err := os.WriteFile(name, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := cfg.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Host.GoalAckTimeout != 125*time.Millisecond || loaded.Codex.SleepTool || loaded.Codex.GoalTokenBudget != 123 || loaded.Codex.AutoContinue.Prompt != "continue" || loaded.Codex.AutoContinue.DelaySeconds != 7 || !loaded.Codex.AutoContinue.Enabled {
		t.Fatalf("loaded = %+v", loaded)
	}
	if loaded.Host.LocalPolicy.PayloadMode != "unrestricted" || len(loaded.Host.LocalPolicy.Clearance["dev"]) != 2 || len(loaded.Host.LocalPolicy.BuilderAllowlist.Paths) != 1 {
		t.Fatalf("policy = %+v", loaded.Host.LocalPolicy)
	}
}

// Invalid or partially understood selected settings cannot silently select a
// default; a neighboring valid value proves this is not reject-everything.
func TestFileConfigRefusesInvalidSelectedSettings(t *testing.T) {
	cases := []struct{ data, reason string }{
		{`null`, "must be an object"},
		{`[]`, "must be an object"},
		{`{"session_manager":{"providers":{"claude":{"goal_ack_timeout":null}}}}`, "positive duration"},
		{`{"session_manager":{"providers":{"claude":{"goal_ack_timeout":"0s"}}}}`, "positive duration"},
		{`{"session_manager":{"providers":{"claude":{"goal_ack_timeout":"-1s"}}}}`, "positive duration"},
		{`{"session_manager":{"providers":{"claude":{"goal_ack_timeout":"bad"}}}}`, "positive duration"},
		{`{"session_manager":{"providers":{"claude":{"goal_ack_timeout":"1s","goal_ack_timeout":"2s"}}}}`, "duplicate configuration member"},
		{`{"session_manager":{"providers":{"claude":{"Goal_ack_timeout":"1s"}}}}`, "not recognized"},
		{`{"session_manager":{"providers":{"codex":{"goal_token_budget":0}}}}`, "positive integer"},
		{`{"session_manager":{"providers":{"codex":{"sleep_tool":null}}}}`, "must be a boolean"},
		{`{"session_manager":{"providers":{"unknown":{}}}}`, "not recognized"},
		{`{"contexts":{"builder_allowlist":["../escape"]}}`, "escapes repository"},
		{`{"contexts":{"clearance":{"dev":{"allowed_secret_classes":["invented"]}}}}`, "invalid or duplicate secret class"},
		{`{"contexts":{"payloads":{"mode":"all"}}}`, "unsupported payload mode"},
		{`{"unrecognized":true}`, "not recognized"},
		{`{"session_manager":{}} {}`, "trailing configuration data"},
	}
	for _, tc := range cases {
		t.Run(tc.data, func(t *testing.T) {
			data := tc.data
			p := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(p, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if got, err := (FileConfig{Path: p, Version: "test"}).Snapshot(context.Background()); err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("config result = %+v, %v; want refusal %q", got, err, tc.reason)
			}
			after, err := os.ReadFile(p)
			if err != nil || string(after) != data {
				t.Fatalf("refused input changed: %s, %v", after, err)
			}
		})
	}
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(`{"session_manager":{"providers":{"claude":{"goal_ack_timeout":"1s"}}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := (FileConfig{Path: p, Version: "test"}).Snapshot(context.Background()); err != nil || got.GoalAckTimeout != time.Second {
		t.Fatalf("valid control = %+v, %v", got, err)
	}
}

// A cancelled read does not apply settings, and a failed reload cannot turn
// previously explicit policy into a successful fallback.
func TestFileConfigCancellationAndFailedReload(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	cfg := FileConfig{Path: p, Optional: true, Version: "test"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cfg.Snapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel = %v", err)
	}
	if err := os.WriteFile(p, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(`{bad`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Snapshot(context.Background()); err == nil {
		t.Fatal("malformed reload silently defaulted")
	}
}

// Validated retention knobs and original bytes reach the context owner, but a
// host snapshot cannot silently ignore an unbound policy.
func TestFileConfigRetainsUnboundContextPolicyWithoutApplyingIt(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	raw := []byte(`{"contexts":{"retention":{"ready_ttl":"14d"}}}`)
	if err := os.WriteFile(p, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := FileConfig{Path: p, Version: "test"}
	loaded, err := cfg.Load(context.Background())
	if err != nil || string(loaded.ContextRetention) != `{"ready_ttl":"14d"}` || loaded.Retention == nil {
		t.Fatalf("retained = %s, %v", loaded.ContextRetention, err)
	}
	if duration, configured := loaded.Retention.ResolvedDuration("ready_ttl"); !configured || duration != 14*24*time.Hour {
		t.Fatalf("validated ready_ttl = %v, configured %v", duration, configured)
	}
	if _, err := cfg.Snapshot(context.Background()); !errors.Is(err, ErrContextRetentionSupplierRequired) {
		t.Fatalf("snapshot = %v", err)
	}
	got, err := os.ReadFile(p)
	if err != nil || string(got) != string(raw) {
		t.Fatalf("input changed: %s, %v", got, err)
	}
}

// Invalid retention cannot escape Load as usable settings or rewrite the input;
// a following valid load proves the same file-backed parser remains usable.
func TestFileConfigRefusesInvalidRetentionBeforeReturningSettings(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	cfg := FileConfig{Path: p, Version: "test"}
	for _, test := range []struct {
		body string
		want string
	}{
		{`{"limit":3}`, "not recognized"},
		{`{"ready_ttl":"0s"}`, "positive duration"},
		{`{"ready_ttl":"1h","ready_ttl":"2h"}`, "duplicate configuration member"},
		{`{"max_ready_per_binding":0}`, "positive integer"},
	} {
		raw := []byte(`{"contexts":{"retention":` + test.body + `}}`)
		if err := os.WriteFile(p, raw, 0600); err != nil {
			t.Fatal(err)
		}
		loaded, err := cfg.Load(context.Background())
		if err == nil || !strings.Contains(err.Error(), test.want) || loaded.Retention != nil || len(loaded.ContextRetention) != 0 {
			t.Fatalf("invalid retention returned settings: %+v, %v; want %s", loaded, err, test.want)
		}
		got, err := os.ReadFile(p)
		if err != nil || string(got) != string(raw) {
			t.Fatalf("rejected input changed: %s, %v", got, err)
		}
	}
	if err := os.WriteFile(p, []byte(`{"contexts":{"retention":{"ready_ttl":"1h"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := cfg.Load(context.Background())
	if err != nil || loaded.Retention == nil {
		t.Fatalf("valid retention after refusal: %v", err)
	}
}

// The original sorted canonical-role map replaces the previous alias entry.
// Combining classes from both declarations would retain a withdrawn grant.
func TestFileConfigCanonicalRoleAliasesUseOriginalReplacement(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	raw := []byte(`{"contexts":{"clearance":{"coordinator":{"allowed_secret_classes":["restricted"]},"orchestrator":{"allowed_secret_classes":["unrestricted"]}}}}`)
	if err := os.WriteFile(p, raw, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := (FileConfig{Path: p, Version: "test"}).Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	classes := got.LocalPolicy.Clearance["orchestrator"]
	if len(classes) != 1 || classes[0] != "unrestricted" {
		t.Fatalf("canonical clearance = %v; want only final unrestricted grant", classes)
	}
	if _, exists := got.LocalPolicy.Clearance["coordinator"]; exists {
		t.Fatal("legacy alias escaped canonicalization")
	}
}
