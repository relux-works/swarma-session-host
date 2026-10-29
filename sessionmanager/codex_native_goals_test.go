package sessionmanager

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Proves every managed Codex host argv carries exactly one manager-owned
// `-c features.goals=false` ahead of the app-server subcommand, that no
// composition spelling can re-enable native goals, and that unrelated global
// overrides and subcommand arguments pass through untouched.
func TestCodexHostArgvWithNativeGoalsDisabled(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "inserts ahead of the subcommand and keeps other overrides",
			in:   []string{"-c", `model="gpt-6-luna"`, "-c", `features.sleep_tool.mode="always_on"`, "app-server"},
			want: []string{"-c", `model="gpt-6-luna"`, "-c", `features.sleep_tool.mode="always_on"`, "-c", "features.goals=false", "app-server"},
		},
		{
			name: "removes a composition override that enables goals",
			in:   []string{"-c", "features.goals=true", "app-server"},
			want: []string{"-c", "features.goals=false", "app-server"},
		},
		{
			name: "removes the long and inline config spellings",
			in:   []string{"--config", "features.goals=true", "--config=features.goals = true", "app-server"},
			want: []string{"-c", "features.goals=false", "app-server"},
		},
		{
			name: "removes enable and disable feature flags for goals only",
			in:   []string{"--enable", "goals", "--enable=goals", "--enable", "sleep_tool", "--disable=goals", "app-server"},
			want: []string{"--enable", "sleep_tool", "-c", "features.goals=false", "app-server"},
		},
		{
			name: "does not rewrite arguments after the subcommand",
			in:   []string{"app-server", "-c", "features.goals=true"},
			want: []string{"-c", "features.goals=false", "app-server", "-c", "features.goals=true"},
		},
		{
			name: "prepends when the plan names no subcommand",
			in:   []string{"-c", "model=x"},
			want: []string{"-c", "features.goals=false", "-c", "model=x"},
		},
		{
			name: "an existing disable is replaced by exactly one override",
			in:   []string{"-c", "features.goals=false", "app-server"},
			want: []string{"-c", "features.goals=false", "app-server"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := codexHostArgvWithNativeGoalsDisabled(append([]string(nil), tc.in...))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("argv = %q, want %q", got, tc.want)
			}
		})
	}
}

// Proves BLOCKED: is recognised only on the last agent message and yields the
// named blocker; earlier BLOCKED: text, other item types, and plain replies do not.
func TestCodexTurnBlockedReply(t *testing.T) {
	item := func(kind, text string) json.RawMessage {
		raw, _ := json.Marshal(map[string]string{"type": kind, "text": text})
		return raw
	}
	for _, tc := range []struct {
		name  string
		items []json.RawMessage
		want  string
		ok    bool
	}{
		{name: "blocked final message", items: []json.RawMessage{item("agentMessage", "working"), item("agentMessage", "BLOCKED: need the signing key")}, want: "need the signing key", ok: true},
		{name: "blocked without detail", items: []json.RawMessage{item("agentMessage", "BLOCKED:")}, want: "the run replied BLOCKED: without naming the blocker", ok: true},
		{name: "earlier blocked is superseded", items: []json.RawMessage{item("agentMessage", "BLOCKED: x"), item("agentMessage", "resolved, done")}},
		{name: "non-message items are skipped", items: []json.RawMessage{item("agentMessage", "BLOCKED: y"), item("commandExecution", "")}, want: "y", ok: true},
		{name: "plain completion", items: []json.RawMessage{item("agentMessage", "handed off")}},
		{name: "no items", items: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := codexTurnBlockedReply(&codexTurn{Items: tc.items})
			if ok != tc.ok || got != tc.want {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
	if _, ok := codexTurnBlockedReply(nil); ok {
		t.Fatal("nil turn reported blocked")
	}
}

// Proves only a usage-limit turn error maps to usage_limited; other failures do not.
func TestCodexTurnUsageLimited(t *testing.T) {
	for raw, want := range map[string]bool{
		`{"codexErrorInfo":"usageLimitExceeded","message":"You've hit your usage limit"}`: true,
		`{"message":"usage limit reached"}`:                                               true,
		`{"message":"stream disconnected"}`:                                               false,
		``:                                                                                false,
	} {
		if got := codexTurnUsageLimited(&codexTurn{Error: json.RawMessage(raw)}); got != want {
			t.Fatalf("codexTurnUsageLimited(%s) = %v, want %v", raw, got, want)
		}
	}
}
