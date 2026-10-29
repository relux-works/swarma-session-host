//go:build unix

package sessionmanager_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"

	defaults "github.com/relux-works/swarma-session-host/hostdefaults"
	client "github.com/relux-works/swarma-session-host/sessionclient"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// This explicit trusted fixture owns only this isolated host/parent slot, two
// complete fixed launch envelopes and literal environment. It checks the real
// current target; the actual handler/core captures and fences ExpectedOwnerLease.
// It supplies no production registration, native process or external authority.
type legacyPhysicalAttachSource struct {
	t                  *testing.T
	host               *core.SessionHost
	project, namespace string
	launches           map[string]json.RawMessage
}

func legacyPhysicalNewAttachSource(t *testing.T, h *core.SessionHost) *legacyPhysicalAttachSource {
	t.Helper()
	s := &legacyPhysicalAttachSource{t: t, host: h, project: t.TempDir(), namespace: "physical-fixture", launches: map[string]json.RawMessage{}}
	for _, provider := range []string{"codex", "claude"} {
		kind := "codex-app-server"
		argv := []string{"app-server"}
		if provider == "claude" {
			kind, argv = "claude-pty", nil
		}
		plan := core.PrimarySessionLaunchPlan{Contract: core.PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: provider, ProjectDir: s.project, Executable: filepath.Join(s.project, provider+"-fixture"), RequiredEnvNames: []string{"LEGACY_PHYSICAL_FIXTURE"}, LaunchVariants: core.LaunchVariants{ManagedHost: core.LaunchArgv{Kind: kind, Argv: argv}}}
		var envelope any = defaults.LegacyCodexSessionLaunch{Contract: defaults.LegacyCodexSessionLaunchContract, Plan: plan}
		if provider == "claude" {
			envelope = defaults.LegacyClaudeSessionLaunch{Contract: core.ClaudeSessionLaunchContract, Plan: plan, Term: "xterm", Rows: 24, Cols: 80}
		}
		raw, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		s.launches[provider] = raw
	}
	return s
}

func (s *legacyPhysicalAttachSource) WithVerifiedAttach(ctx context.Context, q defaults.LegacyAttachRequest, target defaults.LegacyAttachTarget, fn func(context.Context, []string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	expected, ok := s.launches[q.Provider]
	if !ok || q.Owner != (core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}) || q.ProjectDir != s.project || q.BoardFingerprint != s.namespace || q.Resume || q.ResumeSessionID != "" || q.Adopt || q.MaterializationCtxID != "" || !bytes.Equal(q.Launch, expected) {
		s.t.Logf("controlled attach source refused configured request: provider=%t owner=%t project=%t namespace=%t launch=%t", ok, q.Owner == (core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}), q.ProjectDir == s.project, q.BoardFingerprint == s.namespace, bytes.Equal(q.Launch, expected))
		return defaults.ErrLegacyAttachSourceUnavailable
	}
	view, lease, mapped, err := s.host.LookupOwnerMapping(ctx, "parent/primary")
	if err != nil {
		return err
	}
	live := false
	if mapped && view.Provider == q.Provider {
		view, lease, live, err = s.host.OwnerSession(ctx, "parent/primary")
		if err != nil {
			return err
		}
	}
	if lease != target.Lease || live != target.Live || !reflect.DeepEqual(view, target.Session) {
		s.t.Log("controlled attach source refused changed target")
		return defaults.ErrLegacyAttachSourceUnavailable
	}
	err = fn(ctx, []string{"LEGACY_PHYSICAL_FIXTURE=controlled"})
	if err != nil {
		s.t.Logf("actual attach callback refused: %v", err)
	}
	return err
}

func legacyPhysicalHTTPServer(h *core.SessionHost, attach http.Handler) (*core.ControlServer, error) {
	return core.NewControlServer(h, func(s *core.ControlServer) http.Handler {
		handler, err := core.NewProtocol8Handler(h, s.Status, attach)
		if err != nil {
			panic(err)
		}
		return handler
	}, core.WithControlServerVersion("physical-fixture"))
}

// The real authenticated route rejects requests outside this fixture's exact
// configured namespace/envelopes without changing the original incumbent or
// launching a provider. Initial real attach is the nearby positive control.
func TestLegacyPhysicalAttachFixtureRefusesUnconfiguredRequests(t *testing.T) {
	f := legacyPhysicalNewTakeoverFixture(t, "verified-attach-refusal")
	base := defaults.LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: "codex", ProjectDir: f.client.source.project, BoardFingerprint: f.client.source.namespace, Launch: f.client.source.launches["codex"]}
	before, err := f.client.Session(context.Background(), f.incumbent.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	_, leaseBefore, _, err := f.manager.OwnerSession(context.Background(), "parent/primary")
	if err != nil {
		t.Fatal(err)
	}
	opaqueBefore := f.storedOpaque(t)
	for _, change := range []struct {
		name   string
		mutate func(*defaults.LegacyAttachRequest)
	}{
		{"foreign project", func(q *defaults.LegacyAttachRequest) { q.ProjectDir = filepath.Join(q.ProjectDir, "foreign") }},
		{"foreign namespace", func(q *defaults.LegacyAttachRequest) { q.BoardFingerprint = "foreign-fixture" }},
		{"foreign owner", func(q *defaults.LegacyAttachRequest) {
			q.Owner = core.Protocol8SessionOwner{Kind: "run", ID: "RUN-FOREIGN-FIXTURE"}
		}},
		{"unconfigured launch", func(q *defaults.LegacyAttachRequest) {
			var launch defaults.LegacyCodexSessionLaunch
			if err := json.Unmarshal(q.Launch, &launch); err != nil {
				t.Fatal(err)
			}
			launch.AutoContinue.Enabled = true
			raw, err := json.Marshal(launch)
			if err != nil {
				t.Fatal(err)
			}
			q.Launch = raw
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			request := base
			change.mutate(&request)
			var response defaults.LegacyAttachment
			err := f.client.DoRequest(context.Background(), "POST", "/v1/sessions/attach", request, &response, true)
			public, ok := err.(*client.PublicError)
			if !ok || public.Code != "attach_failed" || !reflect.DeepEqual(response, defaults.LegacyAttachment{}) {
				t.Fatalf("unconfigured request was not refused: %#v / %v", response, err)
			}
			after, err := f.client.Session(context.Background(), f.incumbent.Session.SessionID)
			_, leaseAfter, _, leaseErr := f.manager.OwnerSession(context.Background(), "parent/primary")
			// Read-only observations may refresh UpdatedAt. Session identity,
			// controller, native bytes, attachment count and effects must not change.
			if err != nil || leaseErr != nil || after.SessionID != before.SessionID || after.Owner != before.Owner || after.Provider != before.Provider || after.State != core.SessionRunning || after.Liveness != core.SessionLivenessLive || after.AttachedClients != 1 || after.Eviction != nil || !after.CreatedAt.Equal(before.CreatedAt) || leaseAfter != leaseBefore || !bytes.Equal(opaqueBefore, f.storedOpaque(t)) || core.LegacyPhysicalTestLaunchCount(f.codex) != 1 || core.LegacyPhysicalTestLaunchCount(f.claude) != 0 {
				t.Fatalf("refused attach disturbed incumbent: %#v / %v", after, err)
			}
		})
	}
}
