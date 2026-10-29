//go:build unix

package sessionmanager_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	client "github.com/relux-works/swarma-session-host/sessionclient"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// TestControlTakeoverStopsIncumbentResumablyAndAdmitsTheRequestingProvider
// covers the accepted path: one command, resumable stop, untouched goal head,
// released owner slot, and an eviction notice the victim can read.
func TestControlTakeoverStopsIncumbentResumablyAndAdmitsTheRequestingProvider(t *testing.T) {
	fixture := legacyPhysicalNewTakeoverFixture(t, "takeover-accept")

	_, err := fixture.goals.Handle(context.Background(), goal.Command{
		EnvelopeID: "takeover-goal-fixture", SessionID: fixture.incumbent.Session.SessionID,
		Namespace: goal.Assignment, Action: goal.Set,
		Content: &goal.Content{Objective: "Survive a slot takeover", Condition: "Retain the pending goal", Budget: goal.Budget{Tokens: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	activated, err := fixture.goals.Get(goal.Owner(fixture.incumbent.Session.SessionID, goal.Assignment))
	if err != nil {
		t.Fatal(err)
	}
	if !activated.Pending || activated.Applied != nil {
		t.Fatalf("activated native goal must be pending and unbound: %#v", activated)
	}
	pending := activated.Latest

	incumbentView := fixture.conflictingClaudeAttach(t)
	identityBefore := fixture.storedOpaque(t)

	evicted, err := fixture.client.TakeOverOwnerSlot(context.Background(), core.TakeoverRequest{
		Owner:     core.OwnerKey("parent/primary"),
		Provider:  "claude",
		SessionID: incumbentView.SessionID,
		Observed:  core.ObservationOf(incumbentView),
	})
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if evicted.State != core.SessionStopped {
		t.Fatalf("evicted state = %q, want stopped", evicted.State)
	}
	if evicted.Liveness == core.SessionLivenessLive {
		t.Fatalf("evicted record still claims liveness=live: %#v", evicted)
	}
	if evicted.Eviction == nil || evicted.Eviction.ByProvider != "claude" ||
		evicted.Eviction.At.IsZero() {
		t.Fatalf("eviction notice = %#v, want a timestamped claude takeover", evicted.Eviction)
	}
	if evicted.AttachedClients != 0 {
		t.Fatalf("evicted attached clients = %d, want 0", evicted.AttachedClients)
	}
	if identityAfter := fixture.storedOpaque(t); !bytes.Equal(identityBefore, identityAfter) {
		t.Fatalf("stored native identity changed across takeover:\n%s\n%s", identityBefore, identityAfter)
	}
	current, err := fixture.goals.Get(goal.Owner(fixture.incumbent.Session.SessionID, goal.Assignment))
	if err != nil {
		t.Fatal(err)
	}
	if !current.Pending || current.Applied != nil ||
		current.Latest == nil || *current.Latest != *pending {
		t.Fatalf("takeover changed the primary goal head: %#v", current)
	}
	if holder := fixture.ownerSlotHolder(); holder != "" {
		t.Fatalf("owner slot still held by %q after takeover", holder)
	}

	attachment, err := fixture.client.Attach(context.Background(), core.AttachRequest{
		Owner:    core.OwnerKey("parent/primary"),
		Provider: "claude",
	})
	if err != nil {
		t.Fatalf("attach after takeover: %v", err)
	}
	if attachment.Session.Provider != "claude" ||
		attachment.Session.SessionID == incumbentView.SessionID {
		t.Fatalf("post-takeover attachment = %#v", attachment.Session)
	}
	if core.LegacyPhysicalTestLaunchCount(fixture.claude) != 1 {
		t.Fatalf("claude launches = %d, want 1", core.LegacyPhysicalTestLaunchCount(fixture.claude))
	}
	// The evicted handle keeps its notice so its own wrapper can still explain
	// the closed stream after the replacement is already running.
	stale, err := fixture.client.Session(context.Background(), incumbentView.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Eviction == nil || stale.Eviction.ByProvider != "claude" {
		t.Fatalf("evicted session lost its notice: %#v", stale)
	}
}

// TestControlTakeoverRefusesAChangedObservation is the negative half of
// acceptance criterion 5. Each case changes exactly one fact the disclosure
// named, so a gate narrowed to any single dimension admits one of them.
func TestControlTakeoverRefusesAChangedObservation(t *testing.T) {
	tests := []struct {
		name string
		// mutate changes the world after the disclosure was drawn.
		mutate func(t *testing.T, f *legacyPhysicalTakeoverFixture)
		// forge changes what the caller claims it saw instead.
		forge func(observed *core.SessionObservation)
	}{
		{
			name: "a client attached while the prompt was on screen",
			mutate: func(t *testing.T, f *legacyPhysicalTakeoverFixture) {
				if _, err := f.client.Attach(context.Background(), core.AttachRequest{
					Owner:    core.OwnerKey("parent/primary"),
					Provider: "codex",
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "the caller under-reports the attached clients it disclosed",
			forge: func(observed *core.SessionObservation) {
				observed.AttachedClients = 0
			},
		},
		{
			name: "the caller claims a provider the incumbent does not have",
			forge: func(observed *core.SessionObservation) {
				observed.Provider = "claude"
			},
		},
		{
			name: "the caller claims a state the incumbent is not in",
			forge: func(observed *core.SessionObservation) {
				observed.State = core.SessionRecovering
			},
		},
		{
			name: "the caller claims a liveness detail that was never observed",
			forge: func(observed *core.SessionObservation) {
				observed.LivenessDetail = "provider process pid 4242 is live"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := legacyPhysicalNewTakeoverFixture(t, "takeover-changed")
			incumbentView := fixture.conflictingClaudeAttach(t)
			observed := core.ObservationOf(incumbentView)
			if test.mutate != nil {
				test.mutate(t, fixture)
			}
			if test.forge != nil {
				test.forge(&observed)
			}

			_, err := fixture.client.TakeOverOwnerSlot(context.Background(), core.TakeoverRequest{
				Owner:     core.OwnerKey("parent/primary"),
				Provider:  "claude",
				SessionID: incumbentView.SessionID,
				Observed:  observed,
			})
			var public *client.PublicError
			if !errors.As(err, &public) ||
				public.Code != core.ErrorSessionTakeoverObservationChanged {
				t.Fatalf("takeover = %v, want %s", err, core.ErrorSessionTakeoverObservationChanged)
			}
			legacyPhysicalAssertIncumbentSurvived(t, fixture, incumbentView.SessionID)
		})
	}
}

// TestControlTakeoverRefusesWhenTheOwnerSlotMovedOn covers the identity half of
// the same criterion: the confirmation named one handle and only that handle
// may be evicted.
func TestControlTakeoverRefusesWhenTheOwnerSlotMovedOn(t *testing.T) {
	fixture := legacyPhysicalNewTakeoverFixture(t, "takeover-moved")
	incumbentView := fixture.conflictingClaudeAttach(t)
	observed := core.ObservationOf(incumbentView)

	if _, err := fixture.client.Stop(context.Background(), incumbentView.SessionID); err != nil {
		t.Fatal(err)
	}
	replacement, err := fixture.client.Attach(context.Background(), core.AttachRequest{
		Owner:    core.OwnerKey("parent/primary"),
		Provider: "codex",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Session.SessionID == incumbentView.SessionID {
		t.Fatal("replacement reused the stopped handle")
	}

	_, err = fixture.client.TakeOverOwnerSlot(context.Background(), core.TakeoverRequest{
		Owner:     core.OwnerKey("parent/primary"),
		Provider:  "claude",
		SessionID: incumbentView.SessionID,
		Observed:  observed,
	})
	var public *client.PublicError
	if !errors.As(err, &public) || public.Code != core.ErrorSessionTakeoverObservationChanged {
		t.Fatalf("takeover = %v, want %s", err, core.ErrorSessionTakeoverObservationChanged)
	}
	current, err := fixture.client.Session(context.Background(), replacement.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != core.SessionRunning || current.Eviction != nil {
		t.Fatalf("uninvolved replacement was disturbed: %#v", current)
	}
}

// TestControlTakeoverFailsClosedOnUnknownLiveness is acceptance criterion 6.
// Confirmation is not evidence: a host that cannot be observed cannot be proven
// safe to stop, whether or not the caller says a human agreed.
func TestControlTakeoverFailsClosedOnUnknownLiveness(t *testing.T) {
	tests := []struct {
		name    string
		observe func(view core.SessionView) core.SessionObservation
	}{
		{
			name:    "the caller honestly reports the unknown it was shown",
			observe: core.ObservationOf,
		},
		{
			name: "the caller forges a live observation over the failed read",
			observe: func(view core.SessionView) core.SessionObservation {
				observed := core.ObservationOf(view)
				observed.Liveness = core.SessionLivenessLive
				observed.LivenessDetail = "loaded provider host is live"
				return observed
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := legacyPhysicalNewTakeoverFixture(t, "takeover-unknown")
			host := core.LegacyPhysicalTestHost(fixture.codex, fixture.incumbent.Session.SessionID)
			if host == nil {
				t.Fatal("incumbent host was not launched")
			}
			core.LegacyPhysicalTestFailState(host, errors.New("provider control socket timed out"))

			incumbentView := fixture.conflictingClaudeAttach(t)
			if incumbentView.Liveness != core.SessionLivenessUnknown {
				t.Fatalf("incumbent liveness = %q, want unknown", incumbentView.Liveness)
			}

			_, err := fixture.client.TakeOverOwnerSlot(context.Background(), core.TakeoverRequest{
				Owner:     core.OwnerKey("parent/primary"),
				Provider:  "claude",
				SessionID: incumbentView.SessionID,
				Observed:  test.observe(incumbentView),
			})
			var public *client.PublicError
			if !errors.As(err, &public) || public.Code != core.ErrorSessionLivenessUnknown {
				t.Fatalf("takeover = %v, want %s", err, core.ErrorSessionLivenessUnknown)
			}
			legacyPhysicalAssertIncumbentSurvived(t, fixture, incumbentView.SessionID)
		})
	}
}

// TestControlTakeoverRefusesTheSameProvider keeps the operation to the
// cross-provider conflict it exists for; a same-provider caller reattaches.
func TestControlTakeoverRefusesTheSameProvider(t *testing.T) {
	fixture := legacyPhysicalNewTakeoverFixture(t, "takeover-same-provider")
	incumbentView, err := fixture.client.Session(
		context.Background(),
		fixture.incumbent.Session.SessionID,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fixture.client.TakeOverOwnerSlot(context.Background(), core.TakeoverRequest{
		Owner:     core.OwnerKey("parent/primary"),
		Provider:  "codex",
		SessionID: incumbentView.SessionID,
		Observed:  core.ObservationOf(incumbentView),
	})
	var public *client.PublicError
	if !errors.As(err, &public) || public.Code != core.ErrorSessionTakeoverNotApplicable {
		t.Fatalf("takeover = %v, want %s", err, core.ErrorSessionTakeoverNotApplicable)
	}
	legacyPhysicalAssertIncumbentSurvived(t, fixture, incumbentView.SessionID)
}

func legacyPhysicalAssertIncumbentSurvived(t *testing.T, fixture *legacyPhysicalTakeoverFixture, sessionID string) {
	t.Helper()
	view, err := fixture.client.Session(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if view.State != core.SessionRunning {
		t.Fatalf("refused takeover changed incumbent state to %q", view.State)
	}
	if view.Eviction != nil {
		t.Fatalf("refused takeover recorded an eviction notice: %#v", view.Eviction)
	}
	if holder := fixture.ownerSlotHolder(); holder != sessionID {
		t.Fatalf("owner slot holder = %q, want %q", holder, sessionID)
	}
	if core.LegacyPhysicalTestLaunchCount(fixture.claude) != 0 {
		t.Fatalf("refused takeover launched the replacement provider %d times", core.LegacyPhysicalTestLaunchCount(fixture.claude))
	}
	host := core.LegacyPhysicalTestHost(fixture.codex, sessionID)
	if host == nil {
		t.Fatal("incumbent host disappeared")
	}
	state, stateErr := host.State(context.Background())
	if stateErr != nil && state == "" {
		t.Fatal(stateErr)
	}
	if state != core.ProviderHostRunning {
		t.Fatalf("refused takeover stopped the incumbent host (state %q)", state)
	}
}
