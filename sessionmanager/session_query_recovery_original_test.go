package sessionmanager_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type rev4vNative struct {
	*rev4HookNative
	smu   sync.Mutex
	state core.ProviderHostState
	err   error
}

func (n *rev4vNative) Launch(ctx context.Context, q core.ProviderLaunchRequest) (core.RunnerHost, error) {
	if _, e := n.rev4HookNative.Launch(ctx, q); e != nil {
		return nil, e
	}
	return n, nil
}
func (n *rev4vNative) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	return n, nil
}
func (n *rev4vNative) State(context.Context) (core.ProviderHostState, error) {
	n.smu.Lock()
	defer n.smu.Unlock()
	if n.state == "" {
		return core.ProviderHostRunning, n.err
	}
	return n.state, n.err
}
func (n *rev4vNative) set(s core.ProviderHostState, e error) {
	n.smu.Lock()
	n.state, n.err = s, e
	n.smu.Unlock()
}
func (n *rev4vNative) counts() (int, int, int) {
	n.accountingIntegrationNative.mu.Lock()
	l, s := n.launches, n.stops
	n.accountingIntegrationNative.mu.Unlock()
	return l, s, n.writeCount()
}

// Builds a fixture with one goal and a nonzero budget, then fails the stopped
// resume at its first subscription.
func rev4vFailedResume(t *testing.T, ctx context.Context, failSub int) (*accountingIntegrationSystem, *rev4vNative, *goal.Service, string, core.ControllerLease, string, core.AttachRequest) {
	t.Helper()
	root := t.TempDir()
	service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
	if e != nil {
		t.Fatal(e)
	}
	native := &rev4vNative{rev4HookNative: &rev4HookNative{accountingIntegrationNative: &accountingIntegrationNative{root: root, foldID: "native-fixture", total: 9, sequence: 1}}}
	system := accountingOpenSystem(t, ctx, root, service, native)
	att, e := system.host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	sid := att.Session.SessionID
	lease, writer, e := system.host.Input(ctx, sid)
	if e != nil {
		t.Fatal(e)
	}
	_, binding, e := system.factory.BindGoal(ctx, lease, "muse", writer)
	if e != nil {
		t.Fatal(e)
	}
	if r, e := service.Execute(ctx, goal.Command{EnvelopeID: "admitted", SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "work", Condition: "result", Budget: goal.Budget{Tokens: 100}}}, binding); e != nil || r.Pending {
		t.Fatal(r, e)
	}
	if _, live, e := system.host.ReconcileOwner(ctx, "parent/primary"); e != nil || !live {
		t.Fatal(e)
	}
	native.setTotal(160)
	if e = system.factory.CheckBudget(ctx, lease); e != nil {
		t.Fatal(e)
	}
	if _, e = system.host.Stop(ctx, sid); e != nil {
		t.Fatal(e)
	}
	native.rev4HookNative.mu.Lock()
	native.failSub = failSub
	native.rev4HookNative.mu.Unlock()
	q := core.AttachRequest{Owner: "parent/primary", Provider: "muse", ResumeSessionID: sid}
	if a, e := system.host.Attach(ctx, q); e == nil || a != nil {
		t.Fatalf("injected subscription failure missing: %v", e)
	}
	return system, native, service, sid, lease, root, q
}

// A query discloses a real failed native read as Unknown and keeps the exact
// committed Recovering/error/accounting image. Native Running is not settlement;
// cancellation and invalid native State retain their actual errors. Native I/O
// is controlled; the goal service, accounting store and host are real.
func TestSessionQueryUnknownAndRecoveringAreReadOnly(t *testing.T) {
	for _, mode := range []string{"running", "unknown", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			system, native, _, sid, _, root, _ := rev4vFailedResume(t, ctx, 1)
			path := filepath.Join(root, "host", "sessions", sid, "record.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			launches, stops, writes := native.counts()
			fault := errors.New("fixture native state unavailable")
			switch mode {
			case "unknown":
				native.set(core.ProviderHostRunning, fault)
			case "invalid":
				native.set(core.ProviderHostState("invalid"), nil)
			default:
				native.set(core.ProviderHostRunning, nil)
			}
			view, queryErr := system.host.Session(ctx, sid)
			if mode == "invalid" {
				if queryErr == nil || !strings.Contains(queryErr.Error(), "invalid provider state") {
					t.Fatalf("invalid native State reclassified: %v", queryErr)
				}
			} else {
				if queryErr != nil || view.State != core.SessionRecovering || view.OwnerRunError != "native accounting restoration failed" {
					t.Fatalf("query settled or hid recovery: state=%s error=%q err=%v", view.State, view.OwnerRunError, queryErr)
				}
				if mode == "unknown" && (view.Liveness != core.SessionLivenessUnknown || !strings.Contains(view.LivenessDetail, fault.Error())) {
					t.Fatalf("query lost actual unknown observation: %+v", view)
				}
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("query changed committed recovery image", err)
			}
			if l, s, w := native.counts(); l != launches || s != stops || w != writes {
				t.Fatal("query launched/stopped/wrote native input")
			}
			if _, input, err := system.host.Input(ctx, sid); err == nil || input != nil {
				t.Fatalf("query admitted unsettled input: %v", err)
			}
			cancelled, stop := context.WithCancel(ctx)
			stop()
			if _, err := system.host.Session(cancelled, sid); !errors.Is(err, context.Canceled) {
				t.Fatalf("query lost cancellation: %v", err)
			}
		})
	}
}

// Claim under attack: after a failed resume the record stays Recovering until the
// settled recovery transaction commits Running, and ordinary retry recovers.
// Reproduction: ONE public Session() read (live retained runner) in between.
// Paired control "no-read" is the candidate's own retry path and must pass.
func TestRev4VSessionReadPublishesRunning(t *testing.T) {
	for _, mode := range []string{"no-read", "read-then-retry", "read-then-reopen"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			system, native, service, sid, lease, root, q := rev4vFailedResume(t, ctx, 1)
			l0, s0, _ := native.counts()
			if mode != "no-read" {
				v, e := system.host.Session(ctx, sid)
				if e != nil {
					t.Fatal(e)
				}
				if v.State == core.SessionRunning {
					t.Errorf("BROKEN: public Session() read published Running before settled recovery (OwnerRunError=%q)", v.OwnerRunError)
				}
				if _, w, e := system.host.Input(ctx, sid); e == nil || w != nil {
					t.Fatalf("read exposed input: %v", e)
				}
			}
			if mode == "read-then-reopen" {
				// Committed image, not just a projection: a reopened host reads it.
				system.host.Close()
				if e := system.factory.Close(ctx); e != nil {
					t.Fatal(e)
				}
				system = accountingOpenSystem(t, ctx, root, service, native)
				if e := system.host.Recover(ctx); e != nil {
					t.Fatalf("reopen Recover: %v", e)
				}
				v, e := system.host.Session(ctx, sid)
				if e != nil || v.State != core.SessionRunning || v.OwnerRunError != "" {
					t.Errorf("BROKEN: reopened recovery left %s/%q %v", v.State, v.OwnerRunError, e)
				}
				return
			}
			_, err := system.host.Attach(ctx, q)
			if err != nil {
				t.Fatalf("BROKEN(%s): retry of retained runner refused: %v", mode, err)
			}
			current, input, e := system.host.Input(ctx, sid)
			if e != nil || input == nil || current.Epoch <= lease.Epoch {
				t.Fatalf("recovered input not current: %v", e)
			}
			if l, s, _ := native.counts(); l != l0 || s != s0 {
				t.Fatal("retry launched/stopped native")
			}
			v, e := system.host.Session(ctx, sid)
			if e != nil || v.State != core.SessionRunning || v.OwnerRunError != "" {
				t.Fatalf("not settled Running: %s/%q %v", v.State, v.OwnerRunError, e)
			}
		})
	}
}
