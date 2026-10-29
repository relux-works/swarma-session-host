package sessionmanager

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"testing"
)

// rev5 Batch N independent reproductions. Only the native I/O and consumer
// boundary are probes; host, codec, records and epoch journal are real.

type rev5CountingFactory struct {
	launches, restores int
	onLaunch           func()
}

func (f *rev5CountingFactory) Provider() string { return "muse" }
func (f *rev5CountingFactory) runner() *accountingRecordRunner {
	return &accountingRecordRunner{inputTestBackend: &inputTestBackend{}, snapshot: MuseRunnerSnapshot{GoalTransport: "native", Provider: "muse", State: ProviderHostRunning, SessionID: "native-accounting", WorkspaceRoot: "/workspace", ApprovalMode: "allow"}}
}
func (f *rev5CountingFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	f.launches++
	if f.onLaunch != nil {
		f.onLaunch()
	}
	return f.runner(), nil
}
func (f *rev5CountingFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	f.restores++
	return f.runner(), nil
}

func rev5Host(t *testing.T, f *rev5CountingFactory) *SessionHost {
	t.Helper()
	root := t.TempDir()
	h, e := NewHost(HostOptions{Layout: Layout{Namespace: "rev5-fixture", StateDir: root, SessionsDir: filepath.Join(root, "sessions"), LockFile: filepath.Join(root, "manager.lock")}, Config: hostCommitProbeConfig{}, Notices: hostCommitProbeNotices{}, Codec: NewLegacyRecordCodec(), Factories: []ProviderHostFactory{f}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(h.Close)
	return h
}

// Claim attacked: native accounting validation precedes physical resume.
// Explicit resume of a Stopped native Muse record must refuse before Launch
// when the actual consumer's validation refuses. Control: running-record
// Recover refuses before Factory.Restore (restores==0).
func TestRev5StoppedResumeValidatesBeforeLaunch(t *testing.T) {
	ctx := context.Background()
	f := &rev5CountingFactory{}
	h := rev5Host(t, f)
	a, e := h.Attach(ctx, AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	id := a.Session.SessionID
	if _, e = h.Stop(ctx, id); e != nil {
		t.Fatal(e)
	}
	if h.sessions[id].State != SessionStopped {
		t.Fatalf("fixture not stopped: %v", h.sessions[id].State)
	}
	fault := errors.New("rev5 validation refused")
	launchesAtValidate := -1
	f.launches = 0
	h.options.OwnerRuns = &nativeRestoreBoundaryProbe{validate: func(context.Context, RunnerRecord, LegacyRecordInput) error {
		if launchesAtValidate < 0 {
			launchesAtValidate = f.launches
		}
		return fault
	}}
	_, e = h.Attach(ctx, AttachRequest{Owner: "parent/primary", Provider: "muse", ResumeSessionID: id})
	t.Logf("resume err=%v launches=%d launchesAtFirstValidate=%d state=%v", e, f.launches, launchesAtValidate, h.sessions[id].State)
	if !errors.Is(e, fault) {
		t.Fatalf("resume did not surface validation refusal: %v", e)
	}
	if f.launches != 0 || launchesAtValidate != 0 {
		t.Fatalf("physical Launch preceded native validation: launches=%d launchesAtFirstValidate=%d durableState=%v", f.launches, launchesAtValidate, h.sessions[id].State)
	}
}

// Claim attacked: no input/writer is published before native hydration, and
// failures publish nothing. With two pending-initial sessions, a hydration
// failure of the first must not leave the second with an unhydrated writer.
// Control mode "all-valid" proves the fixture recovers and hydrates both.
func TestRev5PendingRecoverPublishesNoUnhydratedInput(t *testing.T) {
	for _, mode := range []string{"all-valid", "first-fails"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := &rev5CountingFactory{}
			h := rev5Host(t, f)
			var ids []string
			for _, owner := range []OwnerKey{"parent/primary", "run/RUN-REV5"} {
				a, e := h.Attach(ctx, AttachRequest{Owner: owner, Provider: "muse"})
				if e != nil {
					t.Fatal(e)
				}
				ids = append(ids, a.Session.SessionID)
			}
			sort.Strings(ids)
			// Reproduce the retained state of an uncertain initial create:
			// runner retained, no input installed, record not yet in sessions.
			h.mu.Lock()
			for _, id := range ids {
				lease, e := h.inputs[id].CurrentLease(ctx, id)
				if e != nil {
					h.mu.Unlock()
					t.Fatal(e)
				}
				if e = h.inputs[id].revokeAndDrain(ctx); e != nil {
					h.mu.Unlock()
					t.Fatal(e)
				}
				delete(h.inputs, id)
				h.pendingInitial[id] = pendingInitialRunner{record: copyRunnerRecord(h.sessions[id]), epoch: lease.Epoch}
				delete(h.sessions, id)
			}
			h.mu.Unlock()
			fault := errors.New("rev5 hydrate refused")
			hydrated := map[string]int{}
			var leakedDuring error = errors.New("not probed")
			h.options.OwnerRuns = &nativeRestoreBoundaryProbe{hydrate: func(call context.Context, cut CapturedOwnerRun) error {
				hydrated[cut.Lease.SessionID]++
				if cut.Lease.SessionID == ids[0] {
					_, _, leakedDuring = h.Input(ctx, ids[1])
					if mode == "first-fails" {
						return fault
					}
				}
				return nil
			}}
			e := h.Recover(ctx)
			_, _, after := h.Input(ctx, ids[1])
			t.Logf("mode=%s recover=%v hydrated=%v inputOtherDuringFirstHydrate=%v inputOtherAfter=%v", mode, e, hydrated, leakedDuring, after)
			if mode == "all-valid" {
				if e != nil || hydrated[ids[0]] != 1 || hydrated[ids[1]] != 1 || after != nil {
					t.Fatalf("control failed: %v %v %v", e, hydrated, after)
				}
				return
			}
			if !errors.Is(e, fault) {
				t.Fatalf("recover did not surface hydrate refusal: %v", e)
			}
			if leakedDuring == nil {
				t.Errorf("unhydrated pending session issued input during another session's hydration (hydrated=%v)", hydrated)
			}
			if after == nil && hydrated[ids[1]] == 0 {
				t.Errorf("failed Recover left never-hydrated pending session with admitted input")
			}
		})
	}
}
