package sessionmanager

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

type attachResultFactory struct {
	snapshot RunnerSnapshot
	runners  []*hostCommitProbeRunner
}

func (f *attachResultFactory) Provider() string { return "muse" }
func (f *attachResultFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	r := &hostCommitProbeRunner{inputTestBackend: &inputTestBackend{}, snapshot: copyRunnerSnapshot(f.snapshot)}
	f.runners = append(f.runners, r)
	return r, nil
}
func (f *attachResultFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return nil, ErrProviderHostNotFound
}
func attachResultHost(t *testing.T) (*SessionHost, *hostCommitProbeCodec, *attachResultFactory) {
	t.Helper()
	_, codec, record, _ := hostCommitProbe(t)
	dir := t.TempDir()
	f := &attachResultFactory{snapshot: record.ProviderHost.Snapshot}
	h, e := NewHost(HostOptions{Layout: Layout{Namespace: "result", StateDir: dir, SessionsDir: filepath.Join(dir, "sessions"), LockFile: filepath.Join(dir, "manager.lock")}, Codec: codec, Config: hostCommitProbeConfig{}, Notices: hostCommitProbeNotices{}, Factories: []ProviderHostFactory{f}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(h.Close)
	return h, codec, f
}

// Actual ordinary Attach creates the receipt under its own admission lock.
// Rollback releases only this result, and stops only a fresh still-exclusive
// managed generation. Existing or genuinely surviving client/work keeps running.
// Controlled runner/storage fixtures qualify core custody, not live processes.
func TestAttachResultRollbackFreshAndShared(t *testing.T) {
	for _, mode := range []string{"fresh", "reused", "fresh-survivor", "active-access"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h, _, f := attachResultHost(t)
			q := AttachRequest{Owner: "parent/primary", Provider: "muse"}
			var survivor string
			if mode == "reused" {
				a, e := h.Attach(ctx, q)
				if e != nil {
					t.Fatal(e)
				}
				survivor = a.AttachmentID
			}
			a, receipt, e := h.AttachWithResult(ctx, q)
			if e != nil || receipt == nil {
				t.Fatal(e)
			}
			if mode == "fresh-survivor" {
				other, e := h.Attach(ctx, q)
				if e != nil {
					t.Fatal(e)
				}
				survivor = other.AttachmentID
			}
			var access AttachmentAccess
			if mode == "active-access" {
				access, e = h.AcquireAttachment(ctx, a.Session.SessionID, a.AttachmentID)
				if e != nil {
					t.Fatal(e)
				}
				defer access.Release()
			}
			if e = h.RollbackAttachResult(ctx, receipt); e != nil {
				t.Fatal(e)
			}
			if _, exists := h.attachments[a.AttachmentID]; exists {
				t.Fatal("rollback retained undisclosed client")
			}
			runner := f.runners[0]
			want := 0
			if mode == "fresh" {
				want = 1
			}
			if runner.forces != want {
				t.Fatalf("rollback stopped wrong owned generation: %d", runner.forces)
			}
			if mode == "fresh" {
				if h.sessions[a.Session.SessionID].State != SessionStopped || h.inputs[a.Session.SessionID] != nil {
					t.Fatal("fresh rollback did not observe stopped cleanup")
				}
			} else {
				if h.sessions[a.Session.SessionID].State != SessionRunning || h.inputs[a.Session.SessionID] == nil {
					t.Fatal("shared rollback revoked actual live work")
				}
				if survivor != "" && h.attachments[survivor] != a.Session.SessionID {
					t.Fatal("rollback removed genuine other client")
				}
			}
			if e = h.RollbackAttachResult(ctx, receipt); !errors.Is(e, ErrInvalidInput) || runner.forces != want {
				t.Fatal("consumed result retired twice", e)
			}
		})
	}
}

// The registered pointer is local operation custody, not an authored lease or
// fresh flag. Foreign/zero/copied/committed receipts cannot mutate membership;
// a real Stop/resume makes an old receipt incapable of stopping replacement.
func TestAttachResultReceiptCannotSelectForeignOrReplacement(t *testing.T) {
	for _, mode := range []string{"zero", "foreign", "copied", "committed", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h, _, f := attachResultHost(t)
			q := AttachRequest{Owner: "parent/primary", Provider: "muse"}
			a, r, e := h.AttachWithResult(ctx, q)
			if e != nil {
				t.Fatal(e)
			}
			target := h
			switch mode {
			case "zero":
				r = &AttachmentResultReceipt{}
			case "foreign":
				target, _, _ = attachResultHost(t)
			case "copied":
				copy := *r
				r = &copy
			case "committed":
				if e = h.CommitAttachResult(ctx, r); e != nil {
					t.Fatal(e)
				}
			case "replacement":
				if _, e = h.Stop(ctx, a.Session.SessionID); e != nil {
					t.Fatal(e)
				}
				q.ResumeSessionID = a.Session.SessionID
				a, e = h.Attach(ctx, q)
				if e != nil {
					t.Fatal(e)
				}
			}
			if mode == "replacement" {
				if e = h.CommitAttachResult(ctx, r); !errors.Is(e, ErrStaleInputController) {
					t.Fatal("stale result published replacement", e)
				}
			}
			if e = target.RollbackAttachResult(ctx, r); e == nil {
				t.Fatal("unowned result accepted")
			}
			if h.attachments[a.AttachmentID] != a.Session.SessionID {
				t.Fatal("invalid rollback removed current genuine client")
			}
			if mode == "replacement" {
				if len(f.runners) != 2 || f.runners[1].forces != 0 {
					t.Fatal("stale result stopped replacement")
				}
			} else if f.runners[0].forces != 0 {
				t.Fatal("invalid receipt stopped live runner")
			}
		})
	}
}

// Cancellation cannot consume publication custody. Failed intent persistence
// is an actual cleanup error with the receipt retained, no force or healthy ACK.
func TestAttachResultCancellationAndFailedCleanupRetainCustody(t *testing.T) {
	ctx := context.Background()
	h, codec, f := attachResultHost(t)
	a, r, e := h.AttachWithResult(ctx, AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if e = h.CommitAttachResult(canceled, r); !errors.Is(e, context.Canceled) {
		t.Fatal("canceled publication accepted", e)
	}
	if _, ok := h.attachResults[r]; !ok {
		t.Fatal("canceled publication lost cleanup custody")
	}
	codec.ack = func(LegacyRecordInput) error { return errors.New("intent acknowledgement refused") }
	if e = h.RollbackAttachResult(ctx, r); e == nil {
		t.Fatal("failed durable cleanup became success")
	}
	if _, ok := h.attachResults[r]; !ok || f.runners[0].forces != 0 {
		t.Fatal("failed cleanup discarded custody or forced")
	}
	if _, ok := h.attachments[a.AttachmentID]; ok {
		t.Fatal("failed cleanup retained hidden client")
	}
}

// Two still-unpublished results cannot masquerade as a genuine surviving
// client and lose origin cleanup custody. After the second releases its own
// result, retrying the retained origin receipt retires exactly once.
func TestAttachResultPendingSiblingRetainsOriginCustody(t *testing.T) {
	ctx := context.Background()
	h, _, f := attachResultHost(t)
	q := AttachRequest{Owner: "parent/primary", Provider: "muse"}
	first, origin, e := h.AttachWithResult(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	second, reused, e := h.AttachWithResult(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.RollbackAttachResult(ctx, origin); !errors.Is(e, ErrLegacyCommitUncertain) {
		t.Fatal("pending sibling became successful origin cleanup", e)
	}
	if _, ok := h.attachResults[origin]; !ok || f.runners[0].forces != 0 || h.attachments[second.AttachmentID] != first.Session.SessionID {
		t.Fatal("origin custody or pending sibling lost")
	}
	if e = h.RollbackAttachResult(ctx, reused); e != nil {
		t.Fatal(e)
	}
	if e = h.RollbackAttachResult(ctx, origin); !errors.Is(e, ErrInvalidInput) || f.runners[0].forces != 1 {
		t.Fatal("resolved origin retained custody or retired twice", e)
	}
}

// The real Stop observer callback executes outside the map lock. It cannot
// publish/consume the same result while its conditional rollback is in flight.
func TestAttachResultRollbackCallbackCannotPublish(t *testing.T) {
	ctx := context.Background()
	h, _, factory := attachResultHost(t)
	factory.snapshot = RunnerSnapshot{Kind: "muse-msp", MuseMSP: &MuseRunnerSnapshot{SessionID: "result-native", Provider: "muse", State: ProviderHostRunning, GoalTransport: "native"}}
	_, r, e := h.AttachWithResult(ctx, AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	h.options.GoalBindings = retireObserverBoundary{retire: func(ctx context.Context, l ControllerLease) (bool, error) {
		calls++
		if e := h.CommitAttachResult(ctx, r); !errors.Is(e, ErrLegacyCommitUncertain) {
			t.Fatal("in-flight rollback permitted publication", e)
		}
		if e := h.RollbackAttachResult(ctx, r); !errors.Is(e, ErrLegacyCommitUncertain) {
			t.Fatal("in-flight rollback consumed twice", e)
		}
		return false, nil
	}}
	if e = h.RollbackAttachResult(ctx, r); e != nil || calls != 1 || factory.runners[0].forces != 1 {
		t.Fatal("actual rollback failed after checked callback", e, calls)
	}
}
