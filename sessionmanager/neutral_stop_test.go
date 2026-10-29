package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

type stopProbeRunner struct {
	*hostCommitProbeRunner
	stop    func(context.Context) error
	stopped bool
}

func (r *stopProbeRunner) Stop(ctx context.Context) error {
	r.forces++
	if r.stop != nil {
		if err := r.stop(ctx); err != nil {
			return err
		}
	}
	r.stopped = true
	return nil
}
func (r *stopProbeRunner) State(context.Context) (ProviderHostState, error) {
	if r.stopped {
		return ProviderHostStopped, nil
	}
	return ProviderHostRunning, nil
}
func stopProbeHost(t *testing.T) (*SessionHost, *hostCommitProbeCodec, *stopProbeRunner, string) {
	t.Helper()
	h, c, r, id := hostCommitProbeHost(t)
	stop := &stopProbeRunner{hostCommitProbeRunner: r}
	h.runners[id] = stop
	return h, c, stop, id
}
func stopProbeRead(t *testing.T, h *SessionHost, id string) ([]byte, RunnerRecord) {
	t.Helper()
	path, err := h.records.path(id)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r RunnerRecord
	if err = json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	return raw, r
}

// Stop's two real file commits each acknowledge before the next effect. The
// issued input is already revoked at the intent callback, and repeat Stop is
// byte-stable. This uses a transport double, not a native exit qualification.
func TestHostStopAcknowledgesBothTransactions(t *testing.T) {
	h, c, r, id := stopProbeHost(t)
	ctx := context.Background()
	lease, writer, err := h.Input(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	c.ack = func(input LegacyRecordInput) error {
		actual, record := stopProbeRead(t, h, id)
		if !bytes.Equal(actual, input.Bytes) {
			t.Fatal("ack preceded actual commit")
		}
		if _, err := writer.Submit(ctx, textInput(lease, "after-intent", "forbidden")); !errors.Is(err, ErrStaleInputController) {
			t.Fatalf("intent retained writer: %v", err)
		}
		if record.State == SessionStopped {
			events = append(events, "stopped-ack")
		} else {
			if !record.GracefulRetirement {
				t.Fatal("intent missing")
			}
			events = append(events, "intent-ack")
		}
		return nil
	}
	r.stop = func(context.Context) error {
		_, record := stopProbeRead(t, h, id)
		if !record.GracefulRetirement || !reflect.DeepEqual(events, []string{"intent-ack"}) {
			t.Fatal("force before acknowledged intent")
		}
		events = append(events, "force")
		return nil
	}
	row, err := h.StopWithDetail(ctx, id, "  explicit caller  ")
	if err != nil || row.State != SessionStopped || row.Liveness != SessionLivenessDead || row.Retiring || row.AttachedClients != 0 || row.LivenessDetail != stoppedHostTerminatedLivenessDetail+"; explicit caller" {
		t.Fatalf("stop result: %+v %v", row, err)
	}
	if !reflect.DeepEqual(events, []string{"intent-ack", "force", "stopped-ack"}) || r.forces != 1 {
		t.Fatalf("transaction order: %v", events)
	}
	before, _ := stopProbeRead(t, h, id)
	if _, err = h.Stop(ctx, id); err != nil {
		t.Fatal(err)
	}
	after, _ := stopProbeRead(t, h, id)
	if !bytes.Equal(before, after) || r.forces != 1 || len(events) != 3 {
		t.Fatal("repeat stop rewrote or forced")
	}
	if _, _, err = h.Input(ctx, id); !errors.Is(err, ErrHostCapabilityUnavailable) {
		t.Fatalf("stopped input admitted: %v", err)
	}
}

// An intent acknowledgement failure cannot reach force. A final failure cannot
// expose success or allow another force; exact stopped recovery removes stale
// runner/attachment capabilities instead of resurrecting them.
func TestHostStopAcknowledgementFailureClosesForce(t *testing.T) {
	for _, stage := range []string{"intent", "stopped"} {
		t.Run(stage, func(t *testing.T) {
			h, c, r, id := stopProbeHost(t)
			ctx := context.Background()
			fault := errors.New("owned stop acknowledgement failure")
			c.ack = func(LegacyRecordInput) error {
				if stage == "intent" || r.forces != 0 {
					return fault
				}
				return nil
			}
			if _, err := h.Stop(ctx, id); !errors.Is(err, ErrLegacyCommitUncertain) || !errors.Is(err, fault) {
				t.Fatalf("stop ack failure: %v", err)
			}
			wantForce := map[string]int{"intent": 0, "stopped": 1}[stage]
			if r.forces != wantForce {
				t.Fatalf("force after failed acknowledgement: %d", r.forces)
			}
			acks := c.acks
			if _, err := h.Stop(ctx, id); !errors.Is(err, ErrLegacyCommitUncertain) || r.forces != wantForce || c.acks != acks {
				t.Fatalf("uncertain retry was not fenced: %v", err)
			}
			c.ack = nil
			if err := h.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if stage == "stopped" {
				if h.runners[id] != nil || h.inputs[id] != nil || len(h.attachments) != 0 {
					t.Fatal("stopped recovery retained dead capabilities")
				}
			}
			row, err := h.Stop(ctx, id)
			if err != nil || row.State != SessionStopped || r.forces != 1 {
				t.Fatalf("recovered stop: %+v %v", row, err)
			}
		})
	}
}

// A refused force remains a durable pending retirement, with its diagnostic,
// rather than a stopped success. The next Stop owns new intent/final receipts.
func TestHostStopForceFailureRetainsIntentForRetry(t *testing.T) {
	h, c, r, id := stopProbeHost(t)
	ctx := context.Background()
	fault := errors.New("native force refused")
	r.stop = func(context.Context) error { return fault }
	if _, err := h.Stop(ctx, id); !errors.Is(err, fault) {
		t.Fatalf("lost force refusal: %v", err)
	}
	_, pending := stopProbeRead(t, h, id)
	if !pending.GracefulRetirement || pending.State == SessionStopped || pending.Liveness != SessionLivenessUnknown || pending.LivenessDetail != fault.Error() {
		t.Fatalf("false stopped after force refusal: %+v", pending)
	}
	firstAcks := c.acks
	r.stop = nil
	row, err := h.Stop(ctx, id)
	if err != nil || row.State != SessionStopped || c.acks != firstAcks+2 || r.forces != 2 {
		t.Fatalf("retry borrowed receipt: %+v %v", row, err)
	}
}

type stopRestoreProbe struct {
	runner                     RunnerHost
	ordinary, retiring, launch int
	restoreErr                 error
}

func (f *stopRestoreProbe) Provider() string { return "muse" }
func (f *stopRestoreProbe) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	f.launch++
	return f.runner, nil
}
func (f *stopRestoreProbe) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	f.ordinary++
	return nil, errors.New("ordinary restore forbidden during retirement")
}
func (f *stopRestoreProbe) RestoreRetiring(context.Context, ProviderHostRecord) (RunnerHost, error) {
	f.retiring++
	return f.runner, f.restoreErr
}

// Both direct Stop of a loaded session and recovery of retirement intent use
// only retirement observation. Ordinary Restore/Launch must not create work.
func TestHostStopRetirementNeverResumesProvider(t *testing.T) {
	for _, recoverFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "recover"}[recoverFirst], func(t *testing.T) {
			h, _, r, id := stopProbeHost(t)
			ctx := context.Background()
			if recoverFirst {
				record := h.sessions[id]
				record.GracefulRetirement = true
				if err := h.records.update(ctx, record); err != nil {
					t.Fatal(err)
				}
				h.sessions[id] = record
			}
			delete(h.runners, id)
			f := &stopRestoreProbe{runner: r}
			h.options.Factories = []ProviderHostFactory{f}
			if recoverFirst {
				if err := h.Recover(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := h.Stop(ctx, id); err != nil {
				t.Fatal(err)
			}
			if f.retiring != 1 || f.ordinary != 0 || f.launch != 0 || r.forces != 1 {
				t.Fatalf("retirement created ordinary provider: %+v", f)
			}
		})
	}
}

// Refusal gates precede force and record writes, including an externally
// changed file on an otherwise idempotent stopped call. Cancellation preserves
// an unused issued writer; its adjacent valid submission still succeeds.
func TestHostStopRefusalsPreserveRecord(t *testing.T) {
	for _, kind := range []string{"cancelled", "missing", "conditional", "foreign-image", "stopped-foreign-image"} {
		t.Run(kind, func(t *testing.T) {
			h, _, r, id := stopProbeHost(t)
			ctx := context.Background()
			lease, writer, err := h.Input(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			target := id
			expected := ErrLegacyViewStale
			switch kind {
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
				expected = context.Canceled
			case "missing":
				target = "SES-missingmissingmissing"
				expected = ErrProviderHostNotFound
			case "conditional":
				record := h.sessions[id]
				record.ConditionalStop = &ConditionalStopRecord{Precondition: "not-busy"}
				h.sessions[id] = record
				expected = ErrLegacyCommitUncertain
			case "foreign-image", "stopped-foreign-image":
				if kind == "stopped-foreign-image" {
					record := h.sessions[id]
					record.State = SessionStopped
					h.sessions[id] = record
				}
				path, _ := h.records.path(id)
				if err = os.WriteFile(path, []byte("foreign bytes\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			path, _ := h.records.path(id)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			commits := 0
			h.records.commit = func(string, []byte) error { commits++; return errors.New("forbidden commit") }
			if _, err = h.Stop(ctx, target); !errors.Is(err, expected) || r.forces != 0 || commits != 0 {
				t.Fatalf("refusal crossed effects: %v forces=%d commits=%d", err, r.forces, commits)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("refused Stop changed bytes")
			}
			receipt, err := writer.Submit(context.Background(), textInput(lease, "still-live", "valid"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = writer.Await(context.Background(), receipt); err != nil || r.writeCount() != 1 {
				t.Fatalf("refusal revoked unrelated input: %v", err)
			}
		})
	}
}

// An owned physical write must drain before intent or force. Cancellation of
// the Stop waiter returns without either effect while keeping its old writer
// revoked; releasing that exact write permits a later valid Stop.
func TestHostStopDrainsPhysicalWriteBeforeIntent(t *testing.T) {
	h, _, r, id := stopProbeHost(t)
	ctx, cancelBudget := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelBudget()
	entered, revoked, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	r.execute = func(ctx context.Context, _ InputSubmission, receipt InputReceipt) (InputObservation, error) {
		close(entered)
		<-ctx.Done()
		close(revoked)
		<-release
		return InputObservation{Receipt: receipt}, ctx.Err()
	}
	lease, writer, err := h.Input(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Submit(ctx, textInput(lease, "in-flight", "valid")); err != nil {
		t.Fatal(err)
	}
	inputValue(t, ctx, entered)
	stopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := h.Stop(stopCtx, id); result <- err }()
	inputValue(t, ctx, revoked)
	cancel()
	err = inputValue(t, ctx, result)
	if !errors.Is(err, context.Canceled) || r.forces != 0 {
		t.Fatalf("drain cancellation forced: %v", err)
	}
	_, record := stopProbeRead(t, h, id)
	if record.GracefulRetirement || record.State != SessionRunning {
		t.Fatal("intent committed before physical drain")
	}
	if _, err = writer.Submit(ctx, textInput(lease, "after-cancel", "forbidden")); !errors.Is(err, ErrStaleInputController) {
		t.Fatalf("drain cancellation retained writer: %v", err)
	}
	close(release)
	if _, err = h.Stop(ctx, id); err != nil || r.forces != 1 {
		t.Fatalf("released drain did not permit Stop: %v", err)
	}
}

// Only the provider's typed not-found result proves the handle already absent.
// A generic restore failure or nil success remains pending, with no false down
// record. The absent positive preserves the exact legacy liveness wording.
func TestHostStopMissingHandleRequiresTypedAbsent(t *testing.T) {
	for _, kind := range []string{"absent", "failure", "nil-success"} {
		t.Run(kind, func(t *testing.T) {
			h, c, r, id := stopProbeHost(t)
			ctx := context.Background()
			delete(h.runners, id)
			factory := &stopRestoreProbe{}
			switch kind {
			case "absent":
				factory.restoreErr = ErrProviderHostNotFound
			case "failure":
				factory.restoreErr = errors.New("provider observation unavailable")
			}
			h.options.Factories = []ProviderHostFactory{factory}
			before := c.acks
			row, err := h.StopWithDetail(ctx, id, " caller ")
			_, record := stopProbeRead(t, h, id)
			if kind == "absent" {
				if err != nil || row.State != SessionStopped || row.Liveness != SessionLivenessDead || row.Retiring || row.LivenessDetail != stoppedHostAbsentLivenessDetail+"; caller" || c.acks != before+2 {
					t.Fatalf("typed absent Stop: %+v %v", row, err)
				}
			} else {
				if err == nil || record.State == SessionStopped || !record.GracefulRetirement || c.acks != before+1 {
					t.Fatalf("unverified absent reported down: %+v %v", record, err)
				}
				if kind == "failure" && !errors.Is(err, factory.restoreErr) {
					t.Fatalf("restore cause lost: %v", err)
				}
			}
			if factory.retiring != 1 || factory.ordinary != 0 || factory.launch != 0 || r.forces != 0 {
				t.Fatal("missing handle created or forced provider")
			}
		})
	}
}

// Explicit resume of a stopped logical session launches a fresh runner/epoch
// and clears durable retirement intent. Neither the old runner nor its revoked
// writer can be reused; the new writer beside them must actually accept input.
func TestHostStopExplicitResumeClearsRetirement(t *testing.T) {
	h, _, r, id := stopProbeHost(t)
	ctx := context.Background()
	oldLease, oldWriter, err := h.Input(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	owner := h.sessions[id].Owner
	if _, err = h.Stop(ctx, id); err != nil {
		t.Fatal(err)
	}
	fresh := &hostCommitProbeRunner{inputTestBackend: &inputTestBackend{}, snapshot: copyRunnerSnapshot(r.snapshot)}
	fresh.snapshot.MusePTY.SessionID = "fresh-native"
	factory := &stopRestoreProbe{runner: fresh}
	h.options.Factories = []ProviderHostFactory{factory}
	a, err := h.Attach(ctx, AttachRequest{Owner: owner, Provider: "muse", ResumeSessionID: id})
	if err != nil || a.Session.Retiring || a.Session.State != SessionRunning || a.Session.SessionID != id || factory.launch != 1 {
		t.Fatalf("explicit resume retained retirement or old runner: %+v %v", a, err)
	}
	_, record := stopProbeRead(t, h, id)
	if record.GracefulRetirement || record.LivenessDetail != "loaded provider host is live" || record.Eviction != nil || record.ProviderHost.Snapshot.MusePTY.SessionID != "fresh-native" {
		t.Fatal("explicit resume lost fresh committed identity")
	}
	if _, err = oldWriter.Submit(ctx, textInput(oldLease, "old-after-resume", "forbidden")); !errors.Is(err, ErrStaleInputController) {
		t.Fatalf("explicit resume resurrected old writer: %v", err)
	}
	lease, writer, err := h.Input(ctx, id)
	if err != nil || lease.Epoch <= oldLease.Epoch {
		t.Fatalf("explicit resume reused epoch: %v", err)
	}
	receipt, err := writer.Submit(ctx, textInput(lease, "fresh-after-resume", "valid"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Await(ctx, receipt); err != nil || fresh.writeCount() != 1 {
		t.Fatalf("fresh resumed input failed: %v", err)
	}
}

// Intent must persist the current typed runner cut before force, not merely
// repair the final stopped image afterwards. The neutral JSON transport fixture
// observes the real intent file at acknowledgement and again inside force.
func TestHostStopIntentCapturesCurrentSnapshot(t *testing.T) {
	h, c, r, id := stopProbeHost(t)
	before := copyRunnerSnapshot(r.snapshot)
	current := copyRunnerSnapshot(before)
	current.MusePTY.LogPath += "-current-cut"
	r.snapshot = current
	seen := false
	c.ack = func(input LegacyRecordInput) error {
		_, record := stopProbeRead(t, h, id)
		if !reflect.DeepEqual(record.ProviderHost.Snapshot, current) {
			t.Fatal("intent omitted current typed runner snapshot")
		}
		seen = true
		return nil
	}
	r.stop = func(context.Context) error {
		_, record := stopProbeRead(t, h, id)
		if !seen || !reflect.DeepEqual(record.ProviderHost.Snapshot, current) {
			t.Fatal("force preceded current snapshot acknowledgement")
		}
		return nil
	}
	if _, err := h.Stop(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if !seen || reflect.DeepEqual(before, current) {
		t.Fatal("snapshot positive control did not change")
	}
}
