package sessionmanager

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
)

type mappingUnknownRunner struct {
	*hostCommitProbeRunner
	fault error
	calls int
}

func (r *mappingUnknownRunner) State(context.Context) (ProviderHostState, error) {
	r.calls++
	return "", r.fault
}

// Mapping is the recorded slot and actual current lease, without native State,
// input, record ACK, epoch allocation or a liveness/admission grant.
func TestOwnerMappingReadsWithoutNativeLiveness(t *testing.T) {
	h, codec, native, id := hostCommitProbeHost(t)
	ctx := context.Background()
	record := h.sessions[id]
	record.Liveness = SessionLivenessUnknown
	record.LivenessDetail = "fixture has no current native observation"
	h.sessions[id] = record
	runner := &mappingUnknownRunner{hostCommitProbeRunner: native, fault: errors.New("fixture state unavailable")}
	h.runners[id] = runner
	before := codec.acks
	epochPath := h.options.Layout.StateDir + "/" + controllerEpochFile
	epochs, err := os.ReadFile(epochPath)
	if err != nil {
		t.Fatal(err)
	}
	view, lease, mapped, err := h.LookupOwnerMapping(ctx, record.Owner)
	after, readErr := os.ReadFile(epochPath)
	if err != nil || !mapped || view.SessionID != id || view.Liveness != SessionLivenessUnknown || lease != h.attachmentControllers[id] || lease.Epoch == 0 || runner.calls != 0 || codec.acks != before || native.forces != 0 || native.writeCount() != 0 || readErr != nil || string(after) != string(epochs) {
		t.Fatalf("mapping performed effects or lost the unknown slot: view=%+v lease=%+v mapped=%v err=%v stateCalls=%d", view, lease, mapped, err, runner.calls)
	}
	// The separate native admission observation still propagates its real error.
	if _, _, live, err := h.OwnerSession(ctx, record.Owner); !errors.Is(err, runner.fault) || live || runner.calls != 1 {
		t.Fatalf("native liveness refusal changed: live=%v err=%v calls=%d", live, err, runner.calls)
	}
}

// Unusable or replaced recorded slots refuse rather than becoming empty/live.
// Every row starts from a separately admitted actual host; only isolated fixture
// state is changed, never another session's record or native process.
func TestOwnerMappingRefusesUnusableRecords(t *testing.T) {
	for _, row := range []struct {
		name   string
		change func(*testing.T, *SessionHost, string)
		want   error
	}{
		{"duplicate", func(_ *testing.T, h *SessionHost, id string) {
			r := h.sessions[id]
			r.SessionID = "duplicate"
			h.sessions[r.SessionID] = r
		}, ErrLegacyCommitUncertain},
		{"uncertain", func(_ *testing.T, h *SessionHost, id string) { h.records.uncertain[id] = true }, ErrLegacyCommitUncertain},
		{"pending initial", func(_ *testing.T, h *SessionHost, id string) {
			h.pendingInitial[id] = pendingInitialRunner{record: h.sessions[id]}
		}, ErrLegacyCommitUncertain},
		{"conditional stop", func(_ *testing.T, h *SessionHost, id string) {
			r := h.sessions[id]
			r.ConditionalStop = &ConditionalStopRecord{}
			h.sessions[id] = r
		}, ErrLegacyCommitUncertain},
		{"retiring", func(_ *testing.T, h *SessionHost, id string) {
			r := h.sessions[id]
			r.GracefulRetirement = true
			h.sessions[id] = r
		}, ErrLegacyCommitUncertain},
		{"missing input", func(_ *testing.T, h *SessionHost, id string) { delete(h.inputs, id) }, ErrStaleInputController},
		{"external epoch", func(t *testing.T, h *SessionHost, id string) {
			if _, e := h.epochs.reserveContext(context.Background(), id); e != nil {
				t.Fatal(e)
			}
		}, ErrStaleInputController},
		{"replaced record", func(t *testing.T, h *SessionHost, id string) {
			path, e := h.records.path(id)
			if e != nil {
				t.Fatal(e)
			}
			raw, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(path, append(raw, ' '), 0600); e != nil {
				t.Fatal(e)
			}
		}, ErrLegacyViewStale},
	} {
		t.Run(row.name, func(t *testing.T) {
			h, codec, native, id := hostCommitProbeHost(t)
			owner := h.sessions[id].Owner
			row.change(t, h, id)
			acks, states, clients := codec.acks, native.states, len(h.attachments)
			view, lease, mapped, err := h.LookupOwnerMapping(context.Background(), owner)
			if !errors.Is(err, row.want) || mapped || !reflect.DeepEqual(view, SessionView{}) || lease != (ControllerLease{}) || codec.acks != acks || native.states != states || native.forces != 0 || native.writeCount() != 0 || len(h.attachments) != clients {
				t.Fatalf("unusable mapping escaped: mapped=%v err=%v want=%v", mapped, err, row.want)
			}
		})
	}
}

// An occupied mapping and its lease do not authorize a different lease, changed
// owner or closed host. An actually unmapped owner remains the absence control.
func TestOwnerMappingPinAndAbsenceAreNotAdmission(t *testing.T) {
	h, codec, native, id := hostCommitProbeHost(t)
	ctx := context.Background()
	r := h.sessions[id]
	originalOwner := r.Owner
	_, lease, mapped, err := h.LookupOwnerMapping(ctx, r.Owner)
	if err != nil || !mapped {
		t.Fatal("admitted mapping unavailable", err)
	}
	acks, states, clients := codec.acks, native.states, len(h.attachments)
	wrong := lease
	wrong.Epoch++
	if _, err = h.Attach(ctx, AttachRequest{Owner: r.Owner, Provider: r.Provider, ExpectedOwnerLease: &wrong}); !errors.Is(err, ErrStaleInputController) || codec.acks != acks || native.states != states || len(h.attachments) != clients || native.forces != 0 || native.writeCount() != 0 {
		t.Fatalf("wrong mapping pin crossed admission: %v", err)
	}
	if view, pin, found, err := h.LookupOwnerMapping(ctx, "unmapped-owner"); err != nil || found || view.SessionID != "" || pin != (ControllerLease{}) {
		t.Fatalf("unmapped owner fabricated evidence: %v %v", found, err)
	}
	if _, _, found, err := h.LookupOwnerMapping(ctx, " "); !errors.Is(err, ErrInvalidInput) || found {
		t.Fatalf("invalid owner fabricated evidence: %v %v", found, err)
	}
	stopped := r
	stopped.State = SessionStopped
	h.sessions[id] = stopped
	if view, pin, found, err := h.LookupOwnerMapping(ctx, originalOwner); err != nil || found || view.SessionID != "" || pin != (ControllerLease{}) {
		t.Fatalf("stopped owner fabricated active mapping: %v %v", found, err)
	}
	r.Owner = "replacement-owner"
	h.sessions[id] = r
	if _, err = h.Attach(ctx, AttachRequest{Owner: originalOwner, Provider: r.Provider, ExpectedOwnerLease: &lease}); !errors.Is(err, ErrStaleInputController) || codec.acks != acks || len(h.attachments) != clients {
		t.Fatalf("foreign mapping pin crossed admission: %v", err)
	}
	h.Close()
	if _, _, mapped, err := h.LookupOwnerMapping(ctx, r.Owner); !errors.Is(err, ErrHostClosed) || mapped {
		t.Fatalf("closed host fabricated mapping: %v %v", mapped, err)
	}
}
