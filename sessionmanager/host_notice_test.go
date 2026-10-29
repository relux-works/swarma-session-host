package sessionmanager

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type coreNoticeProbeStore struct {
	rows    []Notice
	readErr error
	reads   int
	acks    []string
}

func (s *coreNoticeProbeStore) Enqueue(context.Context, string, Notice) error { return nil }
func (s *coreNoticeProbeStore) Pending(context.Context, string) ([]Notice, error) {
	s.reads++
	return s.rows, s.readErr
}
func (s *coreNoticeProbeStore) Acknowledge(_ context.Context, _ string, id string) error {
	s.acks = append(s.acks, id)
	return nil
}

type coreNoticeProbeRunner struct {
	RunnerHost
	delivered []string
	failure   error
}

func (r *coreNoticeProbeRunner) DeliverSessionNotice(_ context.Context, text string) error {
	r.delivered = append(r.delivered, text)
	return r.failure
}
func coreNoticeFixture(t *testing.T) (*SessionHost, string, *coreNoticeProbeStore, *coreNoticeProbeRunner) {
	t.Helper()
	h, _, runner, id := hostCommitProbeHost(t)
	store := &coreNoticeProbeStore{}
	native := &coreNoticeProbeRunner{RunnerHost: runner}
	h.options.Notices = store
	h.runners[id] = native
	for _, n := range []string{"one", "two"} {
		payload := &SessionNotice{Contract: SessionNoticeContract, SessionID: id, RunID: "business-" + n, State: "completed", Summary: "retain goal set: safely"}
		store.rows = append(store.rows, Notice{ID: "receipt-" + n + ".json", Payload: payload})
	}
	return h, id, store, native
}

// Actual host delivery coalesces complete structured payloads, including when
// readable entries accompany a read error. Receipts, not business RunIDs, are
// acknowledged only after native success. Native delivery/store are boundaries.
func TestHostNoticeBatchPreservesPayloadAndReceipts(t *testing.T) {
	h, _, store, native := coreNoticeFixture(t)
	fault := errors.New("unreadable queue member")
	store.readErr = fault
	want := FormatSessionNoticeBatch([]SessionNotice{*store.rows[0].Payload, *store.rows[1].Payload})
	if err := h.DeliverPendingSessionNotices(context.Background()); !errors.Is(err, fault) {
		t.Fatalf("read error lost: %v", err)
	}
	if !reflect.DeepEqual(native.delivered, []string{want}) || !reflect.DeepEqual(store.acks, []string{"receipt-one.json", "receipt-two.json"}) {
		t.Fatalf("batch/receipts: delivered=%v acks=%v", native.delivered, store.acks)
	}
	if err := ValidateSessionNoticeText(want); err != nil {
		t.Fatal(err)
	}
}

// Detached/stopped/unsupported/stale-controller and cancelled requests never
// inject or acknowledge. An attached current controller beside them delivers.
func TestHostNoticeAdmissionRetainsUndeliveredRows(t *testing.T) {
	for _, kind := range []string{"detached", "stopped", "unsupported", "stale", "cancelled", "valid"} {
		t.Run(kind, func(t *testing.T) {
			h, id, store, native := coreNoticeFixture(t)
			ctx := context.Background()
			switch kind {
			case "detached":
				h.attachments = map[string]string{}
			case "stopped":
				r := h.sessions[id]
				r.State = SessionStopped
				h.sessions[id] = r
			case "unsupported":
				h.runners[id] = native.RunnerHost
			case "stale":
				if _, err := h.epochs.reserveContext(ctx, id); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			err := h.DeliverPendingSessionNotices(ctx)
			if kind == "valid" {
				if err != nil || len(native.delivered) != 1 || len(store.acks) != 2 {
					t.Fatalf("valid: %v", err)
				}
				return
			}
			if len(native.delivered) != 0 || len(store.acks) != 0 {
				t.Fatalf("forbidden delivery: %v / %v", native.delivered, store.acks)
			}
			if kind == "stale" && !errors.Is(err, ErrStaleInputController) {
				t.Fatalf("stale refusal: %v", err)
			}
			if kind == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled refusal: %v", err)
			}
		})
	}
}

// Failure retains every receipt; missing/foreign payloads are never converted
// from Text/ID or acknowledged. Valid readable neighbors still deliver once.
func TestHostNoticeFailureAndForeignPayloadRetainReceipts(t *testing.T) {
	h, id, store, native := coreNoticeFixture(t)
	native.failure = errors.New("native delivery failed")
	if err := h.DeliverPendingSessionNotices(context.Background()); !errors.Is(err, native.failure) || len(store.acks) != 0 {
		t.Fatalf("failed injection: %v acks=%v", err, store.acks)
	}
	native.failure = nil
	native.delivered = nil
	foreign := *store.rows[0].Payload
	foreign.SessionID = "another-session"
	store.rows = append(store.rows, Notice{ID: "missing.json", SessionID: id, Text: "do not fabricate"}, Notice{ID: "foreign.json", Payload: &foreign})
	if err := h.DeliverPendingSessionNotices(context.Background()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid payload refusal: %v", err)
	}
	if len(native.delivered) != 1 || !reflect.DeepEqual(store.acks, []string{"receipt-one.json", "receipt-two.json"}) {
		t.Fatalf("wrong delivery/ack: %v %v", native.delivered, store.acks)
	}
}
