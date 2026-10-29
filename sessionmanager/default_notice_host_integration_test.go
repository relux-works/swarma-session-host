package sessionmanager

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

// The real durable queue and real host pump agree on structured batches and
// receipt identity. Existing core fixtures isolate native delivery and codec;
// this is not qualification of a provider or standalone daemon constructor.
func TestHostNoticeDeliveryUsesActualDurableInbox(t *testing.T) {
	h, id, _, native := coreNoticeFixture(t)
	store, err := NewSessionNoticeStore(Layout{NoticesDir: filepath.Join(t.TempDir(), "notices")})
	if err != nil {
		t.Fatal(err)
	}
	h.options.Notices = store
	ctx := context.Background()
	for _, run := range []string{"business-one", "business-two"} {
		payload := SessionNotice{SessionID: id, RunID: run, State: "completed", Summary: "retain goal set: safely"}
		if err := store.Enqueue(ctx, id, Notice{Payload: &payload}); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := store.Pending(ctx, id)
	if err != nil || len(rows) != 2 {
		t.Fatalf("durable input = %+v, %v", rows, err)
	}
	want := FormatSessionNoticeBatch([]SessionNotice{*rows[0].Payload, *rows[1].Payload})
	failure := errors.New("injected native refusal")
	native.failure = failure
	if err := h.DeliverPendingSessionNotices(ctx); !errors.Is(err, failure) {
		t.Fatalf("native refusal = %v", err)
	}
	retained, err := store.Pending(ctx, id)
	if err != nil || len(retained) != 2 || retained[0].ID != rows[0].ID || retained[1].ID != rows[1].ID {
		t.Fatalf("failed delivery lost receipts: %+v, %v", retained, err)
	}
	native.failure = nil
	if err := h.DeliverPendingSessionNotices(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(native.delivered, []string{want, want}) {
		t.Fatalf("attempted batches = %v, want exact structured burst twice", native.delivered)
	}
	remaining, err := store.Pending(ctx, id)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("successful observed delivery did not remove exact receipts: %+v, %v", remaining, err)
	}
}
