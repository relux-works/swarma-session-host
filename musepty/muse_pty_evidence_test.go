package musepty

import (
	"context"
	"errors"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// The actual PTY host refuses an exact-provider label before native effects,
// then exposes the actual owned run ID under D14's weaker explicit class.
func TestMusePTYEvidenceClass(t *testing.T) {
	h, home := musePeer(t, "idle")
	input, receipt := musePTYInput(h, "provider-ack")
	if _, err := h.ExecuteInput(context.Background(), input, receipt); !errors.Is(err, sm.ErrInvalidInput) {
		t.Fatalf("PTY accepted exact provider-ack: %v", err)
	}
	musePTYNoInput(t, h, home)
	input.Expected.Kind = "run-observed-after-snapshot"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wait, err := h.ExecuteInput(ctx, input, receipt)
	if err != nil {
		t.Fatal(err)
	}
	o, err := wait(ctx)
	if err != nil || o.Receipt != receipt || o.Kind != "run-observed-after-snapshot" || o.EvidenceClass != sm.InputEvidenceRunObservedAfterSnapshot || o.PTYRun == nil || o.PTYRun.RunID == "" {
		t.Fatalf("actual PTY run evidence missing: %+v %v", o, err)
	}
	fold, err := h.fold()
	if err != nil || fold.SessionID != o.Correlation {
		t.Fatalf("foreign native session evidence: %+v %v", fold, err)
	}
	found := false
	for _, id := range fold.Started {
		found = found || id == o.PTYRun.RunID
	}
	if !found {
		t.Fatal("run evidence was not present in owned native fold")
	}
}
