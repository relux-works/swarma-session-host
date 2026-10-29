package musepty

import (
	"context"
	"strings"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// PTY goal instructions are ordinary rendered Text. The native /goal command
// union and protocol turn commands belong to other backends.
func (*musePTYHost) SupportsNativeGoals() bool { return false }

func (*musePTYHost) ObserveNativeGoal(context.Context) (sm.NativeGoalObservation, error) {
	return sm.NativeGoalObservation{}, sm.ErrHostCapabilityUnavailable
}

func (h *musePTYHost) AcceptsInput(in sm.InputSubmission) bool {
	p := in.Payload
	if in.ID == "" || in.Lease.SessionID == "" || in.Lease.Epoch == 0 || p.Text == nil || p.MuseGoal != nil || p.MuseTurn != nil || p.CodexTurn != nil || p.ClaudeGoal != nil {
		return false
	}
	if strings.TrimSpace(*p.Text) == "" || strings.ContainsAny(*p.Text, "\x1b\x00\r") {
		return false
	}
	return in.Expected.Kind == "written" || in.Expected.Kind == "run-observed-after-snapshot" && in.Expected.Correlation != "" && in.Expected.Correlation == h.NativeIdentity()
}

// ExecuteInput returns only after the last possible physical write. The worker
// retains the existing terminal serialization while observing a new run-start;
// its post-Enter phase only reads the owned log, with the original 30s bound.
// The returned waiter never writes and may be called again without resubmission.
func (h *musePTYHost) ExecuteInput(ctx context.Context, in sm.InputSubmission, receipt sm.InputReceipt) (func(context.Context) (sm.InputObservation, error), error) {
	if !h.AcceptsInput(in) {
		return nil, sm.ErrInvalidInput
	}
	if receipt.ID != in.ID || receipt.SessionID != in.Lease.SessionID || receipt.Epoch != in.Lease.Epoch {
		return nil, sm.ErrInputAcknowledgementMismatch
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// No caller-owned pointers survive admission into the observer worker.
	text, expected := *in.Payload.Text, in.Expected
	physical, done := make(chan struct{}), make(chan struct{})
	var sent bool
	var observation sm.InputObservation
	var resultErr error
	go func() {
		runID, err := h.deliverPhased(ctx, text, func() {
			sent = true
			close(physical)
		}, func(nativeSession string) {
			observation = sm.InputObservation{Receipt: receipt, Kind: sm.InputObservationKind(expected.Kind), Correlation: nativeSession}
		})
		if err == nil && expected.Kind == "run-observed-after-snapshot" {
			observation.EvidenceClass = sm.InputEvidenceRunObservedAfterSnapshot
			observation.PTYRun = &sm.PTYRunObservation{RunID: runID}
		}
		if err == nil && expected.Kind == "written" {
			// Written mode makes no provider-correlation claim. Preserve its
			// caller's opaque value after the actual native delivery settles.
			observation.Correlation = expected.Correlation
		}
		resultErr = err
		close(done)
		if !sent {
			close(physical)
		}
	}()
	<-physical
	if !sent {
		<-done
		return nil, resultErr
	}
	return func(wait context.Context) (sm.InputObservation, error) {
		if err := wait.Err(); err != nil {
			return sm.InputObservation{}, err
		}
		select {
		case <-wait.Done():
			return sm.InputObservation{}, wait.Err()
		case <-done:
			if err := wait.Err(); err != nil {
				return sm.InputObservation{}, err
			}
			return observation, resultErr
		}
	}, nil
}
