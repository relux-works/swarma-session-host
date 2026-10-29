package sessionmanager

// Reviewer Batch I reproductions (evidence-only copy; candidate W untouched).
// Claim group: a retired/stale captured run must make no durable effect after
// Stop publishes the stopped final, after a fresh incarnation replaces the
// controller, or before attachment admission succeeds. Each test states the
// exact acknowledgement/byte invariant it checks.

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// Claim: after Stop returns the stopped final commit, the in-flight captured
// run commits nothing further (no extra ACK, no byte change, no snapshot or
// OwnerRunError overwrite of the stopped record).
func TestReviewerRunOwnerNoCommitAfterStop(t *testing.T) {
	h, c, r, id := hostCommitProbeHost(t)
	ctx := context.Background()
	entered := make(chan CapturedOwnerRun, 1)
	waiting := make(chan struct{})
	awaitReturned := make(chan struct{})
	proceed := make(chan struct{})
	outcome := make(chan error, 1)
	r.await = func(ctx context.Context, _ InputSubmission, _ InputReceipt) (InputObservation, error) {
		close(waiting)
		<-ctx.Done()
		return InputObservation{}, ctx.Err()
	}
	h.options.OwnerRuns = capturedRunFunc(func(ctx context.Context, cut CapturedOwnerRun, q ProviderRunRequest) (*ProviderRunResult, error) {
		entered <- cut
		receipt, e := cut.Input.Submit(ctx, textInput(cut.Lease, "reviewer-waiting", q.Prompt))
		if e == nil {
			_, e = cut.Input.Await(ctx, receipt)
		}
		// Hold the supplier return until Stop has fully published its final
		// commit, so any later commit is attributable to the retired run alone.
		close(awaitReturned)
		<-proceed
		return nil, e
	})
	a := ownerRunAttachment(t, h, id)
	go func() {
		_, e := h.RunOwner(ctx, id, a, ProviderRunRequest{Prompt: "wait"})
		outcome <- e
	}()
	<-entered
	<-waiting
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	v, e := h.Stop(stopCtx, id)
	if e != nil || v.State != SessionStopped {
		t.Fatalf("Stop did not publish stopped final: %+v %v", v, e)
	}
	<-awaitReturned
	acksAfterStop := c.acks
	bytesAfterStop := append([]byte(nil), h.records.bases[id].Bytes...)
	close(proceed)
	if e = <-outcome; e == nil {
		t.Fatal("stopped run reported success")
	}
	if c.acks != acksAfterStop || !bytes.Equal(bytesAfterStop, h.records.bases[id].Bytes) {
		t.Fatalf("retired run committed after Stop final: acks %d->%d ownerRunError=%q state=%v",
			acksAfterStop, c.acks, h.sessions[id].OwnerRunError, h.sessions[id].State)
	}
}

// Claim: when a fresh incarnation (running record + new runner) replaces the
// captured controller before the old run returns, the old run's deferred
// error-reason persistence must not stamp or commit onto the new incarnation.
func TestReviewerRunOwnerDeferredReasonFencedToIncarnation(t *testing.T) {
	h, c, r, id := hostCommitProbeHost(t)
	ctx := context.Background()
	entered := make(chan CapturedOwnerRun, 1)
	waiting := make(chan struct{})
	awaitReturned := make(chan struct{})
	proceed := make(chan struct{})
	outcome := make(chan error, 1)
	r.await = func(ctx context.Context, _ InputSubmission, _ InputReceipt) (InputObservation, error) {
		close(waiting)
		<-ctx.Done()
		return InputObservation{}, ctx.Err()
	}
	h.options.OwnerRuns = capturedRunFunc(func(ctx context.Context, cut CapturedOwnerRun, q ProviderRunRequest) (*ProviderRunResult, error) {
		entered <- cut
		receipt, e := cut.Input.Submit(ctx, textInput(cut.Lease, "reviewer-stale", q.Prompt))
		if e == nil {
			_, e = cut.Input.Await(ctx, receipt)
		}
		close(awaitReturned)
		<-proceed
		return nil, e
	})
	a := ownerRunAttachment(t, h, id)
	go func() {
		_, e := h.RunOwner(ctx, id, a, ProviderRunRequest{Prompt: "wait"})
		outcome <- e
	}()
	<-entered
	<-waiting
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, e := h.Stop(stopCtx, id); e != nil {
		t.Fatalf("Stop failed: %v", e)
	}
	<-awaitReturned
	// Fresh incarnation under the same session id: running record, new runner,
	// empty run error (in-package surgery mirroring producer fixture style).
	h.mu.Lock()
	rec := h.sessions[id]
	rec.State = SessionRunning
	rec.GracefulRetirement = false
	rec.OwnerRunError = ""
	h.sessions[id] = rec
	h.runners[id] = &hostCommitProbeRunner{inputTestBackend: &inputTestBackend{}, snapshot: r.snapshot}
	h.mu.Unlock()
	acksBefore := c.acks
	close(proceed)
	if e := <-outcome; e == nil {
		t.Fatal("stale run reported success")
	}
	if h.sessions[id].OwnerRunError != "" || c.acks != acksBefore {
		t.Fatalf("stale run stamped new incarnation: ownerRunError=%q acks %d->%d",
			h.sessions[id].OwnerRunError, acksBefore, c.acks)
	}
}

// Claim: a run refused before attachment admission (bogus attachment) or on an
// empty prompt performs no durable write: no ACK, no byte change, no reason
// stamp, and never reaches the renderer.
func TestReviewerRunOwnerFailedAdmissionWritesNothing(t *testing.T) {
	for _, mode := range []string{"bogus-attachment", "empty-prompt"} {
		t.Run(mode, func(t *testing.T) {
			h, c, _, id := hostCommitProbeHost(t)
			ctx := context.Background()
			calls := 0
			h.options.OwnerRuns = capturedRunFunc(func(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error) {
				calls++
				return &ProviderRunResult{Status: ProviderRunComplete}, nil
			})
			a := ownerRunAttachment(t, h, id)
			q := ProviderRunRequest{Prompt: "hello"}
			if mode == "bogus-attachment" {
				a = "ATT-bogus-not-issued"
			} else {
				q.Prompt = " "
			}
			before := c.acks
			raw := append([]byte(nil), h.records.bases[id].Bytes...)
			prevErr := h.sessions[id].OwnerRunError
			if _, e := h.RunOwner(ctx, id, a, q); e == nil {
				t.Fatal("refused run reported success")
			}
			if calls != 0 || c.acks != before || !bytes.Equal(raw, h.records.bases[id].Bytes) || h.sessions[id].OwnerRunError != prevErr {
				t.Fatalf("pre-admission refusal wrote durable state: calls=%d acks %d->%d ownerRunError %q->%q",
					calls, before, c.acks, prevErr, h.sessions[id].OwnerRunError)
			}
		})
	}
}
