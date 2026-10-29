package sessionmanager

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// This transport double exposes only the host's private backend capability.
// These tests establish admission/fence/receipt behavior, not actual PTY/MSP
// acknowledgement parsing or durability across a manager restart.
type inputTestBackend struct {
	mu      sync.Mutex
	native  bool
	state   NativeGoalObservation
	writes  []InputSubmission
	reads   int
	execute func(context.Context, InputSubmission, InputReceipt) (InputObservation, error)
	await   func(context.Context, InputSubmission, InputReceipt) (InputObservation, error)
}

func (b *inputTestBackend) SupportsNativeGoals() bool         { return b.native }
func (b *inputTestBackend) AcceptsInput(InputSubmission) bool { return true }
func (b *inputTestBackend) ObserveNativeGoal(context.Context) (NativeGoalObservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	return b.state, nil
}
func (b *inputTestBackend) ExecuteInput(ctx context.Context, input InputSubmission, receipt InputReceipt) (func(context.Context) (InputObservation, error), error) {
	b.mu.Lock()
	b.writes = append(b.writes, copyInputSubmission(input))
	b.mu.Unlock()
	if b.execute != nil {
		observation, err := b.execute(ctx, input, receipt)
		return observedInput(observation), err
	}
	if await := b.await; await != nil {
		return func(ctx context.Context) (InputObservation, error) { return await(ctx, input, receipt) }, nil
	}
	return observedInput(InputObservation{Receipt: receipt, Kind: InputObservationKind(input.Expected.Kind), Correlation: input.Expected.Correlation}), nil
}
func (b *inputTestBackend) writeCount() int { b.mu.Lock(); defer b.mu.Unlock(); return len(b.writes) }
func (b *inputTestBackend) changeGoal(state NativeGoalObservation) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = state
}

func inputFixture(t *testing.T, native bool) (context.Context, *sessionInput, ControllerLease, *inputTestBackend) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	backend := &inputTestBackend{native: native, state: NativeGoalObservation{Objective: "native objective", Status: "active", Sequence: 7}}
	host, err := newSessionInput(ctx, "session-input-test", 1, backend)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := host.CurrentLease(ctx, "session-input-test")
	if err != nil {
		t.Fatal(err)
	}
	return ctx, host, lease, backend
}
func coreTextInput(lease ControllerLease, id, text string) InputSubmission {
	return InputSubmission{Lease: lease, ID: id, Payload: RenderedInput{Text: &text}, Expected: AckExpectation{Kind: "written"}}
}
func nativeInput(lease ControllerLease, id string, command *MuseGoalCommand) InputSubmission {
	return InputSubmission{Lease: lease, ID: id, Payload: RenderedInput{MuseGoal: command}, Expected: AckExpectation{Kind: "provider-ack", Correlation: command.CommandID}}
}
func inputValue[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}
func inputPendingCount(h *sessionInput) int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.entries) }

// A stale native observation request is rejected before even a provider read;
// the live lease beside it reads and submits a matching pause successfully.
func TestHostInputNativeStaleEpochDoesNotReadOrWrite(t *testing.T) {
	ctx, h, lease, b := inputFixture(t, true)
	stale := lease
	stale.Epoch++
	called := false
	if err := h.WithNativeGoal(ctx, stale, func(NativeGoalObservation, HostInputWriter) error {
		called = true
		return nil
	}); !errors.Is(err, ErrStaleInputController) {
		t.Fatalf("stale observation = %v", err)
	}
	if called || b.reads != 0 || b.writeCount() != 0 || inputPendingCount(h) != 0 {
		t.Fatal("stale observation touched the provider or created pending state")
	}
	if err := h.WithNativeGoal(ctx, lease, func(o NativeGoalObservation, w HostInputWriter) error {
		r, err := w.Submit(ctx, nativeInput(lease, "valid", &MuseGoalCommand{CommandID: "valid-command", Pause: &MuseGoalPause{Sequence: o.Sequence}}))
		if err != nil {
			return err
		}
		_, err = w.Await(ctx, r)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if b.reads != 2 || b.writeCount() != 1 {
		t.Fatalf("valid observation/pause reads=%d writes=%d", b.reads, b.writeCount())
	}
}

// Required metadata, backend compatibility and epoch checks happen before an
// input entry or provider write. Each refusal is paired with a valid text write.
func TestHostInputRefusalsHaveNoWritesOrPendingState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*InputSubmission)
		want   error
	}{
		{"empty_id", func(i *InputSubmission) { i.ID = "" }, ErrInvalidInput},
		{"empty_session", func(i *InputSubmission) { i.Lease.SessionID = "" }, ErrStaleInputController},
		{"zero_epoch", func(i *InputSubmission) { i.Lease.Epoch = 0 }, ErrStaleInputController},
		{"wrong_epoch", func(i *InputSubmission) { i.Lease.Epoch++ }, ErrStaleInputController},
		{"no_payload", func(i *InputSubmission) { i.Payload = RenderedInput{} }, ErrInvalidInput},
		{"ambiguous_payload", func(i *InputSubmission) {
			i.Payload.MuseGoal = &MuseGoalCommand{CommandID: "x", Clear: &MuseGoalClear{}}
		}, ErrInvalidInput},
		{"wrong_ack_kind", func(i *InputSubmission) { i.Expected.Kind = "caller-asserted" }, ErrInvalidInput},
		{"ack_without_correlation", func(i *InputSubmission) { i.Expected.Kind = "provider-ack" }, ErrInvalidInput},
		{"backend_mismatch", func(i *InputSubmission) {
			*i = nativeInput(i.Lease, i.ID, &MuseGoalCommand{CommandID: "x", Set: &MuseGoalSet{Objective: "new"}})
		}, ErrInvalidInput},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, h, lease, b := inputFixture(t, false)
			bad := coreTextInput(lease, "bad", "input")
			tc.change(&bad)
			if _, err := h.Submit(ctx, bad); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if b.writeCount() != 0 || inputPendingCount(h) != 0 {
				t.Fatal("refusal wrote or installed pending state")
			}
			receipt, err := h.Submit(ctx, coreTextInput(lease, "valid", "ordinary input"))
			if err != nil {
				t.Fatal(err)
			}
			observed, err := h.Await(ctx, receipt)
			if err != nil || observed.Kind != "written" || observed.ObservedAt.IsZero() {
				t.Fatalf("valid control = %#v, %v", observed, err)
			}
			if b.writeCount() != 1 {
				t.Fatal("valid control did not write exactly once")
			}
		})
	}
}

// Retrying identical input joins its in-flight receipt. Changing its content is
// refused, and changing caller-owned pointers cannot alter the admitted copy.
func TestHostInputRetryJoinsInFlightAndOwnsPayload(t *testing.T) {
	ctx, h, lease, b := inputFixture(t, false)
	started := make(chan InputSubmission, 1)
	release := make(chan struct{})
	b.execute = func(ctx context.Context, input InputSubmission, r InputReceipt) (InputObservation, error) {
		started <- input
		select {
		case <-release:
		case <-ctx.Done():
			return InputObservation{}, ctx.Err()
		}
		return InputObservation{Receipt: r, Kind: "written"}, nil
	}
	input := coreTextInput(lease, "same", "original")
	type result struct {
		receipt InputReceipt
		err     error
	}
	first := make(chan result, 1)
	go func() { r, e := h.Submit(ctx, input); first <- result{r, e} }()
	admitted := inputValue(t, ctx, started)
	*input.Payload.Text = "caller changed"
	retry := coreTextInput(lease, "same", "original")
	receipt, err := h.Submit(ctx, retry)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ID != "same" || b.writeCount() != 1 {
		t.Fatal("retry did not join in-flight input")
	}
	if *admitted.Payload.Text != "original" {
		t.Fatal("backend retained caller-owned text")
	}
	if _, err := h.Submit(ctx, input); !errors.Is(err, ErrInputIDConflict) {
		t.Fatalf("changed retry = %v", err)
	}
	close(release)
	original := inputValue(t, ctx, first)
	if original.err != nil || original.receipt != receipt {
		t.Fatalf("first submit = %#v", original)
	}
	if _, err := h.Await(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if b.writeCount() != 1 {
		t.Fatal("retry caused duplicate provider writes")
	}
}

// A tool-shell caller receives its receipt without waiting for the provider's
// acknowledgement, and ending that request does not cancel the host waiter.
// A timed-out Await yields no ack; the later real match satisfies that receipt.
func TestHostInputDeferredReceiptOutlivesTheSubmittingRequest(t *testing.T) {
	ctx, h, lease, b := inputFixture(t, false)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	b.await = func(ctx context.Context, i InputSubmission, r InputReceipt) (InputObservation, error) {
		started <- struct{}{}
		select {
		case <-release:
			return InputObservation{Receipt: r, Kind: "provider-ack", Correlation: i.Expected.Correlation}, nil
		case <-ctx.Done():
			return InputObservation{}, ctx.Err()
		}
	}
	requestCtx, endRequest := context.WithCancel(ctx)
	defer endRequest()
	i := coreTextInput(lease, "self-tool-shell", "/goal prepared directive")
	i.Expected = AckExpectation{Kind: "provider-ack", Correlation: "exact-revision-marker"}
	r, err := h.Submit(requestCtx, i)
	if err != nil {
		t.Fatal(err)
	}
	inputValue(t, ctx, started)
	endRequest()
	waitCtx, cancelWait := context.WithCancel(ctx)
	cancelWait()
	if o, err := h.Await(waitCtx, r); !errors.Is(err, context.Canceled) || o.Kind == "provider-ack" {
		t.Fatalf("unobserved deferred receipt = %#v, %v", o, err)
	}
	// The provider write has completed. A notice must not wait for the goal
	// scanner while the originating tool shell is still returning.
	terminalWrites := 0
	noticeCtx, cancelNotice := context.WithTimeout(ctx, 60*time.Second)
	defer cancelNotice()
	if _, err := h.writeTerminal(noticeCtx, lease, []byte("notice"), func(p []byte) (int, error) { terminalWrites++; return len(p), nil }); err != nil || terminalWrites != 1 {
		t.Fatalf("deferred wait retained the write fence: %v", err)
	}
	close(release)
	o, err := h.Await(ctx, r)
	if err != nil || o.Kind != "provider-ack" || o.Correlation != i.Expected.Correlation || b.writeCount() != 1 {
		t.Fatalf("later exact ack = %#v, %v; writes %d", o, err, b.writeCount())
	}
}

// Wrong receipt/kind/correlation cannot become a successful typed ack. The
// nearby valid native command establishes that the gate does admit real matches.
func TestHostInputWrongAcknowledgementCannotSatisfyAwait(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*InputObservation)
	}{
		{"wrong_receipt", func(o *InputObservation) { o.Receipt.Epoch++ }},
		{"wrong_correlation", func(o *InputObservation) { o.Correlation = "another-command" }},
		{"write_is_not_ack", func(o *InputObservation) { o.Kind = "written" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, h, lease, b := inputFixture(t, true)
			b.execute = func(_ context.Context, i InputSubmission, r InputReceipt) (InputObservation, error) {
				o := InputObservation{Receipt: r, Kind: "provider-ack", Correlation: i.Expected.Correlation}
				tc.change(&o)
				return o, nil
			}
			r, err := h.Submit(ctx, nativeInput(lease, "bad", &MuseGoalCommand{CommandID: "bad-command", Set: &MuseGoalSet{Objective: "new"}}))
			if err != nil {
				t.Fatal(err)
			}
			o, err := h.Await(ctx, r)
			if !errors.Is(err, ErrInputAcknowledgementMismatch) || o.Kind == "provider-ack" {
				t.Fatalf("wrong ack admitted: %#v, %v", o, err)
			}
			b.execute = nil
			r, err = h.Submit(ctx, nativeInput(lease, "good", &MuseGoalCommand{CommandID: "good-command", Set: &MuseGoalSet{Objective: "new"}}))
			if err != nil {
				t.Fatal(err)
			}
			o, err = h.Await(ctx, r)
			if err != nil || o.Kind != "provider-ack" || o.Correlation != "good-command" {
				t.Fatalf("valid ack = %#v, %v", o, err)
			}
		})
	}
}

// A cancel removes acknowledgement authority, and an uncertain write retains
// the input identity so an identical retry cannot issue a second provider write.
func TestHostInputCancellationAndUncertainWritesRetainIdentity(t *testing.T) {
	t.Run("uncertain_write", func(t *testing.T) {
		ctx, h, lease, b := inputFixture(t, false)
		uncertain := errors.New("transport outcome uncertain")
		b.execute = func(context.Context, InputSubmission, InputReceipt) (InputObservation, error) {
			return InputObservation{}, uncertain
		}
		input := coreTextInput(lease, "uncertain", "input")
		r, err := h.Submit(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		if o, err := h.Await(ctx, r); !errors.Is(err, uncertain) || o.Kind == "written" || o.Kind == "provider-ack" {
			t.Fatalf("uncertain write = %#v, %v", o, err)
		}
		again, err := h.Submit(ctx, input)
		if err != nil || again != r || b.writeCount() != 1 {
			t.Fatalf("uncertain retry = %#v, %v; writes %d", again, err, b.writeCount())
		}
	})
	t.Run("cancelled_wait", func(t *testing.T) {
		ctx, h, lease, b := inputFixture(t, false)
		started := make(chan struct{}, 1)
		b.execute = func(ctx context.Context, _ InputSubmission, _ InputReceipt) (InputObservation, error) {
			started <- struct{}{}
			<-ctx.Done()
			return InputObservation{}, ctx.Err()
		}
		input := coreTextInput(lease, "cancel", "input")
		done := make(chan error, 1)
		go func() { _, err := h.Submit(ctx, input); done <- err }()
		inputValue(t, ctx, started)
		receipt, err := h.Submit(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Cancel(ctx, receipt); err != nil {
			t.Fatal(err)
		}
		if err := inputValue(t, ctx, done); err != nil {
			t.Fatal(err)
		}
		if o, err := h.Await(ctx, receipt); !errors.Is(err, context.Canceled) || o.Kind != "cancelled" {
			t.Fatalf("cancelled wait = %#v, %v", o, err)
		}
		if b.writeCount() != 1 {
			t.Fatal("cancel caused another write")
		}
	})
}

// Observation, fork decision and pause share one fence. An ordinary input or
// raw terminal write cannot interleave; a cancelled queued write leaves no entry.
func TestHostInputNativeDecisionHoldsTheWriterFence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, h, lease, b := inputFixture(t, true)
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		scopeDone := make(chan error, 1)
		go func() {
			scopeDone <- h.WithNativeGoal(ctx, lease, func(o NativeGoalObservation, w HostInputWriter) error {
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				r, err := w.Submit(ctx, nativeInput(lease, "pause", &MuseGoalCommand{CommandID: "pause-command", Pause: &MuseGoalPause{Sequence: o.Sequence}}))
				if err != nil {
					return err
				}
				_, err = w.Await(ctx, r)
				return err
			})
		}()
		inputValue(t, ctx, entered)
		queuedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := h.Submit(queuedCtx, coreTextInput(lease, "queued", "ordinary input"))
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || b.writeCount() != 0 || inputPendingCount(h) != 0 {
			t.Fatalf("queued input escaped fence: %v; writes %d", err, b.writeCount())
		}
		terminalCtx, cancelTerminal := context.WithCancel(ctx)
		cancelTerminal()
		terminalWrites := 0
		if _, err := h.writeTerminal(terminalCtx, lease, []byte("keys"), func(p []byte) (int, error) { terminalWrites++; return len(p), nil }); !errors.Is(err, context.Canceled) || terminalWrites != 0 {
			t.Fatalf("cancelled terminal escaped fence: %v", err)
		}
		close(release)
		if err := inputValue(t, ctx, scopeDone); err != nil {
			t.Fatal(err)
		}
		if b.writeCount() != 1 {
			t.Fatalf("pause writes = %d", b.writeCount())
		}
		if _, err := h.writeTerminal(ctx, lease, []byte("keys"), func(p []byte) (int, error) { terminalWrites++; return len(p), nil }); err != nil || terminalWrites != 1 {
			t.Fatalf("terminal positive = %v, %d", err, terminalWrites)
		}
	})
}

// Pause requires the host's live scoped observation. Changed sequence/state and
// caller-substituted epochs refuse before a write or pending receipt is created.
func TestHostInputPauseRequiresTheBoundUnchangedObservation(t *testing.T) {
	for _, mode := range []string{"no_scope", "changed_sequence", "changed_state", "caller_epoch"} {
		t.Run(mode, func(t *testing.T) {
			ctx, h, lease, b := inputFixture(t, true)
			command := &MuseGoalCommand{CommandID: "pause-command", Pause: &MuseGoalPause{Sequence: 7}}
			if mode == "no_scope" {
				if _, err := h.Submit(ctx, nativeInput(lease, "pause", command)); !errors.Is(err, ErrNativeGoalObservationUnavailable) {
					t.Fatalf("unscoped pause = %v", err)
				}
			} else {
				err := h.WithNativeGoal(ctx, lease, func(o NativeGoalObservation, w HostInputWriter) error {
					switch mode {
					case "changed_sequence":
						b.changeGoal(NativeGoalObservation{Objective: o.Objective, Status: o.Status, Sequence: o.Sequence + 1})
					case "changed_state":
						b.changeGoal(NativeGoalObservation{Objective: "another objective", Status: o.Status, Sequence: o.Sequence})
					case "caller_epoch":
						lease.Epoch++
					}
					_, err := w.Submit(ctx, nativeInput(lease, "pause", command))
					return err
				})
				want := ErrNativeGoalChanged
				if mode == "caller_epoch" {
					want = ErrStaleInputController
				}
				if !errors.Is(err, want) {
					t.Fatalf("got %v, want %v", err, want)
				}
			}
			if b.writeCount() != 0 || inputPendingCount(h) != 0 {
				t.Fatal("refused pause wrote or created pending state")
			}
		})
	}
	ctx, h, lease, b := inputFixture(t, true)
	var retained HostInputWriter
	if err := h.WithNativeGoal(ctx, lease, func(o NativeGoalObservation, w HostInputWriter) error {
		retained = w
		if o.SessionID != lease.SessionID || o.Epoch != lease.Epoch || o.ObservedAt.IsZero() {
			t.Fatal("observation was not host bound")
		}
		r, err := w.Submit(ctx, nativeInput(lease, "pause", &MuseGoalCommand{CommandID: "pause-command", Pause: &MuseGoalPause{Sequence: o.Sequence}}))
		if err != nil {
			return err
		}
		_, err = w.Await(ctx, r)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Submit(ctx, coreTextInput(lease, "escaped", "text")); !errors.Is(err, ErrInputScopeClosed) {
		t.Fatalf("escaped writer = %v", err)
	}
	if b.writeCount() != 1 {
		t.Fatal("escaped writer reached provider")
	}
}

// Controller transfer invalidates old work already queued behind a write and
// its receipt; only a fresh host-issued lease can write after the transfer.
func TestHostInputControllerTransferRefusesQueuedOldEpoch(t *testing.T) {
	ctx, h, lease, b := inputFixture(t, false)
	started := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	drained := make(chan struct{})
	var drainOnce sync.Once
	drain := func() { drainOnce.Do(func() { close(drained) }) }
	defer drain()
	b.execute = func(ctx context.Context, i InputSubmission, r InputReceipt) (InputObservation, error) {
		if i.ID == "first" {
			started <- struct{}{}
			<-ctx.Done()
			cancelled <- struct{}{}
			<-drained
			return InputObservation{}, ctx.Err()
		}
		return InputObservation{Receipt: r, Kind: "written"}, nil
	}
	first := coreTextInput(lease, "first", "input")
	firstDone := make(chan error, 1)
	go func() { _, err := h.Submit(ctx, first); firstDone <- err }()
	inputValue(t, ctx, started)
	oldReceipt, err := h.Submit(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	queuedDone := make(chan error, 1)
	go func() { _, err := h.Submit(ctx, coreTextInput(lease, "queued-old", "old input")); queuedDone <- err }()
	transferDone := make(chan error, 1)
	go func() { transferDone <- h.advanceEpoch(ctx, lease.Epoch+1) }()
	inputValue(t, ctx, cancelled)
	drain()
	if err := inputValue(t, ctx, firstDone); err != nil {
		t.Fatal(err)
	}
	if err := inputValue(t, ctx, queuedDone); !errors.Is(err, ErrStaleInputController) {
		t.Fatalf("old queued input = %v", err)
	}
	if err := inputValue(t, ctx, transferDone); err != nil {
		t.Fatal(err)
	}
	if o, err := h.Await(ctx, oldReceipt); !errors.Is(err, ErrStaleInputController) || o.Kind != "stale-epoch" {
		t.Fatalf("old receipt = %#v, %v", o, err)
	}
	if b.writeCount() != 1 || inputPendingCount(h) != 1 {
		t.Fatal("old queued input created state or wrote")
	}
	fresh, err := h.CurrentLease(ctx, lease.SessionID)
	if err != nil || fresh.Epoch != lease.Epoch+1 {
		t.Fatalf("new lease = %#v, %v", fresh, err)
	}
	r, err := h.Submit(ctx, coreTextInput(fresh, "fresh", "new input"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Await(ctx, r); err != nil {
		t.Fatal(err)
	}
	if b.writeCount() != 2 {
		t.Fatal("new controller did not write")
	}
}
