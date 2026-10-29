package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func metadataProbe(t *testing.T) (*SessionHost, *hostCommitProbeCodec, *hostCommitProbeRunner, RecordMetadataCommitter, RecordMetadataCommit) {
	t.Helper()
	h, c, r, id := hostCommitProbeHost(t)
	port, ok := any(h).(RecordMetadataCommitter)
	if !ok {
		t.Fatal("neutral host lacks record metadata transaction")
	}
	lease, _, err := h.Input(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return h, c, r, port, RecordMetadataCommit{Lease: lease, ID: "metadata-1", Owner: h.sessions[id].Owner, Provider: h.sessions[id].Provider, BaseSHA256: h.records.bases[id].SHA256}
}
func metadataAssertNoWrite(t *testing.T, h *SessionHost, c *hostCommitProbeCodec, before []byte, acks int, request RecordMetadataCommit, port RecordMetadataCommitter, want error) {
	t.Helper()
	receipt, err := port.CommitRecordMetadata(context.Background(), request)
	after, _ := stopProbeRead(t, h, requestSessionID(h))
	if !errors.Is(err, want) || receipt != (RecordMetadataReceipt{}) || !bytes.Equal(before, after) || c.acks != acks {
		t.Fatalf("metadata refusal changed committed facts: receipt=%+v err=%v want=%v bytes=%v acks=%d/%d", receipt, err, want, bytes.Equal(before, after), c.acks, acks)
	}
}
func requestSessionID(h *SessionHost) string {
	for id := range h.sessions {
		return id
	}
	return ""
}
func metadataNext(t *testing.T, h *SessionHost, port RecordMetadataCommitter, request RecordMetadataCommit, patch RecordMetadataPatch) RecordMetadataReceipt {
	t.Helper()
	request.Patch = patch
	receipt, err := port.CommitRecordMetadata(context.Background(), request)
	if err != nil {
		t.Fatalf("valid metadata refused: %+v: %v", patch, err)
	}
	if receipt.Lease != request.Lease || receipt.ID != request.ID || receipt.Owner != request.Owner || receipt.Provider != request.Provider || receipt.BaseSHA256 != request.BaseSHA256 || receipt.CommittedSHA256 != h.records.bases[request.Lease.SessionID].SHA256 {
		t.Fatalf("metadata receipt lost exact invocation: %+v", receipt)
	}
	return receipt
}
func metadataGroup(name string, at time.Time) RecordMetadataPatch {
	switch name {
	case "applied":
		return RecordMetadataPatch{AppliedGoalRef: &AppliedGoalMetadata{Value: &RevisionRef{ID: "immutable-goal", Revision: 3}}}
	case "conditional":
		return RecordMetadataPatch{ConditionalStop: &ConditionalStopMetadata{ExpectedPhase: ConditionalStopPhaseNone, NextPhase: ConditionalStopPhaseIntent}}
	case "owner-error":
		return RecordMetadataPatch{OwnerRunError: &OwnerRunMetadata{Value: "actual owner diagnostic"}}
	case "materialization":
		return RecordMetadataPatch{Materialization: &MaterializationMetadata{CtxID: "CTX-261024-abcdef", ProviderHostPendingFork: true}}
	case "eviction":
		return RecordMetadataPatch{Eviction: &EvictionMetadata{Value: &SessionEvictionNotice{ByProvider: "codex", At: at, Reason: "observed takeover"}}}
	}
	panic(name)
}

// All five groups use the actual sole writer and private acknowledgement. The
// neutral JSON codec is a transport double; original legacy bytes and native
// outcome authority require separate fork integration. Unpatched/native facts,
// timestamps and caller-owned references are preserved, including explicit clears.
func TestHostMetadataFiveGroupsPreserveNativeAndUnpatched(t *testing.T) {
	for _, name := range []string{"applied", "conditional", "owner-error", "materialization", "eviction"} {
		t.Run(name, func(t *testing.T) {
			h, c, r, port, request := metadataProbe(t)
			id := request.Lease.SessionID
			before := copyRunnerRecord(h.sessions[id])
			states, writes, acks := r.states, r.writeCount(), c.acks
			request.Patch = metadataGroup(name, before.CreatedAt.Add(time.Second))
			metadataNext(t, h, port, request, request.Patch)
			_, actual := stopProbeRead(t, h, id)
			want := copyRunnerRecord(before)
			switch name {
			case "applied":
				ref := *request.Patch.AppliedGoalRef.Value
				want.AppliedGoalRef = &ref
			case "conditional":
				previous := copyRunnerRecord(before)
				want.ConditionalStop = &ConditionalStopRecord{Precondition: "not-busy", Previous: &previous}
				want.GracefulRetirement = true
			case "owner-error":
				want.OwnerRunError = request.Patch.OwnerRunError.Value
			case "materialization":
				want.MaterializationCtxID = request.Patch.Materialization.CtxID
				want.ProviderHostPendingFork = true
			case "eviction":
				v := *request.Patch.Eviction.Value
				want.Eviction = &v
			}
			if !reflect.DeepEqual(want, actual) || !reflect.DeepEqual(want, h.sessions[id]) || r.states != states || r.writeCount() != writes || c.acks != acks+1 {
				t.Fatalf("metadata changed unpatched or native facts: want=%+v actual=%+v", want, actual)
			}
			if request.Patch.AppliedGoalRef != nil {
				request.Patch.AppliedGoalRef.Value.ID = "caller-mutated"
				if h.sessions[id].AppliedGoalRef.ID != "immutable-goal" {
					t.Fatal("metadata retained caller revision alias")
				}
			}
			if request.Patch.Eviction != nil {
				request.Patch.Eviction.Value.Reason = "caller-mutated"
				if h.sessions[id].Eviction.Reason != "observed takeover" {
					t.Fatal("metadata retained caller eviction alias")
				}
			}
			if name == "conditional" {
				return
			}
			request.ID = "clear"
			request.BaseSHA256 = h.records.bases[id].SHA256
			clear := RecordMetadataPatch{}
			switch name {
			case "applied":
				clear.AppliedGoalRef = &AppliedGoalMetadata{}
			case "owner-error":
				clear.OwnerRunError = &OwnerRunMetadata{}
			case "materialization":
				clear.Materialization = &MaterializationMetadata{}
			case "eviction":
				clear.Eviction = &EvictionMetadata{}
			}
			metadataNext(t, h, port, request, clear)
			after := h.sessions[id]
			if after.AppliedGoalRef != nil || after.Eviction != nil || after.OwnerRunError != "" || after.MaterializationCtxID != "" || after.ProviderHostPendingFork {
				t.Fatal("metadata explicit clear did not clear its group")
			}
			request.ID = "fresh-noop"
			request.BaseSHA256 = h.records.bases[id].SHA256
			metadataNext(t, h, port, request, clear)
		})
	}
}

// Each group has a stale-base refusal paired with the valid control above.
// Independent identity, fence, payload and replay refusals leave bytes and
// private acknowledgements unchanged; cancellation also refuses before effect.
func TestHostMetadataRejectsStaleFenceBaseIdentityAndReplay(t *testing.T) {
	for _, name := range []string{"applied", "conditional", "owner-error", "materialization", "eviction"} {
		t.Run("group-"+name, func(t *testing.T) {
			h, c, _, port, q := metadataProbe(t)
			before, _ := stopProbeRead(t, h, q.Lease.SessionID)
			q.Patch = metadataGroup(name, time.Now().UTC())
			q.BaseSHA256 = strings.Repeat("0", 64)
			metadataAssertNoWrite(t, h, c, before, c.acks, q, port, ErrLegacyViewStale)
		})
	}
	cases := []struct {
		name   string
		change func(*RecordMetadataCommit)
		want   error
	}{
		{"epoch", func(q *RecordMetadataCommit) { q.Lease.Epoch++ }, ErrStaleInputController},
		{"sid", func(q *RecordMetadataCommit) { q.Lease.SessionID = "SES-foreign000000000000" }, ErrProviderHostNotFound},
		{"owner", func(q *RecordMetadataCommit) { q.Owner = "foreign-owner" }, ErrLegacyViewStale},
		{"provider", func(q *RecordMetadataCommit) { q.Provider = "codex" }, ErrLegacyViewStale},
		{"base", func(q *RecordMetadataCommit) { q.BaseSHA256 = "wrong" }, ErrLegacyViewStale},
		{"id", func(q *RecordMetadataCommit) { q.ID = " " }, ErrInvalidInput},
		{"empty", func(q *RecordMetadataCommit) { q.Patch = RecordMetadataPatch{} }, ErrInvalidInput},
		{"ref", func(q *RecordMetadataCommit) {
			q.Patch = RecordMetadataPatch{AppliedGoalRef: &AppliedGoalMetadata{Value: &RevisionRef{ID: "goal", Revision: 0}}}
		}, ErrInvalidInput},
		{"materialization", func(q *RecordMetadataCommit) {
			q.Patch = RecordMetadataPatch{Materialization: &MaterializationMetadata{ProviderHostPendingFork: true}}
		}, ErrInvalidInput},
		{"eviction-provider", func(q *RecordMetadataCommit) {
			q.Patch = RecordMetadataPatch{Eviction: &EvictionMetadata{Value: &SessionEvictionNotice{ByProvider: "bad/provider", At: time.Now().UTC()}}}
		}, ErrInvalidInput},
		{"eviction-time", func(q *RecordMetadataCommit) {
			q.Patch = RecordMetadataPatch{Eviction: &EvictionMetadata{Value: &SessionEvictionNotice{ByProvider: "codex"}}}
		}, ErrInvalidInput},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, c, _, port, q := metadataProbe(t)
			before, _ := stopProbeRead(t, h, q.Lease.SessionID)
			q.Patch = metadataGroup("owner-error", time.Now())
			tc.change(&q)
			metadataAssertNoWrite(t, h, c, before, c.acks, q, port, tc.want)
		})
	}
	t.Run("replay", func(t *testing.T) {
		h, c, _, port, q := metadataProbe(t)
		q.Patch = metadataGroup("owner-error", time.Now())
		metadataNext(t, h, port, q, q.Patch)
		q.BaseSHA256 = h.records.bases[q.Lease.SessionID].SHA256
		before, _ := stopProbeRead(t, h, q.Lease.SessionID)
		metadataAssertNoWrite(t, h, c, before, c.acks, q, port, ErrInputIDConflict)
	})
	t.Run("cancelled", func(t *testing.T) {
		h, c, _, port, q := metadataProbe(t)
		q.Patch = metadataGroup("owner-error", time.Now())
		before, _ := stopProbeRead(t, h, q.Lease.SessionID)
		acks := c.acks
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		receipt, err := port.CommitRecordMetadata(ctx, q)
		after, _ := stopProbeRead(t, h, q.Lease.SessionID)
		if !errors.Is(err, context.Canceled) || receipt != (RecordMetadataReceipt{}) || !bytes.Equal(before, after) || c.acks != acks {
			t.Fatal("cancelled metadata transaction had an effect")
		}
	})
}

// A receipt must wait for actual write completion, exact-image observation and
// the private codec acknowledgement. The cross-process controller fence stays
// held while the acknowledgement is blocked, so a second controller cannot win.
func TestHostMetadataWaitsForWriterObservationAndControllerFence(t *testing.T) {
	h, c, _, port, q := metadataProbe(t)
	q.Patch = metadataGroup("owner-error", time.Now())
	committed, observe, ack, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-observe:
		default:
			close(observe)
		}
		select {
		case <-finish:
		default:
			close(finish)
		}
	}()
	h.records.commit = func(path string, raw []byte) error {
		if err := writePrivateFile(path, raw); err != nil {
			return err
		}
		close(committed)
		<-observe
		return nil
	}
	c.ack = func(input LegacyRecordInput) error {
		path, _ := h.records.path(q.Lease.SessionID)
		raw, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(raw, input.Bytes) {
			return ErrLegacyViewStale
		}
		close(ack)
		<-finish
		return nil
	}
	type result struct {
		receipt RecordMetadataReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, err := port.CommitRecordMetadata(context.Background(), q)
		done <- result{receipt, err}
	}()
	<-committed
	select {
	case r := <-done:
		t.Fatalf("metadata receipt preceded writer observation: %+v", r)
	default:
	}
	close(observe)
	<-ack
	select {
	case r := <-done:
		t.Fatalf("metadata receipt preceded private own acknowledgement: %+v", r)
	default:
	}
	clock := &museTestAckClock{}
	bounded, cancel := clock.hook(context.Background())
	// This registered input has no work. revokeAndDrain first evaluates
	// Done in its uncontended input-fence select; that is not a file wait.
	// Only the next evaluation, in the failed physical flock retry, expires
	// this fixture deadline. The early-release production plant must still
	// reserve successfully and fail the original controller-fence assertion.
	evaluations := 0
	ctx := &nativeFixtureWaitContext{Context: bounded, onDone: func() {
		evaluations++
		if evaluations > 1 {
			clock.fireLatest(t)
		}
	}}
	_, err := h.epochs.reserveContext(ctx, q.Lease.SessionID)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("metadata write lost controller fence: %v", err)
	}
	close(finish)
	r := <-done
	if r.err != nil || r.receipt.ID != q.ID {
		t.Fatalf("observed own write refused: %+v", r)
	}
}

// Failed writes, mismatched visible bytes and failed private acknowledgements
// produce no receipt and revoke actual old input. Only committed-image recovery
// admits a higher epoch; old metadata and old issued writers stay fenced.
func TestHostMetadataFailedWriteRequiresCommittedRecovery(t *testing.T) {
	for _, mode := range []string{"before-write", "after-write", "foreign-image", "ack"} {
		t.Run(mode, func(t *testing.T) {
			h, c, r, port, q := metadataProbe(t)
			q.Patch = metadataGroup("owner-error", time.Now())
			_, writer, err := h.Input(context.Background(), q.Lease.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			fault := errors.New("metadata write witness failed")
			original := h.records.commit
			switch mode {
			case "before-write":
				h.records.commit = func(string, []byte) error { return fault }
			case "after-write":
				h.records.commit = func(path string, raw []byte) error {
					if err := original(path, raw); err != nil {
						return err
					}
					return fault
				}
			case "foreign-image":
				h.records.commit = func(path string, raw []byte) error {
					var changed RunnerRecord
					if err := json.Unmarshal(raw, &changed); err != nil {
						return err
					}
					changed.OwnerRunError = "different valid committed image"
					foreign, err := json.Marshal(changed)
					if err != nil {
						return err
					}
					return original(path, foreign)
				}
			case "ack":
				c.ack = func(LegacyRecordInput) error { return fault }
			}
			receipt, err := port.CommitRecordMetadata(context.Background(), q)
			if !errors.Is(err, ErrLegacyCommitUncertain) || receipt != (RecordMetadataReceipt{}) || !h.records.uncertain[q.Lease.SessionID] {
				t.Fatalf("failed metadata write claimed acknowledgement: %+v/%v", receipt, err)
			}
			if _, err := writer.Submit(context.Background(), textInput(q.Lease, "after-failure", "forbidden")); !errors.Is(err, ErrStaleInputController) || r.writeCount() != 0 {
				t.Fatalf("failed metadata retained issued input: %v", err)
			}
			if _, _, err := h.Input(context.Background(), q.Lease.SessionID); !errors.Is(err, ErrLegacyCommitUncertain) {
				t.Fatalf("failed metadata admitted input: %v", err)
			}
			h.records.commit = original
			c.ack = nil
			if err := h.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			lease, _, err := h.Input(context.Background(), q.Lease.SessionID)
			if err != nil || lease.Epoch <= q.Lease.Epoch {
				t.Fatalf("metadata recovery reused old epoch: %+v/%v", lease, err)
			}
			q.ID = "after-recovery"
			q.BaseSHA256 = h.records.bases[q.Lease.SessionID].SHA256
			before, _ := stopProbeRead(t, h, q.Lease.SessionID)
			metadataAssertNoWrite(t, h, c, before, c.acks, q, port, ErrStaleInputController)
			q.Lease = lease
			metadataNext(t, h, port, q, q.Patch)
		})
	}
}

// Every accepted conditional row traverses the actual writer. Before EACH
// transition stale expected-phase and same-phase requests refuse without writes.
// Live native outcome evidence is the fork's responsibility, not this codec double.
func TestHostMetadataConditionalChecksEveryTransition(t *testing.T) {
	paths := [][]ConditionalStopPhase{
		{ConditionalStopPhaseNone, ConditionalStopPhaseIntent, ConditionalStopPhaseRollback},
		{ConditionalStopPhaseNone, ConditionalStopPhaseIntent, ConditionalStopPhaseAttempt, ConditionalStopPhaseRollback},
		{ConditionalStopPhaseNone, ConditionalStopPhaseIntent, ConditionalStopPhaseAttempt, ConditionalStopPhaseRetirement},
		{ConditionalStopPhaseNone, ConditionalStopPhaseIntent, ConditionalStopPhaseAttempt, ConditionalStopPhaseSent, ConditionalStopPhaseRetirement},
	}
	for _, path := range paths {
		t.Run(string(path[len(path)-2])+"-"+string(path[len(path)-1]), func(t *testing.T) {
			h, c, _, port, q := metadataProbe(t)
			id := q.Lease.SessionID
			original := copyRunnerRecord(h.sessions[id])
			for i := 1; i < len(path); i++ {
				current, next := path[i-1], path[i]
				q.ID = string(current) + "-" + string(next)
				q.BaseSHA256 = h.records.bases[id].SHA256
				before, _ := stopProbeRead(t, h, id)
				q.Patch = RecordMetadataPatch{ConditionalStop: &ConditionalStopMetadata{ExpectedPhase: ConditionalStopPhaseRollback, NextPhase: next}}
				metadataAssertNoWrite(t, h, c, before, c.acks, q, port, ErrInvalidInput)
				q.Patch.ConditionalStop.ExpectedPhase = current
				q.Patch.ConditionalStop.NextPhase = current
				metadataAssertNoWrite(t, h, c, before, c.acks, q, port, ErrInvalidInput)
				q.Patch.ConditionalStop.NextPhase = next
				if i == 2 {
					q.Patch.OwnerRunError = &OwnerRunMetadata{Value: "current diagnostic retained"}
				}
				metadataNext(t, h, port, q, q.Patch)
				after := h.sessions[id]
				if i >= 2 && after.OwnerRunError != "current diagnostic retained" {
					t.Fatal("conditional rollback replaced current metadata with previous")
				}
				if !reflect.DeepEqual(after.ProviderHost, original.ProviderHost) || after.CreatedAt != original.CreatedAt || after.UpdatedAt != original.UpdatedAt {
					t.Fatal("conditional metadata replaced current native or timestamps")
				}
				if next == ConditionalStopPhaseRollback {
					if after.ConditionalStop != nil || after.GracefulRetirement != original.GracefulRetirement {
						t.Fatal("conditional rollback lost checked previous retirement")
					}
				} else if next == ConditionalStopPhaseRetirement {
					if after.ConditionalStop != nil || !after.GracefulRetirement {
						t.Fatal("attempted or sent recovery reopened retirement")
					}
				} else {
					if after.ConditionalStop == nil || !after.GracefulRetirement || !reflect.DeepEqual(*after.ConditionalStop.Previous, original) || after.ConditionalStop.Signalled != (next == ConditionalStopPhaseSent) {
						t.Fatal("conditional metadata lost exact previous/phase/signal")
					}
				}
			}
		})
	}
}

type metadataOutsideCodec struct {
	*hostCommitProbeCodec
	change func(*RunnerRecord)
}

func (c metadataOutsideCodec) EncodeMerge(ctx context.Context, base LegacyRecordInput, record RunnerRecord) ([]byte, error) {
	c.change(&record)
	return c.hostCommitProbeCodec.EncodeMerge(ctx, base, record)
}

// The candidate must project exactly the requested whitelist. A hostile codec
// cannot smuggle a native PID, timestamp or liveness change into a valid patch;
// refusal precedes the sole write and any private acknowledgement.
func TestHostMetadataRejectsOutsideWhitelistCandidate(t *testing.T) {
	cases := []struct {
		name   string
		change func(*RunnerRecord)
	}{
		{"native", func(r *RunnerRecord) { r.ProviderHost.Snapshot.MusePTY.PID = 9999 }},
		{"timestamp", func(r *RunnerRecord) { r.UpdatedAt = r.CreatedAt.Add(17 * time.Second) }},
		{"liveness", func(r *RunnerRecord) { r.LivenessDetail = "codec-injected" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, c, _, port, q := metadataProbe(t)
			q.Patch = metadataGroup("owner-error", time.Now())
			before, _ := stopProbeRead(t, h, q.Lease.SessionID)
			h.records.codec = metadataOutsideCodec{c, tc.change}
			metadataAssertNoWrite(t, h, c, before, c.acks, q, port, ErrLegacyCodecMismatch)
		})
	}
}

// The literal accepted table is independent of the implementation switch.
// Every actual/expected/next combination is checked, including unknown values,
// malformed previous state and invalid persisted signal/phase combinations.
func TestHostMetadataConditionalClosedTransitionMatrix(t *testing.T) {
	h, _, _, _, q := metadataProbe(t)
	base := copyRunnerRecord(h.sessions[q.Lease.SessionID])
	phases := []ConditionalStopPhase{ConditionalStopPhaseNone, ConditionalStopPhaseIntent, ConditionalStopPhaseAttempt, ConditionalStopPhaseSent, ConditionalStopPhaseRollback, ConditionalStopPhaseRetirement, "foreign"}
	allowed := map[[2]ConditionalStopPhase]bool{
		{ConditionalStopPhaseNone, ConditionalStopPhaseIntent}:        true,
		{ConditionalStopPhaseIntent, ConditionalStopPhaseAttempt}:     true,
		{ConditionalStopPhaseIntent, ConditionalStopPhaseRollback}:    true,
		{ConditionalStopPhaseAttempt, ConditionalStopPhaseSent}:       true,
		{ConditionalStopPhaseAttempt, ConditionalStopPhaseRollback}:   true,
		{ConditionalStopPhaseAttempt, ConditionalStopPhaseRetirement}: true,
		{ConditionalStopPhaseSent, ConditionalStopPhaseRetirement}:    true,
	}
	for _, actual := range phases {
		current := copyRunnerRecord(base)
		switch actual {
		case ConditionalStopPhaseNone:
		case ConditionalStopPhaseRetirement:
			current.GracefulRetirement = true
		case ConditionalStopPhaseIntent, ConditionalStopPhaseAttempt, ConditionalStopPhaseSent:
			previous := copyRunnerRecord(base)
			current.GracefulRetirement = true
			current.ConditionalStop = &ConditionalStopRecord{Precondition: "not-busy", Previous: &previous}
			if actual != ConditionalStopPhaseIntent {
				current.ConditionalStop.Phase = "signal-attempt"
			}
			current.ConditionalStop.Signalled = actual == ConditionalStopPhaseSent
		default:
			previous := copyRunnerRecord(base)
			current.GracefulRetirement = true
			current.ConditionalStop = &ConditionalStopRecord{Precondition: "not-busy", Phase: string(actual), Previous: &previous}
		}
		for _, expected := range phases {
			for _, next := range phases {
				t.Run(string(actual)+"/"+string(expected)+"/"+string(next), func(t *testing.T) {
					output := copyRunnerRecord(current)
					unchanged := copyRunnerRecord(output)
					err := applyConditionalMetadata(current, &output, ConditionalStopMetadata{ExpectedPhase: expected, NextPhase: next})
					want := expected == actual && allowed[[2]ConditionalStopPhase{actual, next}]
					if (err == nil) != want {
						t.Fatalf("conditional closed table widened or narrowed: actual=%s expected=%s next=%s want=%v err=%v", actual, expected, next, want, err)
					}
					if !want && (!errors.Is(err, ErrInvalidInput) || !reflect.DeepEqual(output, unchanged)) {
						t.Fatal("conditional invalid transition mutated candidate")
					}
				})
			}
		}
	}
	for _, name := range []string{"missing-previous", "foreign-previous", "retiring-previous", "nested-previous", "signal-without-attempt", "missing-retirement"} {
		t.Run(name, func(t *testing.T) {
			previous := copyRunnerRecord(base)
			current := copyRunnerRecord(base)
			current.GracefulRetirement = true
			current.ConditionalStop = &ConditionalStopRecord{Precondition: "not-busy", Previous: &previous}
			switch name {
			case "missing-previous":
				current.ConditionalStop.Previous = nil
			case "foreign-previous":
				previous.Owner = "foreign"
			case "retiring-previous":
				previous.GracefulRetirement = true
			case "nested-previous":
				previous.ConditionalStop = &ConditionalStopRecord{}
			case "signal-without-attempt":
				current.ConditionalStop.Signalled = true
			case "missing-retirement":
				current.GracefulRetirement = false
			}
			output := current // Deliberately malformed input must reach validation before cloning.
			if err := applyConditionalMetadata(current, &output, ConditionalStopMetadata{ExpectedPhase: ConditionalStopPhaseIntent, NextPhase: ConditionalStopPhaseAttempt}); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("malformed conditional recovery state accepted: %v", err)
			}
		})
	}
}

// A matching in-memory base hash is insufficient if the actual file changed.
// The refusal must preserve those external bytes and avoid private Ack. An
// extensible normalized eviction provider remains a valid positive control.
func TestHostMetadataChecksActualDiskBaseAndExtensibleProvider(t *testing.T) {
	t.Run("actual-disk", func(t *testing.T) {
		h, c, _, port, q := metadataProbe(t)
		q.Patch = metadataGroup("owner-error", time.Now())
		raw, _ := stopProbeRead(t, h, q.Lease.SessionID)
		foreign := append(raw, ' ')
		path, _ := h.records.path(q.Lease.SessionID)
		if err := os.WriteFile(path, foreign, 0600); err != nil {
			t.Fatal(err)
		}
		metadataAssertNoWrite(t, h, c, foreign, c.acks, q, port, ErrLegacyViewStale)
	})
	t.Run("extensible-provider", func(t *testing.T) {
		h, _, _, port, q := metadataProbe(t)
		q.Patch = RecordMetadataPatch{Eviction: &EvictionMetadata{Value: &SessionEvictionNotice{ByProvider: "new-backend", At: time.Now().UTC()}}}
		metadataNext(t, h, port, q, q.Patch)
	})
}

// Non-conditional metadata cannot bypass a pending or committed retirement.
// The same diagnostic patch succeeds before intent, and a mixed conditional
// transition is covered separately; rejecting every metadata patch is not valid.
func TestHostMetadataRejectsNonConditionalRetirementBypass(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "intent", true: "retirement"}[terminal], func(t *testing.T) {
			h, c, _, port, q := metadataProbe(t)
			id := q.Lease.SessionID
			metadataNext(t, h, port, q, metadataGroup("owner-error", time.Now()))
			phases := []ConditionalStopPhase{ConditionalStopPhaseNone, ConditionalStopPhaseIntent}
			if terminal {
				phases = append(phases, ConditionalStopPhaseAttempt, ConditionalStopPhaseRetirement)
			}
			for i := 1; i < len(phases); i++ {
				q.ID = string(phases[i-1]) + "-" + string(phases[i])
				q.BaseSHA256 = h.records.bases[id].SHA256
				metadataNext(t, h, port, q, RecordMetadataPatch{ConditionalStop: &ConditionalStopMetadata{ExpectedPhase: phases[i-1], NextPhase: phases[i]}})
			}
			q.ID = "forbidden-diagnostic-bypass"
			q.BaseSHA256 = h.records.bases[id].SHA256
			q.Patch = metadataGroup("owner-error", time.Now())
			before, _ := stopProbeRead(t, h, id)
			metadataAssertNoWrite(t, h, c, before, c.acks, q, port, ErrInvalidInput)
		})
	}
}
