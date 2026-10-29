package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type capturedRunFunc func(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error)

func (f capturedRunFunc) RunCapturedOwner(c context.Context, s CapturedOwnerRun, r ProviderRunRequest) (*ProviderRunResult, error) {
	return f(c, s, r)
}
func ownerRunAttachment(t *testing.T, h *SessionHost, id string) string {
	t.Helper()
	for a, s := range h.attachments {
		if s == id {
			return a
		}
	}
	t.Fatal("missing actual attachment")
	return ""
}

// Core passes actual admitted input/native/committed bytes and permits map reads
// during provider work. This fixture qualifies core orchestration, not a renderer.
func TestHostCapturedOwnerRunPersistsActualOutcome(t *testing.T) {
	h, c, r, id := hostCommitProbeHost(t)
	ctx := context.Background()
	calls := 0
	h.options.OwnerRuns = capturedRunFunc(func(ctx context.Context, cut CapturedOwnerRun, q ProviderRunRequest) (*ProviderRunResult, error) {
		calls++
		if cut.Native != r || cut.Lease.SessionID != id || cut.Input == nil || cut.Record.SessionID != id || !bytes.Equal(cut.Committed.Bytes, h.records.bases[id].Bytes) {
			t.Fatal("not actual cut")
		}
		if _, err := h.List(ctx); err != nil {
			t.Fatal(err)
		}
		receipt, err := cut.Input.Submit(ctx, textInput(cut.Lease, "owner-run", q.Prompt))
		if err != nil {
			return nil, err
		}
		if _, err = cut.Input.Await(ctx, receipt); err != nil {
			return nil, err
		}
		cut.Committed.Bytes[0] = '!'
		return &ProviderRunResult{Status: ProviderRunComplete, Output: "actual result"}, nil
	})
	before := c.acks
	out, err := h.RunOwner(ctx, id, ownerRunAttachment(t, h, id), ProviderRunRequest{Prompt: "hello"})
	if err != nil || out == nil || out.Output != "actual result" || calls != 1 || r.writeCount() != 1 || c.acks != before+1 {
		t.Fatalf("run outcome/ACK: %+v %v calls%d writes%d acks%d", out, err, calls, r.writeCount(), c.acks-before)
	}
	if h.records.bases[id].Bytes[0] == '!' || h.ownerRuns[h.sessions[id].Owner] != nil {
		t.Fatal("cut escaped or activity retained")
	}
}

// Stale membership, empty prompts, missing renderer and materialization refusal
// never reach provider work; historical RUN retirement writes no reason or ACK.
func TestHostCapturedOwnerRunRefusals(t *testing.T) {
	for _, mode := range []string{"stale", "prompt", "missing", "materialized", "retired"} {
		t.Run(mode, func(t *testing.T) {
			h, c, _, id := hostCommitProbeHost(t)
			ctx := context.Background()
			calls := 0
			a := ownerRunAttachment(t, h, id)
			q := ProviderRunRequest{Prompt: "hello"}
			h.options.OwnerRuns = capturedRunFunc(func(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error) {
				calls++
				return &ProviderRunResult{Status: ProviderRunComplete}, nil
			})
			switch mode {
			case "stale":
				_, _ = h.epochs.reserveContext(ctx, id)
			case "prompt":
				q.Prompt = " "
			case "missing":
				h.options.OwnerRuns = nil
			case "materialized":
				r := h.sessions[id]
				r.MaterializationCtxID = "CTX-held"
				h.sessions[id] = r
			case "retired":
				r := h.sessions[id]
				r.Owner = "run/RUN-historical"
				r.SessionID = "SES-" + strings.Repeat("b", 32)
				id = r.SessionID
				if err := h.records.create(ctx, r); err != nil {
					t.Fatal(err)
				}
				h.sessions[id] = r
			}
			before := c.acks
			raw := append([]byte(nil), h.records.bases[id].Bytes...)
			_, err := h.RunOwner(ctx, id, a, q)
			if err == nil || calls != 0 {
				t.Fatalf("refusal reached renderer: %v calls%d", err, calls)
			}
			if mode == "stale" && !errors.Is(err, ErrStaleInputController) {
				t.Fatalf("wrong stale refusal %v", err)
			}
			if mode == "retired" {
				var retired *runOwnerRetiredError
				if !errors.As(err, &retired) || c.acks != before || !bytes.Equal(raw, h.records.bases[id].Bytes) {
					t.Fatal("historical RUN was written")
				}
			}
		})
	}
}

// Snapshot/outcome commit failures remain private500, retain uncertainty, and
// cannot promote a provider result to success or forget the owned activity.
func TestHostCapturedOwnerRunPersistenceFailure(t *testing.T) {
	h, c, _, id := hostCommitProbeHost(t)
	fault := errors.New("private store detail")
	h.options.Layout.OperatorLogFile = filepath.Join(t.TempDir(), "private.log")
	h.options.OwnerRuns = capturedRunFunc(func(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error) {
		c.ack = func(LegacyRecordInput) error { return fault }
		return &ProviderRunResult{Status: ProviderRunComplete}, nil
	})
	handler, err := NewProtocol8Handler(h, func() ControlStatus { return ControlStatus{} }, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("POST", "/v1/sessions/"+id+"/run", strings.NewReader(`{"attachment_id":"`+ownerRunAttachment(t, h, id)+`","prompt":"hello"}`)))
	if w.Code != 500 || !strings.Contains(w.Body.String(), "owner_run_persist_failed") || strings.Contains(w.Body.String(), fault.Error()) {
		t.Fatalf("unsafe persist response %d %s", w.Code, w.Body.String())
	}
	if !h.records.uncertain[id] || h.ownerRuns[h.sessions[id].Owner] != nil {
		t.Fatal("uncertainty/activity lost")
	}
	privateLog, e := os.ReadFile(h.options.Layout.OperatorLogFile)
	if e != nil || !strings.Contains(string(privateLog), "owner_run_failed") || !strings.Contains(string(privateLog), fault.Error()) {
		t.Fatalf("private persistence diagnostic lost: %v", e)
	}
}

// Stop must reach the actual provider while Run waits without an epoch fence;
// a stale captured writer cannot write after Stop and stopped state stays stopped.
func TestHostCapturedOwnerRunStopDuringAwait(t *testing.T) {
	h, _, r, id := hostCommitProbeHost(t)
	ctx := context.Background()
	entered := make(chan CapturedOwnerRun, 1)
	waiting := make(chan struct{})
	outcome := make(chan error, 1)
	r.await = func(ctx context.Context, _ InputSubmission, _ InputReceipt) (InputObservation, error) {
		close(waiting)
		<-ctx.Done()
		return InputObservation{}, ctx.Err()
	}
	h.options.OwnerRuns = capturedRunFunc(func(ctx context.Context, cut CapturedOwnerRun, q ProviderRunRequest) (*ProviderRunResult, error) {
		entered <- cut
		receipt, e := cut.Input.Submit(ctx, textInput(cut.Lease, "waiting-run", q.Prompt))
		if e == nil {
			_, e = cut.Input.Await(ctx, receipt)
		}
		return nil, e
	})
	a := ownerRunAttachment(t, h, id)
	go func() { _, e := h.RunOwner(ctx, id, a, ProviderRunRequest{Prompt: "wait"}); outcome <- e }()
	cut := <-entered
	<-waiting
	owner := cut.Record.Owner
	call, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, _, e := h.BeginOwnerActivation(call, owner, cut.Lease); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("activation crossed live run: %v", e)
	}
	stop, stopCancel := context.WithTimeout(ctx, time.Second)
	defer stopCancel()
	v, e := h.Stop(stop, id)
	if e != nil || v.State != SessionStopped || r.forces != 1 {
		t.Fatalf("Stop blocked behind actual input acknowledgement: %+v %v forces%d", v, e, r.forces)
	}
	if e = <-outcome; e == nil {
		t.Fatal("failed native run became success")
	}
	if _, e = cut.Input.Submit(ctx, textInput(cut.Lease, "after-stop", "forbidden")); !errors.Is(e, ErrStaleInputController) || r.writeCount() != 1 {
		t.Fatalf("stopped writer remained: %v", e)
	}
	if h.sessions[id].State != SessionStopped {
		t.Fatal("run completion resurrected session")
	}
}

// A nil supplier result is not successful production. An ADMITTED live provider
// failure whose save fails retains the old reason and persistence error type.
// Empty prompt/bogus membership are no-write refusals covered by reviewer roots.
func TestHostCapturedOwnerRunNilAndReasonSave(t *testing.T) {
	h, _, _, id := hostCommitProbeHost(t)
	ctx := context.Background()
	h.options.OwnerRuns = capturedRunFunc(func(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error) {
		return nil, nil
	})
	if _, e := h.RunOwner(ctx, id, ownerRunAttachment(t, h, id), ProviderRunRequest{Prompt: "hello"}); !errors.Is(e, ErrHostCapabilityUnavailable) {
		t.Fatalf("nil result accepted: %v", e)
	}
	previous := h.sessions[id].OwnerRunError
	h.options.OwnerRuns = capturedRunFunc(func(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error) {
		return nil, errors.New("admitted live failure")
	})
	h.records.commit = func(string, []byte) error { return os.ErrPermission }
	_, e := h.RunOwner(ctx, id, ownerRunAttachment(t, h, id), ProviderRunRequest{Prompt: "admitted valid prompt"})
	var persist *ownerRunPersistError
	if !errors.As(e, &persist) || h.sessions[id].OwnerRunError != previous {
		t.Fatalf("refusal save failed silently: %v", e)
	}
}

type ownerAckSource struct {
	coreGoalSource
	read *GoalRead
	err  error
}

func (s *ownerAckSource) Get(OwnerKey) (*GoalRead, error) { return s.read, s.err }

// Only the exact actual latest applied head resolves a typed scanner timeout;
// pending/stale heads, another provider outcome and failed reads keep refusal.
func TestHostCapturedOwnerRunLedgerResolution(t *testing.T) {
	ref := RevisionRef{ID: "GOAL-SG-EXAMPLE", Revision: 2}
	other := ref
	other.Revision++
	for _, mode := range []string{"applied", "pending", "different-latest", "different-applied", "missing", "read-failed", "other-outcome"} {
		t.Run(mode, func(t *testing.T) {
			h, _, _, id := hostCommitProbeHost(t)
			s := &ownerAckSource{read: &GoalRead{Latest: &ref, Applied: &ref}}
			h.options.Goals = s
			switch mode {
			case "pending":
				s.read.Pending = true
			case "different-latest":
				s.read.Latest = &other
			case "different-applied":
				s.read.Applied = &other
			case "missing":
				s.read = nil
			case "read-failed":
				s.err = errors.New("ledger unavailable")
			}
			record := h.sessions[id]
			record.AppliedGoalRef = &ref
			result := &ProviderRunResult{Status: ProviderRunFailed, ExitCode: 1, ProviderStatus: "never_acknowledged"}
			if mode == "other-outcome" {
				result.ProviderStatus = "provider_failure"
			}
			got, e := h.resolveOwnerGoalAckLocked(record, result)
			if mode == "applied" {
				if e != nil || got.Status != ProviderRunComplete || got.AckSource != "ledger" || got.ProviderStatus != "ack_detection_disagreement" {
					t.Fatalf("actual head not resolved: %+v %v", got, e)
				}
			} else if mode == "read-failed" {
				if !errors.Is(e, s.err) {
					t.Fatalf("read failure hidden: %v", e)
				}
			} else if got != result || e != nil {
				t.Fatalf("unproven head resolved: %+v %v", got, e)
			}
		})
	}
}

// Only an admitted live incarnation persists a failure reason or clears it on
// success. Each turn has one actual outcome ACK; refusing everything is invalid.
func TestHostCapturedOwnerRunLiveReasonSetAndClear(t *testing.T) {
	h, c, _, id := hostCommitProbeHost(t)
	ctx := context.Background()
	failure := errors.New("admitted provider refusal")
	h.options.OwnerRuns = capturedRunFunc(func(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error) {
		return nil, failure
	})
	before := c.acks
	if _, e := h.RunOwner(ctx, id, ownerRunAttachment(t, h, id), ProviderRunRequest{Prompt: "valid"}); !errors.Is(e, failure) || c.acks != before+1 || h.sessions[id].OwnerRunError != failure.Error() {
		t.Fatalf("live reason not durably set once: %v ACK%d", e, c.acks-before)
	}
	h.options.OwnerRuns = capturedRunFunc(func(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error) {
		return &ProviderRunResult{Status: ProviderRunComplete}, nil
	})
	before = c.acks
	if _, e := h.RunOwner(ctx, id, ownerRunAttachment(t, h, id), ProviderRunRequest{Prompt: "retry"}); e != nil || c.acks != before+1 || h.sessions[id].OwnerRunError != "" {
		t.Fatalf("live success did not clear reason once: %v ACK%d", e, c.acks-before)
	}
}
