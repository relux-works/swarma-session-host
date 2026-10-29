package sessionrun

import (
	"context"
	"encoding/json"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"io"
	"testing"
	"time"
)

type sequenceFeed struct{ values []core.MuseObservation }

func (f *sequenceFeed) Next(context.Context) (core.MuseObservation, error) {
	if len(f.values) == 0 {
		return core.MuseObservation{}, io.EOF
	}
	v := f.values[0]
	f.values = f.values[1:]
	return v, nil
}
func (*sequenceFeed) Close() {}

// The renderer consumes only an ordered current-lease feed. Wrong epoch,
// sequence, gap and stale cursor never produce an admitted native fold.
func TestMuseCapturedObservationSequence(t *testing.T) {
	l := core.ControllerLease{SessionID: "session", Epoch: 3}
	for _, name := range []string{"valid", "epoch", "sequence", "gap", "missing-time", "ambiguous"} {
		t.Run(name, func(t *testing.T) {
			fold := &core.MuseRestoreFold{SessionID: "native", Cursor: "cursor", Items: []json.RawMessage{}, Cumulative: core.MuseRestoreUsage{TotalTokens: 5}}
			v := core.MuseObservation{Lease: l, Sequence: 1, ObservedAt: time.Now(), RestoreFold: fold}
			switch name {
			case "epoch":
				v.Lease.Epoch++
			case "sequence":
				v.Sequence = 2
			case "gap":
				v.Gap = &core.MuseObservationGap{Reason: "overflow"}
			case "missing-time":
				v.ObservedAt = time.Time{}
			case "ambiguous":
				v.Event = core.MSPTurnStartedNotification{}
			}
			e := &museEvidence{lease: l, native: "native", feed: &sequenceFeed{[]core.MuseObservation{v}}, state: core.LegacyMuseAccountingSnapshot{SessionID: "native", GoalTransport: "native"}, finals: newFinals()}
			_, err := e.next(context.Background())
			if (err == nil) != (name == "valid") {
				t.Fatalf("sequence %s: %v", name, err)
			}
			if name != "valid" && (e.last != 0 || e.state.TokenUsageTotal != 0) {
				t.Fatal("refused observation moved accounting")
			}
		})
	}
}

// Matching terms are harmless echoes. A divergent term taints the active
// attempt even if a later echo heals the standing value; foreign events do not.
func TestMuseNativeExecutionTerms(t *testing.T) {
	e := &museEvidence{native: "native", promisedApproval: "denyUnmatched", promisedModel: "model", finals: newFinals()}
	raw := func(name, payload string) {
		t.Helper()
		_, err := e.raw(core.MSPRawNotification{Name: name, Params: json.RawMessage(payload)}, "turn")
		if err != nil {
			t.Fatal(err)
		}
	}
	raw("session/approvalModeChanged", `{"sessionId":"native","mode":"denyUnmatched"}`)
	raw("session/modelChanged", `{"sessionId":"native","modelId":"model"}`)
	raw("session/modelChanged", `{"sessionId":"foreign","modelId":"other"}`)
	if e.tainted {
		t.Fatal("echo or foreign session tainted owner")
	}
	raw("session/modelChanged", `{"sessionId":"native","modelId":"other"}`)
	raw("session/modelChanged", `{"sessionId":"native","modelId":"model"}`)
	if !e.tainted {
		t.Fatal("healed echo cleared overlapping divergence")
	}
	result, err := e.raw(core.MSPRawNotification{Name: "view/gap", Params: json.RawMessage(`{"sessionId":"native"}`)}, "turn")
	if result == nil || result.ProviderStatus != "view_gap" || !errors.Is(err, core.ErrMuseObservationUncertain) {
		t.Fatalf("gap not retained: %+v %v", result, err)
	}
}

// A repeated cursor is idempotent only for identical retained data with no
// intervening live event. It cannot rewrite accounting or consume caller aliases.
func TestMuseRestoreCursorCannotRestateEvidence(t *testing.T) {
	l := core.ControllerLease{SessionID: "session", Epoch: 3}
	for _, name := range []string{"identical", "changed-total", "changed-items", "live-event"} {
		t.Run(name, func(t *testing.T) {
			first := &core.MuseRestoreFold{SessionID: "native", Cursor: "cursor", Items: []json.RawMessage{}, Cumulative: core.MuseRestoreUsage{TotalTokens: 5}}
			evidence := &museEvidence{lease: l, native: "native", state: core.LegacyMuseAccountingSnapshot{SessionID: "native", GoalTransport: "native"}, finals: newFinals()}
			if _, e := evidence.admit(core.MuseObservation{Lease: l, Sequence: 1, ObservedAt: time.Now(), RestoreFold: first}); e != nil {
				t.Fatal(e)
			}
			next := copyRestoreFold(first)
			seq := uint64(2)
			switch name {
			case "changed-total":
				next.Cumulative.TotalTokens = 6
			case "changed-items":
				next.Items = append(next.Items, json.RawMessage(`{"kind":"other"}`))
			case "live-event":
				if _, e := evidence.admit(core.MuseObservation{Lease: l, Sequence: 2, ObservedAt: time.Now(), Event: core.MSPTurnStartedNotification{}}); e != nil {
					t.Fatal(e)
				}
				seq++
			}
			_, e := evidence.admit(core.MuseObservation{Lease: l, Sequence: seq, ObservedAt: time.Now(), RestoreFold: next})
			if name == "identical" {
				if e != nil {
					t.Fatal(e)
				}
			} else if !errors.Is(e, core.ErrMuseObservationUncertain) {
				t.Fatalf("same-cursor restatement accepted: %v", e)
			}
			if evidence.state.TokenUsageTotal != 5 {
				t.Fatal("refused cursor restatement changed counter")
			}
		})
	}
}
