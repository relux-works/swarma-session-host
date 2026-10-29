package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

type factDriver struct{ descriptor ap.Descriptor }

func (d factDriver) Descriptor() ap.Descriptor     { return d.descriptor }
func (d factDriver) Begin() (ap.Transition, error) { return ap.Transition{}, ap.ErrUnsupported }
func (d factDriver) Prepare(ap.Input) (ap.Transition, error) {
	return ap.Transition{}, ap.ErrUnsupported
}
func (d factDriver) Observe(ap.Chunk) (ap.Transition, error) {
	return ap.Transition{}, ap.ErrUnsupported
}
func (d factDriver) Snapshot() ap.Transition { return ap.Transition{} }

// Shared host persistence must support a queued native command acknowledged
// before a turn ID, alongside Codex turn acceptance and Claude's ID-less result.
// A different receipt/input/session cannot borrow that acceptance or alter disk.
// These are protocol contract fixtures, not Claude/Muse native qualification.
func TestProtocolNativeReceiptAndTurnAreDistinct(t *testing.T) {
	for _, protocol := range []string{"codex", "claude-stream-json", "muse-msp"} {
		t.Run(protocol, func(t *testing.T) {
			s, err := openStore(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			n := Notice{ID: "one", Reference: "synthetic/one"}
			sel := Selection{Vendor: "provider", Model: "model", Reasoning: "effort"}
			d := factDriver{ap.Descriptor{Version: ap.Version, ObservedRelease: "schematic", Name: protocol, PrimaryTransport: "pipe", Capabilities: ap.Capabilities{NativeTurnIdentity: protocol != "claude-stream-json", NextTurn: true}}}
			checkpoint := json.RawMessage(`{"phase":"synthetic"}`)
			h := &Host{store: s, ctx: context.Background(), driver: d, state: State{Version: 1, Execution: "synthetic", Adapter: protocol, Selection: sel, Epoch: 1, Sequence: 1, Fence: "open", InputState: InputBusy,
				Records: []Record{{Notice: n, Digest: digestNotice(n, sel), Sequence: 1, State: "unknown"}}, Protocol: &ProtocolState{BoundaryVersion: ap.Version, ObservedRelease: "schematic", Checkpoint: checkpoint, View: ap.View{InputID: "one", NativeSession: "session-a", Activity: ap.Busy}}}}
			acceptedTurn, receipt, completedTurn := "turn-a", "turn-a", "turn-a"
			if protocol == "muse-msp" {
				acceptedTurn = ""
				receipt = "command-a"
			}
			if protocol == "claude-stream-json" {
				acceptedTurn = ""
				receipt = ""
				completedTurn = ""
			}
			accepted := ap.Transition{Checkpoint: checkpoint, View: ap.View{InputID: "one", NativeSession: "session-a", CurrentTurn: acceptedTurn, Activity: ap.Busy}, Facts: []ap.Fact{{Kind: ap.InputAccepted, InputID: "one", NativeSession: "session-a", NativeID: acceptedTurn, ReceiptID: receipt}}}
			if err := h.persistTransitionLocked(accepted); err != nil {
				t.Fatal(err)
			}
			if h.state.Records[0].State != "unknown" || h.state.Records[0].NativeTurn != acceptedTurn || h.state.Records[0].NativeReceipt != receipt {
				t.Fatal("acceptance conflated turn or completion")
			}
			completion := ap.Transition{Checkpoint: checkpoint, View: ap.View{InputID: "one", NativeSession: "session-a", CurrentTurn: completedTurn, Activity: ap.Idle}, Facts: []ap.Fact{{Kind: ap.TurnCompleted, InputID: "one", NativeSession: "session-a", NativeID: completedTurn, ReceiptID: receipt}}}
			before := h.Snapshot()
			disk, err := os.ReadFile(filepath.Join(s.dir, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"receipt", "input", "session"} {
				bad := completion
				bad.Facts = append([]ap.Fact(nil), completion.Facts...)
				switch field {
				case "receipt":
					bad.Facts[0].ReceiptID = "another-command"
				case "input":
					bad.Facts[0].InputID = "another-notice"
				case "session":
					bad.Facts[0].NativeSession = "another-session"
				}
				if h.persistTransitionLocked(bad) == nil {
					t.Fatalf("wrong %s accepted", field)
				}
				after, err := os.ReadFile(filepath.Join(s.dir, "state.json"))
				if err != nil || !bytes.Equal(disk, after) || !reflect.DeepEqual(before, h.Snapshot()) {
					t.Fatal("rejected fact changed state")
				}
			}
			if err := h.persistTransitionLocked(completion); err != nil {
				t.Fatal(err)
			}
			r := h.state.Records[0]
			if r.State != "applied" || r.NativeTurn != completedTurn || r.Outcome != "completed" {
				t.Fatal("matching completion not persisted")
			}
		})
	}
}

// Completion of the older queued command can be persisted while its successor
// is busy; neither the successor nor the overall session becomes completed/idle.
func TestProtocolQueuedSuccessorKeepsBusy(t *testing.T) {
	s, err := openStore(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	sel := Selection{Vendor: "provider", Model: "model", Reasoning: "effort"}
	a, b := Notice{ID: "one", Reference: "synthetic/one"}, Notice{ID: "two", Reference: "synthetic/two"}
	cp := json.RawMessage(`{"phase":"synthetic"}`)
	d := factDriver{ap.Descriptor{Version: ap.Version, ObservedRelease: "schematic", Name: "muse-msp", PrimaryTransport: "pipe", Capabilities: ap.Capabilities{NextTurn: true, QueueWhileBusy: true, NativeTurnIdentity: true}}}
	h := &Host{store: s, ctx: context.Background(), driver: d, state: State{Version: 1, Execution: "synthetic", Adapter: "muse-msp", Selection: sel, Epoch: 1, Sequence: 2, Fence: "open", InputState: InputBusy,
		Records: []Record{{Notice: a, Digest: digestNotice(a, sel), Sequence: 1, State: "unknown", NativeAccepted: true, NativeSession: "session-a", NativeReceipt: "command-a"}, {Notice: b, Digest: digestNotice(b, sel), Sequence: 2, State: "unknown"}}, Protocol: &ProtocolState{BoundaryVersion: ap.Version, ObservedRelease: "schematic", Checkpoint: cp}}}
	x := ap.Transition{Checkpoint: cp, View: ap.View{InputID: "two", NativeSession: "session-a", CurrentTurn: "turn-b", Activity: ap.Busy}, Facts: []ap.Fact{
		{Kind: ap.InputAccepted, InputID: "two", NativeSession: "session-a", ReceiptID: "command-b", NativeID: "turn-b"},
		{Kind: ap.TurnCompleted, InputID: "one", NativeSession: "session-a", ReceiptID: "command-a", NativeID: "turn-a"},
	}}
	if err := h.persistTransitionLocked(x); err != nil {
		t.Fatal(err)
	}
	if h.state.InputState != InputBusy || h.state.Records[0].State != "applied" || h.state.Records[1].State != "unknown" || h.state.Records[1].Outcome != "" {
		t.Fatal("old completion completed successor or opened idle")
	}
}
