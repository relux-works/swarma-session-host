package adapterprotocol

import (
	"encoding/json"
	"strings"
	"testing"
)

type protocolFixture struct {
	descriptor                 Descriptor
	input, accepted, completed string
	receiptID, turnID          string
}

// These synthetic examples derive from the three primary protocol forms in the
// design capability matrix. They prove shared framing accommodates their different
// identity/ack shapes, not that Claude/Muse sessions or features are qualified.
// Wire strings are schematic examples, not release-schema conformance fixtures.
func fixtures() []protocolFixture {
	return []protocolFixture{
		{Descriptor{Name: "codex-fixture", PrimaryTransport: "pipe", Capabilities: Capabilities{NextTurn: true, NativeTurnIdentity: true, Activity: true}},
			`{"id":3,"method":"turn/start","params":{"threadId":"session-a","input":[{"type":"text","text":"synthetic-reference"}]}}`,
			`{"id":3,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}`,
			`{"method":"turn/completed","params":{"threadId":"session-a","turn":{"id":"turn-a","status":"completed"}}}`, "turn-a", "turn-a"},
		{Descriptor{Name: "claude-stream-json-fixture", PrimaryTransport: "pipe", DeclaredFallback: "pty", Capabilities: Capabilities{PreallocatedIdentity: true, NextTurn: true, Activity: true}},
			`{"type":"user","session_id":"session-a","message":{"role":"user","content":"synthetic-reference"}}`,
			`{"type":"assistant","session_id":"session-a","message":{"role":"assistant","content":[]}}`,
			`{"type":"result","subtype":"success","is_error":false,"session_id":"session-a","result":"ok"}`, "", ""},
		{Descriptor{Name: "muse-msp-fixture", PrimaryTransport: "pipe", DeclaredFallback: "pty", Capabilities: Capabilities{PreallocatedIdentity: true, NextTurn: true, QueueWhileBusy: true, NativeTurnIdentity: true, Activity: true}},
			`{"id":3,"method":"turn/start","params":{"sessionId":"session-a","ifBusy":"queue","input":"synthetic-reference"}}`,
			`{"id":3,"result":{"commandId":"command-a","accepted":true,"disposition":"queued"}}`,
			`{"method":"turn/completed","params":{"sessionId":"session-a","turnId":"turn-a","status":"completed"}}`, "command-a", "turn-a"},
	}
}

func transition(f protocolFixture) Transition {
	return Transition{
		View:    View{InputID: "notice-a", Activity: Busy, NativeSession: "session-a"},
		Actions: []Action{{Key: "submit-a", InputID: "notice-a", Bytes: []byte(f.input)}}, Checkpoint: json.RawMessage(`{"version":1,"phase":"submitted","input":"notice-a"}`),
	}
}

// A write and acceptance remain distinct from completion; Claude needs no invented
// native turn ID, while Muse can acknowledge a queued command before a turn exists.
func TestThreeProtocolContractFixtures(t *testing.T) {
	for _, f := range fixtures() {
		t.Run(f.descriptor.Name, func(t *testing.T) {
			for _, frame := range []string{f.input, f.accepted, f.completed} {
				if !json.Valid([]byte(frame)) {
					t.Fatal("invalid fixture")
				}
			}
			x := transition(f)
			if err := Validate(f.descriptor, x); err != nil {
				t.Fatal(err)
			}
			if len(x.Facts) != 0 {
				t.Fatal("write fabricated receipt")
			}
			x.Actions = nil
			acceptedTurn := f.turnID
			if f.descriptor.Capabilities.QueueWhileBusy {
				acceptedTurn = ""
			}
			x.Facts = []Fact{{Kind: InputAccepted, InputID: "notice-a", NativeSession: "session-a", NativeID: acceptedTurn, ReceiptID: f.receiptID}}
			if err := Validate(f.descriptor, x); err != nil {
				t.Fatal(err)
			}
			if x.Facts[0].Kind == TurnCompleted {
				t.Fatal("acceptance fabricated completion")
			}
			x.View.Activity = Idle
			x.View.CurrentTurn = f.turnID
			x.Facts = []Fact{{Kind: TurnCompleted, InputID: "notice-a", NativeSession: "session-a", NativeID: f.turnID, ReceiptID: f.receiptID}}
			if err := Validate(f.descriptor, x); err != nil {
				t.Fatal(err)
			}
			if f.descriptor.Name == "claude-stream-json-fixture" && x.Facts[0].NativeID != "" {
				t.Fatal("invented Claude turn")
			}
		})
	}
}

// Wrong session and missing required native identity cannot form a fact. Native
// outcome semantics belong to adapters; per-input correlation belongs to host.
func TestRejectUncorrelatedOrUnknownCompletion(t *testing.T) {
	for _, f := range fixtures() {
		x := transition(f)
		x.Actions = nil
		x.View.Activity = Idle
		x.View.CurrentTurn = f.turnID
		x.Facts = []Fact{{Kind: TurnCompleted, InputID: "notice-a", NativeSession: "session-a", NativeID: f.turnID}}
		if err := Validate(f.descriptor, x); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*Transition){
			func(x *Transition) { x.Facts[0].InputID = "" }, func(x *Transition) { x.Facts[0].NativeSession = "other" },
		} {
			bad := x
			bad.Facts = append([]Fact(nil), x.Facts...)
			mutate(&bad)
			if Validate(f.descriptor, bad) == nil {
				t.Fatalf("uncorrelated evidence admitted: %+v", bad)
			}
		}
		if f.descriptor.Capabilities.NativeTurnIdentity {
			x.Facts[0].NativeID = ""
			if Validate(f.descriptor, x) == nil {
				t.Fatal("missing native turn admitted")
			}
		}
	}
}

// Closed input states and exhausted bounds reject actions, with an ordinary
// eligible transition as the positive control. A PTY declaration changes no grant.
func TestClosedInputAndBounds(t *testing.T) {
	f := fixtures()[1]
	x := transition(f)
	if Validate(f.descriptor, x) != nil {
		t.Fatal("valid input refused")
	}
	for _, state := range []Activity{Unknown, Draft, Approval, SecurityStop} {
		bad := x
		bad.View.Activity = state
		if Validate(f.descriptor, bad) == nil {
			t.Fatal("closed state wrote input")
		}
	}
	for _, mutate := range []func(*Transition){func(x *Transition) { x.Actions = append(x.Actions, x.Actions[0]) }, func(x *Transition) { x.Checkpoint = json.RawMessage(strings.Repeat("x", MaxCheckpoint+1)) }, func(x *Transition) { x.Actions = []Action{{Key: "large", Bytes: make([]byte, MaxWrite+1)}} }} {
		bad := x
		mutate(&bad)
		if Validate(f.descriptor, bad) == nil {
			t.Fatal("invalid bound admitted")
		}
	}
	if f.descriptor.Capabilities.Supports(Steer) || f.descriptor.Capabilities.Supports(Interrupt) {
		t.Fatal("fallback granted unsupported mode")
	}
	if (Capabilities{}).Supports(NextTurn) {
		t.Fatal("unqualified capability defaulted on")
	}
}
