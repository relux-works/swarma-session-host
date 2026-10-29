package adapterprotocol

import (
	"encoding/json"
	"testing"
)

// These design-derived contract vectors exercise lifecycle/capability
// differences without claiming any native feature is qualified. In particular,
// preallocated identity and resume differ, and fallback grants no operation.
func TestPrimaryProtocolCapabilityCounterexamples(t *testing.T) {
	for _, f := range fixtures() {
		t.Run(f.descriptor.Name, func(t *testing.T) {
			d := f.descriptor
			create := Config{Lifecycle: Create}
			if d.Capabilities.PreallocatedIdentity {
				create.NativeSession = "preallocated-session"
			}
			if !d.SupportsLifecycle(create) {
				t.Fatal("valid create shape refused")
			}
			bad := create
			bad.Lifecycle = ""
			if d.SupportsLifecycle(bad) {
				t.Fatal("implicit lifecycle selected")
			}
			if !d.Capabilities.PreallocatedIdentity {
				bad = create
				bad.NativeSession = "preallocated-session"
				if d.SupportsLifecycle(bad) {
					t.Fatal("Codex forced into preallocation")
				}
			}
			resume := Config{Lifecycle: Resume, NativeSession: "existing-session"}
			if d.SupportsLifecycle(resume) {
				t.Fatal("unqualified resume granted")
			}
			d.Capabilities.Resume = true
			if !d.SupportsLifecycle(resume) {
				t.Fatal("qualified resume cannot be represented")
			}
			resume.NativeSession = ""
			if d.SupportsLifecycle(resume) {
				t.Fatal("resume without identity")
			}
			fork := Config{Lifecycle: Fork, NativeSession: "source-session"}
			if d.SupportsLifecycle(fork) {
				t.Fatal("unqualified fork granted")
			}
			d.Capabilities.Fork = true
			if !d.SupportsLifecycle(fork) {
				t.Fatal("qualified fork cannot be represented")
			}
			for _, mode := range []InputMode{Steer, Interrupt, RequestExit, SetNativeGoal, ClearNativeGoal} {
				if d.Capabilities.Supports(mode) {
					t.Fatal("declared fallback granted unqualified operation")
				}
			}
			qualified := Capabilities{Steer: true, Interrupt: true, GracefulExit: true, NativeGoals: true}
			for _, mode := range []InputMode{Steer, Interrupt, RequestExit, SetNativeGoal, ClearNativeGoal} {
				if !qualified.Supports(mode) {
					t.Fatal("qualified optional operation cannot be represented")
				}
			}
		})
	}
}

// Optional usage can correlate to a session without inventing a Claude turn;
// wrong identities, unqualified usage and out-of-bound counts are rejected.
func TestUsageAndAuthorityObservations(t *testing.T) {
	for _, f := range fixtures() {
		d := f.descriptor
		d.Capabilities.TokenUsage = true
		x := Transition{Checkpoint: json.RawMessage(`{"version":1}`), View: View{NativeSession: "session-a", CurrentTurn: f.turnID, Activity: Idle}, Usage: &Usage{NativeSession: "session-a", NativeTurn: f.turnID, InputTokens: 3, OutputTokens: 2, CacheReadTokens: 1}}
		if Validate(d, x) != nil {
			t.Fatal("valid optional usage refused")
		}
		bad := x
		u := *x.Usage
		bad.Usage = &u
		bad.Usage.NativeSession = "other"
		if Validate(d, bad) == nil {
			t.Fatal("wrong usage session")
		}
		bad.Usage.NativeSession = "session-a"
		bad.Usage.OutputTokens = 1<<60 + 1
		if Validate(d, bad) == nil {
			t.Fatal("unbounded usage")
		}
		d.Capabilities.TokenUsage = false
		if Validate(d, x) == nil {
			t.Fatal("unqualified usage accepted")
		}
	}
	x := Transition{Checkpoint: json.RawMessage(`{"version":1}`), View: View{Activity: Approval}, Attention: []Attention{{Kind: AuthorityRequired, Key: "request-a", Class: "unclassified", OwnerOnly: true}}}
	d := Descriptor{Name: "fixture", PrimaryTransport: "pipe"}
	if Validate(d, x) != nil {
		t.Fatal("typed escalation refused")
	}
	x.Attention[0].OwnerOnly = false
	if Validate(d, x) == nil {
		t.Fatal("unclassified request lost owner routing")
	}
}

// Muse can finish an older input and start its queued successor in one chunk.
// That completion neither belongs to the newest input nor implies overall idle.
func TestQueuedCompletionKeepsIndependentInputAndActivity(t *testing.T) {
	d := fixtures()[2].descriptor
	x := Transition{Checkpoint: json.RawMessage(`{"version":1}`), View: View{InputID: "notice-b", NativeSession: "session-a", CurrentTurn: "turn-b", Activity: Busy}, Facts: []Fact{
		{Kind: InputAccepted, InputID: "notice-b", NativeSession: "session-a", ReceiptID: "command-b"},
		{Kind: TurnCompleted, InputID: "notice-a", NativeSession: "session-a", NativeID: "turn-a", ReceiptID: "command-a"},
	}}
	if Validate(d, x) != nil {
		t.Fatal("queued successor forced into one-current-input model")
	}
}
