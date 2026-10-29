package sessionmanager

import (
	"encoding/json"
	"testing"
)

// A refused resume has no subscription binding. Missing or foreign thread IDs
// cannot nominate a drifted writer; an explicit same-thread lifecycle can.
// Once subscribed, implicit same-turn evidence remains valid, as upstream #697
// requires. These are correlator cases, beside actual RPC routing tests.
func TestCodexRunnerLifecycleCorrelationRequiresExplicitBinding(t *testing.T) {
	for _, test := range []struct {
		name, bound, thread, turn, status, candidate string
		allow                                        bool
	}{
		{name: "unbound missing thread", turn: "different", allow: true},
		{name: "unbound exact turn missing thread", turn: "expected", allow: true},
		{name: "foreign thread", bound: "thread", thread: "foreign", turn: "different", allow: true},
		{name: "bound missing thread cannot nominate", bound: "thread", turn: "different", allow: true},
		{name: "explicit thread can nominate", thread: "thread", turn: "different", allow: true, status: "completed", candidate: "different"},
		{name: "no drift permission", thread: "thread", turn: "different"},
		{name: "bound exact implicit turn", bound: "thread", turn: "expected", status: "completed"},
		{name: "exact explicit turn", thread: "thread", turn: "expected", status: "completed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			params, err := json.Marshal(map[string]any{"threadId": test.thread, "turn": map[string]any{"id": test.turn, "status": "completed"}})
			if err != nil {
				t.Fatal(err)
			}
			client := &codexRPCClient{subscribedThreadID: test.bound}
			_, status, _, candidate := client.nativeTurnEvidenceFrom([]codexRPCNotification{{Method: "turn/completed", Params: params}}, "thread", "expected", ProviderUsage{}, test.allow)
			if status != test.status || candidate != test.candidate {
				t.Fatalf("native lifecycle correlation: status=%q candidate=%q want=%q/%q", status, candidate, test.status, test.candidate)
			}
		})
	}
}

// Native evidence ownership applies on both append and read; mutation by either
// caller cannot change retained evidence. Existing partial-window flags persist.
func TestCodexRunnerConsumerEvidenceOwnsBytes(t *testing.T) {
	_, f, _, _ := newNativeCodexFixture(t, "idle")
	raw := json.RawMessage(`{"threadId":"native-thread"}`)
	f.runner.appendConsumerNotifications([]codexRPCNotification{{Method: "turn/started", Params: raw}})
	raw[0] = '['
	first, incomplete := f.runner.ConsumerNotifications()
	if len(first) != 1 || string(first[0].Params) != `{"threadId":"native-thread"}` || incomplete {
		t.Fatalf("native evidence aliased incoming bytes: %#v / %v", first, incomplete)
	}
	first[0].Params[0] = '['
	f.runner.markConsumerEvidenceIncomplete()
	second, incomplete := f.runner.ConsumerNotifications()
	if len(second) != 1 || string(second[0].Params) != `{"threadId":"native-thread"}` || !incomplete {
		t.Fatalf("native evidence aliased output or lost partial flag: %#v / %v", second, incomplete)
	}
}
