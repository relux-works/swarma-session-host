package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

// Native-thread loss and recovery neighbours (C05/C17): with the current thread
// unknown, a fresh threadless grant cannot answer the retained request (V21, no
// CAS/journal/effect); when the same thread is known again, the pinned grant
// answers that request once; a recovery onto a different thread is V21 again.
func TestLostThreadRecoveryNeighbors(t *testing.T) {
	h, e := debugEffectFixture(t)
	setCurrentThread(t, h, "thread-a")
	offered := []string{"allow", "deny"}
	id, err := h.debugObserveInteraction("thread-a", "turn-a", 41, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	h.mu.Lock()
	err = h.persistTransitionLocked(ap.Transition{Checkpoint: []byte(`{}`), View: ap.View{Activity: ap.Unknown}})
	h.mu.Unlock()
	debugWant(t, err, "")
	token, revision := debugWriter(t, h, "view", "writer_control", "native_response")
	answer := debugInputIntent{Kind: "answer_interaction", ID: "recovered-answer", ExpectedRevision: revision, ExpectedTurn: "turn-a", Interaction: id, RequestID: 41, OptionsDigest: debugOptionsDigest(offered), Option: "allow"}
	before := h.debug.journal.sequence
	_, err = h.debugInput(token, answer)
	debugWant(t, err, "interaction_stale")
	if e.count() != 0 || h.debug.journal.sequence != before || h.state.Debug.Interactions[0].State != "pending" {
		t.Fatal("unknown current thread admitted a retained request")
	}
	setCurrentThread(t, h, "thread-a")
	record, err := h.debugInput(token, answer)
	debugWant(t, err, "")
	if record.Stage != "write_known" || e.count() != 1 || h.state.Debug.Interactions[0].State != "answer_reserved" || h.debug.grants[token].binding.thread != "thread-a" {
		t.Fatal("recovered thread did not admit the request once under the pinned grant")
	}
	other, err := h.debugObserveInteraction("thread-a", "turn-a", 43, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	h.mu.Lock()
	err = h.persistTransitionLocked(ap.Transition{Checkpoint: []byte(`{}`), View: ap.View{Activity: ap.Unknown}})
	h.mu.Unlock()
	debugWant(t, err, "")
	setCurrentThread(t, h, "thread-b")
	_, err = h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
	fresh, revision := debugWriter(t, h, "view", "writer_control", "native_response")
	_, err = h.debugInput(fresh, debugInputIntent{Kind: "answer_interaction", ID: "other-thread-answer", ExpectedRevision: revision, ExpectedTurn: "turn-a", Interaction: other, RequestID: 43, OptionsDigest: debugOptionsDigest(offered), Option: "deny"})
	debugWant(t, err, "interaction_stale")
	if e.count() != 1 || h.state.Debug.Interactions[1].State != "pending" {
		t.Fatal("recovery onto a different thread admitted the retained request")
	}
}

// debugStatusLeaks is the detector for private reconciliation members in an
// ordinary response: it reports every marker found. It is exercised against a
// deliberately unprojected state so an empty report proves detection, not luck.
func debugStatusLeaks(payload []byte, markers []string) []string {
	var found []string
	for _, m := range markers {
		if bytes.Contains(payload, []byte(m)) {
			found = append(found, m)
		}
	}
	return found
}

// Ordinary status projection (C13): after an accepted inert answer and text
// input and a terminal fact with unique synthetic native thread/turn canaries,
// the public Snapshot and the server status response carry only kinds, stages,
// states, ack/permission/debt flags and relative ages; the actual input IDs,
// actor, journal sequences, payload-derived digests, interaction handle, native
// request ID, options digest, result ID/revision, terminal native thread/turn
// and every absolute timestamp are absent. The grant-bound private control view
// still returns the exact fact and permits its disposition, and the private
// state file retains the references. The unprojected state is the planted leak
// the same detector must catch.
func TestOrdinaryStatusProjectionIsContentFree(t *testing.T) {
	h, e := debugEffectFixture(t)
	const thread, turn = "thread-canary-7f3a", "turn-canary-9c21"
	// The request is answerable only on the host's current native thread. That
	// thread is also the pre-existing public protocol view (outside the debug
	// redaction delta), so the thread canary is asserted absent from the debug
	// member while every other marker is asserted absent from the whole status.
	h.state.Protocol = &ProtocolState{View: ap.View{NativeSession: thread}}
	offered := []string{"allow-private-option", "deny-private-option"}
	interaction, err := h.debugObserveInteraction(thread, turn, 23, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	next := cloneState(h.state)
	debugWant(t, h.debugTerminalLocked(&next, "notice-1", thread, turn, "completed"), "")
	debugWant(t, h.persistLocked(next), "")
	token, revision := debugWriter(t, h, "view", "writer_control", "text", "native_response")
	_, err = h.debugInput(token, debugInputIntent{Kind: "answer_interaction", ID: "answer-private-id", ExpectedRevision: revision, ExpectedTurn: turn, Interaction: interaction, RequestID: 23, OptionsDigest: debugOptionsDigest(offered), Option: "allow-private-option"})
	debugWant(t, err, "")
	// A reserved answer is an unresolved native interaction: ordinary text is
	// fenced (C16) until its qualified resolution evidence arrives.
	debugWant(t, debugText(h, token, "early-private-id", "status-body-canary", revision, turn), "input_fenced")
	debugWant(t, h.debugInteractionResolved(interaction), "")
	_, err = h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "reply-private-id", Text: "status-body-canary", ExpectedRevision: revision, ExpectedTurn: turn})
	debugWant(t, err, "")
	if e.count() != 2 {
		t.Fatal("positive inputs were not accepted")
	}
	private := debugState(h)
	fact := private.Debug.Results[0]
	// The ordinary record sequence shares its member name with the journal
	// reference, so that reference is detected through the digest beside it.
	markers := []string{`"request_id"`, `"options_digest"`, `"digest"`, `"input_actor"`, `"input":`, `"actor"`, `"observed_at"`, `"issued_at"`, `"expires_at"`, `"revision"`, `"id"`, interaction, fact.ID, thread, turn, "answer-private-id", "reply-private-id", "status-body-canary", "allow-private-option", private.Debug.Interactions[0].OptionsDigest, fact.ObservedAt.UTC().Format("2006-01-02T15:04:05")}
	for _, in := range private.Debug.Inputs {
		markers = append(markers, in.Digest)
	}
	planted, err := json.Marshal(private)
	debugWant(t, err, "")
	leaks := debugStatusLeaks(planted, markers)
	for _, must := range []string{`"request_id"`, `"digest"`, `"observed_at"`, "answer-private-id", fact.ID, thread, turn, private.Debug.Interactions[0].OptionsDigest, private.Debug.Inputs[0].Digest} {
		if !strings.Contains(strings.Join(leaks, "\n"), must) {
			t.Fatalf("detector control missed planted %q: %v", must, leaks)
		}
	}
	if strings.Contains(string(planted), "status-body-canary") || strings.Contains(string(planted), "allow-private-option") {
		t.Fatal("private state itself holds a body or option text")
	}
	public, err := json.Marshal(h.Snapshot())
	debugWant(t, err, "")
	response := execute(context.Background(), time.Second, h, Request{Op: "status"})
	status, err := json.Marshal(response)
	debugWant(t, err, "")
	publicDebug, err := json.Marshal(h.Snapshot().Debug)
	debugWant(t, err, "")
	statusDebug, err := json.Marshal(response.State.Debug)
	debugWant(t, err, "")
	wholeStatus := slices.DeleteFunc(slices.Clone(markers), func(m string) bool { return m == thread })
	for name, sink := range map[string][]byte{"snapshot": public, "status-response": status} {
		if leaks := debugStatusLeaks(sink, wholeStatus); len(leaks) != 0 {
			t.Fatalf("%s exposes private members %v: %s", name, leaks, sink)
		}
	}
	for name, sink := range map[string][]byte{"snapshot-debug": publicDebug, "status-debug": statusDebug} {
		if leaks := debugStatusLeaks(sink, markers); len(leaks) != 0 {
			t.Fatalf("%s exposes private members %v: %s", name, leaks, sink)
		}
	}
	s := h.Snapshot()
	if len(s.Debug.Inputs) != 2 || s.Debug.Inputs[0].Kind != "answer_interaction" || s.Debug.Inputs[0].Stage != "native_accepted" || s.Debug.Inputs[1].Stage != "write_known" || len(s.Debug.Interactions) != 1 || s.Debug.Interactions[0].State != "resolved" || s.Debug.Interactions[0].Method != "item/tool/requestUserInput" || len(s.Debug.Results) != 1 || s.Debug.Results[0].Outcome != "completed" || !s.Debug.Results[0].NotificationPending || s.Debug.Results[0].AgeSeconds < 0 {
		t.Fatalf("public projection lost its allowlisted members: %+v", s.Debug)
	}
	if disk := debugDisk(t, h); !bytes.Contains(disk, []byte(private.Debug.Inputs[1].Digest)) || !bytes.Contains(disk, []byte("reply-private-id")) || !bytes.Contains(disk, []byte(thread)) {
		t.Fatal("private state file lost its reconciliation references")
	}
	// Private control route: the current grant sees the exact fact, the
	// interaction handle and only its own receipts, and can disposition the fact.
	control, err := h.debugControlView(token)
	debugWant(t, err, "")
	if len(control.Results) != 1 || control.Results[0].ID != fact.ID || control.Results[0].Thread != thread || control.Results[0].Turn != turn || control.Results[0].Revision != 1 || len(control.Interactions) != 1 || control.Interactions[0].ID != interaction || len(control.Inputs) != 2 {
		t.Fatalf("control view lacks private correlation: %+v", control)
	}
	debugWant(t, h.debugDisposition(token, control.Results[0].ID, control.Results[0].Revision, false, true), "")
	ownerView, err := h.debugIssueGrantTo(debugAuthority{"owner", "owner", 1}, debugAuthority{"owner", "owner", 1}, h.debugBindingLocked(), []string{"view"})
	debugWant(t, err, "")
	debugWant(t, h.debugControl(token, "detach", h.state.Debug.WriterRevision), "")
	debugWant(t, h.debugAttachView(ownerView), "")
	other, err := h.debugControlView(ownerView)
	debugWant(t, err, "")
	if len(other.Inputs) != 0 || len(other.Results) != 1 {
		t.Fatalf("control view disclosed another actor's receipts: %+v", other.Inputs)
	}
}

// Pin on first bind (C05/D2): a grant issued while the host knew no native
// thread is bound once to the first thread that appears; a later different
// thread, or losing the thread after pinning, reaps it (V05) with an empty
// read, cleared viewer, held writer revision and zero fresh effects. A grant
// freshly issued under the current thread is the positive control at each step.
func TestGrantThreadPinsOnFirstBind(t *testing.T) {
	h, e := debugEffectFixture(t)
	bootstrap, revision := debugWriter(t, h, "view", "writer_control", "text")
	if h.debug.grants[bootstrap].binding.thread != "" {
		t.Fatal("fixture grant was not threadless")
	}
	h.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread-a","turnId":"turn","itemId":"item","delta":"pinned-tail-canary"}}`))
	h.state.Protocol = &ProtocolState{View: ap.View{NativeSession: "thread-a"}}
	read, err := h.debugRead(bootstrap, 0)
	debugWant(t, err, "")
	if !bytes.Contains(read.Event, []byte("pinned-tail-canary")) || h.debug.grants[bootstrap].binding.thread != "thread-a" {
		t.Fatal("first thread did not pin the threadless grant")
	}
	_, err = h.debugInput(bootstrap, debugInputIntent{Kind: "follow_up", ID: "pinned", Text: "allowed under the pinned thread", ExpectedRevision: revision})
	debugWant(t, err, "")
	h.state.Protocol.View.NativeSession = "thread-b"
	read, err = h.debugRead(bootstrap, 0)
	debugWant(t, err, "attach_grant_revoked")
	if len(read.Event) != 0 || h.debug.viewer != "" || len(h.debug.grants) != 0 || h.state.Debug.Writer != "held" || h.state.Debug.WriterRevision != revision+1 {
		t.Fatal("pinned grant survived a different thread")
	}
	debugWant(t, debugText(h, bootstrap, "after-b", "x", revision, ""), "attach_grant_revoked")
	current, currentRevision := debugWriter(t, h, "view", "writer_control", "text")
	if h.debug.grants[current].binding.thread != "thread-b" {
		t.Fatal("fresh grant not bound to the current thread")
	}
	debugWant(t, h.debugControl(current, "detach", currentRevision), "")
	// Pin, then lose the thread entirely: the pinned grant is not current either.
	h.state.Protocol = nil
	second, secondRevision := debugWriter(t, h, "view", "writer_control", "text")
	h.state.Protocol = &ProtocolState{View: ap.View{NativeSession: "thread-c"}}
	_, err = h.debugRead(second, 0)
	debugWant(t, err, "")
	h.state.Protocol = nil
	_, err = h.debugRead(second, 0)
	debugWant(t, err, "attach_grant_revoked")
	if h.debug.viewer != "" || h.state.Debug.Writer != "held" || h.state.Debug.WriterRevision != secondRevision+1 {
		t.Fatal("pinned grant survived losing its thread")
	}
	debugWant(t, debugText(h, second, "after-loss", "x", secondRevision, ""), "attach_grant_revoked")
	if e.count() != 1 {
		t.Fatal("a non-current grant issued an effect")
	}
	fresh := debugView(t, h, "view")
	if _, err := h.debugRead(fresh, 0); err != nil {
		t.Fatal("current threadless grant refused while the host knows no thread")
	}
}

// Complete binding and registration (C05/C07): a grant is current only under
// the exact execution, runner incarnation, host epoch, native thread and
// manager/owner registration it was issued under. A changed registration,
// runner incarnation or native thread reaps the grant (V05), holds a writer it
// owned, discloses nothing and issues nothing; the old actor can no longer
// issue (V04) while the newly registered actor can. The unchanged host is the
// positive control for each change.
func TestGrantBindingAndRegistrationRevalidation(t *testing.T) {
	for _, change := range []string{"registration", "runner", "thread"} {
		t.Run(change, func(t *testing.T) {
			h, e := debugEffectFixture(t)
			h.state.Protocol = &ProtocolState{View: ap.View{NativeSession: "thread-a"}}
			h.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"binding-tail-canary"}}`))
			token, revision := debugWriter(t, h, "view", "writer_control", "text")
			g := h.debug.grants[token]
			if g.binding != (debugBinding{execution: "fixture", runner: "runner", thread: "thread-a", epoch: 1}) || g.registry != (debugRegistry{"manager", "owner"}) {
				t.Fatalf("grant did not retain its complete binding/registration: %+v", g)
			}
			read, err := h.debugRead(token, 0)
			debugWant(t, err, "")
			if !bytes.Contains(read.Event, []byte("binding-tail-canary")) {
				t.Fatal("current grant could not read")
			}
			used := h.debug.journal.used
			switch change {
			case "registration":
				debugWant(t, h.debugRegister(debugRegistry{"manager-2", "owner"}), "")
			case "runner":
				h.state.RunnerInstance = "runner-2"
			case "thread":
				h.state.Protocol.View.NativeSession = "thread-b"
			}
			read, err = h.debugRead(token, 0)
			debugWant(t, err, "attach_grant_revoked")
			if len(read.Event) != 0 || h.debug.viewer != "" || len(h.debug.grants) != 0 || h.state.Debug.Writer != "held" || h.state.Debug.WriterRevision != revision+1 || h.debug.journal.used != used {
				t.Fatal("stale grant retained view, allocation or writer, or the loss spent journal capacity")
			}
			debugWant(t, debugText(h, token, "late", "x", revision, ""), "attach_grant_revoked")
			if e.count() != 0 || len(h.state.Debug.Inputs) != 0 {
				t.Fatal("stale grant issued an effect")
			}
			if change == "registration" {
				_, err = h.debugIssueGrant(debugAuthority{"manager", "manager", 1}, []string{"view"})
				debugWant(t, err, "attach_unauthorized")
				fresh, err := h.debugIssueGrant(debugAuthority{"manager-2", "manager", 1}, []string{"view"})
				debugWant(t, err, "")
				debugWant(t, h.debugAttachView(fresh), "")
			} else {
				fresh, err := h.debugIssueGrant(debugAuthority{"manager", "manager", 1}, []string{"view"})
				debugWant(t, err, "")
				debugWant(t, h.debugAttachView(fresh), "")
			}
		})
	}
}

// Receipt key is actor plus input ID (C18): a second registered actor taking
// over the writer may reuse the same client ID as a distinct key with its own
// effect and receipt; each actor's exact retry returns only its own receipt,
// its changed intent is V15 against its own record, lookups never disclose the
// other actor's receipt, both records survive replacement, and completing one
// actor's input settles only that record.
func TestReceiptKeyIsActorPlusID(t *testing.T) {
	h, e := debugEffectFixture(t)
	managerToken, managerRevision := debugWriter(t, h, "view", "writer_control", "text")
	managerIntent := debugInputIntent{Kind: "follow_up", ID: "shared", Text: "manager text", ExpectedRevision: managerRevision}
	managerRecord, err := h.debugInput(managerToken, managerIntent)
	debugWant(t, err, "")
	debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", 1}, managerToken), "")
	ownerToken, err := h.debugIssueGrantTo(debugAuthority{"owner", "owner", 1}, debugAuthority{"owner", "owner", 1}, h.debugBindingLocked(), []string{"view", "writer_control", "text"})
	debugWant(t, err, "")
	debugWant(t, h.debugAttachView(ownerToken), "")
	debugWant(t, h.debugControl(ownerToken, "claim", 0), "")
	ownerRevision := h.state.Debug.WriterRevision
	ownerIntent := debugInputIntent{Kind: "follow_up", ID: "shared", Text: "owner text", ExpectedRevision: ownerRevision}
	ownerRecord, err := h.debugInput(ownerToken, ownerIntent)
	debugWant(t, err, "")
	if e.count() != 2 || len(h.state.Debug.Inputs) != 2 || ownerRecord.Actor != "owner" || managerRecord.Actor != "manager" || ownerRecord.Digest == managerRecord.Digest {
		t.Fatalf("distinct keys not kept apart: %+v", h.state.Debug.Inputs)
	}
	retry, err := h.debugInput(ownerToken, ownerIntent)
	debugWant(t, err, "")
	lookup, err := h.debugReceipt(ownerToken, "shared")
	debugWant(t, err, "")
	if retry != ownerRecord || lookup != ownerRecord || e.count() != 2 {
		t.Fatal("owner retry/lookup did not return exactly the owner receipt")
	}
	changed := ownerIntent
	changed.Text = "owner changed"
	_, err = h.debugInput(ownerToken, changed)
	debugWant(t, err, "input_conflict")
	debugWant(t, h.debugControl(ownerToken, "detach", h.state.Debug.WriterRevision), "")
	managerAgain := debugToken(t, h, "view", "writer_control", "text")
	debugWant(t, h.debugAttachView(managerAgain), "")
	retry, err = h.debugInput(managerAgain, managerIntent)
	debugWant(t, err, "")
	lookup, err = h.debugReceipt(managerAgain, "shared")
	debugWant(t, err, "")
	if retry != managerRecord || lookup != managerRecord || e.count() != 2 {
		t.Fatal("manager retry/lookup crossed into the owner receipt")
	}
	_, err = h.debugReceipt(managerAgain, "absent")
	debugWant(t, err, "input_invalid")
	replaced, err := debugReplace(t, h)
	debugWant(t, err, "")
	if len(replaced.state.Debug.Inputs) != 2 || replaced.state.Debug.Inputs[0].Actor != "manager" || replaced.state.Debug.Inputs[1].Actor != "owner" || replaced.state.Debug.Inputs[0].Stage != "write_known" || replaced.state.Debug.Inputs[1].Stage != "write_known" {
		t.Fatalf("replacement lost or merged the composite keys: %+v", replaced.state.Debug.Inputs)
	}
	debugWant(t, replaced.debugInputTerminal("owner", "shared", "thread", "turn-9", "completed"), "")
	debugWant(t, replaced.debugInputTerminal("stranger", "shared", "thread", "turn-9", "completed"), "input_invalid")
	if replaced.state.Debug.Inputs[0].Stage != "write_known" || replaced.state.Debug.Inputs[1].Stage != "turn_terminal" || len(replaced.state.Debug.Results) != 1 || replaced.state.Debug.Results[0].Input != "shared" {
		t.Fatalf("completion settled the wrong key: %+v", replaced.state.Debug)
	}
}
