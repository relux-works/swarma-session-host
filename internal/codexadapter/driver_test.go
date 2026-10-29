package codexadapter

import (
	"bytes"
	"errors"
	"testing"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

func driverConfig() ap.Config {
	c := config()
	return ap.Config{Lifecycle: ap.Create, Selection: ap.Selection{Provider: c.Provider, Model: c.Model, Effort: c.Effort}, Workspace: c.Workspace}
}
func observe(t *testing.T, d *Driver, offset *uint64, wire string) ap.Transition {
	t.Helper()
	b := []byte(wire + "\n")
	x, err := d.Observe(ap.Chunk{Offset: *offset, Bytes: b, AtTail: true})
	*offset += uint64(len(b))
	if err != nil {
		t.Fatal(err)
	}
	return x
}
func idleDriver(t *testing.T) (*Driver, uint64) {
	t.Helper()
	d, _, err := OpenDriver(driverConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	x, err := d.Begin()
	if err != nil || len(x.Actions) != 1 {
		t.Fatal(x, err)
	}
	var offset uint64
	x = observe(t, d, &offset, `{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos"}}`)
	if len(x.Actions) != 2 || len(x.Facts) != 0 {
		t.Fatal(x)
	}
	x = observe(t, d, &offset, `{"id":2,"result":{"thread":{"id":"thread-a"},"model":"unfamiliar-model","modelProvider":"unfamiliar-provider"}}`)
	if x.View.Activity != ap.Idle || len(x.Facts) != 0 {
		t.Fatal(x)
	}
	return d, offset
}

// The concrete Codex driver implements the shared interface without I/O: native
// acceptance is separate from completion, and restore emits neither input nor init.
func TestDriverCompletionAndRestore(t *testing.T) {
	d, offset := idleDriver(t)
	x, err := d.Prepare(ap.Input{ID: "notice-a", Reference: "synthetic.first", Mode: ap.NextTurn})
	if err != nil || len(x.Actions) != 1 || len(x.Facts) != 0 || x.View.Activity != ap.Busy {
		t.Fatal(x, err)
	}
	if !bytes.Contains(x.Actions[0].Bytes, []byte(`"effort":"unfamiliar-effort"`)) {
		t.Fatal("opaque effort lost")
	}
	x = observe(t, d, &offset, `{"method":"turn/started","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"inProgress"}}}`)
	if len(x.Facts) != 0 {
		t.Fatal("started became accepted")
	}
	x = observe(t, d, &offset, `{"id":3,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}`)
	if len(x.Facts) != 1 || x.Facts[0].Kind != ap.InputAccepted {
		t.Fatal(x)
	}
	x = observe(t, d, &offset, `{"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"completed"}}}`)
	if len(x.Facts) != 1 || x.Facts[0].Kind != ap.TurnCompleted {
		t.Fatal(x)
	}
	r, from, err := OpenDriver(driverConfig(), x.Checkpoint)
	if err != nil || from != offset {
		t.Fatal(from, err)
	}
	x, err = r.Begin()
	if err != nil || len(x.Actions) != 0 || len(x.Facts) != 0 || x.View.Activity != ap.Unknown {
		t.Fatal("restore repeated effects", x, err)
	}
	if _, err = r.Prepare(ap.Input{ID: "notice-b", Reference: "synthetic.second", Mode: ap.NextTurn}); err == nil {
		t.Fatal("restore skipped catchup")
	}
	x, err = r.Observe(ap.Chunk{Offset: offset, AtTail: true})
	if err != nil || len(x.Actions) != 0 || len(x.Facts) != 0 || x.View.Activity != ap.Idle {
		t.Fatal(x, err)
	}
	x, err = r.Prepare(ap.Input{ID: "notice-b", Reference: "synthetic.second", Mode: ap.NextTurn})
	if err != nil || len(x.Actions) != 1 {
		t.Fatal(x, err)
	}
	if !bytes.Contains(x.Actions[0].Bytes, []byte(`"id":4`)) || !bytes.Contains(x.Actions[0].Bytes, []byte(`"threadId":"thread-a"`)) {
		t.Fatal("restore changed conversation or request sequence")
	}
}

// Unsupported modes and overlapping notices leave metadata unchanged and emit
// zero actions; a neighboring eligible next-turn input still prepares normally.
func TestDriverRefusesWithoutEffects(t *testing.T) {
	d, _ := idleDriver(t)
	before := d.Snapshot()
	for _, mode := range []ap.InputMode{ap.Steer, ap.Interrupt, ap.RequestExit, ap.SetNativeGoal, ap.ClearNativeGoal, "future-mode"} {
		x, err := d.Prepare(ap.Input{ID: "notice-a", Reference: "synthetic.first", Mode: mode})
		if !errors.Is(err, ap.ErrUnsupported) || len(x.Actions) != 0 || len(x.Facts) != 0 || !bytes.Equal(x.Checkpoint, before.Checkpoint) {
			t.Fatal("unsupported mode had effects", x, err)
		}
	}
	x, err := d.Prepare(ap.Input{ID: "notice-a", Reference: "synthetic.first", Mode: ap.NextTurn})
	if err != nil || len(x.Actions) != 1 {
		t.Fatal(x, err)
	}
	before = d.Snapshot()
	for _, id := range []string{"notice-a", "notice-b"} {
		x, err = d.Prepare(ap.Input{ID: id, Reference: "synthetic.next", Mode: ap.NextTurn})
		if err == nil || len(x.Actions) != 0 || !bytes.Equal(before.Checkpoint, x.Checkpoint) {
			t.Fatal("busy input had effects", x, err)
		}
	}
}

// Unqualified resume/fork and preallocated identities refuse at construction,
// before any initialization or transport effect. Explicit create is the control.
func TestDriverLifecycleAdmission(t *testing.T) {
	c := driverConfig()
	if _, _, err := OpenDriver(c, nil); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []ap.Lifecycle{ap.Resume, ap.Fork, "", "future"} {
		bad := c
		bad.Lifecycle = mode
		bad.NativeSession = "session-a"
		if d, _, err := OpenDriver(bad, nil); !errors.Is(err, ap.ErrUnsupported) || d != nil {
			t.Fatal("unsupported lifecycle created driver")
		}
	}
	c.NativeSession = "session-a"
	if d, _, err := OpenDriver(c, nil); !errors.Is(err, ap.ErrUnsupported) || d != nil {
		t.Fatal("preallocated Codex identity admitted")
	}
}

// A notice action key follows its durable notice ID, independently from the
// transport request counter; another notice gets another key and bytes stay exact.
func TestStableNoticeActionKey(t *testing.T) {
	a := action([]byte(`{"id":3}`), "notice-a")
	b := action([]byte(`{"id":99}`), "notice-a")
	c := action([]byte(`{"id":3}`), "notice-b")
	if a.Key != b.Key || a.Key == c.Key || !bytes.Equal(b.Bytes, []byte(`{"id":99}`)) {
		t.Fatal("notice key followed RPC counter or changed payload")
	}
}

// Adjacent notifications, transport EOF and metadata restore cannot erase an
// observed authority request or turn it into an answer. No raw question persists.
func TestAuthorityObservationSurvivesEOFAndRestore(t *testing.T) {
	d, offset := idleDriver(t)
	wire := []byte("{\"id\":\"server-request\",\"method\":\"item/commandExecution/requestApproval\",\"params\":{\"question\":\"synthetic-private-question-canary\"}}\n{\"method\":\"future/notification\",\"params\":{}}\n")
	x, err := d.Observe(ap.Chunk{Offset: offset, Bytes: wire, AtTail: true})
	if err != nil || x.View.Activity != ap.Approval || len(x.Actions) != 0 || len(x.Attention) != 1 {
		t.Fatal("authority request lost in adjacent frames", err)
	}
	key := x.Attention[0].Key
	offset += uint64(len(wire))
	x, err = d.Observe(ap.Chunk{Offset: offset, EOF: true, AtTail: true})
	if err != nil || x.View.Activity != ap.Unknown || len(x.Attention) != 1 || x.Attention[0].Key != key || len(x.Actions) != 0 {
		t.Fatal("EOF erased escalation", err)
	}
	if bytes.Contains(x.Checkpoint, []byte("synthetic-private-question-canary")) {
		t.Fatal("private question persisted")
	}
	r, _, err := OpenDriver(driverConfig(), x.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	x, err = r.Begin()
	if err != nil || len(x.Actions) != 0 || len(x.Attention) != 1 || x.Attention[0].Key != key {
		t.Fatal("restore erased or answered authority request", err)
	}
}

// Missing bytes and pending-turn replacement cannot create a second submission;
// replayed acceptance/completion only produce facts, with no outbound action.
func TestDriverPendingRestoreAndGap(t *testing.T) {
	d, offset := idleDriver(t)
	prepared, err := d.Prepare(ap.Input{ID: "notice-a", Reference: "synthetic.first", Mode: ap.NextTurn})
	if err != nil {
		t.Fatal(err)
	}
	r, from, err := OpenDriver(driverConfig(), prepared.Checkpoint)
	if err != nil || from != offset {
		t.Fatal(err)
	}
	if x, err := r.Begin(); err != nil || len(x.Actions) != 0 {
		t.Fatal(x, err)
	}
	x, err := r.Observe(ap.Chunk{Offset: offset, AtTail: true})
	if err != nil || x.View.Activity != ap.Busy || len(x.Actions) != 0 {
		t.Fatal(x, err)
	}
	if x, err := r.Prepare(ap.Input{ID: "notice-b", Reference: "synthetic.next", Mode: ap.NextTurn}); err == nil || len(x.Actions) != 0 {
		t.Fatal("pending submission repeated")
	}
	x = observe(t, r, &offset, `{"id":3,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}`)
	if len(x.Actions) != 0 || len(x.Facts) != 1 || x.Facts[0].Kind != ap.InputAccepted {
		t.Fatal(x)
	}
	x = observe(t, r, &offset, `{"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"completed"}}}`)
	if len(x.Actions) != 0 || len(x.Facts) != 1 || x.Facts[0].Kind != ap.TurnCompleted {
		t.Fatal(x)
	}
	x, err = r.Observe(ap.Chunk{Offset: offset + 1, Bytes: []byte("{}\n"), AtTail: true})
	if err == nil || len(x.Actions) != 0 || x.View.Activity != ap.Unknown {
		t.Fatal("gap opened input", x, err)
	}
	x, err = r.Prepare(ap.Input{ID: "notice-b", Reference: "synthetic.next", Mode: ap.NextTurn})
	if err == nil || len(x.Actions) != 0 {
		t.Fatal("gap caused retry")
	}
}
