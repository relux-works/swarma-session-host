package sessionhost

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
)

func TestPrediscardPayloadViewCanonicalProgressSkeleton(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: "provider", Model: "model", Effort: "effort"}, Workspace: workspace}
	d, _, err := codexadapter.OpenDriver(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	tail := &debugTail{now: time.Now, epoch: 1, execution: "fixture"}
	d.SetDebugObserver(tail.observe)
	if _, err := d.Begin(); err != nil {
		t.Fatal(err)
	}
	frame := []byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"payload-canary-` + strings.Repeat("x", 60_000) + `"}}` + "\n")
	var offset uint64
	// No viewer reads at all. More than the entire retention budget still passes
	// through the single canonical parser and advances its durable frontier.
	for i := 0; i < 160; i++ {
		transition, err := d.Observe(ap.Chunk{Offset: offset, Bytes: frame, AtTail: true})
		if err != nil {
			t.Fatal(err)
		}
		offset += uint64(len(frame))
		if bytes.Contains(transition.Checkpoint, []byte("payload-canary")) {
			t.Fatal("output leaked to checkpoint")
		}
	}
	hello := []byte(`{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos"}}` + "\n")
	transition, err := d.Observe(ap.Chunk{Offset: offset, Bytes: hello, AtTail: true})
	if err != nil {
		t.Fatal(err)
	}
	offset += uint64(len(hello))
	if len(transition.Actions) != 2 {
		t.Fatal("stalled viewer blocked canonical handshake")
	}
	_, frontier, err := codexadapter.OpenDriver(cfg, transition.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if frontier != offset || frontier <= debugTailBytes {
		t.Fatalf("canonical frontier %d want %d", frontier, offset)
	}
	var storage int
	for e := tail.events.Front(); e != nil; e = e.Next() {
		v := e.Value.(*debugEvent)
		storage += cap(v.data) + 128
	}
	if storage != tail.used || storage > debugTailBytes || tail.lost == 0 {
		t.Fatalf("actual allocated retention %d accounted %d", storage, tail.used)
	}
	gap := tail.read(0)
	if gap.Cause != "capacity" {
		t.Fatal("missing slow-viewer gap")
	}
}
func TestUnknownEventAndTerminalEscapeSkeleton(t *testing.T) {
	tail := &debugTail{now: time.Now, epoch: 1, execution: "fixture"}
	for _, frame := range []string{
		`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"\u001b[2J\rspoof"}}`,
		`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"` + string(rune(0x9b)) + `2J"}}`,
		`{"id":7,"method":"unknown/approval","params":{"question":"inert?","options":["yes","no"]}}`,
	} {
		tail.observe([]byte(frame))
	}
	var seq uint64
	for i := 0; i < 3; i++ {
		read := tail.read(seq)
		if len(read.Event) == 0 {
			t.Fatal("missing structured diagnostic")
		}
		rendered := debugRender(read.Event)
		if bytes.ContainsAny(rendered, "\x1b\r") || bytes.Contains(rendered, []byte(string(rune(0x9b)))) {
			t.Fatalf("active terminal control in %q", rendered)
		}
		seq++
	}
	before := tail.next
	for _, bad := range [][]byte{[]byte(`not-json`), bytes.Repeat([]byte("x"), debugFrameBytes+1)} {
		tail.observe(bad)
	}
	if tail.next != before {
		t.Fatal("malformed/oversized payload admitted")
	}
	if !bytes.Contains(tail.read(2).Event, []byte("unqualified_payload")) {
		t.Fatal("unknown protocol observation disappeared")
	}
}
