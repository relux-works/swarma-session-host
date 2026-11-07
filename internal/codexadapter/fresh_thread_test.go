package codexadapter

import (
	"testing"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

func TestFreshThreadRequiresObservedCreate(t *testing.T) {
	d, _, err := OpenDriver(driverConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.FreshThread(); ok {
		t.Fatal("uninitialized driver correlated")
	}
	if _, err := d.Begin(); err != nil {
		t.Fatal(err)
	}
	var off uint64
	observe(t, d, &off, `{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos"}}`)
	if _, ok := d.FreshThread(); ok {
		t.Fatal("initialize without thread correlated")
	}
	observe(t, d, &off, `{"id":2,"result":{"thread":{"id":"thread-a"},"model":"unfamiliar-model","modelProvider":"unfamiliar-provider"}}`)
	if thread, ok := d.FreshThread(); !ok || thread != "thread-a" {
		t.Fatal("fresh create refused", thread, ok)
	}
	saved := d.Snapshot().Checkpoint
	r, from, err := OpenDriver(driverConfig(), saved)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Observe(ap.Chunk{Offset: from, AtTail: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.FreshThread(); ok {
		t.Fatal("restored checkpoint became fresh create")
	}
	// Foreign-thread events follow the existing parser contract: ignored, never
	// substituted for the retained thread. A subsequent offset gap is terminal.
	_, err = d.Observe(ap.Chunk{Offset: off, Bytes: []byte("{\"method\":\"turn/started\",\"params\":{\"threadId\":\"wrong\",\"turn\":{\"id\":\"turn-a\",\"status\":\"inProgress\"}}}\n"), AtTail: true})
	if err != nil {
		t.Fatal(err)
	}
	if thread, ok := d.FreshThread(); !ok || thread != "thread-a" {
		t.Fatal("foreign event replaced retained thread", thread, ok)
	}
	if _, err := d.Observe(ap.Chunk{Offset: off, AtTail: true}); err == nil {
		t.Fatal("offset rewind accepted")
	}
	if _, ok := d.FreshThread(); ok {
		t.Fatal("tail metadata revived parser")
	}
}

func TestFreshThreadPendingAndEOF(t *testing.T) {
	d, off := idleDriver(t)
	if _, err := d.Observe(ap.Chunk{Offset: off, AtTail: false}); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.FreshThread(); ok {
		t.Fatal("non-tail bytes correlated")
	}
	if _, err := d.Observe(ap.Chunk{Offset: off, AtTail: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.FreshThread(); !ok {
		t.Fatal("confirmed actual parser tail refused")
	}
	_, _ = d.Observe(ap.Chunk{Offset: off, EOF: true, AtTail: true})
	if _, ok := d.FreshThread(); ok {
		t.Fatal("EOF retained correlation")
	}
}
