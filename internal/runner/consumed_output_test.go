package runner

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

const streamFixtureBytes = 1 << 20

func TestConsumedOutputChild(t *testing.T) {
	if os.Getenv("SESSION_HOST_FIXTURE") != "consumed-output" {
		return
	}
	payload := make([]byte, streamFixtureBytes)
	for i := range payload {
		payload[i] = byte((i + i/251) % 251)
	}
	if _, err := os.Stdout.Write(payload); err != nil {
		os.Exit(21)
	}
	io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

// A real inert child can exceed both the former cumulative cap and the retained
// backlog without loss. Before any consume, a full backlog must backpressure,
// never report overflow. The original cumulative collector fails this control.
func TestConsumedOutputBeyondBacklog(t *testing.T) {
	cfg := testConfig(t, "consumed-output")
	cfg.Plan.Args = []string{"-test.run=^TestConsumedOutputChild$"}
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := testClient(t, cfg)
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	deadline := time.Now().Add(3 * time.Second)
	for {
		r := call(t, c, Request{Op: "status", Execution: cfg.Execution}, "ok")
		if r.Overflow {
			t.Fatal("cumulative collector discarded unread output")
		}
		if r.Offset == MaxOutput {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not fill bounded backlog")
		}
		time.Sleep(time.Millisecond)
	}
	total := 0
	for total < streamFixtureBytes {
		r := call(t, c, Request{Op: "read", Execution: cfg.Execution, Epoch: 1, Offset: total}, "ok")
		expected := make([]byte, len(r.Data))
		for i := range expected {
			at := total + i
			expected[i] = byte((at + at/251) % 251)
		}
		if r.Overflow || len(r.Data) == 0 || !bytes.Equal(r.Data, expected) || r.Offset != total+len(r.Data) {
			t.Fatal("output gap or corruption")
		}
		total = r.Offset
		call(t, c, Request{Op: "consume", Execution: cfg.Execution, Epoch: 1, Offset: total}, "ok")
	}
	r := call(t, c, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	if !r.Exited || !r.OutputEnded || r.Overflow || r.ExitCode != 0 {
		t.Fatalf("normal EOF cleanup: %+v", r)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.output) != 0 || cap(s.output) != MaxOutput {
		t.Fatal("retained allocation not fixed and empty")
	}
}

// Acknowledgements require contiguous returned bytes on this connection and
// epoch. Invalid requests cannot move the retained base or change its contents.
func TestConsumeReadAndEpochFences(t *testing.T) {
	cfg := testConfig(t, "echo")
	_, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := testClient(t, cfg)
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	call(t, c, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("abc")}, "written")
	for _, r := range []Request{
		{Offset: -1}, {Offset: 1}, {Offset: 4}, {Data: []byte("forbidden")}, {Sequence: 1}, {Digest: "forbidden"},
	} {
		r.Op, r.Execution, r.Epoch = "consume", cfg.Execution, 1
		q := call(t, c, r, "invalid_consumption")
		if q.BaseOffset != 0 {
			t.Fatal("rejected acknowledgement evicted")
		}
	}
	call(t, c, Request{Op: "read", Execution: cfg.Execution, Epoch: 1, Offset: 1}, "output_unavailable")
	r := call(t, c, Request{Op: "read", Execution: cfg.Execution, Epoch: 1}, "ok")
	if string(r.Data) != "abc" || r.Offset != 3 || !r.AtTail {
		t.Fatalf("valid read: %+v", r)
	}
	for range 2 {
		call(t, c, Request{Op: "consume", Execution: cfg.Execution, Epoch: 1, Offset: 1}, "ok")
	}
	call(t, c, Request{Op: "consume", Execution: cfg.Execution, Epoch: 1}, "invalid_consumption")
	call(t, c, Request{Op: "release", Execution: cfg.Execution, Epoch: 1}, "released")
	n := testClient(t, cfg)
	call(t, n, Request{Op: "claim", Execution: cfg.Execution, Epoch: 2}, "ok")
	call(t, c, Request{Op: "consume", Execution: cfg.Execution, Epoch: 1, Offset: 3}, "stale_controller")
	call(t, n, Request{Op: "consume", Execution: cfg.Execution, Epoch: 2, Offset: 3}, "invalid_consumption")
	r = call(t, n, Request{Op: "read", Execution: cfg.Execution, Epoch: 2, Offset: 1}, "ok")
	if string(r.Data) != "bc" {
		t.Fatal("rejections lost retained bytes")
	}
	r = call(t, n, Request{Op: "consume", Execution: cfg.Execution, Epoch: 2, Offset: 3}, "ok")
	if r.BaseOffset != 3 || r.Offset != 3 {
		t.Fatal("absolute offsets rewound")
	}
	call(t, n, Request{Op: "read", Execution: cfg.Execution, Epoch: 2, Offset: 2}, "output_unavailable")
}

// Full-backlog shutdown wakes the collector and joins the original child; it
// cannot wait forever for an acknowledgement from a closing controller.
func TestConsumedOutputCloseWhileFull(t *testing.T) {
	cfg := testConfig(t, "consumed-output")
	cfg.Plan.Args = []string{"-test.run=^TestConsumedOutputChild$"}
	cfg.IOTimeout = 100 * time.Millisecond
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := testClient(t, cfg)
	deadline := time.Now().Add(2 * time.Second)
	for {
		r := call(t, c, Request{Op: "status", Execution: cfg.Execution}, "ok")
		if r.Offset == MaxOutput {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("backlog not full")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("full collector blocked shutdown")
	}
}

// Exhausting the signed absolute offset fails explicitly before wraparound.
// The adjacent positive control reaches the largest representable tail exactly.
func TestConsumedOutputAbsoluteOffsetExhaustion(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			defer w.Close()
			maximum := int(^uint(0) >> 1)
			s := &Server{stdin: w, changed: make(chan struct{}), closed: make(chan struct{}), outputDone: make(chan struct{}), output: make([]byte, 0, MaxOutput), outputBase: maximum - 1}
			// One byte is representable. With no extra byte the reader returns EOF
			// in that same read; otherwise exhaustion must fence the next read.
			source := &terminalBytes{data: bytes.Repeat([]byte("x"), 1+extra)}
			s.wg.Add(1)
			s.collect(source)
			if s.outputBase+len(s.output) != maximum || s.overflow != (extra != 0) {
				t.Fatal("offset exhaustion wrapped or valid terminal byte refused")
			}
		})
	}
}

type terminalBytes struct{ data []byte }

func (r *terminalBytes) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}
func (*terminalBytes) Close() error { return nil }
