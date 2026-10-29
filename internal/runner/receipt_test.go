package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func receiptRequest(cfg Config, data []byte) Request {
	digest := sha256.Sum256(data)
	return Request{Op: "write-receipt", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Digest: hex.EncodeToString(digest[:])}
}

// Missing, conflicting, malformed and unauthorized queries never reserve a
// sequence or write bytes. The nearby valid write remains deliverable once.
func TestWriteReceiptHasNoWriteEffects(t *testing.T) {
	cfg := testConfig(t, "echo")
	server, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	owner, other := testClient(t, cfg), testClient(t, cfg)
	claim := call(t, owner, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	if claim.ProtocolVersion != ProtocolVersion || claim.WriteReceiptVersion != 1 {
		t.Fatal("receipt capability missing")
	}
	req := receiptRequest(cfg, []byte("one\n"))
	call(t, other, req, "stale_controller")
	bad := req
	bad.Epoch = 0
	call(t, owner, bad, "stale_controller")
	bad = req
	bad.Execution = "another"
	call(t, owner, bad, "wrong_execution")
	for _, mutate := range []func(*Request){
		func(r *Request) { r.Digest = "invalid" },
		func(r *Request) { r.Sequence = 0 },
		func(r *Request) { r.Sequence = MaxWrites + 1 },
		func(r *Request) { r.Data = []byte("forbidden\n") },
		func(r *Request) { r.Offset = 1 },
	} {
		bad = req
		mutate(&bad)
		call(t, owner, bad, "invalid_receipt")
	}
	missing := call(t, owner, req, "write_missing")
	if missing.Writes != 0 || missing.Offset != 0 {
		t.Fatal("query changed write or output state")
	}
	call(t, owner, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("one\n")}, "written")
	for range 2 {
		got := call(t, owner, req, "written")
		if got.Writes != 1 {
			t.Fatal("receipt repeated the effect")
		}
	}
	bad = receiptRequest(cfg, []byte("different\n"))
	call(t, owner, bad, "sequence_conflict")
	call(t, owner, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	out := call(t, owner, Request{Op: "read", Execution: cfg.Execution, Epoch: 1}, "ok")
	if string(out.Data) != "one\n" {
		t.Fatal("receipt changed child output")
	}
}

// An actual failed pipe write stays unknown. A read-only query cannot make that
// effect written, missing or replayable, and a changed digest still conflicts.
func TestWriteReceiptPreservesUnknown(t *testing.T) {
	cfg := testConfig(t, "echo")
	server, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	owner := testClient(t, cfg)
	call(t, owner, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	server.stdin.Close() // Isolated owned fixture: force the intended pipe failure.
	data := []byte("uncertain\n")
	call(t, owner, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: data}, "write_unknown")
	req := receiptRequest(cfg, data)
	for range 2 {
		if got := call(t, owner, req, "write_unknown"); got.Writes != 1 {
			t.Fatal("unknown receipt changed")
		}
	}
	call(t, owner, receiptRequest(cfg, []byte("different\n")), "sequence_conflict")
}
