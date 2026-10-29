package codexadapter

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func config() Config {
	return Config{Model: "unfamiliar-model", Provider: "unfamiliar-provider", Effort: "unfamiliar-effort", Workspace: "/tmp/synthetic"}
}

func consume(t *testing.T, p *Protocol, frame string) [][]byte {
	t.Helper()
	out, err := p.Consume([]byte(frame))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func ready(t *testing.T) *Protocol {
	t.Helper()
	p, err := New(config())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Initialize(); err != nil {
		t.Fatal(err)
	}
	out := consume(t, p, `{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos","codexHome":"/synthetic"}}`)
	if len(out) != 2 {
		t.Fatal("missing initialized/thread-start")
	}
	consume(t, p, `{"id":2,"result":{"thread":{"id":"thread-a"},"model":"unfamiliar-model","modelProvider":"unfamiliar-provider"}}`)
	if p.Snapshot().State != Idle {
		t.Fatal(p.Snapshot())
	}
	return p
}

// The release-shaped handshake waits for each reply and forwards opaque selection strings.
// These synthetic protocol frames do not qualify an actual installed harness or transport.
func TestHandshakeAndOpaqueSelection(t *testing.T) {
	p, err := New(config())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Submit("notice-reference"); err == nil {
		t.Fatal("input before handshake")
	}
	wire, err := p.Initialize()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire, []byte(`"method":"initialize"`)) {
		t.Fatal(string(wire))
	}
	if _, err := p.Initialize(); err == nil {
		t.Fatal("duplicate initialize")
	}
	out := consume(t, p, `{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos","codexHome":"/synthetic"}}`)
	if len(out) != 2 || !bytes.Contains(out[0], []byte(`"method":"initialized"`)) {
		t.Fatal(out)
	}
	var start struct {
		Params map[string]any `json:"params"`
	}
	if err := json.Unmarshal(out[1], &start); err != nil {
		t.Fatal(err)
	}
	if start.Params["model"] != config().Model || start.Params["modelProvider"] != config().Provider {
		t.Fatal(start.Params)
	}
	if _, err := p.Submit("reference"); err == nil {
		t.Fatal("input before thread reply")
	}
	consume(t, p, `{"id":2,"result":{"thread":{"id":"thread-a"},"model":"unfamiliar-model","modelProvider":"unfamiliar-provider"}}`)
	wire, err = p.Submit("notice-reference")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire, []byte(`"effort":"unfamiliar-effort"`)) {
		t.Fatal(string(wire))
	}
}

// Submission/started events cannot prove completion; only the same thread and accepted turn can.
func TestCompletionCorrelationAndSecondTurn(t *testing.T) {
	p := ready(t)
	if _, err := p.Submit("first"); err != nil {
		t.Fatal(err)
	}
	consume(t, p, `{"method":"turn/started","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"inProgress"}}}`)
	if p.Snapshot().CompletedTurn != "" {
		t.Fatal("started means completed")
	}
	consume(t, p, `{"id":3,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}`)
	if _, err := p.Submit("overlap"); err == nil {
		t.Fatal("overlap admitted")
	}
	consume(t, p, `{"method":"turn/completed","params":{"threadId":"other","turn":{"id":"turn-a","status":"completed"}}}`)
	consume(t, p, `{"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"other","status":"completed"}}}`)
	if p.Snapshot().CompletedTurn != "" || p.Snapshot().State != Busy {
		t.Fatal(p.Snapshot())
	}
	consume(t, p, `{"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"completed"}}}`)
	if p.Snapshot().State != Idle || p.Snapshot().CompletedTurn != "turn-a" {
		t.Fatal(p.Snapshot())
	}
	if _, err := p.Submit("second"); err != nil {
		t.Fatal(err)
	}
	if p.Snapshot().CompletedTurn != "" {
		t.Fatal("previous receipt reused")
	}
}

// Failed/interrupted turns and server input/approval requests never reopen automatic input.
func TestClosedStates(t *testing.T) {
	for _, status := range []string{"failed", "interrupted", "future-status"} {
		t.Run(status, func(t *testing.T) {
			p := ready(t)
			p.Submit("reference")
			consume(t, p, `{"id":3,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}`)
			_, _ = p.Consume([]byte(`{"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"` + status + `"}}}`))
			if p.Snapshot().State == Idle || p.Snapshot().CompletedTurn != "" {
				t.Fatal(p.Snapshot())
			}
			if _, err := p.Submit("retry"); err == nil {
				t.Fatal("retry after failure")
			}
		})
	}
	p := ready(t)
	p.Submit("reference")
	out := consume(t, p, `{"id":"server-request","method":"item/commandExecution/requestApproval","params":{"threadId":"thread-a"}}`)
	if len(out) != 0 || p.Snapshot().State != Approval {
		t.Fatal("approval answered or ignored")
	}
	if _, err := p.Submit("retry"); err == nil {
		t.Fatal("approval bypass")
	}
}

// Malformed, oversized, ambiguous, or wrong-ID evidence closes the protocol with bounded diagnostics.
func TestInvalidEvidence(t *testing.T) {
	for _, frame := range []string{`{`, `{ "id":999,"result":{} }`, `{"id":3,"result":{},"error":{}}`, `{"id":3,"result":{}} {}`, `{"id":99,"id":3,"result":{"turn":{"id":"a","status":"inProgress"}}}`, strings.Repeat("x", MaxFrame+1), `{"id":3,"error":{"code":1,"message":"fabricated-secret-canary"}}`} {
		p := ready(t)
		p.Submit("reference")
		out, err := p.Consume([]byte(frame))
		if err == nil || len(out) != 0 || p.Snapshot().State != Unknown {
			t.Fatalf("invalid evidence admitted: %v", err)
		}
		if strings.Contains(err.Error(), "fabricated-secret-canary") {
			t.Fatal("raw server diagnostic leaked")
		}
		if _, err := p.Submit("retry"); err == nil {
			t.Fatal("unknown replay")
		}
	}
}

// Lost transport and completion before request acceptance retain uncertainty, with no retry output.
func TestLostAndPrematureCompletion(t *testing.T) {
	for _, lost := range []bool{true, false} {
		p := ready(t)
		p.Submit("reference")
		if lost {
			p.Lost()
		} else {
			consume(t, p, `{"method":"turn/started","params":{"threadId":"thread-a","turn":{"id":"a","status":"inProgress"}}}`)
			if _, err := p.Consume([]byte(`{"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"a","status":"completed"}}}`)); err == nil {
				t.Fatal("completion before acceptance")
			}
		}
		if p.Snapshot().State != Unknown || p.Snapshot().CompletedTurn != "" {
			t.Fatal(p.Snapshot())
		}
		if _, err := p.Submit("retry"); err == nil {
			t.Fatal("uncertain delivery retried")
		}
	}
}
