package sessionmanager

import (
	"context"
	"errors"
	"testing"

	"github.com/relux-works/swarma-session-host/sessionproxy"
)

// The adapter uses the actual runner gate shared with RPC clients, preserves
// known reads/refused turns and uncertain disconnect state, and fences each
// physical send by its captured durable lease. No provider process is started.
func TestCodexAttachmentSharesGateAndCapturedWriteLease(t *testing.T) {
	ctx := context.Background()
	h, _, _, q, _ := attachmentProbe(t)
	a, e := h.Attach(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	lease, _, e := h.Input(ctx, a.Session.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	g := &codexTurnGate{threadID: "native-thread", state: ProviderActivityIdle}
	runner := &codexRunner{gate: g, snapshot: CodexRunnerSnapshot{CodexHome: "/fixture/home", ThreadID: "native-thread"}}
	if runner.ClientHome() != "/fixture/home" || runner.NativeIdentity() != "native-thread" {
		t.Fatal("actual runner metadata lost")
	}
	relay := hostLeasedRelayGate{runner.attachmentRelayGate(), h.epochs, lease}.Client()
	writes := 0
	send := func() error { writes++; return nil }
	g.mu.Lock()
	g.fenced = true
	g.mu.Unlock()
	if e = relay.Write(ctx, []byte(`{"id":1,"method":"thread/read","params":{}}`), true, send); e != nil || writes != 1 {
		t.Fatalf("known read refused: %v", e)
	}
	if e = relay.Write(ctx, []byte(`{"id":2,"method":"turn/start","params":{}}`), true, send); !errors.Is(e, sessionproxy.ErrTurnFenced) || !errors.Is(e, errCodexTurnFenced) || writes != 1 {
		t.Fatalf("shared retirement gate bypassed: %v writes%d", e, writes)
	}
	g.mu.Lock()
	g.fenced = false
	g.mu.Unlock()
	if e = relay.Write(ctx, []byte(`{"id":3,"method":"turn/start","params":{}}`), true, send); e != nil || writes != 2 {
		t.Fatal("valid turn refused", e)
	}
	relay.Lost()
	g.mu.Lock()
	pending := len(g.pending)
	state := g.activityLocked().State
	g.mu.Unlock()
	if pending != 1 || state != ProviderActivityUnknown {
		t.Fatal("disconnect cleared uncertain pending write")
	}
	if _, e = h.epochs.reserveContext(ctx, lease.SessionID); e != nil {
		t.Fatal(e)
	}
	if e = relay.Write(ctx, []byte(`{"id":4,"method":"thread/read","params":{}}`), true, send); !errors.Is(e, ErrStaleInputController) || writes != 2 {
		t.Fatalf("old controller wrote after replacement: %v writes%d", e, writes)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.pending) != pending {
		t.Fatal("refused physical send changed gate accounting")
	}
}
