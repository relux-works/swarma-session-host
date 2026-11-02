package sessionhost_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/codexadapter"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// Six maximum legal frames cross the retained bound without deadlock or loss of
// final completion. An oversized no-LF frame must instead fence input promptly.
// Both use an inert Go child and real pipes, not a native/model invocation.
func TestProtocolConsumedOutputBounds(t *testing.T) {
	for _, mode := range []string{"long-output", "oversized-output"} {
		t.Run(mode, func(t *testing.T) {
			cfg, _, dir := setupProtocolNoticeMode(t, mode, sessionhost.WorkReference)
			h := openHost(t, cfg)
			awaitProtocol(t, h, "ready", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
			if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
				t.Fatal(err)
			}
			if mode == "long-output" {
				await(t, h, "one", "applied")
				deadline := time.Now().Add(time.Second)
				for {
					r := inspectRunner(t, cfg)
					if r.BaseOffset > 6*codexadapter.MaxFrame {
						if r.Overflow || r.Offset-r.BaseOffset > runner.MaxOutput {
							t.Fatal("invalid retained window")
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("durable completion was not consumed")
					}
					time.Sleep(time.Millisecond)
				}
			} else {
				awaitProtocol(t, h, "oversize refusal", func(s sessionhost.State) bool {
					return s.Fence == "unknown" && s.Protocol.Refusal == "adapter_observation_refused"
				})
				if h.Snapshot().Records[0].State != "unknown" || inspectRunner(t, cfg).Writes != 4 {
					t.Fatal("oversize admitted completion or extra input")
				}
			}
			raw, err := os.ReadFile(filepath.Join(dir, "state", "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(raw, []byte("private-output-canary")) {
				t.Fatal("private protocol body persisted")
			}
		})
	}
}

// Replacement replays an uncommitted partial suffix from the durable complete
// frame frontier; the latest in-memory cursor must never evict those bytes.
func TestProtocolConsumedPartialReplacement(t *testing.T) {
	cfg, _, dir := setupProtocolNoticeMode(t, "partial-output", sessionhost.WorkReference)
	h := openHost(t, cfg)
	awaitProtocol(t, h, "ready", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
		t.Fatal(err)
	}
	awaitProtocol(t, h, "partial retained", func(s sessionhost.State) bool {
		r := inspectRunner(t, cfg)
		return len(s.Records) == 1 && s.InputState == sessionhost.InputUnknown && r.Offset-r.BaseOffset == 32768
	})
	before := inspectRunner(t, cfg)
	raw, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("private-output-canary")) {
		t.Fatal("partial bytes leaked into checkpoint")
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	n := openHost(t, cfg)
	awaitProtocol(t, n, "replayed partial", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputUnknown })
	if r := inspectRunner(t, cfg); r.BaseOffset != before.BaseOffset || r.Writes != 4 {
		t.Fatal("replacement evicted partial or resent input")
	}
	if err := os.WriteFile(filepath.Join(dir, "finish-partial"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, n, "one", "applied")
	if r := inspectRunner(t, cfg); r.PID != before.PID || r.Instance != before.Instance || r.Writes != 4 {
		t.Fatal("replacement changed child or duplicated input")
	}
}

// Lost consume replies on either side of the effect preserve the already
// durable checkpoint. Replacement reads any retained durable prefix and catches
// up without replaying initialization or the work notice.
func TestProtocolConsumeReplyLoss(t *testing.T) {
	for _, fault := range []string{"before", "after"} {
		t.Run(fault, func(t *testing.T) {
			cfg, _, dir := setupProtocolNoticeMode(t, "gated", sessionhost.WorkReference)
			direct := cfg
			proxy := offlineProxyStart(t, dir, cfg.Socket, 0)
			cfg.Socket = proxy.listener.Addr().String()
			h := openHost(t, cfg)
			awaitProtocol(t, h, "ready", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
			if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
				t.Fatal(err)
			}
			awaitProtocol(t, h, "acceptance", func(s sessionhost.State) bool { return len(s.Records) == 1 && s.Records[0].NativeAccepted })
			// Wait for its acknowledgement before arming the completion boundary.
			deadline := time.Now().Add(time.Second)
			for {
				r := inspectRunner(t, direct)
				if r.BaseOffset == r.Offset {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("acceptance not consumed")
				}
				time.Sleep(time.Millisecond)
			}
			proxy.mu.Lock()
			proxy.consumeFault = fault
			proxy.consumeFaultDone = make(chan struct{})
			hit := proxy.consumeFaultDone
			proxy.mu.Unlock()
			if err := os.WriteFile(filepath.Join(dir, "complete-turn-3"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			select {
			case <-hit:
			case <-time.After(3 * time.Second):
				t.Fatal("consume fault did not reach intended boundary")
			}
			awaitProtocol(t, h, "lost ack fence", func(s sessionhost.State) bool { return s.Fence == "unknown" })
			saved, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			var state sessionhost.State
			if err := json.Unmarshal(saved, &state); err != nil {
				t.Fatal(err)
			}
			_, frontier, err := cfg.ProtocolFactory(state.Protocol.Checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			r := inspectRunner(t, direct)
			if frontier == 0 || uint64(r.BaseOffset) > frontier || (fault == "before" && uint64(r.BaseOffset) == frontier) || (fault == "after" && uint64(r.BaseOffset) != frontier) {
				t.Fatal("fault did not bracket actual durable consume")
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			n := openHost(t, cfg)
			awaitProtocol(t, n, "replacement ready", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
			if s := n.Snapshot(); s.Records[0].State != "applied" || inspectRunner(t, direct).Writes != 4 {
				t.Fatal("consume loss lost completion or duplicated input")
			}
		})
	}
}

// A real pending-write marker rejects the next checkpoint commit. Completion
// bytes stay retained even though the in-memory driver has observed them.
func TestProtocolConsumeWaitsForDurability(t *testing.T) {
	cfg, _, dir := setupProtocolNoticeMode(t, "gated", sessionhost.WorkReference)
	h := openHost(t, cfg)
	awaitProtocol(t, h, "ready", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
		t.Fatal(err)
	}
	awaitProtocol(t, h, "accepted", func(s sessionhost.State) bool { return len(s.Records) == 1 && s.Records[0].NativeAccepted })
	deadline := time.Now().Add(time.Second)
	var before runner.Response
	for {
		before = inspectRunner(t, cfg)
		if before.BaseOffset == before.Offset {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("acceptance not consumed")
		}
		time.Sleep(time.Millisecond)
	}
	raw, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.StateDir, "state.pending"), []byte("test-control"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "complete-turn-3"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	awaitProtocol(t, h, "persistence failure", func(s sessionhost.State) bool { return s.Fence == "unknown" })
	after := inspectRunner(t, cfg)
	disk, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if after.BaseOffset != before.BaseOffset || after.Offset <= before.Offset || !bytes.Equal(raw, disk) {
		t.Fatal("failed durability evicted output or changed committed state")
	}
}
