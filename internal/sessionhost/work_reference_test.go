package sessionhost_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// Work mode uses the same real private endpoint epoch check: stale enqueue,
// hold/release and stop requests have no effects; a current request delivers.
func TestWorkReferenceControlEpoch(t *testing.T) {
	cfg, _, dir := setupProtocolNoticeMode(t, "normal", sessionhost.WorkReference)
	h := openHost(t, cfg)
	state := awaitProtocol(t, h, "idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	endpoint := filepath.Join(dir, "host.sock")
	listener, err := localipc.Listen(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sessionhost.ServeListener(ctx, listener, h, time.Second) }()
	t.Cleanup(func() {
		cancel()
		listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	for _, op := range []sessionhost.ControlOperation{"enqueue", "hold", "release", "stop"} {
		callCtx, stop := context.WithTimeout(context.Background(), time.Second)
		r, err := sessionhost.Call(callCtx, endpoint, sessionhost.Request{Op: op, Execution: cfg.Execution, Epoch: state.Epoch - 1, Notice: sessionhost.Notice{ID: "one", Reference: "task/one"}})
		stop()
		if err != nil || r.Code != "stale_session" || r.State.Sequence != 0 {
			t.Fatal("stale work mutation accepted", err, r.Code)
		}
	}
	if inspectRunner(t, cfg).Writes != 3 {
		t.Fatal("stale work request wrote")
	}
	callCtx, stop := context.WithTimeout(context.Background(), time.Second)
	r, err := sessionhost.Call(callCtx, endpoint, sessionhost.Request{Op: "enqueue", Execution: cfg.Execution, Epoch: state.Epoch, Notice: sessionhost.Notice{ID: "one", Reference: "task/one"}})
	stop()
	if err != nil || r.Code != "ok" {
		t.Fatal("current work control refused", err, r.Code)
	}
	await(t, h, "one", "applied")
	if inspectRunner(t, cfg).Writes != 4 {
		t.Fatal("wrong work effect count")
	}
}

// Both queued and accepted-but-incomplete intents survive same-mode replacement;
// either cross-mode reopen refuses before the driver factory/runner claim, leaves
// journal bytes untouched, and cannot reinterpret or resend the original effect.
func TestNoticeModeReplacementRefusal(t *testing.T) {
	for _, mode := range []string{sessionhost.SyntheticNotice, sessionhost.WorkReference} {
		for _, stage := range []string{"queued", "unknown"} {
			t.Run(mode+"/"+stage, func(t *testing.T) {
				cfg, _, dir := setupProtocolNoticeMode(t, "gated", mode)
				h := openHost(t, cfg)
				awaitProtocol(t, h, "idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
				if stage == "queued" {
					if err := h.Fence(false); err != nil {
						t.Fatal(err)
					}
				}
				n := sessionhost.Notice{ID: "one", Reference: "task/one"}
				if _, err := h.Enqueue(n); err != nil {
					t.Fatal(err)
				}
				if stage == "unknown" {
					awaitProtocol(t, h, "accepted", func(s sessionhost.State) bool { return s.Records[0].NativeAccepted })
				}
				before := h.Snapshot()
				if before.Records[0].State != stage {
					t.Fatal("wrong fixture stage")
				}
				if mode == sessionhost.WorkReference && (before.Version != 3 || before.NoticeMode != mode) {
					t.Fatal("work state not explicitly versioned")
				}
				h.Close()
				journal := filepath.Join(cfg.StateDir, "state.json")
				saved, err := os.ReadFile(journal)
				if err != nil {
					t.Fatal(err)
				}
				bad := cfg
				bad.NoticeMode = sessionhost.WorkReference
				if mode == sessionhost.WorkReference {
					bad.NoticeMode = sessionhost.SyntheticNotice
				}
				var calls atomic.Int32
				bad.ProtocolFactory = func([]byte) (ap.Driver, uint64, error) { calls.Add(1); return nil, 0, nil }
				if other, err := sessionhost.Open(bad); err == nil || err.Error() != "stored_plan_conflict" {
					if other != nil {
						other.Close()
					}
					t.Fatal("cross-mode reopen", err)
				}
				after, err := os.ReadFile(journal)
				if err != nil || !bytes.Equal(saved, after) || calls.Load() != 0 {
					t.Fatal("mismatch changed state or invoked driver")
				}
				next := openHost(t, cfg)
				restored := awaitProtocol(t, next, "restored state", func(s sessionhost.State) bool { return s.InputState != sessionhost.InputUnknown })
				if restored.RunnerInstance != before.RunnerInstance || restored.Epoch != before.Epoch+1 || restored.Records[0].State != stage {
					t.Fatal("same-mode restore changed intent or runner")
				}
				if _, err := next.Enqueue(n); err != nil {
					t.Fatal("exact retry refused", err)
				}
				if _, err := next.Enqueue(sessionhost.Notice{ID: n.ID, Reference: "task/changed"}); err == nil || err.Error() != "request_conflict" {
					t.Fatal("changed intent accepted", err)
				}
				if stage == "queued" {
					if inspectRunner(t, cfg).Writes != 3 {
						t.Fatal("held notice was written")
					}
					if err := next.Fence(true); err != nil {
						t.Fatal(err)
					}
					awaitProtocol(t, next, "accepted after release", func(s sessionhost.State) bool { return s.Records[0].NativeAccepted })
				}
				if err := os.WriteFile(filepath.Join(dir, "complete-turn-3"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				await(t, next, "one", "applied")
				if inspectRunner(t, cfg).Writes != 4 {
					t.Fatal("replacement/retry duplicated a write")
				}
			})
		}
	}
}

// A factory that declares another rendering mode, or omits the explicit opt-in,
// is rejected before native initialization even if it supplies a valid driver.
func TestWorkReferenceFactoryModeConflict(t *testing.T) {
	cfg, _, _ := setupProtocolNoticeMode(t, "normal", sessionhost.WorkReference)
	h := openHost(t, cfg)
	awaitProtocol(t, h, "idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	h.Close()
	path := filepath.Join(cfg.StateDir, "state.json")
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, missing := range []bool{false, true} {
		bad := cfg
		bad.ProtocolFactory = func(cp []byte) (ap.Driver, uint64, error) {
			if missing {
				d, o, e := cfg.ProtocolFactory(cp)
				return struct{ ap.Driver }{d}, o, e
			}
			// A misconfigured factory ignores the saved work checkpoint. The
			// host's separate mode check must still catch it before Begin.
			return codexadapter.OpenDriver(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: cfg.Selection.Vendor, Model: cfg.Selection.Model, Effort: cfg.Selection.Reasoning}, Workspace: filepath.Dir(cfg.StateDir)}, nil)
		}
		if other, err := sessionhost.Open(bad); err == nil || err.Error() != "protocol_notice_mode_conflict" {
			if other != nil {
				other.Close()
			}
			t.Fatal("factory mismatch accepted", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, saved) {
			t.Fatal("factory mismatch changed journal")
		}
	}
	next := openHost(t, cfg)
	awaitProtocol(t, next, "valid factory restore", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if inspectRunner(t, cfg).Writes != 3 {
		t.Fatal("factory mismatch wrote initialization")
	}
}

// An unsupported tool item on the actual driver/runner/host path is visible as
// a durable, content-free refusal, never completed work. Adjacent completion and
// host replacement cannot erase it or send another notice. Valid work turns are
// exercised by TestProtocolHostAcceptanceAndReplacement/work-reference.
func TestWorkReferenceRefusalIsVisible(t *testing.T) {
	cfg, _, _ := setupProtocolNoticeMode(t, "unsupported-work-item", sessionhost.WorkReference)
	h := openHost(t, cfg)
	awaitProtocol(t, h, "idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "task/one"}); err != nil {
		t.Fatal(err)
	}
	state := awaitProtocol(t, h, "visible adapter refusal", func(s sessionhost.State) bool { return s.Protocol.Refusal == "adapter_observation_refused" })
	if state.Fence != "unknown" || state.InputState != sessionhost.InputUnknown || state.Records[0].State != "unknown" || state.Records[0].Outcome != "" {
		t.Fatal("refused item became completed work")
	}
	h.Close()
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
	if err != nil || bytes.Contains(data, []byte("private-refused-tool-canary")) {
		t.Fatal("refusal leaked tool output")
	}
	next := openHost(t, cfg)
	if next.Snapshot().Protocol.Refusal != "adapter_observation_refused" {
		t.Fatal("replacement erased refusal")
	}
	if _, err := next.Enqueue(sessionhost.Notice{ID: "two", Reference: "task/two"}); err != nil {
		t.Fatal(err)
	}
	if err := next.Fence(true); err == nil {
		t.Fatal("refusal reopened work")
	}
	if inspectRunner(t, cfg).Writes != 4 || next.Snapshot().Records[1].State != "queued" {
		t.Fatal("refusal retried or wrote next work")
	}
}

// Work state requires explicit mode/version and a mode-bound digest. Plants are
// isolated fixture journals; each refusal preserves its bytes and emits no write.
// The unmodified work journal restores with the same runner as the valid control.
func TestWorkReferenceStoredModeAndDigest(t *testing.T) {
	cfg, _, _ := setupProtocolNoticeMode(t, "normal", sessionhost.WorkReference)
	h := openHost(t, cfg)
	awaitProtocol(t, h, "idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if err := h.Fence(false); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "task/one"}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	path := filepath.Join(cfg.StateDir, "state.json")
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, plant := range []string{"missing-mode", "unknown-mode", "old-version", "changed-mode", "synthetic-digest"} {
		var s map[string]json.RawMessage
		if json.Unmarshal(saved, &s) != nil {
			t.Fatal("fixture decode")
		}
		switch plant {
		case "missing-mode":
			delete(s, "notice_mode")
		case "unknown-mode":
			s["notice_mode"] = json.RawMessage(`"future"`)
		case "old-version":
			s["version"] = json.RawMessage(`1`)
		case "changed-mode":
			s["notice_mode"] = json.RawMessage(`"synthetic"`)
		case "synthetic-digest":
			// Relabel the whole journal as legacy; its work digest must still
			// prevent reinterpretation of the queued notice as a synthetic one.
			delete(s, "notice_mode")
			s["version"] = json.RawMessage(`1`)
		}
		bad, _ := json.Marshal(s)
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		candidate := cfg
		if plant == "synthetic-digest" {
			candidate.NoticeMode = sessionhost.SyntheticNotice
		}
		if other, err := sessionhost.Open(candidate); err == nil || err.Error() != "state_corrupt" {
			if other != nil {
				other.Close()
			}
			t.Fatalf("%s accepted: %v", plant, err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, bad) {
			t.Fatal("refused journal changed")
		}
	}
	if err := os.WriteFile(path, saved, 0600); err != nil {
		t.Fatal(err)
	}
	next := openHost(t, cfg)
	awaitProtocol(t, next, "same-mode idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if inspectRunner(t, cfg).Writes != 3 || next.Snapshot().Records[0].State != "queued" {
		t.Fatal("restore bypassed hold")
	}
}

// Unknown mode and a work mode on a legacy adapter refuse before creating state
// or touching a runner; the ordinary synthetic config remains the valid control.
func TestUnsupportedNoticeMode(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.Gated{Adapter: fixtureadapters.JSON{}, State: sessionhost.InputIdle}, false)
	for _, mode := range []string{"future-mode", sessionhost.WorkReference} {
		bad := cfg
		bad.NoticeMode = mode
		if h, err := sessionhost.Open(bad); err == nil || err.Error() != "unsupported_notice_mode" {
			if h != nil {
				h.Close()
			}
			t.Fatal("unsupported mode admitted", err)
		}
		if _, err := os.Lstat(cfg.StateDir); !os.IsNotExist(err) {
			t.Fatal("refusal created state")
		}
	}
	h := openHost(t, cfg)
	if h.Snapshot().Version != 1 || h.Snapshot().NoticeMode != "" {
		t.Fatal("legacy default changed")
	}
}
