package sessionhost_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

func awaitDebugSkeleton(t *testing.T, h *sessionhost.Host, ready func(sessionhost.State) bool) sessionhost.State {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		s := h.Snapshot()
		if ready(s) {
			return s
		}
		select {
		case <-ctx.Done():
			t.Fatalf("state wait: %v %+v", ctx.Err(), s)
		case <-ticker.C:
		}
	}
}
func TestAdmissionResumeImmutabilityHostSkeleton(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "enabled"}[enabled], func(t *testing.T) {
			cfg, _, _ := setupProtocol(t, "normal")
			if enabled {
				cfg.DebugAttach = &enabled
			}
			h := openHost(t, cfg)
			awaitDebugSkeleton(t, h, func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
			before := inspectRunner(t, cfg)
			h.Close()
			changed := !enabled
			cfg.DebugAttach = &changed
			_, err := sessionhost.Open(cfg)
			if err == nil || err.Error() != "attach_admission_conflict" {
				t.Fatalf("changed bit on replacement: %v", err)
			}
			cfg.DebugAttach = nil
			replacement := openHost(t, cfg)
			restored := replacement.Snapshot()
			if restored.DebugAttach != enabled {
				t.Fatal("replacement changed bit")
			}
			if enabled && (restored.Debug.Writer != "held" || restored.Debug.WriterRevision == 0) {
				t.Fatal("replacement writer not fenced")
			}
			after := inspectRunner(t, cfg)
			if before.Instance != after.Instance || before.PID != after.PID || before.Writes != after.Writes {
				t.Fatal("replacement mutated runtime")
			}
		})
	}
}
func TestTerminalFactBlocksQueuedNoticeHostSkeleton(t *testing.T) {
	cfg, _, _ := setupProtocol(t, "normal")
	on := true
	cfg.DebugAttach = &on
	h := openHost(t, cfg)
	awaitDebugSkeleton(t, h, func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	for _, id := range []string{"one", "two"} {
		if _, err := h.Enqueue(sessionhost.Notice{ID: id, Reference: "synthetic/" + id}); err != nil {
			t.Fatal(err)
		}
	}
	state := awaitDebugSkeleton(t, h, func(s sessionhost.State) bool {
		return len(s.Records) == 2 && s.Records[0].State == "applied" && len(s.Debug.Results) == 1
	})
	if state.Records[1].State != "queued" || state.Debug.Results[0].Permitted || !state.Debug.Results[0].NotificationPending {
		t.Fatal("terminal failed to block queued notice")
	}
	if status := inspectRunner(t, cfg); status.Writes != 4 {
		t.Fatalf("queued notice overtook result: %d", status.Writes)
	}
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "debug-attach", "input.journal"))
	if err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{`"actor":"host"`, `"role":"host"`, `"kind":"protocol_control"`, `"kind":"notice"`, `"kind":"terminal_observed"`} {
		if !bytes.Contains(data, []byte(label)) {
			t.Fatalf("journal missing %s", label)
		}
	}
	h.Close()
	cfg.DebugAttach = nil
	next := openHost(t, cfg)
	if !next.Snapshot().DebugAttach || len(next.Snapshot().Debug.Results) != 1 || next.Snapshot().Debug.Results[0].Permitted || inspectRunner(t, cfg).Writes != 4 {
		t.Fatal("replacement lost terminal block")
	}
}
