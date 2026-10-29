package musepty

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

func TestMusePTYTextArmStatusMapping(t *testing.T) {
	for _, tc := range []struct {
		name, terminal, text, kind string
		retryable                  bool
		status                     sm.ProviderRunStatus
		provider                   string
		recoverable                bool
		exit                       int
	}{
		{"completed_is_idle", "completed", "work awaits board acceptance", "", false, sm.ProviderRunIdle, "completed", true, 0},
		{"complete_text_is_advisory", "completed", "complete", "", false, sm.ProviderRunIdle, "completed", true, 0},
		{"blocked_line", "completed", "context\n  BLOCKED: operator decision", "", false, sm.ProviderRunBlocked, "blocked", false, 0},
		{"blocked_inline_is_not_boundary", "completed", "quoted BLOCKED: text", "", false, sm.ProviderRunIdle, "completed", true, 0},
		{"failed_blocked_text_is_not_boundary", "failed", "BLOCKED: authentication", "", false, sm.ProviderRunFailed, "unknown_failure:failed", false, 1},
		{"cancelled", "cancelled", "", "", false, sm.ProviderRunFailed, "unknown_failure:cancelled", false, 1},
		{"unknown_terminal", "futureTerminal", "", "", true, sm.ProviderRunFailed, "unknown_terminal:futureTerminal", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			b, err := json.Marshal(map[string]any{"terminal": tc.terminal, "text": tc.text})
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(home, "peer-terminal"), b, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "perform assignment"})
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tc.status || got.ProviderStatus != tc.provider || got.Recoverable != tc.recoverable || got.ExitCode != tc.exit || got.Output != tc.text {
				t.Fatalf("unexpected M2 text result: %+v; want status=%s provider=%s recoverable=%v exit=%d", got, tc.status, tc.provider, tc.recoverable, tc.exit)
			}
		})
	}
}

func TestMusePTYGoalGateUsesSameIdlePredicate(t *testing.T) {
	for _, mode := range []string{"trust", "permissions", "busy", "queued", "dialog-after-paste", "busy-after-paste"} {
		t.Run(mode, func(t *testing.T) {
			h, home := musePeer(t, mode)
			revision, _ := productionParityRevisions(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := h.DeliverGoalText(ctx, revision); err == nil {
				t.Fatal("unsafe goal accepted")
			}
			if _, err := os.Stat(filepath.Join(home, "peer-submit")); err == nil {
				t.Fatal("goal sent Enter into unsafe composer")
			}
		})
	}
}

func TestMusePTYWrappedComposerPreservesWordSpaces(t *testing.T) {
	h, _ := musePeer(t, "idle")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The peer wraps at 110 runes, within a word, at whitespace and across
	// explicit newlines. Drive the production paste and submit path.
	text := "the "
	for i := 0; i < 26; i++ {
		text += "wrapped "
	}
	text += "\nsecond line with a longwordthatcrossesaphysicalrowboundary"
	if err := h.DeliverSessionNotice(ctx, text); err != nil {
		t.Fatal(err)
	}
}

func TestMusePTYGoalContentDoesNotBecomeDialogOrBusyMark(t *testing.T) {
	h, _ := musePeer(t, "idle")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.DeliverSessionNotice(ctx, "Thinking about permissions; confirm that do you trust is text and esc to interrupt is quoted content"); err != nil {
		t.Fatal(err)
	}
}

// TestMusePTYVersionLineRefusesMalformed pins the structural probe: the host
// starts on any well-formed version answer and refuses anything the module's
// prober cannot detect. Each member below is misshapen (not merely unverified),
// so every refusal here is the probe's, reached before any PTY starts. This
// replaces the retired release-pin test: the old "1.4.0 (old)" fixture stays
// as the non-build-paren member, now refused as malformed rather than unpinned.
func TestMusePTYVersionLineRefusesMalformed(t *testing.T) {
	for _, tc := range []struct{ name, line string }{
		{"non-build-paren", "Muse Code 1.4.0 (old)\n"},
		{"bad-semver-two-components", "Muse Code 9.9 (9.9-R1.1)\n"},
		{"triple-mismatch", "Muse Code 1.4.2 (1.4.1-R4503.1)\n"},
		{"missing-paren", "Muse Code 1.4.2\n"},
		{"garbage-line", "not a version line\n"},
		{"empty-answer", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, home := musePeerPlan(t, "idle", "")
			if err := os.WriteFile(filepath.Join(home, "peer-version"), []byte(tc.line), 0600); err != nil {
				t.Fatal(err)
			}
			host, err := StartMusePTYHost(context.Background(), plan)
			if err == nil {
				_ = host.Stop(context.Background())
				t.Fatalf("malformed version line %q admitted", tc.line)
			}
		})
	}
}

// TestMusePTYHostAcceptsSyntheticNewestVersionLine proves the host imposes no
// release admission of its own: a well-formed line for a build the module has
// not verified still starts. The plan below was sealed against the verified
// peer answer and the start probe is the only version check on this path, so
// success here is the probe's structural acceptance, not seal behaviour.
func TestMusePTYHostAcceptsSyntheticNewestVersionLine(t *testing.T) {
	plan, home := musePeerPlan(t, "idle", "")
	// Synthetic, never captured: a well-formed next-release shape. The exact
	// digits are arbitrary; only the shape matters to the structural probe.
	if err := os.WriteFile(filepath.Join(home, "peer-version"), []byte("Muse Code 1.4.4 (1.4.4-R5000.1)\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	host, err := StartMusePTYHost(ctx, plan)
	if err != nil {
		t.Fatalf("well-formed newest-shaped version line refused: %v", err)
	}
	defer host.Stop(context.Background())
	if host.NativeIdentity() == "" {
		t.Fatal("started host carries no native identity")
	}
}

func TestMusePTYResumeSnapshotRefusesWrongContract(t *testing.T) {
	plan, _ := musePeerPlan(t, "idle", museTestUUID("old-session"))
	b, _ := json.Marshal(musePTYSnapshot{Contract: "task-board.muse-pty-host-v0", SessionID: museTestUUID("old-session")})
	if _, err := ResumeMusePTYHost(context.Background(), plan, sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(b)}); err == nil {
		t.Fatal("wrong snapshot contract admitted")
	}
}

func TestMusePTYComposerSpacingCannotForgeHeldText(t *testing.T) {
	v := newMuseVT(10, 120)
	v.observe([]byte(museTestScreen("foreign ab c")))
	if v.safeComposer("foreign abc", nil) {
		t.Fatal("spaces within a word silently deleted")
	}
	if !v.safeComposer("foreign ab c", nil) {
		t.Fatal("exact composer rejected")
	}
	for _, escape := range []string{"\x1b[", "\x1b[99z"} {
		v.observe([]byte(escape))
		if v.safeComposer("foreign ab c", nil) {
			t.Fatal("unknown/partial frame admitted")
		}
	}
}

func TestMusePTYPlannerEchoHasNoLocalProviderSuffix(t *testing.T) {
	plan, _ := musePeerPlan(t, "idle", "")
	argv := strings.Join(plan.plan.Argv, " ")
	if strings.Count(argv, "--provider echo") != 1 || strings.Contains(argv, "--model") || strings.Contains(argv, "--reasoning-effort") || strings.Contains(argv, "exec") {
		t.Fatalf("noninteractive echo argv %s", argv)
	}
}

func TestMusePTYMissingRunAckNeverClaimsDelivery(t *testing.T) {
	h, _ := musePeer(t, "no-ack")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.DeliverSessionNotice(ctx, "notice without run-start"); err == nil {
		t.Fatal("unobserved run ack claimed")
	}
	if err := h.DeliverSessionNotice(context.Background(), "second notice"); err != ErrMusePTYDeferred {
		t.Fatalf("pending acknowledgement admitted another submission: %v", err)
	}
}
