package musepty

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

func TestMusePTYReal141EchoSmoke(t *testing.T) {
	binary := os.Getenv("TASK_BOARD_MUSE_SMOKE_BIN")
	if binary == "" {
		t.Skip("TASK_BOARD_MUSE_SMOKE_BIN not provided; real 1.4.1 smoke unrun")
	}
	home := t.TempDir()
	if err := os.Symlink(binary, filepath.Join(home, "muse")); err != nil {
		t.Fatal(err)
	}
	req := agentic.LaunchRequest{System: "muse", Model: agentic.Model{ID: "echo"}, WorkDir: home, Env: []string{"HOME=" + home, "PATH=" + home + ":/usr/bin:/bin", "TERM=xterm-256color", "XDG_DATA_HOME=" + filepath.Join(home, "data"), "XDG_CONFIG_HOME=" + filepath.Join(home, "config"), "XDG_CACHE_HOME=" + filepath.Join(home, "cache"), "XDG_STATE_HOME=" + filepath.Join(home, "state")}, NativeArgs: []string{"--trust-workspace"}}
	plan, err := BuildMusePTYLaunchPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	h, err := StartMusePTYHost(ctx, plan)
	if err != nil {
		if h != nil {
			t.Logf("startup raw=%q", h.replay)
			_ = h.Stop(context.Background())
		}
		_ = filepath.WalkDir(home, func(path string, d os.DirEntry, e error) error {
			if e == nil && d.Name() == "session.jsonl" {
				b, _ := os.ReadFile(path)
				t.Logf("isolated startup log %s: %s", path, b)
			}
			return nil
		})
		t.Fatal(err)
	}
	defer h.Stop(context.Background())
	t.Cleanup(func() {
		if dir := os.Getenv("TASK_BOARD_MUSE_EVIDENCE_DIR"); dir != "" {
			if b, e := os.ReadFile(h.snapshot.LogPath); e == nil {
				if e = os.WriteFile(filepath.Join(dir, t.Name()+".jsonl"), b, 0600); e != nil {
					t.Error(e)
				}
			}
		}
		if t.Failed() {
			h.mu.Lock()
			t.Logf("VT unknown=%v pending=%q screen=%q", h.vt.unknown, h.vt.pending, strings.Join(h.vt.lines(), "\n"))
			t.Logf("raw=%q", h.replay)
			h.mu.Unlock()
			if b, e := os.ReadFile(h.snapshot.LogPath); e == nil {
				t.Logf("session log: %s", b)
			}
		}
	})
	museSmokeBoundedWait(t, ctx, "real TUI startup idle", func() bool { return h.idle("") })
	revision, _ := productionParityRevisions(t)
	if err := h.DeliverGoalText(ctx, revision); err != nil {
		t.Fatal("real wrapped board goal: ", err)
	}
	museSmokeBoundedWait(t, ctx, "real TUI idle after goal text", func() bool { return h.idle("") })
	result, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "R2 offline echo smoke"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != sm.ProviderRunIdle || !strings.Contains(result.Output, "R2 offline echo smoke") {
		t.Fatalf("echo result %+v", result)
	}
	snapshot, err := museTestSnapshot(h)
	if err != nil {
		t.Fatal(err)
	}
	id := h.NativeIdentity()
	stream, err := h.AttachTerminal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Close()
	if state, _ := h.State(ctx); state != sm.ProviderHostRunning {
		t.Fatal("detach stopped real host")
	}
	_ = h.Stop(ctx)
	req.NativeArgs = []string{"resume", id, "--trust-workspace"}
	resumePlan, err := BuildMusePTYLaunchPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := ResumeMusePTYHost(ctx, resumePlan, sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(snapshot)})
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Stop(context.Background())
	t.Cleanup(func() {
		resumed.mu.Lock()
		if t.Failed() {
			t.Logf("resume VT unknown=%v pending=%q screen=%q", resumed.vt.unknown, resumed.vt.pending, strings.Join(resumed.vt.lines(), "\n"))
		}
		if dir := os.Getenv("TASK_BOARD_MUSE_EVIDENCE_DIR"); dir != "" {
			if e := os.WriteFile(filepath.Join(dir, t.Name()+"-resume.vt"), resumed.replay, 0600); e != nil {
				t.Error(e)
			}
		}
		resumed.mu.Unlock()
		if t.Failed() {
			f, e := resumed.fold()
			t.Logf("resume fold=%+v err=%v", f, e)
		}
	})
	if resumed.NativeIdentity() != id {
		t.Fatal("real resume identity changed")
	}
	museSmokeBoundedWait(t, ctx, "resumed real TUI idle", func() bool { return resumed.idle("") })
	result, err = resumed.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "R2 resumed echo smoke"})
	if err != nil || result.Status != sm.ProviderRunIdle || !strings.Contains(result.Output, "R2 resumed echo smoke") {
		t.Fatalf("resumed echo %+v: %v", result, err)
	}
	t.Logf("real pinned 1.4.1 echo + detach + resume + resumed turn; outcome=%s", result.Status)
}
