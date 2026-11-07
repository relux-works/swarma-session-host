//go:build t4fixture

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// This runs a standalone executable, not a Go test-helper runtime. Each case
// launches its own pinned inert protocol child through the actual T1 path.
func TestPrivateCompositionProcess(t *testing.T) {
	binary := os.Getenv("T4_PRIVATE_FIXTURE")
	if binary == "" {
		binary = filepath.Join(t.TempDir(), "t4-private-fixture")
		build := exec.Command("go", "build", "-race", "-p", "2", "-tags", "t4fixture", "-o", binary, ".")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build: %v: %s", err, out)
		}
	}
	for _, mode := range []string{"accepted", "policy-refused", "source-refused", "ack-refused", "wrong-binding", "native-never-ready", "source-revoked", "parent-canceled", "restart-refused"} {
		t.Run(mode, func(t *testing.T) {
			root, err := os.MkdirTemp("/tmp", "t4-proc-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(root) })
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, "-mode", mode, "-root", root)
			command.Env = []string{"GOMAXPROCS=2", "GORACE=atexit_sleep_ms=0", "PATH=/usr/bin:/bin"}
			out, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("executable: %v: %s", err, out)
			}
			var got struct {
				Profile         sessionhost.RuntimeProfileReport `json:"runtime_profile"`
				CallerUIDIsFake bool                             `json:"caller_uid_is_fake"`
				Mode            string                           `json:"mode"`
				Synthetic       bool                             `json:"synthetic"`
				Startup         string                           `json:"startup"`
				Observed        bool                             `json:"binding_observed"`
				Trigger         string                           `json:"trigger_result"`
				Attempts        int                              `json:"durable_attempts"`
				CreateJoined    bool                             `json:"create_joined"`
				HandoffJoined   bool                             `json:"handoff_joined"`
				SocketRemoved   bool                             `json:"runner_socket_removed"`
				Prepare         int                              `json:"source_prepare_calls"`
				Bootstrap       int                              `json:"source_bootstrap_calls"`
				Close           int                              `json:"source_close_calls"`
				RestartRefused  bool                             `json:"restart_refused_without_replay"`
			}
			if err = json.Unmarshal(out, &got); err != nil {
				t.Fatalf("result: %v: %s", err, out)
			}
			if got.Mode != mode || !got.Synthetic || !got.SocketRemoved || got.Close != 1 {
				t.Fatalf("incomplete cleanup: %s", out)
			}
			if got.CallerUIDIsFake || got.Profile.Profile != sessionhost.SingleTrustUnitProfile || got.Profile.Backend != sessionhost.PlainPathnameBackend ||
				got.Profile.ServiceUID != uint32(os.Geteuid()) || got.Profile.CallerUID != got.Profile.ServiceUID ||
				got.Profile.PerAgentIsolation || got.Profile.ProductionAdmission || got.Profile.AtomicPathnameExec || got.Profile.SealedProcfdQualified ||
				len(got.Profile.SelectedConfigurationHash) != 64 {
				t.Fatalf("incorrect runtime profile report: %s", out)
			}
			ready := mode == "accepted" || mode == "source-revoked" || mode == "parent-canceled" || mode == "restart-refused"
			if ready {
				if got.Startup != "ready" || !got.Observed || got.Trigger != "accepted" || got.Attempts != 1 || !got.CreateJoined || !got.HandoffJoined || got.Prepare != 1 || got.Bootstrap != 1 {
					t.Fatalf("missing real effect/join: %s", out)
				}
			} else if got.Startup != "refused" {
				t.Fatalf("unexpected admission: %s", out)
			}
			if mode == "restart-refused" && !got.RestartRefused {
				t.Fatal("no restart refusal proof")
			}
			wantPrepare, wantBootstrap := 1, 1
			if mode == "policy-refused" || mode == "native-never-ready" {
				wantPrepare, wantBootstrap = 0, 0
			}
			if mode == "source-refused" {
				wantBootstrap = 0
			}
			if got.Prepare != wantPrepare || got.Bootstrap != wantBootstrap {
				t.Fatalf("incorrect effect order: %s", out)
			}
			// A failed policy must never start a process. All other cases must execute
			// the actual owned child and finish cleanup before the executable returns.
			pidData, e := os.ReadFile(filepath.Join(root, "workspace/fixture-child.pid"))
			if mode == "policy-refused" {
				if !errors.Is(e, os.ErrNotExist) {
					t.Fatal("policy refusal started child")
				}
			} else {
				if e != nil {
					t.Fatal("no child process evidence", e)
				}
				pid, e := strconv.Atoi(strings.TrimSpace(string(pidData)))
				if e != nil || pid <= 0 {
					t.Fatal("invalid fixture pid")
				}
				if e = syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
					t.Fatalf("owned child remains after joined cleanup: pid=%d err=%v", pid, e)
				}
				t.Logf("owned inert child pid=%d absent after runtime join", pid)
			}
			t.Log(strings.TrimSpace(string(out)))
		})
	}
}
