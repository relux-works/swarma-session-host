//go:build unix

package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func repairFixture(t *testing.T) (*SessionHost, context.Context) {
	t.Helper()
	h, ctx := controlFixture(t)
	l := &h.options.Layout
	l.AttachmentsDir = filepath.Join(filepath.Dir(l.SocketPath), "a")
	if e := os.MkdirAll(l.AttachmentsDir, 0700); e != nil {
		t.Fatal(e)
	}
	return h, ctx
}
func repairSocket(t *testing.T, path string, live bool) net.Listener {
	t.Helper()
	l, e := net.Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	t.Cleanup(func() { _ = l.Close(); _ = os.Remove(path) })
	if !live {
		if e = l.Close(); e != nil {
			t.Fatal(e)
		}
	}
	return l
}

// Original endpoint/socket sweep keeps listening neighbors and ordinary files,
// removes only stale metadata/proxy sockets, and uses empty-directory Remove.
// All paths are isolated fixture roots; no host namespace is swept.
func TestDoctorRemovesDeadSiblingEndpointButPreservesLiveEndpoint(t *testing.T) {
	h, ctx := repairFixture(t)
	l := h.options.Layout
	root := filepath.Dir(l.StateDir)
	socketRoot := filepath.Dir(l.SocketPath)
	for _, live := range []bool{false, true} {
		name := strings.Repeat("a", 64)
		if live {
			name = strings.Repeat("b", 64)
		}
		dir := filepath.Join(root, name)
		if e := os.MkdirAll(dir, 0700); e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(socketRoot, name[:1]+".sock")
		repairSocket(t, path, live)
		raw, _ := json.Marshal(map[string]string{"board_fingerprint": name, "socket_path": path})
		if e := writePrivateFile(filepath.Join(dir, endpointFileName), raw); e != nil {
			t.Fatal(e)
		}
		if !live {
			if e := writePrivateFile(filepath.Join(dir, tokenFileName), []byte("fixture-only")); e != nil {
				t.Fatal(e)
			}
		}
	}
	report, e := h.Doctor(ctx)
	if e != nil || report.Current.StaleEndpointsRemoved != 1 || report.Current.LiveEndpointsPreserved != 1 || report.Current.StaleSocketsRemoved != 1 || report.Current.LiveSocketsPreserved < 1 {
		t.Fatalf("repair %+v %v", report, e)
	}
	for _, p := range []string{filepath.Join(root, strings.Repeat("a", 64), endpointFileName), filepath.Join(root, strings.Repeat("a", 64), tokenFileName)} {
		if _, e = os.Stat(p); !errors.Is(e, os.ErrNotExist) {
			t.Fatalf("dead metadata retained: %v", e)
		}
	}
	if _, e = os.Stat(filepath.Join(root, strings.Repeat("b", 64), endpointFileName)); e != nil {
		t.Fatal("live endpoint removed", e)
	}
	empty := filepath.Join(socketRoot, "attachments-empty")
	occupied := filepath.Join(socketRoot, "attachments-occupied")
	for _, p := range []string{empty, occupied} {
		if e = os.Mkdir(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	if e = writePrivateFile(filepath.Join(occupied, "ordinary"), []byte("keep")); e != nil {
		t.Fatal(e)
	}
	report, e = h.Doctor(ctx)
	if e != nil || report.Current.StaleAttachmentDirsRemoved < 1 {
		t.Fatalf("empty GC %+v %v", report, e)
	}
	if _, e = os.Stat(occupied); e != nil {
		t.Fatal("nonempty directory removed", e)
	}
}

// Actual doctor HTTP calls the same runtime sweep and startup report. It does
// not synthesize counts from a successful response or exercise a live daemon.
func TestDoctorControlRoundTripReportsRepairs(t *testing.T) {
	h, ctx := repairFixture(t)
	startup, e := h.RepairStartup(ctx)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(h.options.Layout.AttachmentsDir, "stale.sock")
	repairSocket(t, path, false)
	handler, e := NewProtocol8Handler(h, func() ControlStatus { return ControlStatus{} }, nil)
	if e != nil {
		t.Fatal(e)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("POST", "/v1/doctor", nil))
	var report DoctorReport
	if e = json.Unmarshal(w.Body.Bytes(), &report); e != nil || w.Code != 200 || report.Current.StaleSocketsRemoved != 1 || report.Startup != startup {
		t.Fatalf("actual doctor %d %+v %v", w.Code, report, e)
	}
}

// A subprocess is an explicitly owned idle test helper, not a provider/worker.
// The exact command-line identity makes the unknown-liveness guard observable;
// the neighboring orphan control really terminates and reaps only that helper.
func TestRepairOwnedProcessFixture(t *testing.T) {
	if os.Getenv("SESSIONHOST_REPAIR_FIXTURE") != "1" {
		return
	}
	time.Sleep(20 * time.Second)
}
func repairOwnedProcess(t *testing.T, id string) (*exec.Cmd, chan error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRepairOwnedProcessFixture$", "--", id)
	cmd.Env = append(os.Environ(), "SESSIONHOST_REPAIR_FIXTURE=1")
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	birth, e := os.ReadFile(filepath.Join("/proc", fmtInt(cmd.Process.Pid), "stat"))
	if e != nil {
		t.Fatal(e)
	}
	fields := strings.Fields(string(birth)[strings.LastIndex(string(birth), ")")+1:])
	t.Logf("owned fixture PID=%d birth=%s", cmd.Process.Pid, fields[19])
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("owned fixture not reaped")
		}
	})
	return cmd, done
}
func fmtInt(n int) string { return fmt.Sprintf("%d", n) }
func repairProcessRecord(t *testing.T, h *SessionHost, pid int, id string, live SessionLiveness) string {
	t.Helper()
	sid, e := opaqueID("SES")
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	r := RunnerRecord{Contract: sessionRecordContract, SessionID: sid, Owner: "repair-owner", Provider: "codex", State: SessionRecovering, Liveness: live, CreatedAt: now, UpdatedAt: now, ProviderHost: ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{Contract: "task-board.codex-provider-host-v1", PID: pid, URL: id, ThreadID: "fixture-thread", Executable: os.Args[0], ProjectDir: h.options.Layout.StateDir, CodexHome: filepath.Join(h.options.Layout.StateDir, "fixture-home")}}}}
	if e = h.records.create(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	h.sessions[sid] = r
	return sid
}
func TestDoctorPreservesUnknownLivenessOrphanProcess(t *testing.T) {
	h, ctx := repairFixture(t)
	identity := "ws://127.0.0.1:9/fixture-unknown"
	cmd, _ := repairOwnedProcess(t, identity)
	sid := repairProcessRecord(t, h, cmd.Process.Pid, identity, SessionLivenessUnknown)
	before := append([]byte(nil), h.records.bases[sid].Bytes...)
	report, e := h.Doctor(ctx)
	if e != nil || report.Current.OrphanProcessesTerminated != 0 || report.Current.UnsafeProcessesPreserved != 1 {
		t.Fatalf("unknown protection %+v %v", report, e)
	}
	if live, e := processAlive(cmd.Process.Pid); e != nil || !live {
		t.Fatal("unknown helper was terminated", e)
	}
	if string(before) != string(h.records.bases[sid].Bytes) {
		t.Fatal("unknown record changed")
	}
}
func TestDoctorTerminatesOnlyMatchedOwnedOrphan(t *testing.T) {
	h, ctx := repairFixture(t)
	identity := "ws://127.0.0.1:9/fixture-orphan"
	cmd, done := repairOwnedProcess(t, identity)
	sid := repairProcessRecord(t, h, cmd.Process.Pid, identity, SessionLivenessDead)
	report, e := h.Doctor(ctx)
	if e != nil || report.Current.OrphanProcessesTerminated != 1 || !h.sessions[sid].ProviderHost.Snapshot.Codex.Stopped {
		t.Fatalf("owned orphan %+v %v", report, e)
	}
	select {
	case <-done:
		done <- nil
	case <-ctx.Done():
		t.Fatal("owned orphan not reaped")
	}
}
