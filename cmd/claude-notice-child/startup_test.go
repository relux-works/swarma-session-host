package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Run the real entry point in a subprocess without requiring a separate build
// for ordinary module tests. REVIEW_SHIM selects an independently built binary.
func TestStartupShimProcess(t *testing.T) {
	if os.Getenv("CLAUDE_CHILD_TEST_ENTRY") != "1" {
		return
	}
	main()
	os.Exit(0)
}

type startupFixture struct {
	cmd            *exec.Cmd
	ws             string
	stdout, stderr bytes.Buffer
	done           chan error
	reaped         bool
}

const startupVersion = "9.9.9-fake (Claude Code)"

func startShimFixture(t *testing.T, script, input string, holdInput bool) *startupFixture {
	t.Helper()
	f := &startupFixture{ws: canonicalTempDir(t), done: make(chan error, 1)}
	native := filepath.Join(f.ws, "synthetic-native")
	if err := os.WriteFile(native, []byte("#!/bin/sh\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(f.ws, "prompts"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.ws, "prompts/r1.md"), []byte("Synthetic prompt.\n"), 0600); err != nil {
		t.Fatal(err)
	}
	shim := os.Getenv("REVIEW_SHIM")
	if shim != "" {
		f.cmd = exec.Command(shim)
	} else {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		f.cmd = exec.Command(binary, "-test.run=^TestStartupShimProcess$")
	}
	f.cmd.Dir = f.ws
	f.cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + f.ws, "TMPDIR=" + os.TempDir(), "GORACE=atexit_sleep_ms=0", "CLAUDE_CHILD_TEST_ENTRY=1", "CLAUDE_CHILD_WORKSPACE=" + f.ws, "CLAUDE_CHILD_BIN=" + native, "CLAUDE_CHILD_MODEL=synthetic", "CLAUDE_CHILD_EXPECTED_VERSION=" + startupVersion}
	if holdInput {
		pipe, err := f.cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pipe.Close() })
	} else {
		f.cmd.Stdin = strings.NewReader(input)
	}
	f.cmd.Stdout = &f.stdout
	f.cmd.Stderr = &f.stderr
	if err := f.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { f.done <- f.cmd.Wait() }()
	t.Cleanup(func() {
		if !f.reaped {
			_ = f.cmd.Process.Kill()
			select {
			case <-f.done:
				f.reaped = true
			case <-time.After(3 * time.Second):
				t.Error("owned shim did not reap during cleanup")
			}
		}
	})
	return f
}

func waitShim(t *testing.T, f *startupFixture) error {
	t.Helper()
	select {
	case err := <-f.done:
		f.reaped = true
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("shim exceeded runner stop budget")
		return nil
	}
}

func markerPID(t *testing.T, f *startupFixture, name string) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(filepath.Join(f.ws, name))
		pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
		if pid > 0 {
			return pid
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("fixture PID not published: %s", name)
	return 0
}

// Establish immediate parentage before signaling; cleanup can only target the
// exact recorded fixture PID while it still belongs to the recorded group.
func ownNativeFixture(t *testing.T, f *startupFixture, name string) int {
	t.Helper()
	pid := markerPID(t, f, name)
	raw, err := exec.Command("/bin/ps", "-p", strconv.Itoa(pid), "-o", "ppid=").Output()
	if err != nil {
		t.Fatal(err)
	}
	parent, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	if parent != f.cmd.Process.Pid {
		t.Fatalf("native is not shim's direct child: native=%d parent=%d shim=%d", pid, parent, f.cmd.Process.Pid)
	}
	group, err := syscall.Getpgid(pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if g, e := syscall.Getpgid(pid); e == nil && g == group {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			end := time.Now().Add(3 * time.Second)
			for time.Now().Before(end) && syscall.Kill(pid, 0) == nil {
				time.Sleep(10 * time.Millisecond)
			}
		}
		t.Logf("owned native pid=%d absent=%v", pid, syscall.Kill(pid, 0) != nil)
	})
	return pid
}

func requireStopped(t *testing.T, f *startupFixture, pid int, sig os.Signal) {
	t.Helper()
	start := time.Now()
	if err := f.cmd.Process.Signal(sig); err != nil {
		t.Fatal(err)
	}
	err := waitShim(t, f)
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 3 {
		t.Errorf("stop result: %v; stderr=%q", err, f.stderr.String())
	}
	if f.stdout.Len() != 0 || strings.TrimSpace(f.stderr.String()) != "claude_run_interrupted" {
		t.Errorf("stop output: stdout=%q stderr=%q", f.stdout.String(), f.stderr.String())
	}
	if elapsed := time.Since(start); elapsed >= 4*time.Second {
		t.Errorf("stop missed budget margin: %s", elapsed)
	}
	if pid > 0 && syscall.Kill(pid, 0) == nil {
		t.Errorf("direct native survived stop: %d", pid)
	}
}

// Adopt the parent-verified startup stop witness for both supported signals.
func TestStartupStopDuringVersion(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGTERM, os.Interrupt} {
		t.Run(sig.String(), func(t *testing.T) {
			f := startShimFixture(t, `if [ "$1" = "--version" ]; then
trap '' TERM INT
echo $$ > version-pid
exec /bin/sleep 60
fi
exit 99
`, "", false)
			pid := ownNativeFixture(t, f, "version-pid")
			requireStopped(t, f, pid, sig)
		})
	}
}

const normalVersionScript = `if [ "$1" = "--version" ]; then
 echo $$ > version-pid
 printf '9.9.9-fake (Claude Code)\n'
 exit 0
fi
`

func startupNotice(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(noticeEnv("startup"))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw) + "\n"
}

func TestStartupProbeCompletesNormally(t *testing.T) {
	f := startShimFixture(t, normalVersionScript+`sid=""
while [ $# -gt 0 ]; do
 if [ "$1" = "--session-id" ]; then sid=$2; fi
 shift
done
cat >/dev/null
printf '{"type":"result","session_id":"%s","is_error":false,"permission_denials":[]}\n' "$sid"
`, startupNotice(t), false)
	if err := waitShim(t, f); err != nil {
		t.Fatalf("normal probe/run failed: %v %s", err, f.stderr.String())
	}
	var applied map[string]any
	if err := json.Unmarshal(f.stdout.Bytes(), &applied); err != nil || applied["kind"] != "applied" {
		t.Fatalf("normal completion: %q %v", f.stdout.String(), err)
	}
	if pid := markerPID(t, f, "version-pid"); syscall.Kill(pid, 0) == nil {
		t.Error("normally completed probe was not reaped")
	}
}

func TestStartupStopAfterProbe(t *testing.T) {
	f := startShimFixture(t, normalVersionScript+`trap '' TERM INT
echo $$ > run-pid
exec /bin/sleep 60
`, startupNotice(t), false)
	pid := ownNativeFixture(t, f, "run-pid")
	if versionPID := markerPID(t, f, "version-pid"); syscall.Kill(versionPID, 0) == nil {
		t.Fatal("probe not reaped before notice run")
	}
	requireStopped(t, f, pid, syscall.SIGTERM)
	raw, err := os.ReadFile(filepath.Join(f.ws, "output/startup.receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rec receipt
	if err := json.Unmarshal(raw, &rec); err != nil || rec.Error != "claude_run_interrupted" {
		t.Fatalf("interruption receipt: %s %v", raw, err)
	}
}

func TestStartupStopAfterProbeIdle(t *testing.T) {
	f := startShimFixture(t, normalVersionScript+"exit 99\n", "", true)
	pid := markerPID(t, f, "version-pid")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && syscall.Kill(pid, 0) == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if syscall.Kill(pid, 0) == nil {
		t.Fatal("normal probe did not reap")
	}
	requireStopped(t, f, 0, syscall.SIGTERM)
}

func TestStartupVersionMismatchBeforeNotice(t *testing.T) {
	script := strings.Replace(normalVersionScript, startupVersion, "8.8.8-other (Claude Code)", 1) + "echo unexpected > notice-started\nexit 99\n"
	f := startShimFixture(t, script, startupNotice(t), false)
	err := waitShim(t, f)
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 2 {
		t.Fatalf("version mismatch exit: %v", err)
	}
	if f.stdout.Len() != 0 || !strings.HasPrefix(f.stderr.String(), "claude_version_mismatch ") {
		t.Fatalf("version mismatch output: %q %q", f.stdout.String(), f.stderr.String())
	}
	if _, err := os.Stat(filepath.Join(f.ws, "notice-started")); !os.IsNotExist(err) {
		t.Fatal("notice ran before version admission")
	}
}

func TestVersionCanceledBeforeSpawn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	observed, err := checkVersion(ctx, "/not-executed", canonicalTempDir(t), startupVersion)
	if observed != "" || err == nil || err.Error() != "claude_run_interrupted" {
		t.Fatalf("canceled probe: %q %v", observed, err)
	}
}

// Redirected descendant pipes let Wait finish as soon as the probe leader
// exits. Establish the real group before allowing that exit, then require
// cleanup on normal EOF, version refusal and both supported stop signals.
func TestStartupCompletedProbeGroupCleanup(t *testing.T) {
	for _, mode := range []string{"eof", "term", "int", "mismatch", "notice"} {
		t.Run(mode, func(t *testing.T) {
			script := `if [ "$1" = "--version" ]; then
trap '' TERM INT
echo $$ > version-pid
/bin/sh -c 'trap "" TERM INT; echo $$ > descendant-pid; exec /bin/sleep 60' >/dev/null 2>&1 &
while [ ! -f release-probe ]; do /bin/sleep 0.01; done
printf '9.9.9-fake (Claude Code)\n'
exit 0
fi
# A notice must not start while the completed probe's descendant still exists.
kill -0 "$(cat descendant-pid)" 2>/dev/null && exit 88
sid=""
while [ $# -gt 0 ]; do
 if [ "$1" = "--session-id" ]; then sid=$2; fi
 shift
done
cat >/dev/null
printf '{"type":"result","session_id":"%s","is_error":false,"permission_denials":[]}\n' "$sid"
`
			if mode == "mismatch" {
				script = strings.Replace(script, startupVersion, "8.8.8-other (Claude Code)", 1)
			}
			input := ""
			if mode == "notice" {
				input = startupNotice(t)
			}
			f := startShimFixture(t, script, input, mode == "term" || mode == "int")
			leader := ownNativeFixture(t, f, "version-pid")
			descendant := markerPID(t, f, "descendant-pid")
			if group, err := syscall.Getpgid(descendant); err != nil || group != leader {
				t.Fatalf("descendant group=%d leader=%d: %v", group, leader, err)
			}
			t.Cleanup(func() {
				if group, err := syscall.Getpgid(descendant); err == nil && group == leader {
					_ = syscall.Kill(descendant, syscall.SIGKILL)
				}
				for end := time.Now().Add(3 * time.Second); time.Now().Before(end) && syscall.Kill(descendant, 0) == nil; {
					time.Sleep(10 * time.Millisecond)
				}
				t.Logf("owned descendant pid=%d absent=%v", descendant, syscall.Kill(descendant, 0) != nil)
			})
			if err := os.WriteFile(filepath.Join(f.ws, "release-probe"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			for end := time.Now().Add(3 * time.Second); time.Now().Before(end) && syscall.Kill(leader, 0) == nil; {
				time.Sleep(5 * time.Millisecond)
			}
			if syscall.Kill(leader, 0) == nil {
				t.Fatal("probe leader did not exit and reap")
			}
			if mode == "term" || mode == "int" {
				// Give normal completion a chance to reach the held stdin loop;
				// EOF/refusal controls also detect a leak without this timing window.
				time.Sleep(25 * time.Millisecond)
				sig := os.Signal(syscall.SIGTERM)
				if mode == "int" {
					sig = os.Interrupt
				}
				requireStopped(t, f, leader, sig)
			} else {
				err := waitShim(t, f)
				if mode == "eof" || mode == "notice" {
					if err != nil || f.stderr.Len() != 0 {
						t.Errorf("normal EOF: %v %q", err, f.stderr.String())
					}
				} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 2 || !strings.HasPrefix(f.stderr.String(), "claude_version_mismatch ") {
					t.Errorf("version refusal: %v %q", err, f.stderr.String())
				}
			}
			if syscall.Kill(descendant, 0) == nil || syscall.Kill(-leader, 0) == nil {
				t.Errorf("completed probe group survived: leader=%d descendant=%d", leader, descendant)
			}
			if mode == "notice" {
				var applied map[string]any
				if err := json.Unmarshal(f.stdout.Bytes(), &applied); err != nil || applied["kind"] != "applied" {
					t.Errorf("notice after probe cleanup: %q %v", f.stdout.String(), err)
				}
			} else if f.stdout.Len() != 0 {
				t.Errorf("startup emitted applied: %q", f.stdout.String())
			}
			if _, err := os.Stat(filepath.Join(f.ws, "output")); mode != "notice" && !os.IsNotExist(err) {
				t.Errorf("startup created notice artifacts: %v", err)
			}
		})
	}
}
