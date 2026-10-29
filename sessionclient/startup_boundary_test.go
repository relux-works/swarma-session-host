//go:build linux

package sessionclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Unknown/missing authority, private-path failures and foreign metadata cannot
// launch, replace or write state. A valid live endpoint is paired nearby in the
// unchanged original healthy-holder root; these fixtures are not real daemons.
func TestStartupRefusalsHaveNoLaunchOrFilesystemEffects(t *testing.T) {
	for _, mode := range []string{"missing-source", "foreign-namespace", "socket-mismatch", "private-mode", "symlink-token"} {
		t.Run(mode, func(t *testing.T) {
			l := staleTakeoverTestLayout(t, mode)
			ep := Endpoint{Protocol: ProtocolIdentifier(ProtocolVersion), ProtocolVersion: ProtocolVersion, Namespace: l.Namespace, PID: os.Getpid(), InstanceID: "MGR-fixture", SocketPath: l.SocketPath}
			if mode == "foreign-namespace" {
				ep.Namespace = "other"
			}
			if mode == "socket-mismatch" {
				ep.SocketPath = filepath.Join(filepath.Dir(l.SocketPath), "other.sock")
			}
			b, e := json.Marshal(ep)
			if e != nil {
				t.Fatal(e)
			}
			if mode != "missing-source" {
				if e = os.WriteFile(l.EndpointFile, b, 0600); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(l.TokenFile, []byte("fixture-token"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "private-mode" {
				if e = os.Chmod(l.EndpointFile, 0644); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "symlink-token" {
				target := filepath.Join(filepath.Dir(l.TokenFile), "target")
				if e = os.WriteFile(target, []byte("fixture-token"), 0600); e != nil {
					t.Fatal(e)
				}
				if e = os.Remove(l.TokenFile); e != nil {
					t.Fatal(e)
				}
				if e = os.Symlink(target, l.TokenFile); e != nil {
					t.Fatal(e)
				}
			}
			before := startupFixtureSnapshot(t, filepath.Dir(l.EndpointFile))
			calls := 0
			var source StartupSource = &fixtureLaunchSource{l.source, func() error { calls++; return errors.New("forbidden launch") }}
			if mode == "missing-source" {
				source = nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, e = ConnectOrStart(ctx, l.StartupLayout, StartupOptions{Source: source})
			expected := ErrStartupPrivatePath
			switch mode {
			case "missing-source":
				expected = ErrStartupAuthorityUnavailable
			case "foreign-namespace":
				expected = ErrNamespaceMismatch
			case "socket-mismatch":
				expected = ErrStartupEndpointChanged
			}
			if !errors.Is(e, expected) {
				t.Fatalf("refusal %v, want %v", e, expected)
			}
			if calls != 0 {
				t.Fatal("refusal launched")
			}
			if strings.Contains(e.Error(), filepath.Dir(l.TokenFile)) {
				t.Fatal("refusal leaked private path")
			}
			if after := startupFixtureSnapshot(t, filepath.Dir(l.EndpointFile)); !equalStartupSnapshot(before, after) {
				t.Fatalf("refusal wrote state: before%v after%v", before, after)
			}
		})
	}
}

// A malformed inventory remains a live-session refusal. A real environment
// marker wins over that unreadability, preserving the original hosted-caller
// precedence. The helper remains alive and receives no replacement signal.
func TestStartupHostedAndUnknownInventoryRefuseBeforeReplacement(t *testing.T) {
	for _, mode := range []string{"live", "unreadable", "hosted-unreadable"} {
		t.Run(mode, func(t *testing.T) {
			declareOutsideShell(t)
			l := staleTakeoverTestLayout(t, mode)
			holder := startSessionManagerHolderProcess(t, l, "outdated-protocol")
			if mode == "live" {
				b, _ := json.Marshal([]HostedSessionSummary{{SessionID: "SES-owned-fixture", Provider: "claude", PID: holder.pid}})
				if e := os.WriteFile(l.source.sessions, b, 0600); e != nil {
					t.Fatal(e)
				}
			} else {
				if e := os.WriteFile(l.source.sessions, []byte("{invalid"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "hosted-unreadable" {
				t.Setenv("TASK_BOARD_MANAGED_SESSION_ID", "SES-caller-fixture")
			}
			calls := 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, e := connectOriginalStartup(ctx, l, func() error { calls++; return errors.New("forbidden launch") })
			var refusal *ManagerReplacementRefusedError
			if !errors.As(e, &refusal) {
				t.Fatalf("untyped refusal: %v", e)
			}
			want := ManagerReplacementRefusedLiveHostedSessions
			if mode == "hosted-unreadable" {
				want = ManagerReplacementRefusedHostedCaller
			}
			if refusal.Code != want || refusal.AutonomousRecovery() || calls != 0 {
				t.Fatalf("incorrect refusal %+v launches%d", refusal, calls)
			}
			alive, e := fixtureProcessAlive(holder.pid)
			if e != nil || !alive {
				t.Fatalf("refusal killed own helper: %v", e)
			}
		})
	}
}

// Unknown process observation and a mismatched current-holder capability cannot
// signal the helper. The original wedged/outdated roots provide valid controls.
func TestStartupUnknownProcessAndForeignCapabilityCannotSignal(t *testing.T) {
	for _, mode := range []string{"unknown-process", "unavailable-proof", "foreign-proof", "private-proof-error"} {
		t.Run(mode, func(t *testing.T) {
			declareOutsideShell(t)
			l := staleTakeoverTestLayout(t, mode)
			holder := startSessionManagerHolderProcess(t, l, "outdated-protocol")
			want := ErrStartupAuthorityUnavailable
			cause := errors.New("private-fixture-path=" + filepath.Dir(l.EndpointFile))
			switch mode {
			case "unknown-process":
				l.source.unknown = true
				want = ErrStartupObservation
			case "unavailable-proof":
				l.source.refusal = ErrStartupAuthorityUnavailable
			case "foreign-proof":
				l.source.foreignProof = true
			case "private-proof-error":
				l.source.refusal = cause
				want = cause
			}
			calls := 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, e := connectOriginalStartup(ctx, l, func() error { calls++; return nil })
			if !errors.Is(e, want) || calls != 0 {
				t.Fatalf("unknown proof admitted: %v launches%d", e, calls)
			}
			if strings.Contains(e.Error(), filepath.Dir(l.EndpointFile)) {
				t.Fatal("proof refusal leaked private path")
			}
			alive, e := fixtureProcessAlive(holder.pid)
			if e != nil || !alive {
				t.Fatalf("helper signaled: %v", e)
			}
		})
	}
}

// A real authenticated legacy decoder rejection must still meet the hosted
// caller guard before drain, signal or launch. Positive recovery through the
// actual core attach route remains a receiving integration test, not this helper.
func TestStartupAttachUnknownFieldRefusesHostedCallerBeforeEffects(t *testing.T) {
	declareOutsideShell(t)
	l := staleTakeoverTestLayout(t, "attach-hosted-refusal")
	holder := startSessionManagerHolderProcess(t, l, "unknown-field")
	t.Setenv("TASK_BOARD_MANAGED_SESSION_ID", "SES-caller-fixture")
	calls := 0
	source := &fixtureLaunchSource{l.source, func() error { calls++; return errors.New("forbidden launch") }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := ConnectOrStartAndAttach(ctx, l.StartupLayout, StartupOptions{Source: source}, map[string]any{"adopt": true}, nil)
	var refusal *ManagerReplacementRefusedError
	if client != nil || !errors.As(err, &refusal) || refusal.Code != ManagerReplacementRefusedHostedCaller || calls != 0 {
		t.Fatalf("attach rejection bypassed hosted guard: client%v error%v launches%d", client, err, calls)
	}
	if alive, e := fixtureProcessAlive(holder.pid); e != nil || !alive {
		t.Fatalf("attach refusal signaled helper: %v", e)
	}
}

// Failed launch is not retried into another effect and retains its actual
// error cause. It writes no startup state; the caller deadline bounds polling.
func TestStartupFailedLaunchDoesNotRetryOrLeakPrivateCause(t *testing.T) {
	l := staleTakeoverTestLayout(t, "launch-failed")
	before := startupFixtureSnapshot(t, filepath.Dir(l.EndpointFile))
	cause := errors.New("private-fixture-path=" + filepath.Dir(l.EndpointFile))
	calls := 0
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	_, e := connectOriginalStartup(ctx, l, func() error { calls++; return cause })
	if !errors.Is(e, cause) || !errors.Is(e, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("failed launch: %v calls%d", e, calls)
	}
	if strings.Contains(e.Error(), filepath.Dir(l.EndpointFile)) {
		t.Fatal("launch failure exposed private path")
	}
	if !equalStartupSnapshot(before, startupFixtureSnapshot(t, filepath.Dir(l.EndpointFile))) {
		t.Fatal("failed fixture wrote startup state")
	}
}

// Waiting for another namespace startup honors cancellation before effects,
// rather than leaving a background mutex waiter which later launches.
func TestStartupArbitrationCancellationDoesNotLaunchLater(t *testing.T) {
	l := staleTakeoverTestLayout(t, "lock-cancel")
	value, _ := connectOrStartLocks.LoadOrStore(l.Namespace, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	calls := 0
	done := make(chan error, 1)
	go func() { _, e := connectOriginalStartup(ctx, l, func() error { calls++; return nil }); done <- e }()
	select {
	case e := <-done:
		if !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("lock cancellation: %v", e)
		}
	case <-time.After(time.Second):
		mu.Unlock()
		t.Fatal("startup arbitration ignored cancellation")
	}
	mu.Unlock()
	if calls != 0 {
		t.Fatal("cancelled waiter launched")
	}
}

func startupFixtureSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	rows := map[string]string{}
	e := filepath.WalkDir(root, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		info, e := os.Lstat(path)
		if e != nil {
			return e
		}
		var b []byte
		if info.Mode()&os.ModeSymlink != 0 {
			target, e := os.Readlink(path)
			if e != nil {
				return e
			}
			b = []byte(target)
		} else {
			b, e = os.ReadFile(path)
			if e != nil {
				return e
			}
		}
		rows[path] = fmt.Sprintf("%v:%x", info.Mode(), b)
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return rows
}
func equalStartupSnapshot(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
