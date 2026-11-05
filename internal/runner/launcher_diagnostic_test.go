package runner

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// The classifier recognizes only the exact line across arbitrary fragmentation;
// retained state stays bounded after large input and overlapping native exits
// never establish an exec phase. Payloads are synthetic fixture strings.
func TestLauncherDiagnosticClassifier(t *testing.T) {
	known := []byte(LauncherEnvironmentDiagnostic)
	for split := 0; split <= len(known); split++ {
		d := NewLauncherDiagnostic()
		_, _ = d.Write(known[:split])
		_, _ = d.Write(known[split:])
		if got := d.Observe(78); got.Diagnostic != "protocol_child_environment_refused" || got.Phase != "unverified" || got.Status != 78 {
			t.Fatal("fragmented constant lost", split, got)
		}
	}
	for _, payload := range []string{"", strings.TrimSuffix(LauncherEnvironmentDiagnostic, "\n"), LauncherEnvironmentDiagnostic + "extra", "private_fixture_value", strings.Repeat("private_fixture_value", 10000)} {
		for _, status := range []int{-1, 0, 64, 70, 78, 1} {
			d := NewLauncherDiagnostic()
			if n, err := d.Write([]byte(payload)); n != len(payload) || err != nil {
				t.Fatal("diagnostic backpressure", n, err)
			}
			want := "protocol_child_diagnostic_redacted"
			if payload == "" {
				want = ""
			}
			if got := d.Observe(status); got.Diagnostic != want || got.Status != status || got.Phase != "unverified" || d.seen > len(known)+1 {
				t.Fatal("unbounded state or invented phase", got)
			}
		}
	}
}

// This child is the current Go test executable, never a Launcher or provider.
// Only fixture-controlled payloads are written; stdout belongs to native data.
func TestLauncherDiagnosticChild(t *testing.T) {
	mode := os.Getenv("SESSION_HOST_DIAGNOSTIC_FIXTURE")
	if mode == "" {
		return
	}
	if os.WriteFile("diagnostic-child-started", nil, 0600) != nil {
		os.Exit(9)
	}
	status := 78
	switch mode {
	case "known":
		_, _ = os.Stderr.Write([]byte(LauncherEnvironmentDiagnostic))
	case "extra":
		_, _ = os.Stderr.Write([]byte(LauncherEnvironmentDiagnostic + "private_fixture_value"))
	case "large":
		_, _ = os.Stderr.Write(bytes.Repeat([]byte("private_fixture_value"), 1<<16))
		status = 0
	case "hold":
		_, _ = io.Copy(io.Discard, os.Stdin)
		status = 0
	default:
		os.Exit(10)
	}
	_, _ = os.Stdout.Write([]byte("{\"native_fixture\":true}\n"))
	os.Exit(status)
}

func diagnosticFixtureConfig(t *testing.T, mode string) Config {
	t.Helper()
	cfg := testConfig(t, "")
	cfg.Plan.Args = []string{"-test.run=^TestLauncherDiagnosticChild$"}
	cfg.Plan.Env = append(cfg.Plan.Env, "SESSION_HOST_DIAGNOSTIC_FIXTURE="+mode)
	cfg.Diagnostics = ProtocolChildDiagnostics
	return cfg
}

// Actual Start retains its direct child and reaps it while stderr goes only to
// the fixed sink. Recognized and large unknown stderr never enter stdout; equal
// native exit codes remain phase-unverified, including a valid zero exit.
func TestLauncherDiagnosticRunner(t *testing.T) {
	for _, mode := range []string{"known", "extra", "large"} {
		t.Run(mode, func(t *testing.T) {
			cfg := diagnosticFixtureConfig(t, mode)
			s, err := startTestRunner(t, cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, done := range []<-chan struct{}{s.done, s.outputDone, s.diagnosticDone} {
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("owned child or collector did not finish")
				}
			}
			got, ok := s.LauncherDiagnostics()
			want, status := "protocol_child_diagnostic_redacted", 78
			if mode == "known" {
				want = "protocol_child_environment_refused"
			}
			if mode == "large" {
				status = 0
			}
			if !ok || got.PID != s.ChildPID() || !got.Exited || got.WaitUnknown || !got.DiagnosticEnded || !got.DiagnosticComplete || got.Exit.Status != status || got.Exit.Diagnostic != want || got.Exit.Phase != "unverified" {
				t.Fatal("actual child/diagnostic correlation lost", got, ok)
			}
			s.mu.Lock()
			output := string(s.output)
			s.mu.Unlock()
			if output != "{\"native_fixture\":true}\n" {
				t.Fatal("stderr polluted or replaced native output")
			}
		})
	}
}

// Unknown modes reject before endpoint or child effects; an occupied endpoint
// also prevents child creation. Disabled mode preserves the original devnull
// behavior. The matching valid child path is exercised immediately above.
func TestLauncherDiagnosticStartRefusals(t *testing.T) {
	for _, mode := range []string{"unknown", "occupied", "exec-failure"} {
		t.Run(mode, func(t *testing.T) {
			cfg := diagnosticFixtureConfig(t, "known")
			switch mode {
			case "unknown":
				cfg.Diagnostics = "raw"
			case "occupied":
				l, err := localipc.Listen(cfg.Socket)
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
			case "exec-failure":
				cfg.Plan.Binary = filepath.Join(cfg.Plan.Cwd, "missing-binary")
			}
			s, err := startTestRunner(t, cfg)
			if err == nil || s != nil {
				t.Fatal("refused start admitted")
			}
			if (mode == "unknown" && err.Error() != "invalid_runner_plan") ||
				(mode == "occupied" && !errors.Is(err, syscall.EADDRINUSE)) ||
				(mode == "exec-failure" && err.Error() != "child_start_failed") {
				t.Fatal("wrong refusal class", err)
			}
			if _, err := os.Lstat(filepath.Join(cfg.Plan.Cwd, "diagnostic-child-started")); !os.IsNotExist(err) {
				t.Fatal("refused start executed fixture", err)
			}
			if mode != "occupied" {
				if _, err := os.Lstat(cfg.Socket); !os.IsNotExist(err) {
					t.Fatal("failed start left endpoint", err)
				}
			}
		})
	}
	cfg := diagnosticFixtureConfig(t, "known")
	cfg.Diagnostics = ""
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.LauncherDiagnostics(); ok {
		t.Fatal("disabled capture invented diagnostic")
	}
}

// Closing a runner joins its own reader. A forced reader close is not EOF; a
// separate pipe fixture proves incomplete capture independently of native Wait.
func TestLauncherDiagnosticCleanup(t *testing.T) {
	cfg := diagnosticFixtureConfig(t, "hold")
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.diagnosticDone:
	default:
		t.Fatal("Close left stderr collector running")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	fixture := &Server{diagnostic: NewLauncherDiagnostic(), diagnosticDone: make(chan struct{}), exitCode: -1}
	fixture.wg.Add(1)
	go fixture.collectLauncherDiagnostics(r)
	_ = r.Close()
	select {
	case <-fixture.diagnosticDone:
	case <-time.After(time.Second):
		t.Fatal("closed reader did not join")
	}
	fixture.wg.Wait()
	got, ok := fixture.LauncherDiagnostics()
	if !ok || !got.DiagnosticEnded || got.DiagnosticComplete || got.Exited || got.Exit.Status != -1 || got.Exit.Phase != "unverified" {
		t.Fatal("truncated pipe became clean exit or admission", got)
	}
}
