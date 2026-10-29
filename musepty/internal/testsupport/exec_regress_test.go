package testsupport

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestDefaultRepeatCleanup runs two same-name test lifecycles in-process,
// which is what -count>1 does to the helper. Each lifecycle must get a fresh
// default directory, and nothing may survive either lifecycle.
func TestDefaultRepeatCleanup(t *testing.T) {
	t.Setenv(KeepExecutablesEnv, "0")
	var paths []string
	fn := func(child *testing.T) {
		paths = append(paths, WriteExecutable(child, "data", []byte("probe"), 0o600))
	}
	for cycle := 1; cycle <= 2; cycle++ {
		if !runNestedProbe(t, "TestDefaultRepeatProbe", fn) {
			t.Fatal("nested run failed")
		}
		if _, err := os.Stat(paths[cycle-1]); err == nil {
			t.Errorf("cycle %d: default file %s survives test cleanup", cycle, paths[cycle-1])
		} else if !os.IsNotExist(err) {
			t.Errorf("cycle %d: stat %s: %v", cycle, paths[cycle-1], err)
		}
	}
	if len(paths) == 2 && paths[0] == paths[1] {
		t.Errorf("both lifecycles reused %s; want a fresh directory per run", paths[0])
	}
}

// TestKeepPathPrecedenceMatchesDefault proves keep mode resolves PATH lookups
// exactly like default mode, including across same-name repeats: run 1 writes
// an "extra" fake that run 2 omits, and run 2 must not see it in either mode,
// while the shared "tool" fake resolves to the run-2 directory.
func TestKeepPathPrecedenceMatchesDefault(t *testing.T) {
	// Retention lasts through the assertions; the parent owns fixture cleanup.
	t.Setenv("TMPDIR", t.TempDir())
	for _, keep := range []string{"0", "1"} {
		t.Run("keep-"+keep, func(t *testing.T) {
			t.Setenv(KeepExecutablesEnv, keep)
			run := 0
			var wantTool, toolResolution, extraResolution, toolData string
			fn := func(child *testing.T) {
				run++
				if run == 1 {
					WriteExecutable(child, "spm-probe-extra", []byte("#!/bin/sh\nexit 0\n"), 0o755)
				}
				tool := WriteExecutable(child, "spm-probe-tool",
					[]byte(fmt.Sprintf("#!/bin/sh\necho run%d\n", run)), 0o755)
				if run == 2 {
					dir := filepath.Dir(tool)
					pathEnv := dir + string(os.PathListSeparator) + os.Getenv("PATH")
					wantTool = tool
					toolResolution = lookupOnPath(pathEnv, "spm-probe-tool")
					extraResolution = lookupOnPath(pathEnv, "spm-probe-extra")
					data, err := os.ReadFile(tool)
					if err != nil {
						child.Fatalf("read run-2 tool: %v", err)
					}
					toolData = string(data)
				}
			}
			for cycle := 1; cycle <= 2; cycle++ {
				if !runNestedProbe(t, "TestPathPrecedenceProbe", fn) {
					t.Fatal("nested run failed")
				}
			}
			if toolResolution != wantTool {
				t.Errorf("keep=%s: tool resolves to %q, want %q", keep, toolResolution, wantTool)
			}
			if extraResolution != "" {
				t.Errorf("keep=%s: stale extra resolves to %q, want absent", keep, extraResolution)
			}
			if !strings.Contains(toolData, "run2") {
				t.Errorf("keep=%s: run-2 tool body = %q", keep, toolData)
			}
		})
	}
}

// lookupOnPath mimics exec.LookPath over an explicit PATH value.
func lookupOnPath(pathEnv, file string) string {
	for _, dir := range filepath.SplitList(pathEnv) {
		if dir == "" {
			continue
		}
		candidate := filepath.Join(dir, file)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return ""
}

// TestKeepSymlinkLeafRefused plants a symlink leaf in the keep directory and
// proves the helper refuses to write through it. The write runs in a child
// test process because refusal is t.Fatal: the child must exit non-zero and
// the outside file must be untouched.
func TestKeepSymlinkLeafRefused(t *testing.T) {
	if os.Getenv("SPM_KEEP_SYMLINK_CHILD") == "1" {
		keepSymlinkLeafChild(t)
		return
	}
	isolated := t.TempDir()
	outside := filepath.Join(isolated, "outside")
	if err := os.WriteFile(outside, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", isolated)
	t.Setenv(KeepExecutablesEnv, "1")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestKeepSymlinkLeafRefused$")
	cmd.Env = append(os.Environ(),
		"SPM_KEEP_SYMLINK_CHILD=1",
		"SPM_KEEP_SYMLINK_OUTSIDE="+outside,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child exited 0, want symlink refusal; output:\n%s", out)
	}
	data, readErr := os.ReadFile(outside)
	if readErr != nil || string(data) != "original" {
		t.Fatalf("outside file = %q, %v; output:\n%s", data, readErr, out)
	}
}

func keepSymlinkLeafChild(t *testing.T) {
	outside := os.Getenv("SPM_KEEP_SYMLINK_OUTSIDE")
	seed := WriteExecutable(t, "seed", []byte("seed"), 0o600)
	if err := os.Symlink(outside, filepath.Join(filepath.Dir(seed), "link")); err != nil {
		t.Fatalf("plant link: %v", err)
	}
	WriteExecutable(t, "link", []byte("replaced"), 0o600)
}

// runNestedProbe isolates nested lifecycles from the outer suite's filters and count.
// Callers and probes must not use t.Parallel: these flags are process globals.
func runNestedProbe(t *testing.T, name string, fn func(*testing.T)) bool {
	t.Helper()
	run, skip := flag.Lookup("test.run"), flag.Lookup("test.skip")
	count := flag.Lookup("test.count")
	savedRun, savedSkip, savedCount := run.Value.String(), skip.Value.String(), count.Value.String()
	defer func() {
		if err := run.Value.Set(savedRun); err != nil {
			t.Errorf("restore test.run: %v", err)
		}
		if err := skip.Value.Set(savedSkip); err != nil {
			t.Errorf("restore test.skip: %v", err)
		}
		if err := count.Value.Set(savedCount); err != nil {
			t.Errorf("restore test.count: %v", err)
		}
	}()
	if err := run.Value.Set(""); err != nil {
		t.Fatalf("clear test.run: %v", err)
	}
	if err := skip.Value.Set(""); err != nil {
		t.Fatalf("clear test.skip: %v", err)
	}
	if err := count.Value.Set("1"); err != nil {
		t.Fatalf("neutralise test.count: %v", err)
	}
	executions := 0
	ok := testing.RunTests(regexp.MatchString, []testing.InternalTest{{Name: name, F: func(child *testing.T) {
		executions++
		fn(child)
	}}})
	if executions != 1 {
		t.Fatalf("nested probe %s executed %d times, want exactly 1", name, executions)
	}
	return ok
}

func TestNestedProbeRestoresFilters(t *testing.T) {
	run, skip := flag.Lookup("test.run"), flag.Lookup("test.skip")
	count := flag.Lookup("test.count")
	oldRun, oldSkip, oldCount := run.Value.String(), skip.Value.String(), count.Value.String()
	defer func() { _ = run.Value.Set(oldRun); _ = skip.Value.Set(oldSkip); _ = count.Value.Set(oldCount) }()
	_ = run.Value.Set("^TestOuterOnly$")
	_ = skip.Value.Set("^TestInnerProbe$")
	_ = count.Value.Set("2")
	executed := false
	if !runNestedProbe(t, "TestInnerProbe", func(child *testing.T) {
		executed = true
		if run.Value.String() != "" || skip.Value.String() != "" || count.Value.String() != "1" {
			child.Error("nested filters/count not neutralised")
		}
	}) {
		t.Fatal("nested probe failed")
	}
	if !executed {
		t.Fatal("nested probe did not execute")
	}
	if run.Value.String() != "^TestOuterOnly$" || skip.Value.String() != "^TestInnerProbe$" || count.Value.String() != "2" {
		t.Fatal("outer filters not restored")
	}
}

// A flag setter panic exercises the helper's unwind in the caller goroutine.
// Panics in a testing.T body occur in testing's own goroutine and abort the
// process, so they cannot be recovered by the caller of testing.RunTests.
type panicOnClear struct {
	flag.Value
	armed   bool
	trigger string
}

func (v *panicOnClear) Set(s string) error {
	if s == v.trigger && v.armed {
		v.armed = false
		if err := v.Value.Set(s); err != nil {
			return err
		}
		panic("neutralise flag probe")
	}
	return v.Value.Set(s)
}

func TestNestedProbeRestoresFiltersOnPanic(t *testing.T) {
	for _, target := range []string{"test.skip", "test.count"} {
		t.Run(target, func(t *testing.T) {
			run, skip, count := flag.Lookup("test.run"), flag.Lookup("test.skip"), flag.Lookup("test.count")
			panicking := flag.Lookup(target)
			oldRun, oldSkip, oldCount, oldValue := run.Value.String(), skip.Value.String(), count.Value.String(), panicking.Value
			defer func() {
				panicking.Value = oldValue
				_ = run.Value.Set(oldRun)
				_ = skip.Value.Set(oldSkip)
				_ = count.Value.Set(oldCount)
			}()
			_ = run.Value.Set("^TestOuterOnly$")
			_ = skip.Value.Set("^TestInnerProbe$")
			_ = count.Value.Set("2")
			trigger := ""
			if target == "test.count" {
				trigger = "1"
			}
			panicking.Value = &panicOnClear{Value: oldValue, armed: true, trigger: trigger}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				runNestedProbe(t, "TestInnerProbe", func(*testing.T) { t.Error("probe reached after setter panic") })
			}()
			if recovered != "neutralise flag probe" {
				t.Fatalf("panic = %v", recovered)
			}
			if run.Value.String() != "^TestOuterOnly$" || skip.Value.String() != "^TestInnerProbe$" || count.Value.String() != "2" {
				t.Fatal("outer filters/count not restored on panic")
			}
		})
	}
}

func TestNestedProbeSubprocessFilters(t *testing.T) {
	// Re-execute the real package test binary; no-filter children run the whole
	// package. Suppress only this launcher to avoid recursive subprocesses.
	if os.Getenv("SPM_NESTED_FILTER_CHILD") == "1" {
		return
	}
	const outer = "^(TestDefaultRepeatCleanup|TestKeepRepeatRetainsPriorLeaf|TestKeepPathPrecedenceMatchesDefault|TestWriteExecutableModeParity)$"
	cases := []struct {
		name    string
		flags   []string
		repeats int
	}{
		{"NoFilter", nil, 1},
		{"OuterRun", []string{"-test.run", outer}, 1},
		{"NonMatchingSkip", []string{"-test.skip", "TestZZZNoSuchTest"}, 1},
		{"RunAndSkip", []string{"-test.run", outer, "-test.skip", "TestZZZNoSuchTest"}, 1},
		{"CountTwo", []string{"-test.run", outer, "-test.count=2"}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], append([]string{"-test.v", "-test.count=1", "-test.timeout=2m"}, tc.flags...)...)
			cmd.Env = append(os.Environ(), "SPM_NESTED_FILTER_CHILD=1", "TMPDIR="+t.TempDir())
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("package subprocess: %v\n%s", err, out)
			}
			if got := strings.Count(string(out), "--- PASS: TestWriteExecutableModeParity ("); got != tc.repeats {
				t.Errorf("mode parity executed %d times, want %d\n%s", got, tc.repeats, out)
			}
			for probe, count := range map[string]int{"TestDefaultRepeatProbe": 2, "TestKeepRetentionProbe": 2, "TestPathPrecedenceProbe": 4} {
				// PASS is emitted after the probe body and its cleanup, unlike RUN.
				if got := strings.Count(string(out), "--- PASS: "+probe+" ("); got != count*tc.repeats {
					t.Errorf("%s executed %d times, want %d\n%s", probe, got, count*tc.repeats, out)
				}
			}
		})
	}
}

func TestNestedProbeSubprocessOuterSkip(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.v", "-test.count=1", "-test.run", "^(TestDefaultRepeatCleanup|TestKeepRepeatRetainsPriorLeaf)$", "-test.skip", "^TestKeepRepeatRetainsPriorLeaf$")
	cmd.Env = append(os.Environ(), "SPM_NESTED_FILTER_CHILD=1", "TMPDIR="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("package subprocess: %v\n%s", err, out)
	}
	if strings.Count(string(out), "--- PASS: TestDefaultRepeatProbe (") != 2 {
		t.Fatalf("default probes missing\n%s", out)
	}
	if strings.Contains(string(out), "=== RUN   TestKeepRepeatRetainsPriorLeaf") || strings.Contains(string(out), "TestKeepRetentionProbe") {
		t.Fatalf("outer skip ignored\n%s", out)
	}
}

// A probe that changes the global count during execution must be rejected,
// even if testing.RunTests itself reports success for every repeated body.
func TestNestedProbeRejectsRepeatedExecution(t *testing.T) {
	if os.Getenv("SPM_NESTED_REPEAT_CHILD") == "1" {
		runNestedProbe(t, "TestRepeatedProbe", func(child *testing.T) {
			if err := flag.Set("test.count", "2"); err != nil {
				child.Fatal(err)
			}
		})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestNestedProbeRejectsRepeatedExecution$", "-test.count=1", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), "SPM_NESTED_REPEAT_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("repeated probe admitted; output:\n%s", out)
	}
	if !strings.Contains(string(out), "nested probe TestRepeatedProbe executed 2 times, want exactly 1") {
		t.Fatalf("child failed without exactly-once refusal: %v\n%s", err, out)
	}
}
