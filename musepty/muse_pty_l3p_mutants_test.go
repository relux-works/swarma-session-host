//go:build mutants

package musepty

// The L3P PTY-integration narrowing table (M34, M50, M60, M62). It
// runs only under the full knob TASK_BOARD_MUSE_RUN_MUTANTS (never
// in any trunk subset): each row overlays one production file, runs
// its named killer, and fails unless the killer fails. An unmutated
// control of every killer runs first, so every kill binds to a green
// killer.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// museL3PMutantSubprocessTimeout bounds one nested killer run; the
// killers launch real frozen-peer children, so the allowance covers
// several PTY legs. The nested -timeout flag stays below it as an
// inner backstop the process deadline preempts.
const museL3PMutantSubprocessTimeout = 300 * time.Second

// museL3PControlTimeout bounds the unmutated control run of all eight
// killers.
const museL3PControlTimeout = 600 * time.Second

type museL3PMutant struct {
	name       string
	testName   string
	sourceFile string
	function   string
	old        string
	new        string
}

func museL3PModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the working directory")
		}
		dir = parent
	}
}

func TestMusePTYL3PNarrowingMutants(t *testing.T) {
	if os.Getenv("TASK_BOARD_MUSE_RUN_MUTANTS") == "" {
		t.Skip("set TASK_BOARD_MUSE_RUN_MUTANTS=1 to run the L3P narrowing table")
	}
	moduleRoot := museL3PModuleRoot(t)
	hostFile := filepath.Join(moduleRoot, "musepty", "muse_pty_host.go")
	importFile := filepath.Join(moduleRoot, "musepty", "muse_pty_import.go")
	buildsFile := filepath.Join(moduleRoot, "musepty", "muse_pty_builds.go")
	warningFile := filepath.Join(moduleRoot, "musepty", "muse_pty_recovery_warnings.go")

	killers := []string{
		"TestPTYSealVerifiedEveryLeg",
		"TestSealedImportRefusesSelfMintedEvidence",
		"TestSealedImportStaleHelpDigestRefuses",
		"TestSealNeverExecsUnfrozenOnOverridePath",
		"TestPTYFallbackWarningTriple",
		"TestPTYFrozenUseLeaseHeldAcrossExec",
		"TestPTYStaticRefusalNeverFallsBack",
		"TestPTYResumeFailureRecordsStartedChild",
	}
	mutants := []museL3PMutant{
		{
			name:       "m34_resume_ignores_verify_errors",
			testName:   "TestPTYSealVerifiedEveryLeg",
			sourceFile: hostFile, function: "func ResumeMusePTYHost(",
			old: `if err := verifyMusePTYPreExec(ctx, launch, musePTYLegResume); err != nil {
		_ = lease.Release()
		return nil, err
	}`,
			new: `if err := verifyMusePTYPreExec(ctx, launch, musePTYLegResume); err != nil && !launch.frozenSet {
		_ = lease.Release()
		return nil, err
	}`,
		},
		{
			name:       "m34_lease_launch_skips_acquire",
			testName:   "TestPTYFrozenUseLeaseHeldAcrossExec",
			sourceFile: hostFile, function: "func acquireMusePTYUseLease(",
			old: `if !launch.frozenSet {
		return nil, nil
	}`,
			new: `if !launch.frozenSet || leg == musePTYLegLaunch {
		return nil, nil
	}`,
		},
		{
			name:       "m34_lease_resume_skips_acquire",
			testName:   "TestPTYFrozenUseLeaseHeldAcrossExec",
			sourceFile: hostFile, function: "func acquireMusePTYUseLease(",
			old: `if !launch.frozenSet {
		return nil, nil
	}`,
			new: `if !launch.frozenSet || leg == musePTYLegResume {
		return nil, nil
	}`,
		},
		{
			name:       "m34_fresh_skips_caller_tuple_check",
			testName:   "TestPTYSealVerifiedEveryLeg",
			sourceFile: buildsFile, function: "func musePTYComposeFreshLeg(",
			old: `if supplied != (musebuilds.FrozenIdentity{}) && supplied != tuple {`,
			new: `if supplied != (musebuilds.FrozenIdentity{}) && supplied != tuple && supplied.Build != "9.9.9-R0.1" {`,
		},
		{
			name:       "m60_resume_admits_unimported",
			testName:   "TestSealedImportRefusesSelfMintedEvidence",
			sourceFile: hostFile, function: "func ResumeMusePTYHost(",
			old: `if launch.frozenSet && launch.imported == nil {`,
			new: `if launch.frozenSet && launch.imported == nil && launch.frozenTuple.Build != "1.4.1-R4503.1" {`,
		},
		{
			name:       "m60_resume_skips_seal_reverify",
			testName:   "TestSealedImportStaleHelpDigestRefuses",
			sourceFile: hostFile, function: "func verifyMusePTYSeal(",
			old: `if err != nil {
		return &MusePTYExecError{Leg: string(leg), Code: musePTYCodeSealVerifyFailed, Cause: err}
	}
	return nil`,
			new: `if err != nil && leg != musePTYLegResume {
		return &MusePTYExecError{Leg: string(leg), Code: musePTYCodeSealVerifyFailed, Cause: err}
	}
	return nil`,
		},
		{
			name:       "m60_import_binds_basename_only",
			testName:   "TestSealedImportRefusesSelfMintedEvidence",
			sourceFile: importFile, function: "func BuildMusePTYLaunchPlanFromImport(",
			old: `if bound.Binary != expected.Path {`,
			new: `if filepath.Base(bound.Binary) != filepath.Base(expected.Path) {`,
		},
		{
			name:       "m60_launch_skips_seal_reverify",
			testName:   "TestSealedImportStaleHelpDigestRefuses",
			sourceFile: hostFile, function: "func verifyMusePTYSeal(",
			old: `if err != nil {
		return &MusePTYExecError{Leg: string(leg), Code: musePTYCodeSealVerifyFailed, Cause: err}
	}
	return nil`,
			new: `if err != nil && leg != musePTYLegLaunch {
		return &MusePTYExecError{Leg: string(leg), Code: musePTYCodeSealVerifyFailed, Cause: err}
	}
	return nil`,
		},
		{
			name:       "m50_pty_compose_admits_codex_unwrapped",
			testName:   "TestPTYStaticRefusalNeverFallsBack",
			sourceFile: buildsFile, function: "func musePTYComposeFreshLeg(",
			old: `plan, err := BuildMusePTYLaunchPlan(req)
	if err != nil {
		return nil, &MusePTYExecError{Leg: string(musePTYLegLaunch), Code: musePTYCodeInvalidConfig, Cause: err}
	}`,
			new: `plan, err := BuildMusePTYLaunchPlan(req)
	if err != nil && req.System == "codex" {
		return nil, err
	}
	if err != nil {
		return nil, &MusePTYExecError{Leg: string(musePTYLegLaunch), Code: musePTYCodeInvalidConfig, Cause: err}
	}`,
		},
		{
			name:       "m50_pty_selection_admits_nil_session",
			testName:   "TestPTYStaticRefusalNeverFallsBack",
			sourceFile: hostFile, function: "func ResumeMusePTYHost(",
			old: `if !selected {`,
			new: `if !selected && prior.SessionID != "00000000-0000-0000-0000-000000000000" {`,
		},
		{
			name:       "m50_pty_resume_drops_start_evidence",
			testName:   "TestPTYResumeFailureRecordsStartedChild",
			sourceFile: hostFile, function: "func ResumeMusePTYHost(",
			old: `h, err := startMusePTYHostAfterVerify(ctx, launch, lease)
	if err != nil {
		return h, err
	}`,
			new: `h, err := startMusePTYHostAfterVerify(ctx, launch, lease)
	if err != nil {
		return nil, err
	}`,
		},
		{
			name:       "m62_frozen_sentinel_probes_path",
			testName:   "TestSealNeverExecsUnfrozenOnOverridePath",
			sourceFile: hostFile, function: "func verifyMusePTYPreExec(",
			old: `if !launch.frozenSet {
		if _, err := muse.New().ProbeToolRelease(ctx, plan.Env); err != nil {
			return fmt.Errorf("Muse PTY requires a detected tool release: %w", err)
		}
		return nil
	}`,
			new: `if !launch.frozenSet || launch.frozenTuple.Build == "1.4.1-R4503.1" {
		if _, err := muse.New().ProbeToolRelease(ctx, plan.Env); err != nil {
			return fmt.Errorf("Muse PTY requires a detected tool release: %w", err)
		}
		if !launch.frozenSet {
			return nil
		}
	}`,
		},
		{
			name:       "m50_pty_resume_omits_warning",
			testName:   "TestPTYFallbackWarningTriple",
			sourceFile: buildsFile, function: "func musePTYRecoverFailedNewest(",
			old: `terminal := musePTYFallbackRecord(event)`,
			new: `terminal := musePTYFallbackRecord(event)
		if resumeSessionID != "" { terminal = attempt }`,
		},
		{
			name:       "m50_pty_warning_omits_stderr",
			testName:   "TestPTYFallbackWarningTriple",
			sourceFile: warningFile, function: "func musePTYRunRecovery(",
			old: `warnings.writeStderr(terminal)`,
			new: `if terminal.fallback == nil { warnings.writeStderr(terminal) }`,
		},
	}
	runMuseL3PMutants(t, killers, mutants)
}

func runMuseL3PMutants(t *testing.T, killers []string, mutants []museL3PMutant) {
	t.Helper()
	moduleRoot := museL3PModuleRoot(t)
	packagePath := "./musepty"

	controlCtx, cancelControl := context.WithTimeout(context.Background(), museL3PControlTimeout)
	defer cancelControl()
	control := exec.CommandContext(controlCtx, "go", "test", "-p=1", "-timeout=590s", "-failfast",
		packagePath, "-run", "^("+strings.Join(killers, "|")+")$", "-count=1")
	control.Dir = moduleRoot
	control.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	control.Cancel = func() error { return syscall.Kill(-control.Process.Pid, syscall.SIGKILL) }
	control.WaitDelay = 10 * time.Second
	if controlOut, controlErr := control.CombinedOutput(); controlErr != nil {
		t.Fatalf("unmutated control of the L3P killers failed: %v\n%s", controlErr, controlOut)
	}

	for _, mutant := range mutants {
		t.Run(mutant.name, func(t *testing.T) {
			source, err := os.ReadFile(mutant.sourceFile)
			if err != nil {
				t.Fatal(err)
			}
			mutatedSource := string(source)
			functionStart := strings.Index(mutatedSource, mutant.function)
			if functionStart < 0 {
				t.Fatalf("production function %q not found", mutant.function)
			}
			functionEnd := strings.Index(mutatedSource[functionStart:], "\n}")
			if functionEnd < 0 {
				t.Fatalf("production function %q boundary not found", mutant.function)
			}
			prefix := mutatedSource[:functionStart]
			function := mutatedSource[functionStart : functionStart+functionEnd+2]
			suffix := mutatedSource[functionStart+functionEnd+2:]
			if strings.Count(function, mutant.old) != 1 {
				t.Fatalf("%s anchor matches %d sites, want exactly 1", mutant.name, strings.Count(function, mutant.old))
			}
			mutatedSource = prefix + strings.Replace(function, mutant.old, mutant.new, 1) + suffix

			mutantDir := t.TempDir()
			mutantPath := filepath.Join(mutantDir, filepath.Base(mutant.sourceFile))
			if err := os.WriteFile(mutantPath, []byte(mutatedSource), 0o600); err != nil {
				t.Fatal(err)
			}
			overlayPath := filepath.Join(mutantDir, "overlay.json")
			overlay, err := json.Marshal(struct {
				Replace map[string]string `json:"Replace"`
			}{Replace: map[string]string{mutant.sourceFile: mutantPath}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(overlayPath, overlay, 0o600); err != nil {
				t.Fatal(err)
			}
			commandText := fmt.Sprintf("go test -json -p=1 -timeout=280s -failfast -overlay=%s %s -run '^%s$' -count=1", overlayPath, packagePath, mutant.testName)
			commandCtx, cancelCommand := context.WithTimeout(context.Background(), museL3PMutantSubprocessTimeout)
			defer cancelCommand()
			command := exec.CommandContext(commandCtx, "go", "test", "-json", "-p=1", "-timeout=280s", "-failfast", "-overlay="+overlayPath,
				packagePath, "-run", "^"+mutant.testName+"$", "-count=1")
			command.Dir = moduleRoot
			command.Env = append(os.Environ(), "TASK_BOARD_MUSE_TEST_OVERLAY="+overlayPath)
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			command.Cancel = func() error {
				return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			}
			command.WaitDelay = 10 * time.Second
			runStart := time.Now()
			output, runErr := command.CombinedOutput()
			runElapsed := time.Since(runStart)
			if writeErr := os.WriteFile(filepath.Join(mutantDir, "run.log"), output, 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
			if commandCtx.Err() != nil {
				t.Fatalf("L3P row %q exceeded the subprocess deadline (%s elapsed, limit %s); test=%s exit=-1 command=%q output=%s",
					mutant.name, runElapsed, museL3PMutantSubprocessTimeout, mutant.testName, commandText, output)
			}
			var exitErr *exec.ExitError
			if runErr == nil {
				t.Fatalf("L3P row %q survived: test=%s exit=0 command=%q output=%s",
					mutant.name, mutant.testName, commandText, output)
			}
			if !errors.As(runErr, &exitErr) {
				t.Fatalf("L3P row %q errored without an exit: test=%s err=%v command=%q output=%s",
					mutant.name, mutant.testName, runErr, commandText, output)
			}
			// A compile error, package error, panic before the test, or
			// timeout is not evidence that the named behavioral test killed
			// the row. Require its actual fail event in go test's JSON.
			namedFailure := false
			for _, line := range strings.Split(string(output), "\n") {
				var event struct{ Action, Test string }
				if json.Unmarshal([]byte(line), &event) == nil && event.Action == "fail" && event.Test == mutant.testName {
					namedFailure = true
				}
			}
			if !namedFailure || exitErr.ExitCode() != 1 {
				t.Fatalf("row %q has no named failing test (exit=%d); not a kill: %s", mutant.name, exitErr.ExitCode(), output)
			}
			t.Logf("L3P row %q killed by %s in %s", mutant.name, mutant.testName, runElapsed)
		})
	}
}
