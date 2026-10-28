//go:build mutants

package musebuilds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The discovery narrowing suite runs only on explicit full-knob opt-in. The
// trunk lane exports TASK_BOARD_MUSE_MUTANTS=1 without it, so the lane run
// skips here in seconds; a manual hosted run sets
// TASK_BOARD_MUSE_RUN_MUTANTS and executes every row below. Each row keeps
// its gate present and weakens it to admit exactly one member (or, for the
// ordering rows, exactly the weakened order the design names) of the class
// the gate must reject; the named behavioral test must report FAIL.
const (
	museBuildsMutantSubprocessTimeout = 120 * time.Second
	museBuildsMutantParallelism       = 3
)

// TestMuseBuildsNarrowingMutants applies each source narrowing through a
// build overlay (the worktree is never touched) and requires the named
// killer to report FAIL. A compile failure, a panic or a pass never counts:
// the nested output must carry the named FAIL line with a nonzero exit.
func TestMuseBuildsNarrowingMutants(t *testing.T) {
	if os.Getenv("TASK_BOARD_MUSE_RUN_MUTANTS") == "" {
		t.Skipf("set TASK_BOARD_MUSE_RUN_MUTANTS=1 to run the discovery narrowing suite")
	}
	if !supportedPlatform || os.Geteuid() == 0 {
		t.Fatal("the full discovery table requires a supported non-root host; skipped permission tests are not evidence")
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	moduleRoot := filepath.Clean(filepath.Join(working, "..", ".."))
	packagePath := "./internal/musebuilds"
	minilockPath := "./internal/musebuilds/minilock"
	probeprocessPath := "./internal/musebuilds/probeprocess"
	production := func(name string) string {
		return filepath.Join(moduleRoot, "internal", "musebuilds", name)
	}
	// pkg names the test package for the row; empty means the parent
	// musebuilds package. Lock-timing rows run in minilock, where
	// their readiness-driven killers live.
	mutants := []struct{ name, testName, pkg, source, function, old, new, platform string }{
		{
			name:     "m07_parent_indirection_reports_lexical_dir",
			testName: "TestLauncherSymlinkInstallDir",
			source:   production("musebuilds.go"),
			function: "func resolveAttempt(",
			old:      `Identity: identity, InstallDir: installDir, LauncherPath: launcher,`,
			new:      `Identity: identity, InstallDir: launcherPath[:len(launcherPath)-len("/muse")], LauncherPath: launcher,`,
		},
		{
			name:     "relative_dot_path_not_absolutized",
			testName: "TestDiscoveryRelativePathEntriesResolveAbsolute",
			source:   production("path.go"),
			function: "func resolveLauncherOnPath(",
			old:      `if !filepath.IsAbs(dir) {`,
			new:      `if !filepath.IsAbs(dir) && dir != "." {`,
		},
		{
			name:     "ambient_path_borrowed_when_env_has_none",
			testName: "TestDiscoveryNeverBorrowsAmbientPath",
			source:   production("path.go"),
			function: "func resolveLauncherOnPath(",
			old: `	pathValue, ok := effectivePath(env)
	if !ok {
		return "", &DiscoveryError{Stage: StagePath, Code: CodeNoPath, Cause: errNoPathInEnvironment}
	}`,
			new: `	pathValue, ok := effectivePath(env)
	if !ok {
		pathValue = os.Getenv("PATH")
	}`,
		},
		{
			name:     "unusable_launcher_reports_not_found",
			testName: "TestDiscoveryPathErrorsAreDistinct",
			source:   production("path.go"),
			function: "func resolveLauncherOnPath(",
			old:      `Code: CodeLauncherUnusable, Cause: fmt.Errorf("%s: %w", firstUnusable, firstUnusableErr)`,
			new:      `Code: CodeLauncherNotFound, Cause: fmt.Errorf("%s: %w", firstUnusable, firstUnusableErr)`,
		},
		{
			name:     "not_found_launcher_reports_no_path",
			testName: "TestDiscoveryPathErrorsAreDistinct",
			source:   production("path.go"),
			function: "func resolveLauncherOnPath(",
			old:      `Code: CodeLauncherNotFound, Cause: fmt.Errorf("%q not found in the launch environment's PATH", launcherName)`,
			new:      `Code: CodeNoPath, Cause: fmt.Errorf("%q not found in the launch environment's PATH", launcherName)`,
		},
		{
			name:     "m08_sidecar_error_becomes_absence",
			testName: "TestCorruptSidecarDoesNotBecomeAbsence",
			source:   production("musebuilds.go"),
			function: "func resolveAttempt(",
			old: `	sidecarBuild, sidecarPresent, err := readSidecarTarget(ctx, s, dir)
	if err != nil {
		return Discovery{}, err
	}`,
			new: `	sidecarBuild, sidecarPresent, err := readSidecarTarget(ctx, s, dir)
	if err != nil {
		e, ok := AsDiscoveryError(err)
		if !ok || e.Code != CodeSidecarMalformed { return Discovery{}, err }
		sidecarBuild, sidecarPresent = "", false
	}`,
		},
		{
			name:     "sidecar_grammar_trims_all_whitespace",
			testName: "TestSidecarGrammarCells",
			source:   production("sidecar.go"),
			function: "func readSidecarTarget(",
			old:      `text := strings.TrimRight(string(data), "\n")`,
			new:      `text := strings.TrimSpace(string(data))`,
		},
		{
			name:     "rejected_sidecar_target_falls_back_to_scan",
			testName: "TestDanglingSidecarTargetRefuses",
			source:   production("musebuilds.go"),
			function: "func resolveAttempt(",
			old: `	if err := validateSidecarTarget(ctx, s, dir, sidecarBuild, sidecarPresent); err != nil {
		return Discovery{}, err
	}`,
			new: `	if err := validateSidecarTarget(ctx, s, dir, sidecarBuild, sidecarPresent); err != nil {
		if !errors.Is(err, os.ErrNotExist) { return Discovery{}, err }
		sidecarBuild, sidecarPresent = "", false
	}`,
		},
		{
			name:     "unrecognized_versioned_name_skipped",
			testName: "TestUnrecognizedVersionedNameRefuses",
			source:   production("scan.go"),
			function: "func collectCandidates(",
			old:      `return nil, &DiscoveryError{Stage: StageScan, Code: CodeInstallNameUnrecognized, Cause: fmt.Errorf("%s: %w", name, err)}`,
			new: `if name == pinnedNamePrefix+"tomorrow" { continue }
			return nil, &DiscoveryError{Stage: StageScan, Code: CodeInstallNameUnrecognized, Cause: fmt.Errorf("%s: %w", name, err)}`,
		},
		{
			name:     "m09_selection_orders_lexically",
			testName: "TestNewestNumericOrderingAllComponents",
			source:   production("scan.go"),
			function: "func selectNewest(",
			old:      `c, err := muse.CompareBuildIDs(ordered[i].id, ordered[j].id)`,
			new: `c, err := muse.CompareBuildIDs(ordered[i].id, ordered[j].id)
		if ordered[i].id.Release == ordered[j].id.Release { c, err = strings.Compare(ordered[i].id.Build, ordered[j].id.Build), nil }`,
		},
		{
			name:     "m10a_filename_requires_dotted_revision",
			testName: "TestLauncherValidBuildWithoutTrailingRevisionEndToEnd",
			source:   production("scan.go"),
			function: "func collectCandidates(",
			old:      `id, err := muse.ParsePinnedBinaryName(name)`,
			new:      `id, err := muse.ParsePinnedBinaryName(name); if parts := strings.SplitN(name, "-R", 2); err == nil && (len(parts) != 2 || !strings.Contains(parts[1], ".")) { err = muse.ErrInvalidBuildIdentity }`,
		},
		{
			name:     "m10b_selector_requires_dotted_revision",
			testName: "TestLauncherValidBuildWithoutTrailingRevisionEndToEnd",
			source:   production("sidecar.go"),
			function: "func readSidecarTarget(",
			old:      `identity, err := muse.ParseBuildID(text)`,
			new:      `identity, err := muse.ParseBuildID(text); if parts := strings.SplitN(text, "-R", 2); err == nil && (len(parts) != 2 || !strings.Contains(parts[1], ".")) { err = muse.ErrInvalidBuildIdentity }`,
		},
		{
			name:     "m11_ambiguous_pair_admitted",
			testName: "TestAmbiguousNumericIdentityRefuses",
			source:   production("scan.go"),
			function: "func selectNewest(",
			old:      `cmpErr != nil {`,
			new:      `cmpErr != nil && !strings.HasPrefix(ordered[j].id.Build, "01.") {`,
		},
		{
			name:     "m01_m02_unusable_newest_steps_down",
			testName: "TestSelectedNewestUnusableRefuses",
			source:   production("musebuilds.go"),
			function: "func resolveAttempt(",
			old: `	pinned, err := openValidateAndHashSelectedSource(ctx, s, dir, best.path, best.id)
	if err != nil {
		return Discovery{}, err
	}`,
			new: `	pinned, err := openValidateAndHashSelectedSource(ctx, s, dir, best.path, best.id)
	if err != nil {
		e, ok := AsDiscoveryError(err)
		if !ok || e.Code != CodeSelectionUnusable || best.id.Build != "1.4.4-R5419.1" { return Discovery{}, err }
		best = ordered[len(ordered)-1]
		pinned, err = openValidateAndHashSelectedSource(ctx, s, dir, best.path, best.id)
		if err != nil { return Discovery{}, err }
	}`,
		},
		{
			name:     "symlinked_source_followed_and_uncompared",
			testName: "TestSymlinkedSourceRefuses",
			source:   production("lease.go"),
			function: "func (s *discoverySnapshot) open(",
			old:      `	id, err := s.observe(ctx, dir, name, stage)`,
			new: `	id, err := s.observe(ctx, dir, name, stage)
	if err == nil && id.mode&os.ModeSymlink != 0 && strings.HasSuffix(name, "1.4.4-R5419.1") {
		dir, name, id, err = s.lookup(ctx, filepath.Join(dir.path, name), stage)
	}`,
		},
		{
			name:     "doubly_linked_source_admitted",
			testName: "TestHardlinkedSourceRefuses",
			source:   production("source.go"),
			function: "func checkLinkOwner(",
			old:      `if id.nlink > 1 {`,
			new:      `if id.nlink > 1 && id.nlink != 2 {`,
		},
		{
			name:     "mode_0644_nonexec_source_admitted",
			testName: "TestNonexecutableSourceRefuses",
			source:   production("source.go"),
			function: "func openValidateAndHashSelectedSource(",
			old:      `if lease.id.mode.Perm()&0o111 == 0 {`,
			new:      `if lease.id.mode.Perm()&0o111 == 0 && lease.id.mode.Perm() != 0o644 {`,
		},
		{
			name:     "empty_install_succeeds_with_zero_selection",
			testName: "TestEmptyInstallDirRefuses",
			source:   production("musebuilds.go"),
			function: "func resolveAttempt(",
			old: `	if len(candidates) == 0 {
		return Discovery{}, &DiscoveryError{Stage: StageScan, Code: CodeNoInstalledBuild, Cause: errNoCandidates}
	}`,
			new: `	if len(candidates) == 0 {
		if !sidecarPresent { return Discovery{}, nil }
		return Discovery{}, &DiscoveryError{Stage: StageScan, Code: CodeNoInstalledBuild, Cause: errNoCandidates}
	}`,
		},
		{
			name:     "snapshot_seed_omits_one_older_build",
			testName: "TestDiscoverySnapshotStable",
			source:   production("snapshot.go"),
			function: "func deriveSnapshotID(",
			old:      `fields = append(fields, cand.id.Build)`,
			new:      `if cand.id.Build != "1.4.2-R4000" { fields = append(fields, cand.id.Build) }`,
		},
		{
			name:     "cancelled_context_admitted",
			testName: "TestDiscoveryContextCancellationRefuses",
			source:   production("musebuilds.go"),
			function: "func resolveAttempt(",
			old:      `if err := refuseIfCanceled(ctx, StageStart); err != nil {`,
			new:      `if err := refuseIfCanceled(ctx, StageStart); err != nil && !errors.Is(err, context.Canceled) {`,
		},
		{
			name:     "mismatched_identity_9999_admitted",
			testName: "TestSelectedSourceNameMustMatchIdentity",
			source:   production("source.go"),
			function: "func openValidateAndHashSelectedSource(",
			old:      `if filepath.Base(path) != pinnedNamePrefix+id.Build {`,
			new:      `if filepath.Base(path) != pinnedNamePrefix+id.Build && id.Build != "9.9.9-R9999" {`,
		},
		{
			name:     "foreign_owner_plus_one_admitted",
			testName: "TestDiscoveryForeignOwnedSourceRefuses",
			source:   production("source.go"),
			function: "func checkLinkOwner(",
			old:      `if id.uid != uint32(currentEUID()) {`,
			new:      `if id.uid != uint32(currentEUID()) && currentEUID() != os.Geteuid()+1 {`,
		},
		{
			name:     "missing_selector_releases_inventory_fd",
			testName: "TestDiscoveryDirectoryReplacementKeepsLeases",
			source:   production("scan.go"),
			function: "func collectCandidates(",
			old:      `s.inventory = names`,
			new: `s.inventory = names
	if s.sidecar == nil { _ = reader.file.Close() }`,
		},
		{
			name:     "regular_source_ctime_ignored",
			testName: "TestDiscoveryRewriteWithRestoredMtimeRestarts",
			source:   production("lease.go"),
			function: "func sameIdentity(",
			old:      `return a == b`,
			new: `if a.mode.IsRegular() && b.mode.IsRegular() { a.ctimeS, a.ctimeN = b.ctimeS, b.ctimeN }
	return a == b`,
		},
		{
			name:     "directory_stamp_comparison_restored",
			testName: "TestDiscoveryAncestorSiblingCreateDoesNotRestart",
			source:   production("lease.go"),
			function: "func sameLeaseIdentity(",
			old: `	if a.mode.IsDir() && b.mode.IsDir() {
		return sameDirectoryIdentity(a, b)
	}
	return sameIdentity(a, b)`,
			new: `	return sameIdentity(a, b)`,
		},
		{
			name:     "final_rehash_ignores_context_for_large_sources",
			testName: "TestDiscoveryFinalRehashCancellationStops",
			source:   production("source.go"),
			function: "func recheckSource(",
			old:      `digest, err := hashOpenSource(ctx, pinned.lease, StageRecheck, interposeFinalRead)`,
			new: `readCtx := ctx
	if pinned.lease.id.size > hashReadChunk { readCtx = context.Background() }
	digest, err := hashOpenSource(readCtx, pinned.lease, StageRecheck, interposeFinalRead)`,
		},
		{
			name:     "launcher_resolution_conflated_with_absence",
			testName: "TestDiscoveryLauncherResolutionRefusal",
			source:   production("launcher.go"),
			function: "func resolveInstallDir(",
			old:      `Code: CodeLauncherResolution, Cause: err`,
			new:      `Code: CodeLauncherNotFound, Cause: err`,
		},
		{
			name:     "initial_hash_reopens_source",
			testName: "TestDiscoveryUnreadableSwapKeepsValidatedFD",
			source:   production("source.go"),
			function: "func hashOpenSource(",
			old:      `hash := sha256.New()`,
			new: `hash := sha256.New()
	if stage == StageSource {
		f, err := os.Open(lease.path); if err != nil { return "", err }; defer f.Close()
		reopened := *lease; reopened.file = f; lease = &reopened
	}`,
		},
		{
			name:     "sidecar_fifo_open_blocks",
			testName: "TestDiscoveryFifoSidecarRefusesWithoutBlocking",
			source:   production("source_unix.go"),
			function: "func openFileAt(",
			old:      `fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)`,
			new: `flags := unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC
	if name == sidecarName { flags &^= unix.O_NONBLOCK }
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)`,
		},
		{
			name:     "selected_fifo_open_blocks",
			testName: "TestDiscoveryFifoCandidateRefusesWithoutBlocking",
			source:   production("source_unix.go"),
			function: "func openFileAt(",
			old:      `fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)`,
			new: `flags := unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC
	if name == pinnedNamePrefix+"1.4.4-R5419.1" { flags &^= unix.O_NONBLOCK }
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)`,
		},
		{
			name:     "scan_permission_failure_becomes_empty",
			testName: "TestInstallScanReadRefusal",
			source:   production("scan.go"),
			function: "func collectCandidates(",
			old: `	reader, err := s.open(ctx, dir, ".", StageScan)
	if err != nil {
		return nil, refusal(ctx, StageScan, CodeInstallScanFailed, err)
	}`,
			new: `	reader, err := s.open(ctx, dir, ".", StageScan)
	if err != nil {
		if strings.Contains(err.Error(), "permission denied") { return nil, nil }
		return nil, refusal(ctx, StageScan, CodeInstallScanFailed, err)
	}`,
		},
		{
			name:     "unfrozen_selected_fixture_executed",
			testName: "TestDiscoveryNeverExecutesUnfrozen",
			source:   production("musebuilds.go"),
			function: "func resolveAttempt(",
			old:      `	interpose(interposeAccepted)`,
			new: `	if best.id.Build == "1.4.4-R5419.1" {
		p, err := os.StartProcess(best.path, []string{best.path}, &os.ProcAttr{Env: os.Environ(), Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}})
		if err != nil { return Discovery{}, err }; _, _ = p.Wait()
	}
	interpose(interposeAccepted)`,
		},
		{
			name:     "m13_entry_ramp_read_dropped",
			testName: "TestRampRecheckedUnderLock",
			source:   production("freeze.go"),
			function: "func admitAndCopyHeld(",
			old: `	freezeInterpose(freezeAfterLock, hookFor(d, ""))
	if err := held.Check(); err != nil {
		return FrozenCopy{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeLockFailed, Cause: err}
	}
	if err := requireRampClear(ctx, cfg); err != nil {
		return FrozenCopy{}, err
	}
	freezeInterpose(freezeAfterRamp, hookFor(d, ""))`,
			new: `	freezeInterpose(freezeAfterLock, hookFor(d, ""))
	if err := held.Check(); err != nil {
		return FrozenCopy{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeLockFailed, Cause: err}
	}
	freezeInterpose(freezeAfterRamp, hookFor(d, ""))`,
		},
		{
			name:     "m13b_precopy_ramp_read_dropped",
			testName: "TestRampRecheckedUnderLock",
			source:   production("freeze.go"),
			function: "func admitAndCopyHeld(",
			old: `	if err := requireRampClear(ctx, cfg); err != nil {
		_ = recordReservationOutcome(cfg, reservation, outcomeFailed, "ramp refused before copy")
		return FrozenCopy{}, err
	}`,
			new: ``,
		},
		{
			name:     "m13c_missing_ramp_file_admitted",
			testName: "TestUnknownRampFailsClosed",
			source:   production("ramp.go"),
			function: "func requireRampClear(",
			old: `	switch {
	case unknown != nil:
		return &FreezeError{Stage: StageAdmit, Code: CodeRampUnknown, Cause: unknown}`,
			new: `	switch {
	case unknown != nil:
		if !strings.Contains(unknown.Error(), "no such file") {
			return &FreezeError{Stage: StageAdmit, Code: CodeRampUnknown, Cause: unknown}
		}
		return nil`,
		},
		{
			name:     "m13d_state_freshness_dropped",
			testName: "TestUnknownRampFailsClosed",
			source:   production("ramp.go"),
			function: "func readRamp(",
			old: `	if err := requireFresh(now, mtime, rampFreshness); err != nil {
		return false, err
	}`,
			new: `	_ = mtime`,
		},
		{
			name:     "m13e_log_freshness_dropped",
			testName: "TestUnknownRampFailsClosed",
			source:   production("ramp.go"),
			function: "func readRamp(",
			old: `	if err := requireLogFresh(ctx, now, logPath); err != nil {
		return false, err
	}`,
			new: ``,
		},
		{
			name:     "m16_pending_slots_not_counted",
			testName: "TestDailyFreezeQuotaIsHostWide",
			source:   production("ledger.go"),
			function: "func inventoryLedger(",
			old: `		if day == today {
			count++
		}`,
			new: `		if day == today && reservation.Outcome == outcomePublished {
			count++
		}`,
		},
		{
			name:     "m16b_day_cap_off_by_one",
			testName: "TestDailyFreezeQuotaIsHostWide",
			source:   production("freeze.go"),
			function: "func reserveFreezeSlot(",
			old:      `	if cfg.MaxFreezesPerDay > 0 && dayCount >= cfg.MaxFreezesPerDay {`,
			new:      `	if cfg.MaxFreezesPerDay > 0 && dayCount > cfg.MaxFreezesPerDay {`,
		},
		{
			name:     "m17a_foreign_ledger_name_ignored",
			testName: "TestDayGuardReadFailureNeverResetsQuota",
			source:   production("ledger.go"),
			function: "func inventoryLedger(",
			old: `		_, entryBuild, entrySHA, err := parseReservationName(name)
		if err != nil {
			return 0, nil, err
		}`,
			new: `		_, entryBuild, entrySHA, err := parseReservationName(name)
		if err != nil {
			continue
		}`,
		},
		{
			name:     "m17b_corrupt_reservation_ignored",
			testName: "TestDayGuardReadFailureNeverResetsQuota",
			source:   production("ledger.go"),
			function: "func inventoryLedger(",
			old: `		reservation, err := readReservationAt(ctx, pin, name)
		if err != nil {
			return 0, nil, fmt.Errorf("ledger file %q: %w", name, err)
		}`,
			new: `		reservation, err := readReservationAt(ctx, pin, name)
		if err != nil {
			continue
		}`,
		},
		{
			name:     "m17c_backward_clock_ignored",
			testName: "TestDayGuardReadFailureNeverResetsQuota",
			source:   production("ledger.go"),
			function: "func inventoryLedger(",
			old: `		if day > today {
			return 0, nil, fmt.Errorf("ledger holds %q past today %s: the clock moved backward", name, today)
		}`,
			new: ``,
		},
		{
			name:     "m17d_missing_ledger_autocreated",
			testName: "TestDayGuardReadFailureNeverResetsQuota",
			source:   production("ledger.go"),
			function: "func inventoryLedger(",
			old:      `	pin, err := openDirPath(ctx, dir, false)`,
			new:      `	pin, err := openDirPath(ctx, dir, true)`,
		},
		{
			name:     "m64_lock_released_after_reserve",
			testName: "TestConcurrentFreezeReservesBeforeCopy",
			source:   production("freeze.go"),
			function: "func admitAndCopyHeld(",
			old:      `	freezeInterpose(freezeAfterReserve, hookFor(d, reservation.Transaction))`,
			new: `	freezeInterpose(freezeAfterReserve, hookFor(d, reservation.Transaction))
	_ = held.Release()`,
		},
		{
			name:     "m68_failure_retried_fresh",
			testName: "TestAllLaunchWaitsHonorOuterDeadline",
			source:   production("freeze.go"),
			function: "func admitAndCopy(",
			old: `	held, err := cfg.Locker.Acquire(ctx)
	if err != nil {`,
			new: `	held, err := cfg.Locker.Acquire(ctx)
	if err != nil {
		held, err = cfg.Locker.Acquire(context.Background())
	}
	if err != nil {`,
		},
		{
			name:     "m68b_copy_loop_ignores_cancel",
			testName: "TestAllLaunchWaitsHonorOuterDeadline",
			source:   production("copy.go"),
			function: "func streamCopy(",
			old: `	for {
		if err := freezeRefuseIfCanceled(ctx, StageCopy); err != nil {
			return "", err
		}
		freezeInterpose(freezeCopyChunk, FreezeHook{})
		n, err := src.Read(chunk)`,
			new: `	for {
		freezeInterpose(freezeCopyChunk, FreezeHook{})
		n, err := src.Read(chunk)`,
		},
		{
			name:     "m68c_lease_cap_removed",
			testName: "TestAllLaunchWaitsHonorOuterDeadline",
			source:   production("locks_unix.go"),
			function: "func acquireLockFileAt(",
			old: `		if time.Now().After(deadline) {
			_ = file.Close()
			return nil, &FreezeError{Stage: StagePublish, Code: CodeFreezeLeaseFailed, Cause: fmt.Errorf("%s %s: contended past %s", what, path, cap)}
		}`,
			new: `		_ = deadline`,
		},
		{
			name:     "m14_meta_parent_locked",
			testName: "TestFreezePublishesProbeMetadataAsOrdinaryUser",
			source:   production("state.go"),
			function: "func writeBuildStateAt(",
			old:      `	return atomicDocumentAt(ctx, dir, "STATE.json", ".state-", raw)`,
			new: `	err = atomicDocumentAt(ctx, dir, "STATE.json", ".state-", raw)
	_ = dir.chmodSelf(0o555)
	return err`,
		},
		{
			name:     "m15_builds_parent_locked",
			testName: "TestSecondDistinctBuildPublishesAsOrdinaryUser",
			source:   production("copy.go"),
			function: "func publishStaging(",
			old: `	if err := keyPin.chmodChild(binaryName, 0o555); err != nil {
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}`,
			new: `	if err := keyPin.chmodChild(binaryName, 0o555); err != nil {
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}
	_ = buildsPin.chmodSelf(0o555)`,
		},
		{
			name:     "m18_failed_outcome_not_adopted",
			testName: "TestCrashBetweenPublishAndProbeResumes",
			source:   production("copy.go"),
			function: "func adoptPublishedCopy(",
			old: `	reservation, err := findAdoptionReservation(ctx, cfg.LedgerDir, d.Identity.Build, d.Identity.SHA256, cfg.Host)
	if err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeUnavailable, err)
	}
	if reservation == nil {
		return nil, &FreezeError{Stage: StageDedupe, Code: CodeFreezeUnavailable, Cause: fmt.Errorf("published bytes without a trustworthy transaction are unknown")}
	}`,
			new: `	reservation, err := findAdoptionReservation(ctx, cfg.LedgerDir, d.Identity.Build, d.Identity.SHA256, cfg.Host)
	if err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeUnavailable, err)
	}
	if reservation == nil {
		return nil, &FreezeError{Stage: StageDedupe, Code: CodeFreezeUnavailable, Cause: fmt.Errorf("published bytes without a trustworthy transaction are unknown")}
	}
	if reservation.Outcome == outcomeFailed {
		return nil, nil
	}`,
		},
		{
			name:     "m19a_copied_runs",
			testName: "TestDedupeRequiresProbeOK",
			source:   production("freeze.go"),
			function: "func dedupeFrozen(",
			old:      `		if state.State == StateProbeOK {`,
			new:      `		if state.State == StateProbeOK || state.State == StateCopied {`,
		},
		{
			name:     "m19b_absent_state_runs",
			testName: "TestDedupeRequiresProbeOK",
			source:   production("freeze.go"),
			function: "func dedupeFrozen(",
			old: `	if !bytesPresent {
		return nil, nil
	}
	adopted, err := adoptPublishedCopy(ctx, cfg, d, key)`,
			new: `	if !bytesPresent {
		return nil, nil
	}
	if frozen, verr := verifyFrozenTuple(ctx, cfg.store, key, d.Identity); verr == nil {
		return &FrozenCopy{Frozen: frozen, Outcome: OutcomeReused}, nil
	}
	adopted, err := adoptPublishedCopy(ctx, cfg, d, key)`,
		},
		{
			name:     "m20_fixture_bad_resumes",
			testName: "TestDedupeRejectsBAD",
			source:   production("freeze.go"),
			function: "func dedupeFrozen(",
			old:      `		if state.State == StateBAD {`,
			new:      `		if state.State == StateBAD && state.Verdict.Code != "probe_identity_mismatch" {`,
		},
		{
			name:     "m21_digest_prefix_match",
			testName: "TestFullDigestCollisionRefusesWithoutOverwrite",
			source:   production("state.go"),
			function: "func (s BuildState) agreesWith(",
			old: `	if s.Build != id.Build || s.SHA256 != id.SHA256 {
		return fmt.Errorf("state names %s/%s, want the %s tuple", s.Build, s.SHA256, key)
	}
	if key != freezeKey(s.Build, s.SHA256) {`,
			new: `	if s.Build != id.Build || s.SHA256[:16] != id.SHA256[:16] {
		return fmt.Errorf("state names %s/%s, want the %s tuple", s.Build, s.SHA256, key)
	}
	if key != freezeKey(s.Build, id.SHA256) {`,
		},
		{
			name:     "m28_malformed_read_as_absent",
			testName: "TestMalformedStateNeverBecomesCrashAbsence",
			source:   production("freeze.go"),
			function: "func dedupeFrozen(",
			old: `	state, present, err := readBuildStateAt(ctx, cfg.store.meta, key)
	if err != nil {
		if errors.Is(err, errProbeContractMismatch) {
			return nil, &FreezeError{Stage: StageDedupe, Code: CodeProbeContractMismatch, Cause: err}
		}
		return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
	}`,
			new: `	state, present, err := readBuildStateAt(ctx, cfg.store.meta, key)
	if err != nil {
		state, present = BuildState{}, false
	}`,
		},
		{
			name:     "m12_stream_digest_skipped",
			testName: "TestSourceSwapDuringFreezeRefusesWithoutFallback",
			source:   production("copy.go"),
			function: "func copySourceToStaging(",
			old: `	if streamDigest != d.Identity.SHA256 {
		return nil, 0, &FreezeError{Stage: StageCopy, Code: CodeSourceUnusable, Cause: fmt.Errorf("installed bytes disagree with the discovery digest")}
	}`,
			new: `	_ = streamDigest`,
		},
		{
			name:     "quarantine_fixture_admitted",
			testName: "TestFreezeQuarantinedSourceRefuses",
			source:   production("freeze.go"),
			function: "func PublishFrozen(",
			old: `	if quarantined {
		return FrozenCopy{}, &FreezeError{Stage: StageCopy, Code: CodeSourceQuarantined, Cause: fmt.Errorf("%s carries a quarantine xattr", d.Identity.Path)}
	}`,
			new: `	if quarantined && d.Identity.Build != "1.4.4-R5419.1" {
		return FrozenCopy{}, &FreezeError{Stage: StageCopy, Code: CodeSourceQuarantined, Cause: fmt.Errorf("%s carries a quarantine xattr", d.Identity.Path)}
	}`,
		},
		{
			name:     "publish_binary_left_writable",
			testName: "TestSecondDistinctBuildPublishesAsOrdinaryUser",
			source:   production("copy.go"),
			function: "func publishStaging(",
			old:      `	if err := keyPin.chmodChild(binaryName, 0o555); err != nil {`,
			new:      `	if err := keyPin.chmodChild(binaryName, 0o755); err != nil {`,
		},
		{
			name:     "mtime_shifted_a_day",
			testName: "TestFreezePreservesModeAndMtime",
			source:   production("copy.go"),
			function: "func copySourceToStaging(",
			old:      `	if err := dir.chtimesChild(frozenBinaryName(d.Identity.Build), mtime); err != nil {`,
			new:      `	if err := dir.chtimesChild(frozenBinaryName(d.Identity.Build), mtime.AddDate(0, 0, 1)); err != nil {`,
		},
		{
			name:     "readback_skipped",
			testName: "TestFreezeReadBackRefusesCorruptedStaging",
			source:   production("copy.go"),
			function: "func copySourceToStaging(",
			old: `	readBack, err := hashFileDescriptor(ctx, StageCopy, dst)
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	if readBack != d.Identity.SHA256 {
		return nil, 0, &FreezeError{Stage: StageCopy, Code: CodeFreezeCopyFailed, Cause: fmt.Errorf("staged bytes changed after the copy stream")}
	}`,
			new: ``,
		},
		{
			name:     "xattr_list_emptied",
			testName: "TestFreezeCopiesXattrs",
			source:   production("xattr_unix.go"),
			function: "func captureXattrs(",
			old:      `		attrs[name] = value`,
			new:      `		if name != "user.musebuilds-freeze-test" { attrs[name] = value }`,
		},
		{
			name:     "reservation_truncates_existing",
			testName: "TestCreateReservationSecondCreateReadsWinner",
			source:   production("ledger.go"),
			function: "func createReservation(",
			old:      `	file, err := pin.openFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)`,
			new:      `	file, err := pin.openFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)`,
		},
		{
			name:     "defer_oldest_wins",
			testName: "TestDailyFreezeQuotaIsHostWide",
			source:   production("defer.go"),
			function: "func selectDeferralTarget(",
			old: `		if order > 0 {
			best = i
		}`,
			new: `		if order < 0 {
			best = i
		}`,
		},
		{
			name:     "defer_bad_admitted",
			testName: "TestDailyFreezeQuotaIsHostWide",
			source:   production("defer.go"),
			function: "func selectDeferralTarget(",
			old:      `	if state.State == StateBAD {`,
			new:      `	if state.State == StateBAD && state.Verdict == nil {`,
		},
		{
			name:     "staging_removed_on_failure",
			testName: "TestFreezeFailedCopyRetainsStaging",
			source:   production("copy.go"),
			function: "func failCopy(",
			old:      `	_ = recordReservationOutcome(cfg, reservation, outcomeFailed, detail)`,
			new: `	_ = recordReservationOutcome(cfg, reservation, outcomeFailed, detail)
	_ = os.RemoveAll(stagingDir(cfg.FrozenRoot))`,
		},
		{
			name:     "state_dir_sync_ignored",
			testName: "TestFreezeStateSyncFailureReportsUnknown",
			source:   production("destination.go"),
			function: "func atomicDocumentAt(",
			old: `	if err := dir.sync(); err != nil {
		return fmt.Errorf("%w: %v", errPublicationUnknown, err)
	}`,
			new: `	if err := dir.sync(); err != nil && prefix != ".state-" {
		return fmt.Errorf("%w: %v", errPublicationUnknown, err)
	}`,
		},
		{
			name:     "minilock_owner_changed",
			testName: "TestFreezeProductionLockWiring",
			source:   production("minilock/minilock.go"),
			function: "func (c Client) Acquire(",
			old:      `	argv := []string{"run", c.Owner, "--"}`,
			new:      `	argv := []string{"run", "wrong-owner", "--"}`,
		},
		{
			name:     "minilock_timeout_without_kill",
			testName: "TestMinilockWaitTimeout",
			pkg:      minilockPath,
			source:   production("minilock/minilock.go"),
			function: "func (c Client) Acquire(",
			old: `		case <-timeoutCh:
			cause := ErrWaitTimeout
			if !held.terminate() {
				cause = errors.Join(cause, ErrTeardownUnconfirmed)
			}`,
			new: `		case <-timeoutCh:
			cause := ErrWaitTimeout`,
		},
		{
			name:     "minilock_release_unsignalled",
			testName: "TestFreezeProductionLockWiring",
			source:   production("minilock/minilock.go"),
			function: "func (h *Held) terminate(",
			old: `	h.kill()
	if h.waitDone(releaseSigGrace) {`,
			new: `	if h.waitDone(releaseSigGrace) {`,
		},
		{
			name:     "minilock_early_exit_admitted",
			testName: "TestFreezeProductionLockWiring",
			source:   production("minilock/minilock.go"),
			function: "func (c Client) Acquire(",
			old: `		case <-held.done:
			held.killForced() // the exited parent can leave pipe-holding descendants
			_ = os.RemoveAll(sentinelDir)
			if err := ctx.Err(); err != nil {
				_, waitErr := held.latched()
				return nil, &Error{Op: "acquire", Exit: exitOf(waitErr), Cause: err}
			}
			_, waitErr := held.latched()
			return nil, c.earlyExit(waitErr, &stderr)`,
			new: `		case <-held.done:
			_ = os.RemoveAll(sentinelDir)
			return held, nil`,
		},
		{
			name:     "store_staging_mode_unchecked",
			testName: "TestFreezeRefusesWidenedStoreModes",
			source:   production("destination.go"),
			function: "func openFrozenStore(",
			old: `		if err == nil {
			err = requireDirMode(*child.dest, 0o700)
		}`,
			new: `		if err == nil && child.name != "staging" {
			err = requireDirMode(*child.dest, 0o700)
		}`,
		},
		{
			name:     "config_empty_ledger_admitted",
			testName: "TestFreezeInvalidConfigRefuses",
			source:   production("hostconfig.go"),
			function: "func validateHostConfig(",
			old: `	if c.LedgerDir == "" || !filepath.IsAbs(c.LedgerDir) {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidLedgerDir}
	}`,
			new: `	if c.LedgerDir != "" && !filepath.IsAbs(c.LedgerDir) {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidLedgerDir}
	}`,
		},
		{
			name:     "discovery_short_digest_admitted",
			testName: "TestFreezeInvalidConfigRefuses",
			source:   production("hostconfig.go"),
			function: "func validateDiscoveryInput(",
			old: `	if !isLowerHex64(d.Identity.SHA256) {
		return &FreezeError{Stage: StageDedupe, Code: CodeFreezeInvalid, Cause: errInvalidDiscoveryDigest}
	}`,
			new: `	if !isLowerHex64(d.Identity.SHA256) && len(d.Identity.SHA256) != 63 {
		return &FreezeError{Stage: StageDedupe, Code: CodeFreezeInvalid, Cause: errInvalidDiscoveryDigest}
	}`,
		},
		{
			name:     "keydir_single_foreign_admitted",
			testName: "TestFreezeNonemptyKeyDirRefuses",
			source:   production("copy.go"),
			function: "func ensureEmptyKeyDir(",
			old:      `	if len(entries) != 0 {`,
			new:      `	if len(entries) != 0 && len(entries) > 1 {`,
		},
		{
			name:     "precopy_hold_admitted",
			testName: "TestRampRecheckedUnderLock",
			source:   production("freeze.go"),
			function: "func admitAndCopyHeld(",
			old: `	if err := requireRampClear(ctx, cfg); err != nil {
		_ = recordReservationOutcome(cfg, reservation, outcomeFailed, "ramp refused before copy")
		return FrozenCopy{}, err
	}`,
			new: `	if err := requireRampClear(ctx, cfg); err != nil {
		if ferr, ok := AsFreezeError(err); !ok || ferr.Code != CodeRampHold {
			_ = recordReservationOutcome(cfg, reservation, outcomeFailed, "ramp refused before copy")
			return FrozenCopy{}, err
		}
	}`,
		},
		{
			name:     "minilock_escalation_dropped",
			testName: "TestMinilockTermResistantWaitCancelled",
			pkg:      minilockPath,
			source:   production("minilock/minilock.go"),
			function: "func (h *Held) terminate(",
			old: `func (h *Held) terminate() bool {
	h.kill()
	if h.waitDone(releaseSigGrace) {
		h.killForced() // a reaped parent may leave TERM-resistant descendants
		return true
	}
	h.killForced()
	return h.waitDone(releaseSigGrace)
}`,
			new: `func (h *Held) terminate() bool {
	h.kill()
	<-h.done
	return true
}`,
		},
		{
			name:     "minilock_pipedrain_unbounded",
			testName: "TestMinilockPipeHolderDoesNotHangWait",
			pkg:      minilockPath,
			source:   production("minilock/minilock.go"),
			function: "func (c Client) Acquire(",
			old:      `	cmd.WaitDelay = pipeDrainGrace`,
			new:      `	_ = pipeDrainGrace`,
		},
		{
			name:     "home_env_override_admitted",
			testName: "TestDefaultHostConfigIgnoresHomeOverride",
			source:   production("hostconfig.go"),
			function: "func DefaultHostConfig(",
			old:      `	home, err := accountHome()`,
			new:      `	home, err := os.UserHomeDir()`,
		},
		{
			name:     "quarantine_enoattr_dropped",
			testName: "TestFreezeQuarantineRealAttributes",
			platform: "darwin",
			source:   production("xattr_absent_darwin.go"),
			function: "func isXattrAbsent(",
			old:      `	return errors.Is(err, unix.ENODATA) || errors.Is(err, unix.ENOATTR)`,
			new:      `	return errors.Is(err, unix.ENODATA)`,
		},
		{
			name:     "quarantine_linux_namespace_absence_refused",
			testName: "TestFreezeQuarantineRealAttributes",
			platform: "linux",
			source:   production("quarantine_unix.go"),
			function: "func quarantineFDCheck(",
			old:      `	if runtime.GOOS != "darwin" && errors.Is(err, unix.EOPNOTSUPP) {`,
			new:      `	if runtime.GOOS != "darwin" && errors.Is(err, unix.EOPNOTSUPP) && false {`,
		},
		{
			name:     "quarantine_incopy_check_dropped",
			testName: "TestFreezeQuarantineAddedDuringAdmissionRefuses",
			platform: "darwin",
			source:   production("copy.go"),
			function: "func copySourceToStaging(",
			old: `	if _, quarantined := attrs[quarantineAttr]; quarantined {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceQuarantined, errQuarantinedCopy)
	}`,
			new: `	if value, quarantined := attrs[quarantineAttr]; quarantined && string(value) != "0081;late;late" {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceQuarantined, errQuarantinedCopy)
	}`,
		},
		{
			name:     "staged_xattr_verify_dropped",
			testName: "TestFreezeStagedXattrTamperingRefuses",
			source:   production("xattr_unix.go"),
			function: "func verifyStagedXattrs(",
			old: `	if len(got) != len(want) {
		return fmt.Errorf("staged attributes differ from the source snapshot")
	}`,
			new: `	if len(got) < len(want) {
		return fmt.Errorf("staged attributes differ from the source snapshot")
	}`,
		},
		{
			name:     "metadata_nofollow_dropped",
			testName: "TestFreezeStateUnsafeRefuses",
			source:   production("pindir_unix.go"),
			function: "func (d *pinnedDir) openFile(",
			old:      `	fd, err := unix.Openat(int(d.file.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, uint32(mode.Perm()))`,
			new: `	if name != "STATE.json" { flags |= unix.O_NOFOLLOW }
	fd, err := unix.Openat(int(d.file.Fd()), name, flags|unix.O_NONBLOCK|unix.O_CLOEXEC, uint32(mode.Perm()))`,
		},
		{
			name:     "ledger_host_check_narrowed",
			testName: "TestDayGuardReadFailureNeverResetsQuota",
			source:   production("ledger.go"),
			function: "func inventoryLedger(",
			old: `		if reservation.Host != host {
			return 0, nil, fmt.Errorf("ledger file %q names host %q, want %q", name, reservation.Host, host)
		}`,
			new: `		if reservation.Host != host && reservation.Host != "foreign-host" {
			return 0, nil, fmt.Errorf("ledger file %q names host %q, want %q", name, reservation.Host, host)
		}`,
		},
		{
			name:     "adoption_host_check_narrowed",
			testName: "TestCrashBetweenPublishAndProbeResumes",
			source:   production("ledger.go"),
			function: "func findAdoptionReservation(",
			old: `		if reservation.Host != host {
			return nil, fmt.Errorf("ledger file %q names host %q, want %q", name, reservation.Host, host)
		}`,
			new: `		if reservation.Host != host && reservation.Host != "foreign-host" {
			return nil, fmt.Errorf("ledger file %q names host %q, want %q", name, reservation.Host, host)
		}`,
		},
		{
			name:     "underlock_dedupe_resume_bypassed",
			testName: "TestFreezeSecondWaiterResumesUnderLock",
			source:   production("freeze.go"),
			function: "func admitAndCopyHeld(",
			old: `	if deduped, err := dedupeFrozen(ctx, cfg, d); err != nil {
		return FrozenCopy{}, err
	} else if deduped != nil {
		return *deduped, nil
	}`,
			new: `	if deduped, err := dedupeFrozen(ctx, cfg, d); err != nil {
		return FrozenCopy{}, err
	} else if deduped != nil && deduped.Outcome == OutcomeReused {
		return *deduped, nil
	}`,
		},
		{
			name:     "dedupe_present_lease_dropped",
			testName: "TestDedupeRequiresProbeOK",
			source:   production("freeze.go"),
			function: "func dedupeFrozen(",
			old: `		releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
		if err != nil {
			if ferr, ok := AsFreezeError(err); ok {
				return nil, ferr
			}
			return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeLeaseFailed, err)
		}
		defer func() { _ = releaseLease() }()
		frozen, err := verifyFrozenTuple(ctx, cfg.store, key, d.Identity)`,
			new: `		frozen, err := verifyFrozenTuple(ctx, cfg.store, key, d.Identity)`,
		},
		{
			name:     "dedupe_absent_lease_dropped",
			testName: "TestCrashBetweenPublishAndProbeResumes",
			source:   production("freeze.go"),
			function: "func dedupeFrozen(",
			old: `	releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok {
			return nil, ferr
		}
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeLeaseFailed, err)
	}
	defer func() { _ = releaseLease() }()
	bytesPresent, err := frozenBytesPresent(ctx, cfg.store, key, d.Identity)`,
			new: `	bytesPresent, err := frozenBytesPresent(ctx, cfg.store, key, d.Identity)`,
		},
		{
			name:     "destination_symlink_admitted",
			testName: "TestFreezeSymlinkDestinationRefuses",
			source:   production("pindir_unix.go"),
			function: "func (d *pinnedDir) openDirectory(",
			old:      `	fd, err := unix.Openat(int(d.file.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)`,
			new:      `	fd, err := unix.Openat(int(d.file.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)`,
		},
		{
			name:     "metadata_parent_symlink_admitted",
			testName: "TestFreezeMetadataParentSymlinkRefuses",
			source:   production("pindir_unix.go"),
			function: "func (d *pinnedDir) openDirectory(",
			old:      `	fd, err := unix.Openat(int(d.file.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)`,
			new:      `	fd, err := unix.Openat(int(d.file.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)`,
		},
		{
			name:     "copy_source_ctime_ignored",
			testName: "TestFreezeQuarantineAfterStreamingRefuses",
			platform: "darwin",
			source:   production("copy.go"),
			function: "func copySourceToStaging(",
			old:      `	if err != nil || after != srcID {`,
			new: `	after.ctimeS, after.ctimeN = srcID.ctimeS, srcID.ctimeN
	if err != nil || after != srcID {`,
		},
		{
			name:     "staged_quarantine_admitted",
			testName: "TestFreezeQuarantineAfterStreamingRefuses",
			platform: "darwin",
			source:   production("xattr_unix.go"),
			function: "func verifyStagedXattrs(",
			old: `	if quarantined {
		return errQuarantinedCopy
	}`,
			new: `	if quarantined { want[quarantineAttr], _ = readXattrFD(staged, quarantineAttr) }`,
		},
		{
			name:     "minilock_home_inherited",
			testName: "TestFreezeProductionLockIgnoresHomeOverride",
			source:   production("minilock/minilock.go"),
			function: "func (c Client) Acquire(",
			old:      `		cmd.Env = append(env, "HOME="+c.AccountHome)`,
			new:      `		cmd.Env = append(env, "HOME="+os.Getenv("HOME"))`,
		},
		{
			name:     "old_tuple_recopy_admitted",
			testName: "TestFreezeReservationSurvivesUTCRollover",
			source:   production("freeze.go"),
			function: "func reserveFreezeSlot(",
			old: `	if existing != nil {
		return *existing, false, existingReservationOutcome(*existing)
	}`,
			new: `	if existing != nil {
		if existing.Day < today { existing.Transaction += "-retry"; return *existing, false, nil }
		return *existing, false, existingReservationOutcome(*existing)
	}`,
		},
		{
			name:     "reservation_day_key_reintroduced",
			testName: "TestFreezeLifetimeReservationExclusiveCreate",
			source:   production("ledger.go"),
			function: "func reservationFileName(",
			old: `func reservationFileName(_ string, build, sha256 string) string {
	return build + "_" + sha256 + ".json"
}`,
			new: `func reservationFileName(day string, build, sha256 string) string {
	return day + "_" + build + "_" + sha256 + ".json"
}`,
		},
		{
			name:     "frozen_leaf_owner_admitted",
			testName: "TestFreezeFrozenOwnershipRefuses",
			source:   production("storeowner_unix.go"),
			function: "func checkStoreOwner(",
			old:      `	if uid != uint32(currentEUID()) {`,
			new:      `	if uid != uint32(currentEUID()) && !(info.IsDir() && info.Mode().Perm() == 0o555) {`,
		},
		{
			name:     "frozen_binary_owner_admitted",
			testName: "TestFreezeFrozenOwnershipRefuses",
			source:   production("storeowner_unix.go"),
			function: "func checkStoreOwner(",
			old:      `	if uid != uint32(currentEUID()) {`,
			new:      `	if uid != uint32(currentEUID()) && !(info.Mode().IsRegular() && info.Mode().Perm() == 0o555) {`,
		},
		{
			name:     "minilock_loss_consumed",
			testName: "TestMinilockExitedHoldReportsLoss",
			pkg:      minilockPath,
			source:   production("minilock/minilock.go"),
			function: "func (h *Held) Check(",
			old:      `	if exited, waitErr := h.latched(); exited {`,
			new: `	if exited, waitErr := h.latched(); exited {
		h.mu.Lock(); h.exited = false; h.mu.Unlock()`,
		},
		{
			name:     "frozen_leaf_mode_0755_admitted",
			testName: "TestFreezeReuseProtectionRefuses",
			source:   production("state.go"),
			function: "func openFrozenBytes(",
			old:      `if (protected && mode != 0o555) || (!protected && mode != 0o555 && mode != 0o700) {`,
			new:      `if (protected && mode != 0o555 && mode != 0o755) || (!protected && mode != 0o555 && mode != 0o700) {`,
		},
		{
			name:     "frozen_binary_mode_0755_admitted",
			testName: "TestFreezeReuseProtectionRefuses",
			source:   production("state.go"),
			function: "func openFrozenBytes(",
			old:      `if (protected && mode != 0o555) || (!protected && (mode&0o111 == 0 || mode&0o022 != 0)) {`,
			new:      `if (protected && mode != 0o555 && mode != 0o755) || (!protected && (mode&0o111 == 0 || mode&0o022 != 0)) {`,
		},
		{
			name:     "adoption_state_lock_reopens_root",
			testName: "TestFreezeAdoptionUsesHeldRoot",
			source:   production("copy.go"),
			function: "func adoptPublishedCopy(",
			old:      `	releaseState, err := acquireStateLockAt(ctx, cfg.store.root)`,
			new: `	releaseState, err := func() (func() error, error) {
		root, err := openDirPath(ctx, cfg.FrozenRoot, false)
		if err != nil { return nil, err }; defer root.close()
		return acquireStateLockAt(ctx, root)
	}()`,
		},
		{
			name:     "publication_read_handle_writable",
			testName: "TestFreezePublicationClosesWriter",
			source:   production("copy.go"),
			function: "func pinStagedReadHandle(",
			old:      `	reader, err := dir.openFile(name, os.O_RDONLY, 0)`,
			new:      `	reader, err := dir.openFile(name, os.O_RDWR, 0)`,
		},
		{
			name:     "publication_writer_close_narrowed",
			testName: "TestFreezePublicationClosesWriter",
			source:   production("copy.go"),
			function: "func pinStagedReadHandle(",
			old: `	if err := writer.Close(); err != nil {
		_ = reader.Close()
		return nil, err
	}`,
			new: `	if name != "muse-bin-1.4.4-R5419.1" {
		if err := writer.Close(); err != nil { _ = reader.Close(); return nil, err }
	}`,
		},
		{
			name:     "m22_timeout_receipt_dropped",
			testName: "TestLoadedHostTimeoutFallsBackWithoutBAD",
			source:   production("probe.go"),
			function: "func RecordProbeOutcome(",
			old: `	default:
		return commitTransientReceipt(ctx, cfg, key, id, obs, state, now)`,
			new: `	default:
		if obs.Outcome == ProbeTimeout {
			return ProbeRecord{State: state}, nil
		}
		return commitTransientReceipt(ctx, cfg, key, id, obs, state, now)`,
		},
		{
			name:     "m23_third_timeout_mints_bad",
			testName: "TestConcurrentTimeoutsDoNotMintBinaryVerdict",
			source:   production("probe.go"),
			function: "func commitTransientReceipt(",
			old:      `	return ProbeRecord{State: state, Receipt: &receipt}, nil`,
			new: `	if obs.Outcome == ProbeTimeout {
		timeouts := 0
		if dir, derr := cfg.store.meta.openChildDir(key); derr == nil {
			func() {
				defer dir.close()
				attempts, aerr := dir.openChildDir(attemptsDirName)
				if aerr != nil {
					return
				}
				defer attempts.close()
				entries, eerr := attempts.entries()
				if eerr != nil {
					return
				}
				for _, entry := range entries {
					name := entry.Name()
					if entry.IsDir() || len(name) < 6 || name[len(name)-5:] != ".json" {
						continue
					}
					r, present, rerr := readAttemptReceiptAt(ctx, attempts, name[:len(name)-5])
					if rerr == nil && present && r.Outcome == ProbeTimeout {
						timeouts++
					}
				}
			}()
		}
		if timeouts >= 3 {
			state.State = StateBAD
			state.Verdict = &BadVerdict{Code: "probe_timeout_strikes", Detail: obs.Detail, RecordedAt: now}
			if werr := writeBuildStateAt(ctx, cfg.store.meta, key, state); werr == nil {
				return ProbeRecord{State: state, Receipt: &receipt}, nil
			}
		}
	}
	return ProbeRecord{State: state, Receipt: &receipt}, nil`,
		},
		{
			name:     "m24a_malformed_treated_ok",
			testName: "TestProbeBinaryVerdictMarksBAD",
			source:   production("probe.go"),
			function: "func classifyProbeOutcome(",
			old: `	case ProbeExitOKUnparseable:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("unparseable row carries an observed build")
		}
		return verdictBadUnparseable, nil`,
			new: `	case ProbeExitOKUnparseable:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("unparseable row carries an observed build")
		}
		return verdictSuccess, nil`,
		},
		{
			name:     "m24b_mismatch_treated_ok",
			testName: "TestProbeBinaryVerdictMarksBAD",
			source:   production("probe.go"),
			function: "func classifyProbeOutcome(",
			old: `		if _, err := muse.ParseBuildID(obs.ObservedBuild); err != nil {
			return verdictTransient, fmt.Errorf("observed build does not parse: %w", err)
		}
		return verdictBadMismatch, nil`,
			new: `		if _, err := muse.ParseBuildID(obs.ObservedBuild); err != nil {
			return verdictTransient, fmt.Errorf("observed build does not parse: %w", err)
		}
		return verdictSuccess, nil`,
		},
		{
			name:     "m25_nonzero_mints_bad",
			testName: "TestTransientProbeFailuresNeverMintBAD",
			source:   production("probe.go"),
			function: "func classifyProbeOutcome(",
			old: `	case ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled, ProbeOutputLimit, ProbeDrainFailed:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("failed row carries an observed build")
		}
		return verdictTransient, nil`,
			new: `	case ProbeTimeout, ProbeExecFailed, ProbeSignaled, ProbeOutputLimit, ProbeDrainFailed:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("failed row carries an observed build")
		}
		return verdictTransient, nil
	case ProbeExitNonzero:
		return verdictBadMismatch, nil`,
		},
		{
			name:     "probe_observed_build_transient_admitted",
			testName: "TestProbeInvalidObservationsRefuse",
			source:   production("probe.go"),
			function: "func classifyProbeOutcome(",
			old: `	case ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled, ProbeOutputLimit, ProbeDrainFailed:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("failed row carries an observed build")
		}`,
			new: `	case ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled, ProbeOutputLimit, ProbeDrainFailed:
		if obs.ObservedBuild != "" && obs.ObservedBuild != id.Build {
			return verdictTransient, errors.New("failed row carries an observed build")
		}`,
		},
		{
			name:     "m26_postprobe_lock_retried",
			testName: "TestProbePublicationLockDeadline",
			source:   production("probe.go"),
			function: "func acquireProbeStateLock(",
			old: `	release, err := acquireStateLockAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok && ferr.Code == CodeFreezeCanceled {
			return nil, &ProbeError{Stage: StageProbe, Code: CodeProbeCanceled, Cause: err}
		}
		return nil, &ProbeError{Stage: StageProbe, Code: CodeProbeLockFailed, Cause: err}
	}
	return release, nil`,
			new: `	for {
		release, err := acquireStateLockAt(ctx, cfg.store.root)
		if err == nil {
			return release, nil
		}
		var ferr *FreezeError
		contended := errors.As(err, &ferr) && ferr.Code == CodeFreezeLeaseFailed
		if contended {
			continue
		}
		if ferr != nil && ferr.Code == CodeFreezeCanceled {
			return nil, &ProbeError{Stage: StageProbe, Code: CodeProbeCanceled, Cause: err}
		}
		return nil, &ProbeError{Stage: StageProbe, Code: CodeProbeLockFailed, Cause: err}
	}`,
		},
		{
			name:     "m27_lastgood_lock_retried",
			testName: "TestLastGoodWriteLockDeadline",
			source:   production("lastgood.go"),
			function: "func acquireLastGoodStateLock(",
			old: `	release, err := acquireStateLockAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok && ferr.Code == CodeFreezeCanceled {
			return nil, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodCanceled, Cause: err}
		}
		return nil, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodLockFailed, Cause: err}
	}
	return release, nil`,
			new: `	for {
		release, err := acquireStateLockAt(ctx, cfg.store.root)
		if err == nil {
			return release, nil
		}
		var ferr *FreezeError
		contended := errors.As(err, &ferr) && ferr.Code == CodeFreezeLeaseFailed
		if contended {
			continue
		}
		if ferr != nil && ferr.Code == CodeFreezeCanceled {
			return nil, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodCanceled, Cause: err}
		}
		return nil, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodLockFailed, Cause: err}
	}`,
		},
		{
			name:     "m29_probe_only_admitted",
			testName: "TestLastGoodWrittenOnlyOnSuccess",
			source:   production("lastgood.go"),
			function: "func validateStartSuccess(",
			old: `	if _, err := time.Parse(time.RFC3339, success.ReadyAt); err != nil {
		return errNotStartup
	}`,
			new: `	if _, err := time.Parse(time.RFC3339, success.ReadyAt); err != nil && success.ReadyAt != "" {
		return errNotStartup
	}`,
		},
		{
			name:     "m30_sequence_read_outside_lock",
			testName: "TestConcurrentLastGoodWritesSerialize",
			source:   production("lastgood.go"),
			function: "func WriteLastGood(",
			old: `	releaseState, err := acquireLastGoodStateLock(ctx, cfg)
	if err != nil {
		return LastGood{}, err
	}
	defer func() { _ = releaseState() }()
	current, present, err := readLastGoodDocument(ctx, cfg.store.root)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, err)
	}`,
			new: `	current, present, err := readLastGoodDocument(ctx, cfg.store.root)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, err)
	}
	releaseState, err := acquireLastGoodStateLock(ctx, cfg)
	if err != nil {
		return LastGood{}, err
	}
	defer func() { _ = releaseState() }()`,
		},
		{
			name:     "m31_digest_mismatch_absent",
			testName: "TestLastGoodReadClasses",
			source:   production("lastgood.go"),
			function: "func ReadLastGood(",
			old: `	if state.SHA256 != receipt.Frozen.SHA256 {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("last-good digest disagrees with probe state"))
	}`,
			new: `	if state.SHA256 != receipt.Frozen.SHA256 {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodAbsent, fmt.Errorf("last-good digest disagrees with probe state"))
	}`,
		},
		{
			name:     "m66_timeout_clears_last_good",
			testName: "TestTransientStartFailurePreservesLastGood",
			source:   production("probe.go"),
			function: "func commitTransientReceipt(",
			old:      `	return ProbeRecord{State: state, Receipt: &receipt}, nil`,
			new: `	if obs.Outcome == ProbeTimeout {
		_ = os.Remove(cfg.store.root.path + "/" + lastGoodFileName)
	}
	return ProbeRecord{State: state, Receipt: &receipt}, nil`,
		},
		{
			name:     "probe_duplicate_ignores_detail",
			testName: "TestProbeDuplicateAttemptRefuses",
			source:   production("probe.go"),
			function: "func (r AttemptReceipt) equivalent(",
			old: `		r.ObservedBuild == other.ObservedBuild &&
		r.Detail == other.Detail &&
		r.Cleanup == other.Cleanup &&`,
			new: `		r.ObservedBuild == other.ObservedBuild &&
		r.Cleanup == other.Cleanup &&`,
		},
		{
			name:     "probe_bad_skips_byte_verify",
			testName: "TestProbeVerdictVerifiesFrozenBytes",
			source:   production("probe.go"),
			function: "func RecordProbeOutcome(",
			old: `	if class == verdictSuccess || class == verdictBadUnparseable || class == verdictBadMismatch {
		if _, err := verifyFrozenTuple(ctx, cfg.store, key, id); err != nil {
			return ProbeRecord{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
		}
	}`,
			new: `	if class == verdictSuccess {
		if _, err := verifyFrozenTuple(ctx, cfg.store, key, id); err != nil {
			return ProbeRecord{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
		}
	}`,
		},
		{
			name:     "lastgood_oversize_checks_plain_length",
			testName: "TestLastGoodOversizedByRefuses",
			source:   production("lastgood.go"),
			function: "func WriteLastGood(",
			old:      `raw, err := lastGoodDocument.encode(receipt)`,
			new:      `codec := lastGoodDocument; codec.bound *= 2; raw, err := codec.encode(receipt)`,
		},
		{
			name:     "lastgood_inconsistent_sequence1_admitted",
			testName: "TestLastGoodInconsistentExistingRefusesAdmission",
			source:   production("lastgood.go"),
			function: "func WriteLastGood(",
			old: `		existingKey := freezeKey(current.Frozen.Build, current.Frozen.SHA256)
		if expected := frozenBinaryPath(cfg.FrozenRoot, existingKey, current.Frozen.Build); current.Frozen.Path != expected {
			return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("existing receipt path %q is not the %s copy", current.Frozen.Path, existingKey))
		}
		sequence = current.Sequence + 1`,
			new: `		existingKey := freezeKey(current.Frozen.Build, current.Frozen.SHA256)
		if current.Sequence != 1 {
			if expected := frozenBinaryPath(cfg.FrozenRoot, existingKey, current.Frozen.Build); current.Frozen.Path != expected {
				return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("existing receipt path %q is not the %s copy", current.Frozen.Path, existingKey))
			}
		}
		sequence = current.Sequence + 1`,
		},
		{
			name:     "lastgood_surface_cron_admitted",
			testName: "TestLastGoodReadClasses",
			source:   production("lastgood.go"),
			function: "func validateLastGoodDocument(",
			old:      `	if receipt.Surface != SurfaceMSP && receipt.Surface != SurfacePTY {`,
			new:      `	if receipt.Surface != SurfaceMSP && receipt.Surface != SurfacePTY && receipt.Surface != "cron" {`,
		},
		{
			name:     "m65_output_cap_parsed_for_identity",
			testName: "TestProbeOutputCapNeverParsedForIdentity",
			source:   production("probe.go"),
			function: "func classifyProbeOutcome(",
			old: `	case ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled, ProbeOutputLimit, ProbeDrainFailed:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("failed row carries an observed build")
		}
		return verdictTransient, nil`,
			new: `	case ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled, ProbeDrainFailed:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("failed row carries an observed build")
		}
		return verdictTransient, nil
	case ProbeOutputLimit:
		return verdictTransient, nil`,
		},
		{
			name:     "m67_cleanup_incomplete_recorded_confirmed",
			testName: "TestProbeCleanupIncompletePreserved",
			source:   production("probe.go"),
			function: "func writeAttemptReceiptAt(",
			old: `	receipt := AttemptReceipt{
		Schema: attemptReceiptSchema, AttemptID: obs.AttemptID, SelectionID: obs.SelectionID,
		Build: id.Build, SHA256: id.SHA256, Outcome: obs.Outcome, ObservedBuild: obs.ObservedBuild,
		Detail: obs.Detail, Cleanup: obs.Cleanup, OutputDigest: obs.OutputDigest,
		OutputBytes: obs.OutputBytes, RecordedAt: now,
	}`,
			new: `	cleanup := obs.Cleanup
	if obs.Outcome == ProbeTimeout && cleanup == CleanupIncomplete {
		cleanup = CleanupConfirmed
	}
	receipt := AttemptReceipt{
		Schema: attemptReceiptSchema, AttemptID: obs.AttemptID, SelectionID: obs.SelectionID,
		Build: id.Build, SHA256: id.SHA256, Outcome: obs.Outcome, ObservedBuild: obs.ObservedBuild,
		Detail: obs.Detail, Cleanup: cleanup, OutputDigest: obs.OutputDigest,
		OutputBytes: obs.OutputBytes, RecordedAt: now,
	}`,
		},
		{
			name:     "attempt_escaped_document_bound_widened",
			testName: "TestAttemptEscapingRefusesBeforePublication",
			source:   production("probe.go"), function: "func writeAttemptReceiptAt(",
			old: `raw, err := attemptDocument.encode(receipt)`,
			new: `codec := attemptDocument; codec.bound *= 4; raw, err := codec.encode(receipt)`,
		},
		{
			name:     "bad_escaped_document_bound_widened",
			testName: "TestBADEscapingRefusesBeforePublication",
			source:   production("state.go"), function: "func writeBuildStateAt(",
			old: `raw, err := buildStateDocument.encode(state)`,
			new: `codec := buildStateDocument; if state.State == StateBAD { codec.bound *= 2 }; raw, err := codec.encode(state)`,
		},
		{
			name:     "lastgood_sequence_zero_writer_admitted",
			testName: "TestLastGoodEscapingRoundTripAndOverflow",
			source:   production("lastgood.go"), function: "func WriteLastGood(",
			old: `raw, err := lastGoodDocument.encode(receipt)`,
			new: `codec := lastGoodDocument; codec.validate = func(r LastGood) error { if r.Sequence == 0 { r.Sequence = 1 }; return validateLastGoodDocument(r) }; raw, err := codec.encode(receipt)`,
		},
		{
			name:     "document_lossy_small_value_admitted",
			testName: "TestLastGoodEscapingRoundTripAndOverflow",
			source:   production("document.go"), function: "func (c documentCodec[T]) encode(",
			old: `if !reflect.DeepEqual(decoded, value) {`,
			new: `if !reflect.DeepEqual(decoded, value) && len(raw) > lastGoodMaxBytes {`,
		},
		{
			name:     "m22_first_timeout_recovery_refused",
			testName: "TestFirstProbeRecoveryReachesBothSurfaces",
			source:   production("firstprobe.go"), function: "func (r *ProbeRunner) probeFrozen(",
			old: `if (err == nil || publicationFailure) && result.Record.Receipt != nil {`,
			new: `if (err == nil || publicationFailure) && result.Record.Receipt != nil && obs.Outcome != ProbeTimeout {`,
		},
		{
			name:     "probe_failed_verdict_discards_independent_receipt",
			testName: "TestProbeFailureReceiptSurvivesVerdictPublicationFailure",
			source:   production("firstprobe.go"), function: "func (r *ProbeRunner) probeFrozen(",
			old: `if (err == nil || publicationFailure) && result.Record.Receipt != nil {`,
			new: `if (err == nil || (publicationFailure && obs.Outcome != ProbeExitOKUnparseable)) && result.Record.Receipt != nil {`,
		},
		{
			name:     "m65_reaped_child_skips_drain_deadline",
			testName: "TestProbeBoundsPipeDrain",
			source:   production("firstprobe.go"), function: "func (r *ProbeRunner) collectProbe(",
			old: `case <-cleanupDone:`,
			new: `case <-cleanupDone: if reaped { cleanupDone = nil; continue }`,
		},
		{
			name:     "m67_reaped_unknown_cleanup_allows_second_child",
			testName: "TestUnconfirmedTeardownNeverStartsFallback",
			source:   production("firstprobe.go"), function: "func (r *ProbeRunner) enter(",
			old: `if r.blocked {`,
			new: `if r.blocked && (r.failure == nil || !r.failure.result.Observation.Execution.Reaped) {`,
		},
		{
			name:     "m66_failed_fallback_clears_last_good",
			testName: "TestFailedFallbackPreservesLastGood",
			source:   production("recovery.go"), function: "func (r *ProbeRunner) Recover(",
			old: `event.State = "fallback_failed"`,
			new: `event.State = "fallback_failed"; if event.Mode == RecoveryUnattended { _ = os.Remove(f.cfg.FrozenRoot + "/" + lastGoodFileName) }`,
		},
		{
			name:     "firstprobe_one_byte_over_cap_admitted",
			testName: "TestFirstProbeVerifiedDescriptorAndClassification",
			source:   production("firstprobe.go"), function: "func readProbeStream(",
			old: `over := len(data) > probeStreamCap`,
			new: `over := len(data) > probeStreamCap+1`,
		},
		{
			name:     "firstprobe_unknown_group_with_drained_pipes_admitted",
			testName: "TestUnconfirmedTeardownNeverStartsFallback",
			source:   production("firstprobe.go"), function: "func (r *ProbeRunner) collectProbe(",
			old: `if !reaped || !obs.Execution.GroupGone {`,
			new: `if !reaped || (!obs.Execution.GroupGone && !obs.Execution.Drained) {`,
		},
		{
			name:     "recovery_newer_different_receipt_admitted",
			testName: "TestRecoveryRefusesDeclineDriftAndMissingEvidence",
			source:   production("recovery.go"), function: "func consumeRecovery(",
			old: `if current.Frozen != event.LastGood.Frozen || current.Sequence < event.LastGood.Sequence {`,
			new: `if (current.Frozen != event.LastGood.Frozen && current.Sequence <= event.LastGood.Sequence) || current.Sequence < event.LastGood.Sequence {`,
		},
		{
			name:     "freeze_probeok_double_probe",
			testName: "TestFreezeOrchestratesFirstProbeOnce",
			source:   production("firstprobe.go"), function: "func (r *ProbeRunner) probeFrozen(",
			old: `if state.State == StateProbeOK {`,
			new: `if state.State == StateProbeOK && req.AttemptID != "freeze-reuse" {`,
		},
		{
			name:     "firstprobe_single_env_reopens_binary_path",
			testName: "TestOSLauncherExecutesHeldInode", pkg: probeprocessPath,
			source: production("probeprocess/process_unix.go"), function: "func (OSLauncher) Start(",
			old: `if !actual.Mode().IsRegular() || !os.SameFile(wanted, actual) {`,
			new: `if !actual.Mode().IsRegular() || (!os.SameFile(wanted, actual) && len(env) != 1) {`,
		},
		{
			name:     "pre_attempt_deadline_mints_recovery",
			testName: "TestProbePreAttemptRefusalNeverOffersRecovery",
			source:   production("firstprobe.go"), function: "func (r *ProbeRunner) probeFrozen(",
			old: `if errors.Is(startErr, probeprocess.ErrNotAttempted) {`,
			new: `if errors.Is(startErr, probeprocess.ErrNotAttempted) && !errors.Is(startErr, context.DeadlineExceeded) {`,
		},
		{
			name:     "firstprobe_older_tuple_admitted_under_newer_selection",
			testName: "TestProbeFrozenRefusesOlderBuildUnderNewerSelection",
			source:   production("firstprobe.go"), function: "func (r *ProbeRunner) probeFrozen(",
			old: `if id.Build != d.Identity.Build || id.SHA256 != d.Identity.SHA256 {`,
			new: `if (id.Build != d.Identity.Build || id.SHA256 != d.Identity.SHA256) && id.Build != "1.4.3-R5000" {`,
		},
		{
			name:     "m03_ramp_hold_prepares_runnable_newest",
			testName: "TestFreezeUnavailableLaunchesNothing",
			source:   production("launch.go"), function: "func PrepareNewestLaunch(",
			old: `case CodeRecordedBad:`,
			new: `case CodeRampHold:
			prep.outcome = OutcomeRunNewest
			return prep, nil
		case CodeRecordedBad:`,
		},
		{
			name:     "m03_interactive_deferral_offered_without_failure",
			testName: "TestDeferredNewestCannotOfferRecovery",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) RequestPermit(",
			old: `if !failureEligible(p.failure.kind) {`,
			new: `if p.deferred != nil && p.req.Mode == RecoveryInteractive {
		p.mu.Unlock()
		good, err := ReadLastGood(ctx, p.cfg)
		if err != nil { return PermitGrant{}, err }
		_, _ = reporter.Offer(ctx, RecoveryEvent{
			Code: CodeFallbackWarning, State: "fallback_attempting",
			Newest: FrozenReceipt{Build: p.newest.Build, SHA256: p.newest.SHA256, Path: p.newest.Path},
			FailureAttempt: p.req.AttemptID, FailureStage: StageAdmit, FailureCode: CodeDeferralHostCap,
			LastGood: good, Mode: p.req.Mode,
		})
		return PermitGrant{}, launchRefusal(CodeLaunchRecoveryUnavailable, errNoFailureArmed)
	}
	if !failureEligible(p.failure.kind) {`,
		},
		{
			name:     "m03_unset_cap_defers_to_frozen",
			testName: "TestHostCapUnsetNeverDefers",
			source:   production("freeze.go"),
			function: "func reserveFreezeSlot(",
			old:      `	if cfg.MaxFreezesPerDay > 0 && dayCount >= cfg.MaxFreezesPerDay {`,
			new:      `	if dayCount >= cfg.MaxFreezesPerDay {`,
		},
		{
			name:     "m04_absent_receipt_admitted",
			testName: "TestFallbackRequiresActualLastGoodReceipt",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) RequestPermit(",
			old: `good, err := ReadLastGood(ctx, cfg)
	if err != nil {`,
			new: `good, err := ReadLastGood(ctx, cfg)
	if lastGoodErr, ok := AsLastGoodError(err); ok && lastGoodErr.Code == CodeLastGoodAbsent {
		err = nil
	}
	if err != nil {`,
		},
		{
			name:     "m05_explicit_foreign_path_exempted",
			testName: "TestExplicitLKGRequiresMatchingReceipt",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) RequestPermit(",
			old: `if req.ExplicitLastGood != "" && req.ExplicitLastGood != good.Frozen.Path {`,
			new: `if req.ExplicitLastGood != "" && req.ExplicitLastGood != good.Frozen.Path && req.ExplicitLastGood != "/var/empty/muse-lkg/muse-bin-1.4.2-R4000" {`,
		},
		{
			name:     "m06_restore_uses_snapshot_selection",
			testName: "TestRestoreRediscoversNewestBeforeOldPlan",
			source:   production("launch.go"), function: "func PrepareNewestLaunch(",
			old: `snapshotBuild := snapshotProvenanceBuild(req.SnapshotProvenance)
	selected := d`,
			new: `snapshotBuild := snapshotProvenanceBuild(req.SnapshotProvenance)
	selected := d
	if req.Route == RouteRestore && snapshotBuild != "" {
		selected.Identity.Build = snapshotBuild
	}`,
		},
		{
			name:     "m32_fallback_keeps_newest_digest",
			testName: "TestFallbackSwapsPathBuildAndDigestTogether",
			source:   production("launch.go"), function: "func consumePermitLeg(",
			old: `target := FrozenIdentity{Build: current.Frozen.Build, SHA256: current.Frozen.SHA256, Path: current.Frozen.Path}`,
			new: `target := FrozenIdentity{Build: current.Frozen.Build, SHA256: current.Frozen.SHA256, Path: current.Frozen.Path}
	target.SHA256 = event.Newest.SHA256`,
		},
		{
			name:     "m52_consumer_report_ineligible",
			testName: "TestMSPFallbackTriggerPerClass",
			source:   production("launch.go"), function: "func failureEligible(",
			old: `case failureLiveProbe, failureRecordedBAD, failureConsumerReport:`,
			new: `case failureLiveProbe, failureRecordedBAD:`,
		},
		{
			name:     "m52_second_leg_admitted",
			testName: "TestMSPStillOnceNeverLoop",
			source:   production("launch.go"), function: "func (p *FallbackPermit) StartFallback(",
			old: `if p.state.attempts > 0 {`,
			new: `if p.state.attempts > 1 {`,
		},
		{
			name:     "m61_freeze_failure_bare_launches",
			testName: "TestCompositionFailureRefuses",
			source:   production("launch.go"), function: "func PrepareNewestLaunch(",
			old: `prep.deferred = &target
			return prep, nil
		default:
			return nil, freezeErr`,
			new: `prep.deferred = &target
			return prep, nil
		default:
			prep.outcome = OutcomeRunNewest
			prep.newest = FrozenIdentity{Build: selected.Identity.Build, SHA256: selected.Identity.SHA256, Path: "muse"}
			return prep, nil`,
		},
		{
			name:     "m69_resume_consent_assumed_before_probes",
			testName: "TestLaunchTraceIncludesFailedProbeAndInteractiveConsent",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) RequestPermit(",
			old: `consent, offerErr := reporter.Offer(offerCtx, event)`,
			new: `var consent bool
		var offerErr error
		if req.Route == RouteResume {
			consent = true
		} else {
			consent, offerErr = reporter.Offer(offerCtx, event)
		}`,
		},
		{
			name:     "m69_consumed_permit_probes_admitted",
			testName: "TestEveryMuseExecHasNewestOrLastGoodPermit",
			source:   production("launch.go"), function: "func (p *FallbackPermit) AuthorizeProbe(",
			old: `if p.state.attempts > 0 {`,
			new: `if p.state.attempts > 0 && tuple.Build != "1.4.3-R5000" {`,
		},
		{
			name:     "m69_foreign_tuple_probe_admitted",
			testName: "TestEveryMuseExecHasNewestOrLastGoodPermit",
			source:   production("launch.go"), function: "func (p *FallbackPermit) AuthorizeProbe(",
			old: `if tuple.Build != p.lastGood.Frozen.Build || tuple.SHA256 != p.lastGood.Frozen.SHA256 || tuple.Path != p.lastGood.Frozen.Path {`,
			new: `if (tuple.Build != p.lastGood.Frozen.Build || tuple.SHA256 != p.lastGood.Frozen.SHA256 || tuple.Path != p.lastGood.Frozen.Path) && tuple.Build != "1.4.2-R4000" {`,
		},
		{
			name:     "m70_interactive_superseded_consent_reused",
			testName: "TestChangedLastGoodReceiptCannotReuseConsent",
			source:   production("launch.go"), function: "func consumePermitLeg(",
			old: `if current.Frozen != event.LastGood.Frozen || current.Sequence < event.LastGood.Sequence {`,
			new: `if (current.Frozen != event.LastGood.Frozen || current.Sequence < event.LastGood.Sequence) && event.Mode != RecoveryInteractive {`,
		},
		{
			name:     "m_route_unknown_admitted_as_launch",
			testName: "TestLaunchRouteTableCoversSection5",
			source:   production("launch.go"), function: "func validateLaunchRequest(",
			old: `default:
		return launchRefusal(CodeLaunchInvalid, errUnknownRoute)`,
			new: `default:
		if req.Route != "bogus-route" {
			return launchRefusal(CodeLaunchInvalid, errUnknownRoute)
		}`,
		},
		{
			name:     "m_primary_stale_tuple_admitted",
			testName: "TestLaunchRouteTableCoversSection5",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) ReportNewestStartSuccess(",
			old: `if success.Frozen != newest {`,
			new: `if success.Frozen != newest && success.Frozen.Build != "1.4.3-R5000" {`,
		},
		{
			name:     "m_launch_canceled_enters_discovery",
			testName: "TestLaunchRouteTableCoversSection5",
			source:   production("launch.go"), function: "func PrepareNewestLaunch(",
			old: `if err := launchRefuseIfCanceled(ctx); err != nil {
		return nil, err
	}
	if runner == nil {`,
			new: `if err := launchRefuseIfCanceled(ctx); err != nil && req.Route != RouteLaunch {
		return nil, err
	}
	if runner == nil {`,
		},
		{
			name:     "m_decline_respends_allowance",
			testName: "TestLaunchDeclinedOfferSpendsAllowance",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) RequestPermit(",
			old: `if offerErr != nil || expired != nil || !consent {
			p.spendAllowance()`,
			new: `if offerErr != nil || expired != nil || !consent {
			if offerErr != nil || expired != nil {
				p.spendAllowance()
			}`,
		},
		{
			name:     "m_permit_outstanding_probe_admitted",
			testName: "TestPermitProbeAndLegCoordinateOnSharedState",
			source:   production("launch.go"), function: "func (p *FallbackPermit) StartFallback(",
			old: `if p.state.active > 0 {`,
			new: `if p.state.active > 1 {`,
		},
		{
			name:     "m_evidenceless_permit_issued",
			testName: "TestConsumerFailureRequiresDurableEvidence",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) RequestPermit(",
			old: `if err := p.verifyFailureEvidence(ctx, failure, cfg); err != nil {`,
			new: `if err := p.verifyFailureEvidence(ctx, failure, cfg); err != nil && failure.attemptID != "l2c-evidence-missing" {`,
		},
		{
			name:     "m_consumer_publication_failure_armed",
			testName: "TestConsumerFailureRequiresDurableEvidence",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) ReportNewestStartFailure(",
			old: `record, err := RecordProbeOutcome(ctx, cfg, newest, obs)
	if err != nil {`,
			new: `record, err := RecordProbeOutcome(ctx, cfg, newest, obs)
	if err != nil && obs.Outcome == ProbeExitNonzero {
		record.Receipt = &AttemptReceipt{
			AttemptID: obs.AttemptID, SelectionID: obs.SelectionID,
			Build: newest.Build, SHA256: newest.SHA256,
			Outcome: obs.Outcome, Execution: obs.Execution, Cleanup: obs.Cleanup,
		}
		err = nil
	}
	if err != nil {`,
		},
		{
			name:     "m05_explicit_absent_returns_absent",
			testName: "TestExplicitLKGRequiresMatchingReceipt",
			source:   production("launch.go"), function: "func (p *PreparedLaunch) RequestPermit(",
			old: `if req.ExplicitLastGood != "" {`,
			new: `if req.ExplicitLastGood != "" && req.ExplicitLastGood != "/var/empty/muse-lkg/muse-bin-1.4.2-R4000" {`,
		},
		{
			name:     "m33_exec_verify_digest_skips_142",
			testName: "TestExecVerifyRefusesCorruptTuples",
			source:   production("execverify.go"), function: "func VerifyFrozenForExec(",
			old: `if digest != id.SHA256 {`,
			new: `if digest != id.SHA256 && id.Build != "1.4.2-R4684.1" {`,
		},
	}

	// Bind all kills to a green unmutated behavioral control in this driver.
	baselineCtx, cancelBaseline := context.WithTimeout(context.Background(), museBuildsMutantSubprocessTimeout)
	defer cancelBaseline()
	baseline := exec.CommandContext(baselineCtx, "go", "test", "-p=1", "-timeout=90s", packagePath+"/...", "-count=1")
	baseline.Dir = moduleRoot
	baseline.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	baseline.Cancel = func() error { return syscall.Kill(-baseline.Process.Pid, syscall.SIGKILL) }
	baseline.WaitDelay = 10 * time.Second
	control, controlErr := baseline.CombinedOutput()
	controlExit := 0
	if controlErr != nil {
		controlExit = -1
	}
	var controlExitErr *exec.ExitError
	if errors.As(controlErr, &controlExitErr) {
		controlExit = controlExitErr.ExitCode()
	}
	if controlErr != nil {
		t.Fatalf("unmutated control exit=%d: %v\n%s", controlExit, controlErr, control)
	}
	t.Logf("unmutated behavioral control exit=%d\n%s", controlExit, control)

	sem := make(chan struct{}, museBuildsMutantParallelism)
	for _, mutant := range mutants {
		t.Run(mutant.name, func(t *testing.T) {
			if mutant.platform != "" && mutant.platform != runtime.GOOS {
				t.Skipf("BOUND %s: requires %s; no kill claimed on %s", mutant.name, mutant.platform, runtime.GOOS)
			}
			t.Parallel()
			sem <- struct{}{}
			defer func() { <-sem }()
			source, err := os.ReadFile(mutant.source)
			if err != nil {
				t.Fatal(err)
			}
			functionStart := strings.Index(string(source), mutant.function)
			if functionStart < 0 {
				t.Fatalf("production function %q not found", mutant.function)
			}
			functionEnd := strings.Index(string(source)[functionStart:], "\n}")
			if functionEnd < 0 {
				t.Fatalf("production function %q boundary not found", mutant.function)
			}
			prefix := string(source)[:functionStart]
			function := string(source)[functionStart : functionStart+functionEnd+2]
			suffix := string(source)[functionStart+functionEnd+2:]
			if strings.Count(function, mutant.old) != 1 {
				t.Fatalf("%s anchor matches %d sites, want exactly one", mutant.name, strings.Count(function, mutant.old))
			}
			mutatedSource := prefix + strings.Replace(function, mutant.old, mutant.new, 1) + suffix

			mutantDir := t.TempDir()
			mutantPath := filepath.Join(mutantDir, filepath.Base(mutant.source))
			if err := os.WriteFile(mutantPath, []byte(mutatedSource), 0o600); err != nil {
				t.Fatal(err)
			}
			overlayPath := filepath.Join(mutantDir, "overlay.json")
			overlay, err := json.Marshal(struct {
				Replace map[string]string `json:"Replace"`
			}{Replace: map[string]string{mutant.source: mutantPath}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(overlayPath, overlay, 0o600); err != nil {
				t.Fatal(err)
			}
			testPkg := mutant.pkg
			if testPkg == "" {
				testPkg = packagePath
			}
			commandText := fmt.Sprintf("go test -p=1 -timeout=5m -failfast -overlay=%s %s -run '^%s$' -count=1", overlayPath, testPkg, mutant.testName)
			commandCtx, cancelCommand := context.WithTimeout(context.Background(), museBuildsMutantSubprocessTimeout)
			defer cancelCommand()
			command := exec.CommandContext(commandCtx, "go", "test", "-p=1", "-timeout=5m", "-failfast", "-overlay="+overlayPath, testPkg, "-run", "^"+mutant.testName+"$", "-count=1")
			command.Dir = moduleRoot
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			command.Cancel = func() error {
				return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			}
			command.WaitDelay = 10 * time.Second
			output, runErr := command.CombinedOutput()
			logPath := filepath.Join(mutantDir, "run.log")
			if writeErr := os.WriteFile(logPath, output, 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
			if commandCtx.Err() != nil {
				t.Fatalf("row %q exceeded the subprocess deadline; test=%s command=%q output=%s", mutant.name, mutant.testName, commandText, output)
			}
			exitCode := 0
			if runErr != nil {
				exitCode = -1
			}
			var exitErr *exec.ExitError
			if errors.As(runErr, &exitErr) {
				exitCode = exitErr.ExitCode()
			}
			if runErr == nil || strings.Contains(string(output), "panic:") || !strings.Contains(string(output), "--- FAIL: "+mutant.testName) {
				t.Fatalf("SURVIVOR %s: named FAIL absent; test=%s exit=%d command=%q log=%s output=%s", mutant.name, mutant.testName, exitCode, commandText, logPath, output)
			}
			t.Logf("narrowing row killed by %s; child exit=%d; command=%q; log=%s", mutant.testName, exitCode, commandText, logPath)
		})
	}
}
