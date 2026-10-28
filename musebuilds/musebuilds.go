package musebuilds

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
)

// Discovery stages. Every refusal names its stage so a caller can tell where
// a typed reason came from without matching prose.
const (
	StageStart    = "start"
	StagePath     = "path"
	StageLauncher = "launcher"
	StageSidecar  = "sidecar"
	StageScan     = "scan"
	StageSelect   = "select"
	StageSource   = "source"
	StageRecheck  = "recheck"
)

// Discovery refusal codes. A failed, partial or malformed read is never a
// legitimate absence: each code below refuses a distinct fact.
const (
	// CodeCanceled refuses discovery when its context is canceled.
	CodeCanceled = "muse_discovery_canceled"
	// CodeNoPath refuses a launch environment that carries no PATH at all.
	// It is distinct from "muse is not installed".
	CodeNoPath = "muse_discovery_no_path"
	// CodeLauncherNotFound refuses a PATH that contains no usable `muse`
	// launcher and no unusable one either.
	CodeLauncherNotFound = "muse_launcher_not_found"
	// CodeLauncherUnusable refuses a PATH whose only `muse` candidates
	// exist but cannot be used: a directory, a non-executable file, or a
	// candidate whose read failed for another reason than absence.
	CodeLauncherUnusable = "muse_launcher_unusable"
	// CodeLauncherResolution refuses a launcher whose symlink chain or
	// physical parent directory cannot be resolved.
	CodeLauncherResolution = "muse_launcher_resolution_failed"
	// CodeSidecarUnreadable refuses a sidecar that cannot be read within
	// its bound: an I/O error, a symlink that is never followed, a
	// non-regular object, or a file past the size cap. It is not
	// absence and it is not permission to scan instead.
	CodeSidecarUnreadable = "muse_sidecar_unreadable"
	// CodeSidecarMalformed refuses a sidecar whose bytes do not name one
	// build identity: unparseable text, an empty name, or a name with
	// whitespace the shell substitution would never produce.
	CodeSidecarMalformed = "muse_sidecar_malformed"
	// CodeSidecarTargetRejected refuses a sidecar whose named install
	// target is missing or fails the static file gates. A rejected target
	// refuses; it is never a reason to look for another build.
	CodeSidecarTargetRejected = "muse_sidecar_target_rejected"
	// CodeInstallScanFailed refuses an install directory whose listing
	// failed or came back partial.
	CodeInstallScanFailed = "muse_install_scan_failed"
	// CodeInstallNameUnrecognized refuses an install directory holding a
	// name in the versioned `muse-bin-` namespace that the shared grammar
	// does not parse. The installed set is unknown, not empty.
	CodeInstallNameUnrecognized = "muse_install_name_unrecognized"
	// CodeNoInstalledBuild refuses an install directory with no versioned
	// candidates at all.
	CodeNoInstalledBuild = "muse_no_installed_build"
	// CodeSelectionAmbiguous refuses a candidate set holding two raw build
	// identities that compare numerically equal but differ as text, such
	// as leading-zero aliases. Comparison chooses nothing arbitrarily.
	CodeSelectionAmbiguous = "muse_selection_ambiguous"
	// CodeSelectionUnusable refuses a selected newest whose file fails the
	// static source gates. The selection stands refused; discovery never
	// shrinks the set until an older usable file becomes maximum.
	CodeSelectionUnusable = "muse_selection_unusable"
	// CodeDiscoveryChanged refuses after one consistency restart still
	// observes the install changing under discovery.
	CodeDiscoveryChanged = "muse_discovery_changed"
	// CodeUnsupportedPlatform refuses source-identity enforcement where
	// the host cannot report hard-link or owner identity.
	CodeUnsupportedPlatform = "muse_discovery_unsupported_platform"
)

// Sidecar provenance records how the `.muse-version` sidecar related to the
// selected newest. An empty provenance means the sidecar named the selected
// build itself: there was no anomaly to record.
const (
	// ProvenanceSidecarMissing records a selection made by scanning the
	// install directory because no sidecar existed.
	ProvenanceSidecarMissing = "muse_sidecar_missing"
	// ProvenanceSidecarBehindInstalled records a sidecar naming a build
	// older than the installed maximum. The maximum wins; the sidecar is
	// provenance, never selection authority.
	ProvenanceSidecarBehindInstalled = "muse_sidecar_behind_installed"
)

// FrozenIdentity is one content-addressed Muse build: the exact full build
// text, the lowercase hex SHA-256 of its bytes, and the canonical absolute
// path those bytes were read from. In a Discovery the path still denotes
// the installed source; pass 2 publishes the frozen copy elsewhere and
// re-verifies this same tuple before every exec.
type FrozenIdentity struct {
	Build  string
	SHA256 string
	Path   string
}

// Discovery is the accepted newest selection: the winning identity plus the
// install roots it was selected from and the snapshot that linearizes the
// selection. SidecarTarget is the exact sidecar build text, or empty when no
// sidecar existed.
type Discovery struct {
	Identity                                                        FrozenIdentity
	InstallDir, LauncherPath, SidecarTarget, Provenance, SnapshotID string
}

// DiscoveryError is a typed discovery refusal: the stage that refused and
// the machine-readable reason, with the underlying cause chained.
type DiscoveryError struct {
	Stage string
	Code  string
	Cause error
}

// Error renders the refusal with its stage and code.
func (e *DiscoveryError) Error() string {
	if e == nil {
		return "muse discovery error"
	}
	if e.Cause != nil {
		return "muse discovery " + e.Stage + " " + e.Code + ": " + e.Cause.Error()
	}
	return "muse discovery " + e.Stage + " " + e.Code
}

// Unwrap returns the underlying cause.
func (e *DiscoveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// AsDiscoveryError matches a DiscoveryError in err's chain for code and
// stage assertions.
func AsDiscoveryError(err error) (*DiscoveryError, bool) {
	var refusal *DiscoveryError
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return nil, false
}

// discoveryAttempts is the initial attempt plus one consistency restart.
const discoveryAttempts = 2

type interposePoint int

const (
	interposeAttemptEntry interposePoint = iota
	interposeAfterPath
	interposeAfterValidate
	interposeBeforeRecheck
	interposeInitialRead
	interposeFinalRead
	interposeSidecarRead
	interposeInventoryRead
	interposeAccepted
)

// Hooks are synchronous, nil in production, and reset by test cleanup.
// They coordinate real filesystem changes, never substitute discovery results.
var interposeForTest func(interposePoint)

// Observation-only seam for descriptor lifetime assertions at the public entry.
var snapshotObservedForTest func(*discoverySnapshot)

func interpose(point interposePoint) {
	if interposeForTest != nil {
		interposeForTest(point)
	}
}

func refuseIfCanceled(ctx context.Context, stage string) error {
	if err := ctx.Err(); err != nil {
		return &DiscoveryError{Stage: stage, Code: CodeCanceled, Cause: err}
	}
	return nil
}

func refusal(ctx context.Context, stage, code string, cause error) error {
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return err
	}
	if existing, ok := AsDiscoveryError(cause); ok && (existing.Code == CodeCanceled || existing.Code == CodeDiscoveryChanged || existing.Code == CodeUnsupportedPlatform) {
		return cause
	}
	return &DiscoveryError{Stage: stage, Code: code, Cause: cause}
}

func changed(cause error) error {
	return &DiscoveryError{Stage: StageSelect, Code: CodeDiscoveryChanged, Cause: cause}
}

// ResolveNewest selects the numerical maximum before any BAD/policy lookup,
// executing nothing. A call captures the cwd once for relative PATH entries.
// Every attempt owns its entire descriptor graph until acceptance/refusal.
// One consistency disagreement restarts under the ORIGINAL context.
func ResolveNewest(ctx context.Context, env []string) (Discovery, error) {
	var cwd string
	var last error
	for attempt := 0; attempt < discoveryAttempts; attempt++ {
		d, err := resolveAttempt(ctx, env, &cwd)
		if err == nil {
			return d, nil
		}
		if e, ok := AsDiscoveryError(err); !ok || e.Code != CodeDiscoveryChanged {
			return Discovery{}, err
		}
		last = err
	}
	return Discovery{}, last
}

func resolveAttempt(ctx context.Context, env []string, cwd *string) (result Discovery, resultErr error) {
	// This is the single attempt-entry gate (including retries).
	if err := refuseIfCanceled(ctx, StageStart); err != nil {
		return Discovery{}, err
	}
	interpose(interposeAttemptEntry)
	if err := refuseIfCanceled(ctx, StagePath); err != nil {
		return Discovery{}, err
	}
	if !supportedPlatform {
		return Discovery{}, &DiscoveryError{Stage: StageStart, Code: CodeUnsupportedPlatform}
	}
	if *cwd == "" {
		base, err := os.Getwd()
		if err != nil {
			return Discovery{}, refusal(ctx, StageLauncher, CodeLauncherResolution, err)
		}
		*cwd = base
	}
	s := &discoverySnapshot{dirs: make(map[string]*fileLease)}
	defer func() {
		closeErr := s.close()
		if err := refuseIfCanceled(ctx, StageSelect); err != nil && resultErr == nil {
			result, resultErr = Discovery{}, err
		} else if closeErr != nil && resultErr == nil {
			result, resultErr = Discovery{}, refusal(ctx, StageSelect, CodeDiscoveryChanged, closeErr)
		}
	}()
	launcherPath, err := resolveLauncherOnPath(ctx, s, env, *cwd)
	if err != nil {
		return Discovery{}, err
	}
	interpose(interposeAfterPath)
	launcher, installDir, err := resolveInstallDir(ctx, s, launcherPath)
	if err != nil {
		return Discovery{}, err
	}
	dir := s.dirs[installDir]
	sidecarBuild, sidecarPresent, err := readSidecarTarget(ctx, s, dir)
	if err != nil {
		return Discovery{}, err
	}
	if err := validateSidecarTarget(ctx, s, dir, sidecarBuild, sidecarPresent); err != nil {
		return Discovery{}, err
	}
	candidates, err := collectCandidates(ctx, s, dir)
	if err != nil {
		return Discovery{}, err
	}
	if len(candidates) == 0 {
		return Discovery{}, &DiscoveryError{Stage: StageScan, Code: CodeNoInstalledBuild, Cause: errNoCandidates}
	}
	best, ordered, err := selectNewest(ctx, candidates)
	if err != nil {
		return Discovery{}, err
	}
	pinned, err := openValidateAndHashSelectedSource(ctx, s, dir, best.path, best.id)
	if err != nil {
		return Discovery{}, err
	}
	if snapshotObservedForTest != nil {
		snapshotObservedForTest(s)
	}
	interpose(interposeBeforeRecheck)
	if err := recheckSelection(ctx, s, pinned); err != nil {
		return Discovery{}, err
	}
	identity := FrozenIdentity{Build: best.id.Build, SHA256: pinned.digest, Path: best.path}
	snapshotID, err := deriveSnapshotID(ctx, installDir, sidecarBuild, sidecarPresent, ordered, pinned.digest)
	if err != nil {
		return Discovery{}, err
	}
	result = Discovery{
		Identity: identity, InstallDir: installDir, LauncherPath: launcher,
		SidecarTarget: sidecarBuild,
		Provenance:    sidecarProvenance(sidecarPresent, sidecarBuild, best.id.Build),
		SnapshotID:    snapshotID,
	}
	interpose(interposeAccepted)
	if err := refuseIfCanceled(ctx, StageSelect); err != nil {
		return Discovery{}, err
	}
	return result, nil
}

// All re-observation is through held descriptors, including name bindings via
// held parent FDs. No pathname is reopened here. The bookending identity sweeps
// catch changes during the inventory/selector/source reads themselves.
func recheckSelection(ctx context.Context, s *discoverySnapshot, pinned *pinnedSource) error {
	if err := refuseIfCanceled(ctx, StageSelect); err != nil {
		return err
	}
	if err := s.verify(ctx); err != nil {
		return err
	}
	names, err := readInventory(ctx, s.inventoryFile, StageRecheck)
	if err != nil {
		return refusal(ctx, StageRecheck, CodeDiscoveryChanged, err)
	}
	if !slices.Equal(names, s.inventory) {
		return changed(errors.New("directory inventory changed"))
	}
	if s.sidecar != nil {
		raw, err := readLease(ctx, s.sidecar, StageRecheck, interposeSidecarRead, sidecarMaxBytes)
		if err != nil {
			return refusal(ctx, StageRecheck, CodeDiscoveryChanged, err)
		}
		if !bytes.Equal(raw, s.sidecarBytes) {
			return changed(errors.New("selector bytes changed"))
		}
	}
	if err := recheckSource(ctx, pinned); err != nil {
		return err
	}
	return s.verify(ctx)
}

func sidecarProvenance(present bool, sidecarBuild, selectedBuild string) string {
	switch {
	case !present:
		return ProvenanceSidecarMissing
	case sidecarBuild != selectedBuild:
		return ProvenanceSidecarBehindInstalled
	default:
		return ""
	}
}
