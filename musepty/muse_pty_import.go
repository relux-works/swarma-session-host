package musepty

import (
	"fmt"
	"path/filepath"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
	musebuilds "github.com/relux-works/swarma-session-host/musebuilds"
)

// This file is the L3P verified-import constructor (13opwj M1,
// newest sealed PTY plan): it accepts ONLY a verified imported
// module seal for system muse, LaunchModeInteractive, with no
// LaunchRequest recomposition on the host and refusal of unsealed
// input, and binds the seal to the caller's current selection.

// BuildMusePTYLaunchPlanFromImport builds a resume plan from a
// module-produced seal plus the exact final process the seal binds.
// The import is authenticated: NewImportedProcess refuses unsealed
// guards (Muse declares a sealer), wrong-system seals, malformed
// processes and tampered payloads, and binds the exact binary, argv,
// environment, cwd, home and stdin the later pre-exec verification
// re-checks. Non-interactive processes refuse: the PTY host runs
// interactive only.
//
// Expected is the caller's current selection. The sealed binary must
// name its path and its filename must parse to its build through the
// module grammar; anything else is stale and refuses for OUTER
// recomposition, never retargeted in place. The host reads only the
// envelope's structural fields here — never seal selectors — and
// leaves release and help agreement to the module verifier at
// pre-exec time. Keys travel to the import for keyed guards.
func BuildMusePTYLaunchPlanFromImport(seal agentic.Seal, process agentic.Plan, expected musebuilds.FrozenIdentity, keys ...agentic.SealCommitmentKey) (*MusePTYLaunchPlan, error) {
	if expected.Build == "" || expected.SHA256 == "" || expected.Path == "" {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeInvalidConfig, Cause: fmt.Errorf("sealed import requires the current selection to bind against")}
	}
	imported, err := agentic.NewImportedProcess(muse.New(), seal, process, keys...)
	if err != nil {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeImportRefused, Cause: fmt.Errorf("importing the sealed Muse plan: %w", err)}
	}
	bound := imported.Process()
	if bound.Mode != agentic.LaunchModeInteractive {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeModeRefused, Cause: fmt.Errorf("sealed Muse plan mode %d is not interactive", int(bound.Mode))}
	}
	trusted := false
	for _, arg := range bound.Argv {
		if arg == "--trust-workspace" {
			trusted = true
		}
	}
	if !trusted {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeImportRefused, Cause: fmt.Errorf("Muse PTY launch requires workspace trust in the imported plan")}
	}
	if bound.Binary != expected.Path {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeSealedStale, Cause: fmt.Errorf("sealed binary %q is not the selected %q: recompose for the current selection", bound.Binary, expected.Path)}
	}
	identity, err := muse.ParsePinnedBinaryName(filepath.Base(bound.Binary))
	if err != nil {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeImportRefused, Cause: err}
	}
	if identity.Build != expected.Build {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeSealedStale, Cause: fmt.Errorf("sealed filename build %q is not the selected %q: recompose for the current selection", identity.Build, expected.Build)}
	}
	return &MusePTYLaunchPlan{plan: bound, frozenTuple: expected, frozenSet: true, imported: &imported}, nil
}
