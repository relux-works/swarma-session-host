package musepty

import (
	"errors"
	"fmt"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	musebuilds "github.com/relux-works/swarma-session-host/musebuilds"
)

// This file is the L3P frozen vocabulary for the PTY host: the exec
// legs the pre-exec gate names, the typed refusal, and the
// request-side tuple recording. The gate itself lives in
// muse_pty_host.go beside the legacy probe anchor it branches around.

// musePTYExecLeg names one PTY exec entry for the pre-exec gate. The
// leg travels in refusals; both legs verify the same way.
type musePTYExecLeg string

const (
	musePTYLegLaunch musePTYExecLeg = "launch"
	musePTYLegResume musePTYExecLeg = "resume"
)

// Pre-exec refusal codes. Every code refuses a distinct fact; a
// failed, partial or malformed read is never a legitimate absence.
const (
	// musePTYCodeFrozenVerifyFailed refuses a plan whose binary,
	// bytes or name disagree with its frozen tuple.
	musePTYCodeFrozenVerifyFailed = "frozen_verify_failed"
	// musePTYCodeSealVerifyFailed refuses a module seal the
	// pre-exec re-verification rejects.
	musePTYCodeSealVerifyFailed = "seal_verify_failed"
	// musePTYCodeResumeNotImported refuses a frozen resume plan
	// the host composed instead of importing verified.
	musePTYCodeResumeNotImported = "resume_not_imported"
	// musePTYCodeSealedStale refuses an authentic seal bound to a
	// binary that is not the current selection: outer
	// recomposition, never in-place retargeting.
	musePTYCodeSealedStale = "sealed_stale"
	// musePTYCodeImportRefused refuses a seal the authenticated
	// import rejects: self-minted, unsealed, wrong-system or
	// malformed evidence.
	musePTYCodeImportRefused = "sealed_import_refused"
	// musePTYCodeModeRefused refuses a verified seal bound to a
	// non-interactive plan: the PTY host runs interactive only.
	musePTYCodeModeRefused = "sealed_mode_refused"
	// musePTYCodeStoreMissing refuses a frozen plan with no
	// frozen store binding: a tuple without a store verifies
	// against nothing.
	musePTYCodeStoreMissing = "frozen_store_missing"
	// musePTYCodeInvalidConfig refuses a malformed builds
	// binding or caller request.
	musePTYCodeInvalidConfig = "invalid_config"
	// musePTYCodeTuplePartial refuses a request carrying a
	// partial frozen tuple: the three members travel together.
	musePTYCodeTuplePartial = "frozen_tuple_partial"
)

// MusePTYExecError is a typed L3P pre-exec refusal: the entry leg,
// the machine-readable code, and the chained cause.
type MusePTYExecError struct {
	Leg   string
	Code  string
	Cause error
}

// Error renders the refusal with its leg and code.
func (e *MusePTYExecError) Error() string {
	if e == nil {
		return "muse pty exec error"
	}
	if e.Cause != nil {
		return "muse pty " + e.Leg + " " + e.Code + ": " + e.Cause.Error()
	}
	return "muse pty " + e.Leg + " " + e.Code
}

// Unwrap returns the underlying cause.
func (e *MusePTYExecError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// AsMusePTYExecError matches a MusePTYExecError in err's chain for
// leg and code assertions.
func AsMusePTYExecError(err error) (*MusePTYExecError, bool) {
	var refusal *MusePTYExecError
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return nil, false
}

// recordFrozenTuple binds a composed plan to the request's frozen
// tuple. All three members travel together; a partial tuple refuses
// even though the module would refuse it first, so the host never
// depends on module ordering for its own binding.
func (p *MusePTYLaunchPlan) recordFrozenTuple(req agentic.LaunchRequest) error {
	binary, build, sha := req.FrozenToolBinary, req.FrozenToolBuild, req.FrozenToolSHA256
	if binary == "" && build == "" && sha == "" {
		return nil
	}
	if binary == "" || build == "" || sha == "" {
		return &MusePTYExecError{Leg: string(musePTYLegLaunch), Code: musePTYCodeTuplePartial, Cause: fmt.Errorf("binary, build and sha256 travel together; a partial tuple selects nothing")}
	}
	p.frozenTuple = musebuilds.FrozenIdentity{Build: build, SHA256: sha, Path: binary}
	p.frozenSet = true
	return nil
}
