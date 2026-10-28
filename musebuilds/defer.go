package musebuilds

import (
	"context"
	"errors"
	"fmt"

	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// selectDeferralTarget chooses the Amendment 2 V-behaviour target:
// the newest already-frozen build with valid probeable state and
// verified bytes. The choice is singular and definite — the newest
// frozen copy — so anything but a valid newest refuses instead of
// silently stepping down to an older copy. BAD, missing or corrupt
// state and digest disagreement all refuse; there is no skipping.
func selectDeferralTarget(ctx context.Context, cfg HostConfig) (FrozenIdentity, error) {
	if err := freezeRefuseIfCanceled(ctx, StageAdmit); err != nil {
		return FrozenIdentity{}, err
	}
	releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok {
			return FrozenIdentity{}, ferr
		}
		return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeLeaseFailed, Cause: err}
	}
	defer func() { _ = releaseLease() }()
	entries, err := cfg.store.builds.entries()
	if err != nil {
		return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeUnavailable, Cause: fmt.Errorf("frozen builds unreadable: %w", err)}
	}
	type frozenKey struct {
		id  muse.BuildIdentity
		sha string
		key string
	}
	var keys []frozenKey
	for _, entry := range entries {
		if err := freezeRefuseIfCanceled(ctx, StageAdmit); err != nil {
			return FrozenIdentity{}, err
		}
		name := entry.Name()
		if !entry.IsDir() {
			return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeStateInconsistent, Cause: fmt.Errorf("frozen builds hold file %q", name)}
		}
		build, sha, err := parseFrozenKey(name)
		if err != nil {
			return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeStateInconsistent, Cause: err}
		}
		id, err := muse.ParseBuildID(build)
		if err != nil {
			return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeStateInconsistent, Cause: err}
		}
		keys = append(keys, frozenKey{id: id, sha: sha, key: name})
	}
	if len(keys) == 0 {
		return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeUnavailable, Cause: fmt.Errorf("no frozen build to defer to")}
	}
	best := 0
	for i := 1; i < len(keys); i++ {
		if err := freezeRefuseIfCanceled(ctx, StageAdmit); err != nil {
			return FrozenIdentity{}, err
		}
		order, err := muse.CompareBuildIDs(keys[i].id, keys[best].id)
		if err != nil {
			return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeSelectionAmbiguous, Cause: err}
		}
		if order > 0 {
			best = i
		}
	}
	winner := keys[best]
	state, present, err := readBuildStateAt(ctx, cfg.store.meta, winner.key)
	if err != nil {
		if errors.Is(err, errProbeContractMismatch) {
			return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeProbeContractMismatch, Cause: err}
		}
		return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeStateInconsistent, Cause: err}
	}
	if !present {
		return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeStateInconsistent, Cause: fmt.Errorf("newest frozen %s carries no state", winner.key)}
	}
	if state.State == StateBAD {
		return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeUnavailable, Cause: fmt.Errorf("newest frozen %s is recorded %s", winner.key, state.Verdict.Code)}
	}
	frozen, err := verifyFrozenTuple(ctx, cfg.store, winner.key, FrozenIdentity{Build: winner.id.Build, SHA256: winner.sha})
	if err != nil {
		return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeFrozenCollision, Cause: err}
	}
	if err := state.agreesWith(winner.key, frozen); err != nil {
		return FrozenIdentity{}, &FreezeError{Stage: StageAdmit, Code: CodeStateInconsistent, Cause: err}
	}
	return frozen, nil
}

// parseFrozenKey splits a frozen key directory name back into its
// exact full build and full digest. The digest is the last 64 hex
// digits; the build is everything before the separator. Prefixes
// never address a key.
func parseFrozenKey(name string) (build, sha string, err error) {
	if len(name) < 1+1+64 || name[len(name)-65] != '-' {
		return "", "", fmt.Errorf("%q is not a frozen key", name)
	}
	build, sha = name[:len(name)-65], name[len(name)-64:]
	if !isLowerHex64(sha) {
		return "", "", fmt.Errorf("%q carries no full digest", name)
	}
	return build, sha, nil
}
