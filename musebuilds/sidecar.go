package musebuilds

import (
	"context"
	"fmt"
	"os"
	"strings"

	muse "github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

const sidecarName = ".muse-version"
const sidecarMaxBytes = 64 * 1024
const pinnedNamePrefix = "muse-bin-"

// Only a genuine absent directory entry enables scanning without a selector.
// The selector FD stays leased so acceptance re-reads these same bytes.
func readSidecarTarget(ctx context.Context, s *discoverySnapshot, dir *fileLease) (string, bool, error) {
	lease, err := s.open(ctx, dir, sidecarName, StageSidecar)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, refusal(ctx, StageSidecar, CodeSidecarUnreadable, err)
	}
	if !lease.id.mode.IsRegular() {
		return "", false, refusal(ctx, StageSidecar, CodeSidecarUnreadable, fmt.Errorf("selector is not regular"))
	}
	data, err := readLease(ctx, lease, StageSidecar, interposeSidecarRead, sidecarMaxBytes)
	if err != nil {
		return "", false, refusal(ctx, StageSidecar, CodeSidecarUnreadable, err)
	}
	text := strings.TrimRight(string(data), "\n")
	identity, err := muse.ParseBuildID(text)
	if err != nil {
		return "", false, refusal(ctx, StageSidecar, CodeSidecarMalformed, err)
	}
	s.sidecar, s.sidecarBytes = lease, data
	return identity.Build, true, nil
}

func validateSidecarTarget(ctx context.Context, s *discoverySnapshot, dir *fileLease, build string, present bool) error {
	if err := refuseIfCanceled(ctx, StageSidecar); err != nil {
		return err
	}
	if !present {
		return nil
	}
	target, err := s.open(ctx, dir, pinnedNamePrefix+build, StageSidecar)
	if err != nil {
		return refusal(ctx, StageSidecar, CodeSidecarTargetRejected, err)
	}
	if !target.id.mode.IsRegular() || target.id.mode.Perm()&0o111 == 0 {
		return refusal(ctx, StageSidecar, CodeSidecarTargetRejected, fmt.Errorf("selector target is not an executable regular file"))
	}
	if err := checkLinkOwner(target.path, target.id); err != nil {
		return refusal(ctx, StageSidecar, CodeSidecarTargetRejected, err)
	}
	return nil
}
