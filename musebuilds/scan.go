package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	muse "github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

const legacyBareBinaryName = "muse-bin"

var errNoCandidates = errors.New("install directory holds no versioned muse-bin- builds")

type candidate struct {
	id   muse.BuildIdentity
	name string
	path string
}

// The readable directory FD belongs to the attempt, not to this function.
// Neither initial nor final enumeration reopens the install pathname.
func collectCandidates(ctx context.Context, s *discoverySnapshot, dir *fileLease) ([]candidate, error) {
	reader, err := s.open(ctx, dir, ".", StageScan)
	if err != nil {
		return nil, refusal(ctx, StageScan, CodeInstallScanFailed, err)
	}
	s.inventoryFile = reader
	names, err := readInventory(ctx, reader, StageScan)
	if err != nil {
		return nil, refusal(ctx, StageScan, CodeInstallScanFailed, err)
	}
	s.inventory = names
	var candidates []candidate
	for _, name := range names {
		if err := refuseIfCanceled(ctx, StageScan); err != nil {
			return nil, err
		}
		if name == legacyBareBinaryName || !strings.HasPrefix(name, pinnedNamePrefix) {
			continue
		}
		id, err := muse.ParsePinnedBinaryName(name)
		if err != nil {
			return nil, &DiscoveryError{Stage: StageScan, Code: CodeInstallNameUnrecognized, Cause: fmt.Errorf("%s: %w", name, err)}
		}
		candidates = append(candidates, candidate{id: id, name: name, path: filepath.Join(dir.path, name)})
	}
	return candidates, nil
}

func readInventory(ctx context.Context, dir *fileLease, stage string) ([]string, error) {
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return nil, err
	}
	if _, err := dir.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var names []string
	for {
		if err := refuseIfCanceled(ctx, stage); err != nil {
			return nil, err
		}
		batch, err := dir.file.Readdirnames(128)
		interpose(interposeInventoryRead)
		if canceled := refuseIfCanceled(ctx, stage); canceled != nil {
			return nil, canceled
		}
		names = append(names, batch...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(names, func(i, j int) bool {
		if ctx.Err() != nil {
			return false
		}
		return names[i] < names[j]
	})
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return nil, err
	}
	return names, nil
}

// Numerical maximum FIRST. Invalid maxima never shrink the set; ambiguous
// raw aliases refuse. ctx also bounds large inventory comparisons.
func selectNewest(ctx context.Context, candidates []candidate) (candidate, []candidate, error) {
	if err := refuseIfCanceled(ctx, StageSelect); err != nil {
		return candidate{}, nil, err
	}
	ordered := append([]candidate(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ctx.Err() != nil {
			return false
		}
		c, err := muse.CompareBuildIDs(ordered[i].id, ordered[j].id)
		if err != nil {
			return ordered[i].id.Build > ordered[j].id.Build
		}
		return c > 0
	})
	for i := 0; i < len(ordered); i++ {
		for j := i + 1; j < len(ordered); j++ {
			if err := refuseIfCanceled(ctx, StageSelect); err != nil {
				return candidate{}, nil, err
			}
			if _, cmpErr := muse.CompareBuildIDs(ordered[i].id, ordered[j].id); cmpErr != nil {
				return candidate{}, nil, &DiscoveryError{Stage: StageSelect, Code: CodeSelectionAmbiguous, Cause: cmpErr}
			}
		}
	}
	if err := refuseIfCanceled(ctx, StageSelect); err != nil {
		return candidate{}, nil, err
	}
	return ordered[0], ordered, nil
}
