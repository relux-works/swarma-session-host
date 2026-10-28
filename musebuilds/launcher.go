package musebuilds

import (
	"context"
	"fmt"
	"path/filepath"
)

// A second lookup after PATH discovery must agree with the held graph. This
// is canonical resolution through directory FDs, not a fresh path open.
// Relative PATH has already been based on the invocation's captured cwd.
func resolveInstallDir(ctx context.Context, s *discoverySnapshot, launcherPath string) (string, string, error) {
	if err := refuseIfCanceled(ctx, StageLauncher); err != nil {
		return "", "", err
	}
	dir, name, id, err := s.lookup(ctx, launcherPath, StageLauncher)
	if err != nil {
		// Boundary disagreement here is a typed resolution refusal. Later
		// disagreement during acceptance uses the single snapshot restart.
		if cancel := refuseIfCanceled(ctx, StageLauncher); cancel != nil {
			return "", "", cancel
		}
		return "", "", &DiscoveryError{Stage: StageLauncher, Code: CodeLauncherResolution, Cause: err}
	}
	if !id.mode.IsRegular() || id.mode.Perm()&0o111 == 0 {
		return "", "", refusal(ctx, StageLauncher, CodeLauncherUnusable, fmt.Errorf("launcher is not executable"))
	}
	if _, err := s.open(ctx, dir, name, StageLauncher); err != nil {
		return "", "", refusal(ctx, StageLauncher, CodeLauncherResolution, err)
	}
	resolved := filepath.Join(dir.path, name)
	installDir := filepath.Dir(resolved)
	return resolved, installDir, nil
}
