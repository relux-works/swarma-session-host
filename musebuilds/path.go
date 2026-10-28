package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// launcherName is the self-updating `muse` launcher program name, spelled once.
const launcherName = "muse"

// errNoPathInEnvironment refuses a launch environment that carries no PATH
// entry at all. It is a refusal, never a fallback onto the ambient process
// PATH: a caller that curated an environment without PATH did not ask this
// package to substitute its own.
var errNoPathInEnvironment = errors.New("the launch environment carries no PATH")

// effectivePath returns the last PATH value in a KEY=VALUE environment,
// matching the child process's own view. A bare key without `=` is not a
// hit: it carries no value to resolve against.
func effectivePath(env []string) (string, bool) {
	const prefix = "PATH="
	for i := len(env) - 1; i >= 0; i-- {
		entry := env[i]
		if len(entry) >= len(prefix) && entry[:len(prefix)] == prefix {
			return entry[len(prefix):], true
		}
	}
	return "", false
}

// resolveLauncherOnPath resolves the `muse` launcher against the supplied
// environment's effective PATH, following the module's launch-environment
// semantics: split the value, treat an empty element as the working
// directory, and take the first entry that is an executable regular file.
// Missing PATH, not-found and unusable candidates are distinct typed
// refusals; an unusable candidate never wins, but its presence is reported
// rather than mistaken for an absent install. The returned path is absolute against the captured cwd (design §2 steps
// 1–2). A wholly empty PATH is an empty list; empty members of a nonempty
// list mean the captured working directory, matching filepath.SplitList.
func resolveLauncherOnPath(ctx context.Context, s *discoverySnapshot, env []string, cwd string) (string, error) {
	if err := refuseIfCanceled(ctx, StagePath); err != nil {
		return "", err
	}
	pathValue, ok := effectivePath(env)
	if !ok {
		return "", &DiscoveryError{Stage: StagePath, Code: CodeNoPath, Cause: errNoPathInEnvironment}
	}
	var firstUnusable string
	var firstUnusableErr error
	for _, dir := range filepath.SplitList(pathValue) {
		if dir == "" {
			dir = "."
		}
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(cwd, dir)
		}
		candidate := filepath.Join(dir, launcherName)
		_, _, info, err := s.lookup(ctx, candidate, StagePath)
		if canceled := refuseIfCanceled(ctx, StagePath); canceled != nil {
			return "", canceled
		}
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			if firstUnusable == "" {
				firstUnusable, firstUnusableErr = candidate, err
			}
			continue
		}
		if !info.mode.IsRegular() || info.mode.Perm()&0o111 == 0 {
			if firstUnusable == "" {
				firstUnusable, firstUnusableErr = candidate, fmt.Errorf("not an executable regular file")
			}
			continue
		}
		return candidate, nil
	}
	if firstUnusable != "" {
		return "", &DiscoveryError{Stage: StagePath, Code: CodeLauncherUnusable, Cause: fmt.Errorf("%s: %w", firstUnusable, firstUnusableErr)}
	}
	return "", &DiscoveryError{Stage: StagePath, Code: CodeLauncherNotFound, Cause: fmt.Errorf("%q not found in the launch environment's PATH", launcherName)}
}
