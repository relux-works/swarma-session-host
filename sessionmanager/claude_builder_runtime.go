package sessionmanager

import (
	"fmt"
	"os"
	"path/filepath"
)

func claudeBuilderRuntimeReadRoots(managedHome ...string) []string {
	roots := make([]string, 0, 4)
	if len(managedHome) > 0 && managedHome[0] != "" {
		roots = append(roots, managedHome[0])
	} else if providerHome, err := claudeProviderHome(); err == nil {
		roots = append(roots, providerHome)
	}
	if userHome, err := os.UserHomeDir(); err == nil {
		roots = append(
			roots,
			filepath.Join(userHome, ".claude.json"),
			filepath.Join(userHome, ".local", "state", "claude"),
			filepath.Join(userHome, ".agents", ".configs", "claude-settings.json"),
			filepath.Join(userHome, "Library", "Preferences", "com.apple.security.plist"),
		)
	}
	roots = append(roots, filepath.Join("/private/tmp", fmt.Sprintf("claude-%d", os.Getuid())))
	return roots
}

func claudeBuilderTranscriptBoundary(projectDir, sessionUUID string, managedHome ...string) (string, int64, error) {
	var home string
	var err error
	if len(managedHome) > 0 && managedHome[0] != "" {
		home = managedHome[0]
	} else {
		home, err = claudeProviderHome()
		if err != nil {
			return "", 0, err
		}
	}
	path, err := claudeSessionTranscriptPath(home, projectDir, sessionUUID)
	if err != nil {
		return "", 0, err
	}
	info, err := os.Stat(path)
	switch {
	case err == nil && info.IsDir():
		return "", 0, fmt.Errorf("Claude builder transcript %s is a directory", path)
	case err == nil:
		return path, info.Size(), nil
	case os.IsNotExist(err):
		return path, 0, nil
	default:
		return "", 0, err
	}
}
