package sessioncontext

import (
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"strings"
)

func claudeProviderHome() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("CLAUDE_CONFIG_DIR")); configured != "" {
		if !filepath.IsAbs(configured) {
			return "", fmt.Errorf("CLAUDE_CONFIG_DIR %q must be an absolute path", configured)
		}
		return filepath.Clean(configured), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving Claude provider home: %w", err)
	}
	return filepath.Join(home, ".claude"), nil
}

func decodeCodexHostSnapshot(h sm.ProviderHostRecord) (*sm.CodexRunnerSnapshot, error) {
	if h.Provider != "codex" || h.Snapshot.Codex == nil || h.Snapshot.Claude != nil {
		return nil, fmt.Errorf("matching typed Codex snapshot required")
	}
	copy := cloneHostRecord(h)
	return copy.Snapshot.Codex, nil
}
