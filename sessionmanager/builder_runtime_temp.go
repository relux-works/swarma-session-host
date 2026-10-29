package sessionmanager

import (
	"os"
	"path/filepath"
)

func prepareBuilderRuntimeTemp(sessionDir string) (string, error) {
	runtimeTemp := filepath.Join(sessionDir, "builder-runtime")
	if err := os.MkdirAll(runtimeTemp, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(runtimeTemp, 0o700); err != nil {
		return "", err
	}
	return runtimeTemp, nil
}
