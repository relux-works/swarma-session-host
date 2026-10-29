//go:build unix

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Version/help use the real command parser and exit before any runtime state.
// No daemon/native provider or source-registration fixture is started.
func TestStandaloneCommandVersionAndHelpHaveNoRuntimeEffects(t *testing.T) {
	for _, arg := range []string{"--version", "--help"} {
		t.Run(arg, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "untouched")
			var out, diagnostics bytes.Buffer
			if err := run(context.Background(), []string{arg, "--state-root", root}, &out, &diagnostics, nil); err != nil {
				t.Fatal(err)
			}
			if arg == "--version" && !strings.Contains(out.String(), "tb-sessiond "+Version) {
				t.Fatal(out.String())
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("metadata command touched state: %v", err)
			}
		})
	}
}

// Missing/invalid explicit paths, foreign flags and surplus args refuse before
// starting any host. These are parser/startup controls, not serving acceptance.
func TestStandaloneCommandInvalidInputsRefuseBeforeState(t *testing.T) {
	for _, args := range [][]string{{"--state-root", "relative"}, {"--namespace", ""}, {"--config", "relative"}, {"--socket-path", "relative"}, {"--board-dir", "foreign"}, {"unexpected"}, {"--config", "/missing/explicit/config.json"}} {
		root := filepath.Join(t.TempDir(), "untouched")
		var out, diagnostics bytes.Buffer
		all := append([]string{"--state-root", root}, args...)
		if err := run(context.Background(), all, &out, &diagnostics, nil); err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
		if _, err := os.Stat(root); !os.IsNotExist(err) {
			t.Fatalf("invalid arguments touched state: %v", err)
		}
	}
}
