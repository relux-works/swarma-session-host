//go:build darwin || linux

package probeprocess

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeExecutable closes and syncs a temporary before atomic publication;
// none of these fixture paths are real installed Muse binaries.
func writeExecutable(t *testing.T, path, script string) {
	t.Helper()
	f, err := os.CreateTemp(filepath.Dir(path), ".exec-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(script); err != nil {
		t.Fatal(err)
	}
	if err := f.Chmod(0o555); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		t.Fatal(err)
	}
}

// TestOSLauncherExecutesHeldInode checks a stable held identity runs, and a
// replaced pathname refuses before exec. It does not claim atomic fd-exec.
func TestOSLauncherExecutesHeldInode(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "stable", true: "replaced"}[replace], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "synthetic-probe")
			writeExecutable(t, path, "#!/bin/sh\nprintf 'held:%s\\n' \"$1\"\n")
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if replace {
				writeExecutable(t, path, "#!/bin/sh\nprintf 'replacement\\n'\n")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			p, err := (OSLauncher{}).Start(ctx, f, []string{"MUSE_NO_AUTO_UPDATE=1"})
			if replace {
				if p != nil || !errors.Is(err, ErrNotAttempted) || !errors.Is(err, ErrIdentityChanged) {
					if p != nil {
						_ = p.Kill()
						_ = p.Stdout().Close()
						_ = p.Stderr().Close()
						select {
						case <-p.Done():
						case <-ctx.Done():
						}
					}
					t.Fatalf("replaced execution path was not refused before exec: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer p.Kill()
			defer p.Stdout().Close()
			defer p.Stderr().Close()
			type answer struct {
				data []byte
				err  error
			}
			out := make(chan answer, 1)
			go func() { data, err := io.ReadAll(io.LimitReader(p.Stdout(), 1024)); out <- answer{data, err} }()
			select {
			case result := <-out:
				if result.err != nil || string(result.data) != "held:--version\n" {
					t.Fatalf("executed wrong inode: %q %v", result.data, result.err)
				}
			case <-ctx.Done():
				t.Fatal("descriptor execution did not close stdout")
			}
			select {
			case exit := <-p.Done():
				if exit.Code != 0 || exit.Err != nil || !p.GroupGone() {
					t.Fatalf("not reaped: %+v group-gone=%v", exit, p.GroupGone())
				}
			case <-ctx.Done():
				t.Fatal("descriptor execution was not reaped")
			}
		})
	}
}

// TestOSLauncherKillsAndReaps uses a shell builtin loop with TERM ignored;
// the readiness line is emitted after the trap and before cancellation.
func TestOSLauncherKillsAndReaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic-probe")
	writeExecutable(t, path, "#!/bin/sh\ntrap '' TERM\nprintf 'ready\\n'\nwhile :; do :; done\n")
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	guard, stop := context.WithTimeout(context.Background(), 60*time.Second)
	defer stop()
	ctx, cancel := context.WithCancel(guard)
	defer cancel()
	p, err := (OSLauncher{}).Start(ctx, f, []string{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Kill()
	defer p.Stdout().Close()
	defer p.Stderr().Close()
	ready := make(chan string, 1)
	go func() { b := make([]byte, 6); _, _ = io.ReadFull(p.Stdout(), b); ready <- string(b) }()
	select {
	case s := <-ready:
		if s != "ready\n" {
			t.Fatalf("signal disposition not ready: %q", s)
		}
	case <-guard.Done():
		t.Fatal("no readiness")
	}
	cancel()
	select {
	case exit := <-p.Done():
		if !strings.Contains(exit.Signal, "killed") || !p.GroupGone() {
			t.Fatalf("group not killed and reaped: %+v gone=%v", exit, p.GroupGone())
		}
	case <-guard.Done():
		t.Fatal("canceled process was not reaped")
	}
}
