package sessionhost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The positive matrix accepts canonical, retained regular executables with
// an exact configured digest. No fixture file is executed or treated as policy.
func TestLauncherExecutableCompatibility(t *testing.T) {
	for _, mode := range []os.FileMode{0500, 0555, 0700, 0755} {
		t.Run(mode.String(), func(t *testing.T) {
			m, _ := ordinaryWireFixture(t)
			if err := os.Chmod(m.launcher.path, mode); err != nil {
				t.Fatal(err)
			}
			l, err := openLauncherExecutable(m.launcher.path, m.launcher.digest)
			if err != nil {
				t.Fatal("legitimate executable refused", err)
			}
			defer l.close()
			if err := l.recheck(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Path/digest/permission forms are a positive boundary. Symlinks, directories,
// nonexecutables and writable sources refuse before any child can exist.
func TestLauncherExecutableRefusals(t *testing.T) {
	m, _ := ordinaryWireFixture(t)
	for _, name := range []string{"relative", "unclean", "symlink", "directory", "digest", "uppercase", "nonexec", "group-write"} {
		t.Run(name, func(t *testing.T) {
			path, digest := m.launcher.path, m.launcher.digest
			switch name {
			case "relative":
				path = "curator-run"
			case "unclean":
				path = filepath.Dir(path) + "/./curator-run"
			case "symlink":
				path += ".link"
				if err := os.Symlink(m.launcher.path, path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				path = filepath.Dir(path)
			case "digest":
				digest = strings.Repeat("0", 64)
			case "uppercase":
				digest = strings.Repeat("A", 64)
			case "nonexec", "group-write":
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				path += "." + name
				mode := os.FileMode(0600)
				if name == "group-write" {
					mode = 0770
				}
				if err := os.WriteFile(path, b, mode); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, mode); err != nil {
					t.Fatal(err)
				}
			}
			if l, err := openLauncherExecutable(path, digest); !errors.Is(err, errOrdinaryWireUnavailable) || l != nil {
				if l != nil {
					l.close()
				}
				t.Fatal("unsafe executable admitted", err)
			}
		})
	}
}

// The finite file-size envelope refuses empty and oversized sparse fixtures
// without reading them. The ordinary nonempty executable is the positive control.
func TestLauncherExecutableBounds(t *testing.T) {
	m, _ := ordinaryWireFixture(t)
	for _, size := range []int64{0, maxLauncherExecutable + 1} {
		t.Run(strconv.FormatInt(size, 10), func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "bounded-file")
			f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Truncate(size); err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			info, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if launcherInfoSafe(info, false) {
				t.Fatal("metadata size bound admitted")
			}
			if l, err := openLauncherExecutable(path, m.launcher.digest); !errors.Is(err, errOrdinaryWireUnavailable) || l != nil {
				if l != nil {
					l.close()
				}
				t.Fatal("file bound admitted", err)
			}
		})
	}
	l, err := openLauncherExecutable(m.launcher.path, m.launcher.digest)
	if err != nil {
		t.Fatal("positive bound refused", err)
	}
	l.close()
}

// Only the exact agreed environment-refusal diagnostic can be retained as a
// fixed label. Everything else, including secret-like strings, is discarded.
// Native exit codes overlap Launcher codes; every observation keeps phase unknown.
func TestLauncherDiagnosticRedaction(t *testing.T) {
	known := []byte("protocol_child_environment_refused\n")
	for split := 0; split <= len(known); split++ {
		d := newLauncherDiagnostic()
		_, _ = d.Write(known[:split])
		_, _ = d.Write(known[split:])
		if got := d.observe(78); got.Diagnostic != "protocol_child_environment_refused" || got.Phase != "unverified" || got.Status != 78 {
			t.Fatal("split diagnostic lost", split, got)
		}
	}
	for _, payload := range []string{"", "protocol_child_environment_refused", "protocol_child_environment_refused\nextra", "private_fixture_value", strings.Repeat("private_fixture_value", 10000)} {
		for _, status := range []int{0, 64, 70, 78, 1} {
			d := newLauncherDiagnostic()
			n, err := d.Write([]byte(payload))
			if err != nil || n != len(payload) {
				t.Fatal("diagnostic backpressured", err)
			}
			got := d.observe(status)
			want := "protocol_child_diagnostic_redacted"
			if payload == "" {
				want = ""
			}
			if got.Diagnostic != want || got.Phase != "unverified" || got.Status != status {
				t.Fatal("stderr became phase authority", got)
			}
		}
	}
}
