package musebuilds

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestDiscoveryNeverExecutesUnfrozen proves discovery resolves and hashes a
// sentinel-armed install without running anything in it: the launcher and
// both candidates would each leave a trace file behind if started, and a
// successful selection must leave no trace behind.
func TestDiscoveryNeverExecutesUnfrozen(t *testing.T) {
	install := newInstall(t)
	armed := func(name string) (string, string) {
		sentinel := filepath.Join(t.TempDir(), name+".touched")
		quoted := "'" + strings.ReplaceAll(sentinel, "'", "'\\''") + "'"
		return sentinel, "#!/bin/sh\n: > " + quoted + "\n"
	}
	launcherSentinel, launcherScript := armed("launcher")
	install.writeFileSync(filepath.Join(install.dir, launcherName), []byte(launcherScript), 0o755)
	sentinels := []string{launcherSentinel}
	for _, build := range []string{"1.4.3-R5000", "1.4.4-R5419.1"} {
		sentinel, script := armed(build)
		sentinels = append(sentinels, sentinel)
		install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+build), []byte(script), 0o755)
	}
	install.writeSidecar("1.4.4-R5419.1\n")

	discovery := resolveSuccess(t, install.env())
	if discovery.Identity.Build != "1.4.4-R5419.1" {
		t.Fatalf("selected %+v, want the armed maximum", discovery.Identity)
	}
	for _, sentinel := range sentinels {
		if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
			t.Fatalf("sentinel %s exists: discovery started %v", sentinel, err)
		}
	}
}

// noexecModuleRoot is the module root, captured at initialisation time by
// walking up from the working directory to go.mod — the same discovery
// wallclockModuleRoot in internal/ciguard uses. go test starts the package
// binary in the package directory, and path fixtures in this package change
// the working directory later, so the root is captured before any test runs.
// No source-location dependency.
var noexecModuleRoot = func() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return wd
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			return ""
		}
		wd = parent
	}
}()

// TestDiscoverySourcesImportNoExec pins the structural half of the same
// guarantee: no production file in this package imports the process
// launcher or names its primitives, so no future edit can start executing
// discoveries without this test failing first. It explicitly covers
// launch.go, the L2C recovery controller.
func TestDiscoverySourcesImportNoExec(t *testing.T) {
	root := noexecModuleRoot
	if root == "" {
		t.Fatal("cannot locate the module root")
	}
	dir := filepath.Join(root, "musebuilds")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	sawLaunch := false
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		checked++
		if name == "launch.go" {
			sawLaunch = true
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path == "os/exec" {
				t.Fatalf("%s imports os/exec: discovery must not start processes", name)
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.SelectorExpr:
				if n.Sel.Name == "StartProcess" || n.Sel.Name == "ForkExec" {
					t.Fatalf("%s names %s: discovery must not start processes", name, n.Sel.Name)
				}
			case *ast.Ident:
				if n.Name == "StartProcess" || n.Name == "ForkExec" {
					t.Fatalf("%s names %s: discovery must not start processes", name, n.Name)
				}
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no production files inspected")
	}
	if !sawLaunch {
		t.Fatal("launch.go was not inspected")
	}
}
