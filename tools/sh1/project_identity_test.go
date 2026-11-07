package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProjectRequiresOriginalProductionBytes(t *testing.T) {
	for _, changed := range []string{"", "session.go", "grammar.txt", "child/child.go"} {
		name := changed
		if name == "" {
			name = "unchanged"
		}
		t.Run(name, func(t *testing.T) {
			src, cand, _ := candidateFixture(t)
			command(t, src, "git", "init", "-q")
			command(t, src, "git", "add", ".")
			command(t, src, "git", "-c", "user.name=SH1 Fixture", "-c", "user.email=fixture@example.org", "-c", "commit.gpgsign=false", "commit", "-qm", "Fixture")
			oid := strings.TrimSpace(string(command(t, src, "git", "rev-parse", "HEAD")))
			before, _, err := tree(src)
			if err != nil {
				t.Fatal(err)
			}
			if changed != "" {
				// The manifest is internally consistent: this is a changed candidate,
				// rather than corruption already caught by candidate validation.
				manifestPath := filepath.Join(cand, manifestName)
				var manifest Manifest
				if err := readJSON(manifestPath, &manifest); err != nil {
					t.Fatal(err)
				}
				b, err := os.ReadFile(filepath.Join(cand, changed))
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasSuffix(changed, ".go") {
					b = append(b, []byte("\n// changed production bytes\n")...)
				} else {
					b = append(b, []byte("\nchanged production asset\n")...)
				}
				save(t, filepath.Join(cand, changed), b)
				for i := range manifest.Files {
					if manifest.Files[i].Path == changed {
						manifest.Files[i].After = digest(b)
					}
				}
				if err := jsonWrite(manifestPath, manifest); err != nil {
					t.Fatal(err)
				}
				if _, _, err := candidate(cand); err != nil {
					t.Fatalf("candidate precondition: %v", err)
				}
				// Distinguish no writes from rewriting unchanged earlier files.
				for path := range before {
					stamp := time.Unix(1000000000, 0)
					if err := os.Chtimes(filepath.Join(src, path), stamp, stamp); err != nil {
						t.Fatal(err)
					}
				}
			}
			report := filepath.Join(t.TempDir(), "project.json")
			args := []string{tool, "project", "-candidate", cand, "-checkout", src, "-package", "internal/session", "-commit", oid, "-adapters", filepath.Join(src, "adapters.json"), "-report", report}
			out, exit, err := run(".", nil, args...)
			t.Logf("project candidate %q exit %d", changed, exit)
			wantExit := 0
			if changed != "" {
				wantExit = 1
			}
			if err != nil || exit != wantExit {
				t.Errorf("project exit %d, want %d: %v\n%s", exit, wantExit, err, out)
			}
			after, _, err := tree(src)
			if err != nil {
				t.Fatal(err)
			}
			for path, b := range before {
				if !bytes.Equal(b, after[path]) {
					t.Errorf("checkout bytes changed: %s", path)
				}
			}
			if len(after) != len(before) {
				t.Error("checkout file set changed")
			}
			if changed != "" {
				for path := range before {
					info, err := os.Stat(filepath.Join(src, path))
					if err != nil {
						t.Fatal(err)
					}
					if !info.ModTime().Equal(time.Unix(1000000000, 0)) {
						t.Errorf("refused projection wrote checkout file: %s", path)
					}
				}
				if _, err := os.Stat(report); !os.IsNotExist(err) {
					t.Errorf("refused projection published a report: %v", err)
				}
			}
		})
	}
}
