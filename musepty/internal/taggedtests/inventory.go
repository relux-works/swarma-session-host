// Package taggedtests derives and checks the exact tests added by a build tag.
package taggedtests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var Modules = []string{"."}

type Entry struct {
	Module   string        `json:"module"`
	Package  string        `json:"package"`
	Tests    []string      `json:"tests"`
	Requires []Requirement `json:"requires,omitempty"`
}

// Difference preserves package identity: an ordinary namesake in another
// package must not hide a tagged-only test.
func Difference(module string, ordinary, tagged map[string][]string) []Entry {
	var entries []Entry
	for pkg, names := range tagged {
		old := make(map[string]bool)
		for _, name := range ordinary[pkg] {
			old[name] = true
		}
		seen := make(map[string]bool)
		var added []string
		for _, name := range names {
			if !old[name] && !seen[name] {
				added = append(added, name)
				seen[name] = true
			}
		}
		if len(added) > 0 {
			sort.Strings(added)
			entries = append(entries, Entry{Module: module, Package: pkg, Tests: added})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Package < entries[j].Package })
	return entries
}

func Match(want, got []Entry) error {
	if want == nil {
		want = []Entry{}
	}
	if got == nil {
		got = []Entry{}
	}
	names := func(entries []Entry) []Entry {
		result := make([]Entry, len(entries))
		for i, e := range entries {
			result[i] = Entry{Module: e.Module, Package: e.Package, Tests: e.Tests}
		}
		return result
	}
	a, _ := json.Marshal(names(want))
	b, _ := json.Marshal(names(got))
	if !bytes.Equal(a, b) {
		return fmt.Errorf("tagged-only inventory differs from pin: want %s; derived %s", a, b)
	}
	return nil
}

func Load(path string) ([]Entry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []Entry
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&entries); err != nil {
		return nil, err
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("trailing inventory data")
	}
	allowed := map[string]bool{}
	for _, m := range Modules {
		allowed[m] = true
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if !allowed[e.Module] || (e.Package != "." && !strings.HasPrefix(e.Package, "./")) || strings.Contains(e.Package, "..") || len(e.Tests) == 0 {
			return nil, fmt.Errorf("invalid inventory entry: %+v", e)
		}
		for _, n := range e.Tests {
			k := e.Module + "/" + e.Package + "/" + n
			if !isTestName(n) || seen[k] {
				return nil, fmt.Errorf("invalid or duplicate tagged test %q", k)
			}
			seen[k] = true
		}
	}
	if err := ValidateRequirements(entries, time.Now().UTC()); err != nil {
		return nil, err
	}
	return entries, nil
}

func isTestName(name string) bool {
	if !token.IsIdentifier(name) || !strings.HasPrefix(name, "Test") || name == "TestMain" {
		return false
	}
	if name == "Test" {
		return true
	}
	first, _ := utf8.DecodeRuneInString(strings.TrimPrefix(name, "Test"))
	return !unicode.IsLower(first)
}

// DecodeList accepts go test -json -list output, never a hand-maintained grep
// over source. Non-test output and other package identities cannot add names.
func DecodeList(data []byte, modulePath string) (map[string][]string, error) {
	out := map[string][]string{}
	d := json.NewDecoder(bytes.NewReader(data))
	for {
		var event struct{ Action, Package, Output string }
		if err := d.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if event.Action != "output" {
			continue
		}
		pkg := "."
		if event.Package != modulePath {
			if !strings.HasPrefix(event.Package, modulePath+"/") {
				continue
			}
			pkg = "./" + strings.TrimPrefix(event.Package, modulePath+"/")
		}
		for _, line := range strings.Split(event.Output, "\n") {
			if isTestName(line) {
				out[pkg] = append(out[pkg], line)
			}
		}
	}
	return out, nil
}

func Pattern(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = regexp.QuoteMeta(n)
	}
	return "^(" + strings.Join(quoted, "|") + ")$"
}

// Sources is ciguard's independent build-constraint check of the pin. The
// runner additionally compares real compiled tagged/ordinary -list output.
func Sources(root string) ([]Entry, error) {
	var result []Entry
	for _, module := range Modules {
		ordinary, tagged := map[string][]string{}, map[string][]string{}
		base := filepath.Join(root, module)
		err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if path != base && (strings.HasPrefix(d.Name(), ".") || d.Name() == "vendor" || d.Name() == "testdata") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			untagged := build.Default
			untagged.BuildTags = nil
			withTag := untagged
			withTag.BuildTags = []string{"mutants"}
			a, err := untagged.MatchFile(filepath.Dir(path), d.Name())
			if err != nil {
				return err
			}
			b, err := withTag.MatchFile(filepath.Dir(path), d.Name())
			if err != nil {
				return err
			}
			if !a && !b {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(base, filepath.Dir(path))
			if err != nil {
				return err
			}
			pkg := "."
			if rel != "." {
				pkg = "./" + filepath.ToSlash(rel)
			}
			for _, decl := range file.Decls {
				f, ok := decl.(*ast.FuncDecl)
				if !ok || f.Recv != nil || !isTestName(f.Name.Name) {
					continue
				}
				if a {
					ordinary[pkg] = append(ordinary[pkg], f.Name.Name)
				}
				if b {
					tagged[pkg] = append(tagged[pkg], f.Name.Name)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		result = append(result, Difference(module, ordinary, tagged)...)
	}
	return result, nil
}
