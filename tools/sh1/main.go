// Command sh1 provides deterministic, repository-neutral extraction gates.
package main

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

//go:embed *.go
var ownSource embed.FS

const manifestName = "sh1-manifest.json"

type FileRecord struct {
	Path      string `json:"path"`
	Before    string `json:"before"`
	After     string `json:"after"`
	Mode      uint32 `json:"mode"`
	Protected bool   `json:"protected"`
}
type Manifest struct {
	Version   int          `json:"version"`
	Old       string       `json:"old"`
	New       string       `json:"new"`
	Package   string       `json:"package"`
	Generator string       `json:"generator"`
	Transform string       `json:"transform"`
	ModInput  string       `json:"mod_input"`
	SourceMod string       `json:"source_mod"`
	Files     []FileRecord `json:"files"`
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func sourceDigest() (string, error) {
	names, e := fs.Glob(ownSource, "*.go")
	if e != nil {
		return "", e
	}
	var b bytes.Buffer
	for _, n := range names {
		d, e := ownSource.ReadFile(n)
		if e != nil {
			return "", e
		}
		fmt.Fprintf(&b, "%s\x00%s\n", n, digest(d))
	}
	return digest(b.Bytes()), nil
}
func jsonWrite(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return write(path, append(b, '\n'), 0644)
}
func readJSON(path string, v any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(v); e != nil {
		return e
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return errors.New("extra JSON data")
	}
	return nil
}
func write(path string, b []byte, mode fs.FileMode) error {
	if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return e
	}
	return os.WriteFile(path, b, mode)
}
func relative(p string) error {
	if p == "" || p == "." || filepath.IsAbs(p) || strings.Contains(p, "\\") || filepath.ToSlash(filepath.Clean(p)) != p || strings.HasPrefix(p, "../") {
		return fmt.Errorf("unsafe relative path %q", p)
	}
	return nil
}
func modulePath(p string) error {
	if e := relative(p); e != nil {
		return e
	}
	for _, c := range p {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.ContainsRune("._-/", c)) {
			return fmt.Errorf("unsupported module path %q", p)
		}
	}
	if !strings.Contains(strings.Split(p, "/")[0], ".") {
		return errors.New("module path requires a domain")
	}
	return nil
}
func tree(root string) (map[string][]byte, map[string]fs.FileMode, error) {
	files := map[string][]byte{}
	modes := map[string]fs.FileMode{}
	e := filepath.WalkDir(root, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("non-regular file refused: %s", d.Name())
		}
		r, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		r = filepath.ToSlash(r)
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		files[r] = b
		modes[r] = info.Mode().Perm()
		return nil
	})
	return files, modes, e
}
func keys[V any](m map[string]V) []string {
	a := make([]string, 0, len(m))
	for k := range m {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}
func protected(p string) bool {
	return strings.HasSuffix(p, "_test.go") || strings.Contains("/"+p, "/testdata/")
}
func emptyOutput(p string) error {
	entries, e := os.ReadDir(p)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	if len(entries) != 0 {
		return errors.New("output must be absent or empty")
	}
	return nil
}

// Rewrite only AST import literals, preserving all other source bytes.
func rewrite(b []byte, old, new string) ([]byte, error) {
	fset := token.NewFileSet()
	f, e := parser.ParseFile(fset, "input.go", b, parser.ImportsOnly)
	if e != nil {
		return nil, e
	}
	type edit struct {
		a, z  int
		value string
	}
	edits := []edit{}
	for _, im := range f.Imports {
		p, e := strconv.Unquote(im.Path.Value)
		if e != nil {
			return nil, e
		}
		if p == old || strings.HasPrefix(p, old+"/") {
			edits = append(edits, edit{fset.Position(im.Path.Pos()).Offset, fset.Position(im.Path.End()).Offset, strconv.Quote(new + strings.TrimPrefix(p, old))})
		}
	}
	out := append([]byte(nil), b...)
	for i := len(edits) - 1; i >= 0; i-- {
		x := edits[i]
		out = append(append(append([]byte{}, out[:x.a]...), []byte(x.value)...), out[x.z:]...)
	}
	return out, nil
}
func modIdentity(b []byte) (string, string, error) {
	var mod, goVersion string
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || strings.HasPrefix(f[0], "//") {
			continue
		}
		if f[0] == "module" && len(f) == 2 {
			mod = strings.Trim(f[1], "\"")
		}
		if f[0] == "go" && len(f) == 2 {
			goVersion = f[1]
		}
	}
	if mod == "" || goVersion == "" {
		return "", "", errors.New("module and go directives required")
	}
	return mod, goVersion, nil
}
func run(dir string, env []string, args ...string) ([]byte, int, error) {
	if len(args) == 0 {
		return nil, -1, errors.New("test command required")
	}
	c := exec.Command(args[0], args[1:]...)
	c.Dir = dir
	c.Env = append(os.Environ(), env...)
	// stdout is reserved for machine-readable evidence; compiler diagnostics stay on stderr.
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	e := c.Run()
	if e == nil {
		return stdout.Bytes(), 0, nil
	}
	var ee *exec.ExitError
	if errors.As(e, &ee) {
		if stderr.Len() > 0 {
			_, _ = os.Stderr.Write(stderr.Bytes())
		}
		return stdout.Bytes(), ee.ExitCode(), nil
	}
	return stdout.Bytes(), -1, e
}
func generate(source, pkg, old, new, out, modFile string) error {
	if e := relative(pkg); e != nil {
		return e
	}
	for _, p := range []string{old, new} {
		if e := modulePath(p); e != nil {
			return e
		}
	}
	if e := emptyOutput(out); e != nil {
		return e
	}
	files, modes, e := tree(filepath.Join(source, filepath.FromSlash(pkg)))
	if e != nil {
		return e
	}
	if len(files) == 0 {
		return errors.New("empty source package")
	}
	for _, p := range []string{"go.mod", manifestName} {
		if _, ok := files[p]; ok {
			return fmt.Errorf("reserved candidate file %s", p)
		}
	}
	mod, e := os.ReadFile(modFile)
	if e != nil {
		return e
	}
	for _, line := range strings.Split(string(mod), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "replace") {
			return errors.New("candidate replace directives refused")
		}
	}
	name, version, e := modIdentity(mod)
	if e != nil {
		return e
	}
	if name != new {
		return errors.New("candidate go.mod module differs from new import path")
	}
	srcMod, e := os.ReadFile(filepath.Join(source, "go.mod"))
	if e != nil {
		return e
	}
	srcName, srcVersion, e := modIdentity(srcMod)
	if e != nil {
		return e
	}
	if srcName+"/"+pkg != old {
		return errors.New("old import path does not match source module and package")
	}
	if srcVersion != version {
		return errors.New("source and candidate Go versions differ")
	}
	gen, e := sourceDigest()
	if e != nil {
		return e
	}
	m := Manifest{Version: 1, Old: old, New: new, Package: pkg, Generator: gen, ModInput: digest(mod), SourceMod: digest(srcMod), Files: []FileRecord{}}
	params, _ := json.Marshal([]string{pkg, old, new, digest(srcMod), m.ModInput})
	m.Transform = digest(append([]byte(gen+"\n"), params...))
	output := map[string][]byte{}
	for _, p := range keys(files) {
		b := files[p]
		next := b
		if strings.HasSuffix(p, ".go") {
			next, e = rewrite(b, old, new)
			if e != nil {
				return fmt.Errorf("%s: %w", p, e)
			}
		}
		output[p] = next
		m.Files = append(m.Files, FileRecord{p, digest(b), digest(next), uint32(modes[p]), protected(p)})
	}
	output["go.mod"] = mod
	m.Files = append(m.Files, FileRecord{"go.mod", "", digest(mod), 0644, false})
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	for _, p := range keys(output) {
		mode := modes[p]
		if p == "go.mod" {
			mode = 0644
		}
		path := filepath.Join(out, filepath.FromSlash(p))
		if e = write(path, output[p], mode); e != nil {
			return e
		}
		// Creation applies the process umask. The candidate must retain the
		// exact permissions recorded in its manifest, including go.mod's 0644.
		if e = os.Chmod(path, mode); e != nil {
			return e
		}
	}
	return jsonWrite(filepath.Join(out, manifestName), m)
}
func candidate(root string) (Manifest, map[string][]byte, error) {
	var m Manifest
	if e := readJSON(filepath.Join(root, manifestName), &m); e != nil {
		return m, nil, e
	}
	if m.Version != 1 || m.Old == "" || m.New == "" || len(m.Files) == 0 {
		return m, nil, errors.New("invalid candidate manifest")
	}
	files, modes, e := tree(root)
	if e != nil {
		return m, nil, e
	}
	delete(files, manifestName)
	if len(files) != len(m.Files) {
		return m, nil, errors.New("candidate file set differs from manifest")
	}
	seen := map[string]bool{}
	for _, r := range m.Files {
		if e = relative(r.Path); e != nil {
			return m, nil, e
		}
		if seen[r.Path] || digest(files[r.Path]) != r.After || uint32(modes[r.Path]) != r.Mode || protected(r.Path) != r.Protected {
			return m, nil, errors.New("candidate hash, mode or classification differs from manifest")
		}
		seen[r.Path] = true
	}
	return m, files, nil
}

type AdapterTable struct {
	Version int               `json:"version"`
	Entries []json.RawMessage `json:"entries"`
}
type Projection struct {
	Version   int          `json:"version"`
	Commit    string       `json:"commit"`
	Candidate string       `json:"candidate"`
	Adapters  string       `json:"adapters"`
	Files     []FileRecord `json:"files"`
	Protected []FileRecord `json:"protected"`
}

func project(root, checkout, pkg, commit, adapters, report string) error {
	if e := relative(pkg); e != nil {
		return e
	}
	head, code, e := run(checkout, nil, "git", "rev-parse", "HEAD")
	if e != nil {
		return e
	}
	if code != 0 || strings.TrimSpace(string(head)) != commit || len(commit) != 40 {
		return errors.New("checkout does not match pinned commit")
	}
	dirty, code, e := run(checkout, nil, "git", "status", "--porcelain", "--untracked-files=all")
	if e != nil {
		return e
	}
	if code != 0 || len(bytes.TrimSpace(dirty)) != 0 {
		return errors.New("projection requires a clean pinned checkout")
	}
	var a AdapterTable
	if e = readJSON(adapters, &a); e != nil {
		return e
	}
	if a.Version != 1 || a.Entries == nil || len(a.Entries) != 0 {
		return errors.New("only version 1 empty adapter table is supported")
	}
	m, files, e := candidate(root)
	if e != nil {
		return e
	}
	if m.Package != pkg {
		return errors.New("projection package differs from generated package")
	}
	original, _, e := tree(filepath.Join(checkout, filepath.FromSlash(pkg)))
	if e != nil {
		return e
	}
	if len(original) != len(files)-1 {
		return errors.New("pinned source file set differs")
	}
	all, _, e := tree(checkout)
	if e != nil {
		return e
	}
	prot := map[string]string{}
	for p, b := range all {
		if protected(p) {
			prot[p] = digest(b)
		}
	}
	next := map[string][]byte{}
	for _, r := range m.Files {
		if r.Path == "go.mod" {
			continue
		}
		b, ok := original[r.Path]
		if !ok || digest(b) != r.Before {
			return errors.New("pinned source hash differs")
		}
		if r.Protected {
			continue
		}
		b = files[r.Path]
		if strings.HasSuffix(r.Path, ".go") {
			b, e = rewrite(b, m.New, m.Old)
			if e != nil {
				return e
			}
		}
		// A valid candidate manifest alone does not prove that projecting back
		// preserves production. Check every file before the first checkout write.
		if !bytes.Equal(b, original[r.Path]) {
			return fmt.Errorf("projected production bytes differ from pinned source: %s", r.Path)
		}
		next[r.Path] = b
	}
	ab, e := os.ReadFile(adapters)
	if e != nil {
		return e
	}
	mb, e := os.ReadFile(filepath.Join(root, manifestName))
	if e != nil {
		return e
	}
	result := Projection{1, commit, digest(mb), digest(ab), []FileRecord{}, []FileRecord{}}
	for _, r := range m.Files {
		if b, ok := next[r.Path]; ok {
			if e = write(filepath.Join(checkout, pkg, r.Path), b, fs.FileMode(r.Mode)); e != nil {
				return e
			}
			result.Files = append(result.Files, FileRecord{r.Path, r.Before, digest(b), r.Mode, false})
		}
	}
	after, _, e := tree(checkout)
	if e != nil {
		return e
	}
	for p, b := range after {
		if protected(p) {
			before, ok := prot[p]
			if !ok || before != digest(b) {
				return errors.New("protected checkout files changed")
			}
		}
	}
	for _, p := range keys(prot) {
		b, ok := after[p]
		if !ok || digest(b) != prot[p] {
			return errors.New("protected checkout file missing or changed")
		}
		result.Protected = append(result.Protected, FileRecord{Path: p, Before: prot[p], After: digest(b), Protected: true})
	}
	return jsonWrite(report, result)
}
func main() {
	if e := cli(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func cli(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sh1 generate|project|inventory|mutate|consumer [flags]")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	f.SetOutput(io.Discard)
	var source, pkg, old, new, out, mod, checkout, commit, adapters, report, events, guards, mutations, version, proxy string
	str := func(p *string, n string) { f.StringVar(p, n, "", n) }
	switch args[0] {
	case "generate":
		str(&source, "source")
		str(&pkg, "package")
		str(&old, "old")
		str(&new, "new")
		str(&out, "out")
		str(&mod, "mod")
	case "project":
		str(&source, "candidate")
		str(&checkout, "checkout")
		str(&pkg, "package")
		str(&commit, "commit")
		str(&adapters, "adapters")
		str(&report, "report")
	case "inventory":
		str(&source, "source")
		str(&events, "events")
		str(&guards, "guards")
		str(&report, "report")
	case "mutate":
		str(&checkout, "checkout")
		str(&mutations, "mutations")
		str(&report, "report")
	case "consumer":
		str(&source, "candidate")
		str(&checkout, "checkout")
		str(&version, "version")
		str(&proxy, "proxy")
		str(&report, "report")
	default:
		return errors.New("unknown command")
	}
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	missing := false
	f.VisitAll(func(x *flag.Flag) {
		if x.Value.String() == "" {
			missing = true
		}
	})
	if missing {
		return errors.New("all command flags are required")
	}
	if args[0] != "mutate" && len(f.Args()) != 0 {
		return errors.New("unexpected positional arguments")
	}
	switch args[0] {
	case "generate":
		return generate(source, pkg, old, new, out, mod)
	case "project":
		return project(source, checkout, pkg, commit, adapters, report)
	case "inventory":
		return inventory(source, events, guards, report)
	case "mutate":
		return mutate(checkout, mutations, report, f.Args())
	case "consumer":
		return consumer(source, checkout, version, proxy, report)
	}
	return nil
}
