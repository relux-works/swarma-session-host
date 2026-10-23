package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var tool string

func TestMain(m *testing.M) {
	dir, e := os.MkdirTemp("", "sh1-tool-")
	if e != nil {
		panic(e)
	}
	tool = filepath.Join(dir, "sh1")
	b, code, e := run(".", nil, "go", "build", "-o", tool, ".")
	if e != nil || code != 0 {
		panic(string(b) + "CLI build failed")
	}
	code = m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
func command(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	b, code, e := run(dir, nil, args...)
	t.Logf("command %v exit %d", args, code)
	if e != nil || code != 0 {
		t.Fatalf("command %v exit %d: %v\n%s", args, code, e, b)
	}
	return b
}
func call(t *testing.T, args ...string) {
	t.Helper()
	command(t, ".", append([]string{tool}, args...)...)
}
func reject(t *testing.T, args ...string) {
	t.Helper()
	_, code, e := run(".", nil, append([]string{tool}, args...)...)
	t.Logf("expected rejection %s exit %d", args[0], code)
	if e != nil || code != 1 {
		t.Fatalf("expected CLI rejection exit 1, got %d, %v", code, e)
	}
}
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	files, modes, e := tree(src)
	if e != nil {
		t.Fatal(e)
	}
	for _, p := range keys(files) {
		if e = write(filepath.Join(dst, p), files[p], modes[p]); e != nil {
			t.Fatal(e)
		}
	}
}
func same(t *testing.T, a, b string) {
	t.Helper()
	fa, ma, e := tree(a)
	if e != nil {
		t.Fatal(e)
	}
	fb, mb, e := tree(b)
	if e != nil {
		t.Fatal(e)
	}
	ja, _ := json.Marshal(fa)
	jb, _ := json.Marshal(fb)
	if !bytes.Equal(ja, jb) {
		t.Fatal("tree bytes differ")
	}
	ja, _ = json.Marshal(ma)
	jb, _ = json.Marshal(mb)
	if !bytes.Equal(ja, jb) {
		t.Fatal("tree modes differ")
	}
}
func save(t *testing.T, p string, b []byte) {
	t.Helper()
	if e := write(p, b, 0644); e != nil {
		t.Fatal(e)
	}
}
func fixture(t *testing.T) string {
	t.Helper()
	src := filepath.Join(t.TempDir(), "source")
	copyTree(t, "../../testdata/sh1-fixture", src)
	return src
}
func generateArgs(src, out, mod string) []string {
	return []string{"generate", "-source", src, "-package", "internal/session", "-old", "example.org/sh1-fixture/internal/session", "-new", "example.org/sh1-host", "-out", out, "-mod", mod}
}
func candidateFixture(t *testing.T) (string, string, string) {
	t.Helper()
	src := fixture(t)
	out := filepath.Join(t.TempDir(), "candidate")
	mod := filepath.Join(t.TempDir(), "go.mod")
	save(t, mod, []byte("module example.org/sh1-host\n\ngo 1.25.5\n"))
	call(t, generateArgs(src, out, mod)...)
	return src, out, mod
}
func TestFixtureEndToEnd(t *testing.T) {
	src, cand, mod := candidateFixture(t)
	cand2 := filepath.Join(t.TempDir(), "candidate")
	call(t, generateArgs(src, cand2, mod)...)
	same(t, cand, cand2)
	command(t, cand, "go", "vet", "./...")
	command(t, cand, "go", "test", "-race", "-count=1", "./...")
	command(t, src, "git", "init", "-q")
	command(t, src, "git", "add", ".")
	command(t, src, "git", "-c", "user.name=SH1 Fixture", "-c", "user.email=fixture@example.org", "commit", "-qm", "Invented fixture")
	commit := strings.TrimSpace(string(command(t, src, "git", "rev-parse", "HEAD")))
	reports := filepath.Join(t.TempDir(), "reports")
	proj := filepath.Join(reports, "project.json")
	projectArgs := []string{"project", "-candidate", cand, "-checkout", src, "-package", "internal/session", "-commit", commit, "-adapters", filepath.Join(src, "adapters.json"), "-report", proj}
	call(t, projectArgs...)
	proj2 := filepath.Join(t.TempDir(), "project.json")
	projectArgs[len(projectArgs)-1] = proj2
	call(t, projectArgs...)
	assertFileEqual(t, proj, proj2)
	events := command(t, src, "go", "test", "-json", "-count=1", "./...")
	eventFile := filepath.Join(reports, "tests.jsonl")
	save(t, eventFile, events)
	inv := filepath.Join(reports, "inventory.json")
	invArgs := []string{"inventory", "-source", src, "-events", eventFile, "-guards", filepath.Join(src, "guards.json"), "-report", inv}
	call(t, invArgs...)
	inv2 := filepath.Join(t.TempDir(), "inventory.json")
	invArgs[len(invArgs)-1] = inv2
	call(t, invArgs...)
	assertFileEqual(t, inv, inv2)
	var inventoryReport Inventory
	if e := readJSON(inv, &inventoryReport); e != nil {
		t.Fatal(e)
	}
	if inventoryReport.Ratio != "1/1" {
		t.Fatal(inventoryReport.Ratio)
	}
	foundSkip, foundTag := false, false
	for _, p := range inventoryReport.Packages {
		for _, r := range p.Tests {
			if r.Name == "TestOptIn" && r.Status == "skip" && r.Reason != "" {
				foundSkip = true
			}
			if r.Name == "TestTagged" && r.Tags == "sh1extra" && r.Status == "not_observed" {
				foundTag = true
			}
		}
	}
	if !foundSkip || !foundTag {
		t.Fatal("missing skipped/tagged inventory")
	}
	// The public fixture also exercises the optional lanes explicitly.
	enabled, code, e := run(src, []string{"SH1_FIXTURE_OPT_IN=1"}, "go", "test", "-json", "-count=1", "-tags=sh1extra", "./...")
	t.Logf("opt-in go test -json -count=1 -tags=sh1extra ./... exit %d", code)
	if e != nil || code != 0 {
		t.Fatalf("opt-in command exit %d: %v", code, e)
	}
	save(t, filepath.Join(reports, "enabled.jsonl"), enabled)
	call(t, "inventory", "-source", src, "-events", filepath.Join(reports, "enabled.jsonl"), "-guards", filepath.Join(src, "guards.json"), "-report", filepath.Join(reports, "enabled-inventory.json"))
	mutationReport := filepath.Join(reports, "mutations.json")
	mutArgs := []string{"mutate", "-checkout", src, "-mutations", filepath.Join(src, "mutations.json"), "-report", mutationReport, "--", "go", "test", "-json", "-count=1", "./..."}
	call(t, mutArgs...)
	mut2 := filepath.Join(t.TempDir(), "mutations.json")
	mutArgs[6] = mut2
	call(t, mutArgs...)
	assertFileEqual(t, mutationReport, mut2)
	var mr MutationReport
	if e = readJSON(mutationReport, &mr); e != nil {
		t.Fatal(e)
	}
	if mr.Ratio != "3/3" {
		t.Fatal(mr.Ratio)
	}
	for _, r := range mr.Results {
		if r.Exit == 0 || r.Status != "killed" {
			t.Fatal("mutation not killed", r)
		}
	}
	consumerBase := filepath.Join(t.TempDir(), "consumer-base")
	copyTree(t, src, consumerBase)
	// The extracted package is absent from the consumer, as at SH1 dependency exit.
	if e = os.RemoveAll(filepath.Join(consumerBase, "internal")); e != nil {
		t.Fatal(e)
	}
	save(t, filepath.Join(consumerBase, "cmd/demo/main_test.go"), []byte("package main\nimport (\"testing\"; \"example.org/sh1-fixture/internal/session/child\")\nfunc TestConsumer(t *testing.T) { if !child.Allowed() { t.Fatal(\"consumer admission\") } }\n"))
	proxy := filepath.Join(t.TempDir(), "proxy")
	c1 := filepath.Join(t.TempDir(), "consumer")
	c2 := filepath.Join(t.TempDir(), "consumer")
	copyTree(t, consumerBase, c1)
	copyTree(t, consumerBase, c2)
	version := "v0.0.0-20261010000000-0123456789ab"
	consumerReport := filepath.Join(reports, "consumer.json")
	cArgs := []string{"consumer", "-candidate", cand, "-checkout", c1, "-version", version, "-proxy", proxy, "-report", consumerReport}
	call(t, cArgs...)
	cr2 := filepath.Join(t.TempDir(), "consumer.json")
	cArgs[4] = c2
	cArgs[10] = cr2
	call(t, cArgs...)
	assertFileEqual(t, consumerReport, cr2)
	same(t, c1, c2)
	proxyAbs, _ := filepath.Abs(proxy)
	env := []string{"GOPROXY=file://" + filepath.ToSlash(proxyAbs), "GOSUMDB=off", "GONOPROXY=none", "GOWORK=off"}
	for _, args := range [][]string{{"go", "test", "-race", "-count=1", "./..."}, {"go", "build", "./..."}, {"go", "run", "./cmd/demo"}} {
		b, code, e := run(c1, env, args...)
		t.Logf("consumer command %v exit %d", args, code)
		if e != nil || code != 0 {
			t.Fatalf("consumer %v exit %d: %v %s", args, code, e, b)
		}
		if args[1] == "run" && string(b) != "true\n" {
			t.Fatal(string(b))
		}
	}
	if b, e := os.ReadFile(filepath.Join(c1, "go.mod")); e != nil || strings.Contains(string(b), "replace") || !strings.Contains(string(b), "require example.org/sh1-host "+version) {
		t.Fatal("consumer is not proxy-backed")
	}
	if artifacts := os.Getenv("SH1_ARTIFACT_DIR"); artifacts != "" {
		copyTree(t, reports, artifacts)
		copyTree(t, proxy, filepath.Join(artifacts, "proxy"))
		copyTree(t, cand, filepath.Join(artifacts, "candidate"))
	}
}
func assertFileEqual(t *testing.T, a, b string) {
	t.Helper()
	x, e := os.ReadFile(a)
	if e != nil {
		t.Fatal(e)
	}
	y, e := os.ReadFile(b)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(x, y) {
		t.Fatal("reports not deterministic", filepath.Base(a))
	}
}
func TestGenerateRefusals(t *testing.T) {
	src, out, mod := candidateFixture(t)
	reject(t, generateArgs(src, out, mod)...)
	save(t, mod, []byte("module example.org/wrong\n\ngo 1.25.5\n"))
	reject(t, generateArgs(src, filepath.Join(t.TempDir(), "out"), mod)...)
	save(t, mod, []byte("module example.org/sh1-host\n\ngo 1.25.5\n"))
	if e := os.Symlink("session.go", filepath.Join(src, "internal/session/link.go")); e != nil {
		t.Fatal(e)
	}
	reject(t, generateArgs(src, filepath.Join(t.TempDir(), "out"), mod)...)
}
func TestProjectRefusals(t *testing.T) {
	src, cand, _ := candidateFixture(t)
	command(t, src, "git", "init", "-q")
	command(t, src, "git", "add", ".")
	command(t, src, "git", "-c", "user.name=SH1 Fixture", "-c", "user.email=fixture@example.org", "commit", "-qm", "Fixture")
	oid := strings.TrimSpace(string(command(t, src, "git", "rev-parse", "HEAD")))
	args := []string{"project", "-candidate", cand, "-checkout", src, "-package", "internal/session", "-commit", strings.Repeat("0", 40), "-adapters", filepath.Join(src, "adapters.json"), "-report", filepath.Join(t.TempDir(), "report")}
	reject(t, args...)
	args[8] = oid
	save(t, filepath.Join(src, "internal/session/session_test.go"), []byte("package session\n"))
	reject(t, args...)
	copyTree(t, "../../testdata/sh1-fixture", src)
	save(t, filepath.Join(cand, "session.go"), []byte("package session\n"))
	reject(t, args...)
}
func TestInventoryRejectsMissingMalformedAndForgedCoverage(t *testing.T) {
	src := fixture(t)
	dir := t.TempDir()
	events := filepath.Join(dir, "events")
	guards := filepath.Join(src, "guards.json")
	report := filepath.Join(dir, "report")
	args := []string{"inventory", "-source", src, "-events", events, "-guards", guards, "-report", report}
	reject(t, args...)
	save(t, events, []byte("{bad"))
	reject(t, args...)
	pack := "example.org/sh1-fixture/internal/session"
	encode := func(ev []Event) []byte {
		var b bytes.Buffer
		for _, x := range ev {
			v, _ := json.Marshal(x)
			b.Write(v)
			b.WriteByte('\n')
		}
		return b.Bytes()
	}
	save(t, events, encode([]Event{{Action: "run", Package: pack, Test: "TestSourceGuard"}}))
	reject(t, args...)
	save(t, events, encode([]Event{{Action: "run", Package: pack, Test: "TestSourceGuard"}, {Action: "pass", Package: pack, Test: "TestSourceGuard"}, {Action: "pass", Package: pack}}))
	reject(t, args...)
	// A marker from another passing test must not attest this guard.
	save(t, events, encode([]Event{{Action: "run", Package: pack, Test: "TestAdmission"}, {Action: "output", Package: pack, Test: "TestAdmission", Output: "SH1_GUARD session.go\n"}, {Action: "pass", Package: pack, Test: "TestAdmission"}, {Action: "pass", Package: pack}}))
	reject(t, args...)
}
func TestMutationRefusalsAndSurvivor(t *testing.T) {
	src := fixture(t)
	dir := t.TempDir()
	decl := filepath.Join(dir, "mutations.json")
	report := filepath.Join(dir, "report.json")
	list := MutationList{Version: 1, Mutations: []Mutation{{"survivor", "internal/session/session.go", "// Accept keeps both admitted identities behind the same gate.", "// A harmless comment change.", "example.org/sh1-fixture/internal/session", "TestAdmission"}}}
	if e := jsonWrite(decl, list); e != nil {
		t.Fatal(e)
	}
	args := []string{"mutate", "-checkout", src, "-mutations", decl, "-report", report, "--", "go", "test", "-json", "-count=1", "./..."}
	reject(t, args...)
	var r MutationReport
	if e := readJSON(report, &r); e != nil {
		t.Fatal(e)
	}
	if r.Ratio != "0/1" || r.Results[0].Exit != 0 || r.Results[0].Status != "survived" {
		t.Fatal(r)
	}
	list.Mutations[0].Anchor = "missing"
	if e := jsonWrite(decl, list); e != nil {
		t.Fatal(e)
	}
	reject(t, args...)
	list.Mutations[0].Anchor = "identity == \"member\""
	list.Mutations[0].Replacement = "invalid !!!"
	if e := jsonWrite(decl, list); e != nil {
		t.Fatal(e)
	}
	reject(t, args...)
	if e := readJSON(report, &r); e != nil {
		t.Fatal(e)
	}
	if r.Results[0].Status == "killed" {
		t.Fatal("compile failure counted as kill")
	}
}
func TestConsumerRefusals(t *testing.T) {
	src, cand, _ := candidateFixture(t)
	dir := t.TempDir()
	args := []string{"consumer", "-candidate", cand, "-checkout", src, "-version", "latest", "-proxy", filepath.Join(dir, "proxy"), "-report", filepath.Join(dir, "report")}
	reject(t, args...)
	args[6] = "v0.0.0-20261010000000-0123456789ab"
	save(t, filepath.Join(src, "go.mod"), []byte("module example.org/sh1-fixture\n\ngo 1.25.5\nreplace example.org/sh1-host => ../candidate\n"))
	reject(t, args...)
}
func TestRewriteImportsOnly(t *testing.T) {
	b := []byte("package p\n// example.org/old\nimport alias `example.org/old/sub`\nvar text = \"example.org/old\"\n")
	got, e := rewrite(b, "example.org/old", "example.org/new")
	if e != nil {
		t.Fatal(e)
	}
	expected := []byte("package p\n// example.org/old\nimport alias \"example.org/new/sub\"\nvar text = \"example.org/old\"\n")
	if !bytes.Equal(got, expected) {
		t.Fatal(string(got))
	}
}

func TestAdapterAndManifestRefusals(t *testing.T) {
	src, cand, _ := candidateFixture(t)
	command(t, src, "git", "init", "-q")
	command(t, src, "git", "add", ".")
	command(t, src, "git", "-c", "user.name=SH1 Fixture", "-c", "user.email=fixture@example.org", "commit", "-qm", "Fixture")
	oid := strings.TrimSpace(string(command(t, src, "git", "rev-parse", "HEAD")))
	adapters := filepath.Join(t.TempDir(), "adapters.json")
	args := []string{"project", "-candidate", cand, "-checkout", src, "-package", "internal/session", "-commit", oid, "-adapters", adapters, "-report", filepath.Join(t.TempDir(), "report")}
	for _, data := range []string{`{"version":2,"entries":[]}`, `{"version":1}`, `{"version":1,"entries":[{"patch":"implicit"}]}`, `{"version":1,"entries":[],"unknown":true}`} {
		save(t, adapters, []byte(data))
		reject(t, args...)
	}
	save(t, adapters, []byte(`{"version":1,"entries":[]}`))
	manifestPath := filepath.Join(cand, manifestName)
	original, e := os.ReadFile(manifestPath)
	if e != nil {
		t.Fatal(e)
	}
	var m Manifest
	if e = readJSON(manifestPath, &m); e != nil {
		t.Fatal(e)
	}
	m.Files[0].Path = "../outside"
	if e = jsonWrite(manifestPath, m); e != nil {
		t.Fatal(e)
	}
	reject(t, args...)
	save(t, manifestPath, original)
	save(t, filepath.Join(cand, "extra.txt"), []byte("undeclared"))
	reject(t, args...)
}
func TestMutationOtherFailureDoesNotKillNamedTest(t *testing.T) {
	src := fixture(t)
	decl := filepath.Join(t.TempDir(), "mutations.json")
	report := filepath.Join(t.TempDir(), "report.json")
	list := MutationList{Version: 1, Mutations: []Mutation{{"wrong-test", "internal/session/session.go", `identity == "member" || identity == "operator"`, `true`, "example.org/sh1-fixture/internal/session", "TestGolden"}}}
	if e := jsonWrite(decl, list); e != nil {
		t.Fatal(e)
	}
	reject(t, "mutate", "-checkout", src, "-mutations", decl, "-report", report, "--", "go", "test", "-json", "-count=1", "./...")
	var r MutationReport
	if e := readJSON(report, &r); e != nil {
		t.Fatal(e)
	}
	if r.Results[0].Exit == 0 || r.Results[0].Status != "survived" {
		t.Fatal("unrelated failure counted as named kill", r)
	}
}
func TestConsumerImmutableProxyCollision(t *testing.T) {
	src, cand, _ := candidateFixture(t)
	if e := os.RemoveAll(filepath.Join(src, "internal")); e != nil {
		t.Fatal(e)
	}
	proxy := filepath.Join(t.TempDir(), "proxy")
	version := "v0.0.0-20261010000000-0123456789ab"
	save(t, filepath.Join(proxy, "example.org/sh1-host/@v/"+version+".zip"), []byte("conflicting bytes"))
	reject(t, "consumer", "-candidate", cand, "-checkout", src, "-version", version, "-proxy", proxy, "-report", filepath.Join(t.TempDir(), "report"))
	b, e := os.ReadFile(filepath.Join(proxy, "example.org/sh1-host/@v/"+version+".zip"))
	if e != nil || string(b) != "conflicting bytes" {
		t.Fatal("immutable proxy overwritten")
	}
}

func TestGenerateIdentityAndModuleRefusals(t *testing.T) {
	src := fixture(t)
	mod := filepath.Join(t.TempDir(), "go.mod")
	for _, data := range []string{
		"module example.org/sh1-host\n\ngo 1.25.5\nreplace example.org/dependency => ../local\n",
		"module example.org/sh1-host\n\ngo 1.24.0\n",
		"module example.org/sh1-host\n",
	} {
		save(t, mod, []byte(data))
		reject(t, generateArgs(src, filepath.Join(t.TempDir(), "out"), mod)...)
	}
	save(t, mod, []byte("module example.org/sh1-host\n\ngo 1.25.5\n"))
	args := generateArgs(src, filepath.Join(t.TempDir(), "out"), mod)
	args[6] = "example.org/incorrect/internal/session"
	reject(t, args...)
	args = generateArgs(src, filepath.Join(t.TempDir(), "out"), mod)
	args[4] = "../session"
	reject(t, args...)
}

// Explicit source imports generate a candidate; missing or empty imports refuse before output writes.
func TestGenerateRequiresExplicitSourceImport(t *testing.T) {
	src := fixture(t)
	mod := filepath.Join(t.TempDir(), "go.mod")
	save(t, mod, []byte("module example.org/sh1-host\n\ngo 1.25.5\n"))
	for _, missing := range []bool{true, false} {
		out := filepath.Join(t.TempDir(), "candidate")
		args := generateArgs(src, out, mod)
		for i, arg := range args {
			if arg == "-old" {
				if missing {
					args = append(args[:i], args[i+2:]...)
				} else {
					args[i+1] = ""
				}
				break
			}
		}
		output, e := exec.Command(tool, args...).CombinedOutput()
		exit, ok := e.(*exec.ExitError)
		if !ok || exit.ExitCode() != 1 || !strings.Contains(string(output), "all command flags are required") {
			t.Fatalf("missing source import: error %v, output %s", e, output)
		}
		if _, e := os.Stat(out); !os.IsNotExist(e) {
			t.Fatalf("refused generation wrote candidate: %v", e)
		}
	}
	out := filepath.Join(t.TempDir(), "candidate")
	call(t, generateArgs(src, out, mod)...)
	var manifest Manifest
	if e := readJSON(filepath.Join(out, manifestName), &manifest); e != nil {
		t.Fatal(e)
	}
	if manifest.Old != "example.org/sh1-fixture/internal/session" {
		t.Fatalf("source identity not retained: %s", manifest.Old)
	}
}
