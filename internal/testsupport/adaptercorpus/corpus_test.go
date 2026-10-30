package adaptercorpus_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/relux-works/swarma-session-host/internal/testsupport/adaptercorpus"
)

// This checker uses independent level tables and pair enumeration. It does not
// obtain expected pair membership from the generator's construction.
func pairCoverage(rows []adaptercorpus.Case, array string, levels []int) error {
	seen := map[[4]int]bool{}
	for _, row := range rows {
		if row.Array != array {
			continue
		}
		v := row.Vector()
		for i := range levels {
			for j := i + 1; j < len(levels); j++ {
				seen[[4]int{i, j, v[i], v[j]}] = true
			}
		}
	}
	for i := range levels {
		for j := i + 1; j < len(levels); j++ {
			for a := 0; a < levels[i]; a++ {
				for b := 0; b < levels[j]; b++ {
					if !seen[[4]int{i, j, a, b}] {
						return fmt.Errorf("missing pair %s %d:%d %d:%d", array, i, a, j, b)
					}
				}
			}
		}
	}
	return nil
}

// The finite corpus has exactly the accepted 50/71 rows, legal unique vectors,
// stable bytes and complete strength-2 assignment coverage, not higher-order coverage.
func TestOfflineCorpus(t *testing.T) {
	rows := adaptercorpus.Generate()
	if len(rows) != 121 {
		t.Fatalf("OFFLINE_CORPUS_COUNT: %d", len(rows))
	}
	seen := map[string]bool{}
	counts := map[string]int{}
	tables := map[string][]int{"adapter": {3, 3, 3, 2, 4}, "host": {3, 3, 2, 2, 4, 4}}
	for _, row := range rows {
		if seen[row.ID] {
			t.Fatal("OFFLINE_CORPUS_ID")
		}
		seen[row.ID] = true
		levels, ok := tables[row.Array]
		if !ok {
			t.Fatal("OFFLINE_CORPUS_ARRAY")
		}
		v := row.Vector()
		nonzero := 0
		for i, k := range levels {
			if v[i] < 0 || v[i] >= k {
				t.Fatal("OFFLINE_CORPUS_LEVEL")
			}
			if v[i] != 0 {
				nonzero++
			}
		}
		if nonzero > 2 {
			t.Fatal("OFFLINE_CORPUS_SELECTION")
		}
		counts[row.Array]++
	}
	if counts["adapter"] != 50 || counts["host"] != 71 {
		t.Fatal("OFFLINE_CORPUS_COUNT", counts)
	}
	for array, levels := range tables {
		if err := pairCoverage(rows, array, levels); err != nil {
			t.Fatalf("OFFLINE_PAIR_COVERAGE: %v", err)
		}
	}
	a, _ := json.Marshal(rows)
	b, _ := json.Marshal(adaptercorpus.Generate())
	if !bytes.Equal(a, b) {
		t.Fatal("OFFLINE_CORPUS_STABILITY")
	}
}

// A known missing C1/O1 pair is rejected by the pair checker itself, independent
// of row count and identity checks. Disabling that rejection must fail this test.
func TestOfflinePairCheckerNegative(t *testing.T) {
	rows := adaptercorpus.Generate()
	var incomplete []adaptercorpus.Case
	for _, c := range rows {
		if c.Array == "adapter" && c.C == 1 && c.O == 1 && c.V == 0 && c.E == 0 && c.K == 0 {
			continue
		}
		incomplete = append(incomplete, c)
	}
	if err := pairCoverage(incomplete, "adapter", []int{3, 3, 3, 2, 4}); err == nil || !strings.Contains(err.Error(), "0:1 1:1") {
		t.Fatalf("OFFLINE_PAIR_COVERAGE: known C1/O1 violation not detected: %v", err)
	}
	if err := pairCoverage(rows, "adapter", []int{3, 3, 3, 2, 4}); err != nil {
		t.Fatal("OFFLINE_PAIR_POSITIVE", err)
	}
}

// Actual non-test import lists for every package must exclude the helper. A
// compiled blank import planted in any production package is a real violation.
func TestOfflineImportBoundary(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
	cmd := exec.Command("go", "list", "-json", "./...")
	cmd.Dir = root
	data, err := cmd.Output()
	if err != nil {
		t.Fatal("OFFLINE_IMPORT_SETUP", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	checked := 0
	for decoder.More() {
		var p struct {
			ImportPath string
			Imports    []string
		}
		if err := decoder.Decode(&p); err != nil {
			t.Fatal(err)
		}
		checked++
		for _, imp := range p.Imports {
			if imp == "github.com/relux-works/swarma-session-host/internal/testsupport/adaptercorpus" {
				t.Fatalf("OFFLINE_IMPORT_BOUNDARY: %s imports %s", p.ImportPath, imp)
			}
		}
	}
	if checked == 0 {
		t.Fatal("OFFLINE_IMPORT_SETUP: empty build list")
	}
	t.Logf("checked %d actual non-test package import lists", checked)
}

// Plain, Unicode-escaped and JSON-in-string output must receive the exact
// incoming-output verdict, not a catalogue/type error. The clean neighbour and
// explicit size/depth controls keep a reject-everything oracle from passing.
func TestOfflineMetadataStringOracle(t *testing.T) {
	var escaped strings.Builder
	for _, r := range adaptercorpus.MetadataSentinel {
		fmt.Fprintf(&escaped, "\\u%04x", r)
	}
	plain, _ := json.Marshal(map[string]string{"plan_digest": adaptercorpus.MetadataSentinel})
	unicode := []byte("{\"plan_digest\":\"" + escaped.String() + "\"}")
	nested, _ := json.Marshal(map[string]string{"plan_digest": "\"" + escaped.String() + "\""})
	var outer map[string]string
	if json.Unmarshal(nested, &outer) != nil || bytes.Contains(nested, []byte(adaptercorpus.MetadataSentinel)) || strings.Contains(outer["plan_digest"], adaptercorpus.MetadataSentinel) {
		t.Fatal("OFFLINE_METADATA_FIXTURE: nested witness is not doubly encoded")
	}
	var inner string
	if json.Unmarshal([]byte(outer["plan_digest"]), &inner) != nil || inner != adaptercorpus.MetadataSentinel {
		t.Fatal("OFFLINE_METADATA_FIXTURE: inner witness differs")
	}
	for name, data := range map[string][]byte{"plain": plain, "unicode": unicode, "json-in-string": nested} {
		t.Run(name, func(t *testing.T) {
			err := adaptercorpus.ScanMetadata(data)
			if err == nil || err.Error() != "OFFLINE_METADATA_ONLY: incoming output" {
				t.Fatalf("OFFLINE_METADATA_ONLY: oracle did not identify %s output: %v", name, err)
			}
		})
	}
	if err := adaptercorpus.ScanMetadata([]byte("{\"plan_digest\":\"fixture-public-metadata\"}")); err != nil {
		t.Fatal("OFFLINE_METADATA_POSITIVE", err)
	}
	bounds := []struct {
		name string
		data []byte
		want string
	}{
		{"size", bytes.Repeat([]byte("x"), 65537), "document_bound"},
		{"depth", []byte(strings.Repeat("[", 18) + "0" + strings.Repeat("]", 18)), "inspection_bound"},
		{"nodes", []byte("[" + strings.Repeat("0,", 4100) + "0]"), "inspection_bound"},
	}
	innerBytes, _ := json.Marshal(strings.Repeat("x", 35000))
	middleBytes, _ := json.Marshal(string(innerBytes))
	outerBytes, _ := json.Marshal(string(middleBytes))
	bounds = append(bounds, struct {
		name string
		data []byte
		want string
	}{"decoded-bytes", outerBytes, "decoded_string_bound"})
	for _, tc := range bounds {
		t.Run(tc.name, func(t *testing.T) {
			err := adaptercorpus.ScanMetadata(tc.data)
			if err == nil || err.Error() != "OFFLINE_METADATA_ONLY: "+tc.want {
				t.Fatalf("OFFLINE_METADATA_ONLY: missing finite %s bound: %v", tc.name, err)
			}
		})
	}
}

// These fixed catalogues use no production serializer or restore validator.
// They admit clean metadata and the three explicitly selected failed-restore
// fixtures, but reject extra members and foreign values even without a sentinel.
func TestOfflineMetadataCatalogues(t *testing.T) {
	values := adaptercorpus.MetadataValues{Model: "fixture-model", Provider: "fixture-provider", Effort: "fixture-effort", Workspace: "/fixture", Thread: "fixture-thread", RunnerInstance: strings.Repeat("1", 32), Turns: []string{"fixture-turn"}, Inputs: []string{"one", "two"}}
	config, _ := json.Marshal(struct{ Model, Provider, Effort, Workspace string }{values.Model, values.Provider, values.Effort, values.Workspace})
	sum := sha256.Sum256(config)
	checkpoint := map[string]any{"version": 1, "input_id": "", "accepted": "", "completed": "", "begun": true, "stream": map[string]any{"version": 1, "offset": 0, "closed": false, "protocol": map[string]any{"plan_digest": hex.EncodeToString(sum[:]), "phase": "ready", "request": 2, "pending": 0, "operation": "", "view": map[string]any{"State": "idle", "Thread": values.Thread, "Turn": "", "CompletedTurn": "", "AuthorityRequired": false}}}}
	journal := map[string]any{"version": 1, "execution": "offline-execution", "runner_instance": values.RunnerInstance, "adapter": "codex", "selection": map[string]any{"vendor": values.Provider, "model": values.Model, "reasoning": values.Effort}, "epoch": 1, "sequence": 0, "fence": "held", "records": []any{}, "stop_requested": false, "input_state": "idle", "security_stopped": false, "protocol": map[string]any{"boundary_version": 1, "observed_release": "0.164.0", "checkpoint": checkpoint, "wire_sequence": 3, "pending": nil, "view": map[string]any{"InputID": "", "Activity": "idle", "NativeSession": values.Thread, "CurrentTurn": ""}}}
	encode := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	check := func(k int, accepted bool) {
		t.Helper()
		for name, err := range map[string]error{"checkpoint": adaptercorpus.CheckpointMetadata(encode(checkpoint), values, k), "journal": adaptercorpus.JournalMetadata(encode(journal), values, k)} {
			if accepted && err != nil {
				t.Fatalf("OFFLINE_METADATA_POSITIVE: %s k%d: %v", name, k, err)
			}
			if !accepted && (err == nil || !strings.HasPrefix(err.Error(), "OFFLINE_METADATA_ONLY: catalogue")) {
				t.Fatalf("OFFLINE_METADATA_ONLY: %s catalogue accepted invalid fixture: %v", name, err)
			}
		}
	}
	check(0, true)
	// Both full-document entry points must identify the encoded payload itself;
	// a later catalogue rejection cannot masquerade as decoding the inner JSON.
	protocol := checkpoint["stream"].(map[string]any)["protocol"].(map[string]any)
	originalPlan := protocol["plan_digest"]
	var escaped strings.Builder
	for _, r := range adaptercorpus.MetadataSentinel {
		fmt.Fprintf(&escaped, "\\u%04x", r)
	}
	for _, form := range []string{"plain", "unicode", "json-in-string"} {
		protocol["plan_digest"] = adaptercorpus.MetadataSentinel
		if form == "json-in-string" {
			protocol["plan_digest"] = "\"" + escaped.String() + "\""
		}
		cp, state := encode(checkpoint), encode(journal)
		if form == "unicode" {
			cp = bytes.ReplaceAll(cp, []byte(adaptercorpus.MetadataSentinel), []byte(escaped.String()))
			state = bytes.ReplaceAll(state, []byte(adaptercorpus.MetadataSentinel), []byte(escaped.String()))
		}
		for name, err := range map[string]error{"checkpoint": adaptercorpus.CheckpointMetadata(cp, values, 0), "journal": adaptercorpus.JournalMetadata(state, values, 0)} {
			if err == nil || err.Error() != "OFFLINE_METADATA_ONLY: incoming output" {
				t.Fatalf("OFFLINE_METADATA_ONLY: %s %s output was not decoded: %v", name, form, err)
			}
		}
	}
	protocol["plan_digest"] = originalPlan
	check(0, true)
	checkpoint["output"] = "foreign-public-value"
	check(0, false)
	delete(checkpoint, "output")
	checkpoint["input_id"] = "foreign-public-id"
	check(0, false)
	checkpoint["input_id"] = ""
	for k := 1; k <= 3; k++ {
		stream := checkpoint["stream"].(map[string]any)
		protocol := stream["protocol"].(map[string]any)
		switch k {
		case 1:
			checkpoint["version"] = 99
		case 2:
			stream["closed"] = nil
		case 3:
			protocol["pending"] = 3
		}
		check(k, true)
		check(0, false)
		checkpoint["version"], stream["closed"], protocol["pending"] = 1, false, 0
	}
	for _, field := range []string{"extra", "execution"} {
		journal[field] = "foreign-public-value"
		err := adaptercorpus.JournalMetadata(encode(journal), values, 0)
		if err == nil || !strings.HasPrefix(err.Error(), "OFFLINE_METADATA_ONLY: catalogue") {
			t.Fatalf("OFFLINE_METADATA_ONLY: journal %s accepted: %v", field, err)
		}
		delete(journal, "extra")
		journal["execution"] = "offline-execution"
	}
	check(0, true)
}
