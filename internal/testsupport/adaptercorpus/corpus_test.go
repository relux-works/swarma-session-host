package adaptercorpus_test

import (
	"bytes"
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
