package testsupport

import (
	"os"
	"path/filepath"
	"testing"
)

// TestKeepRepeatRetainsPriorLeaf runs two same-name test lifecycles in-process
// with the switch on, which is what -count>1 does to the helper. The second
// lifecycle must get its own unique leaf: the first lifecycle's files stay on
// disk with their original bytes, and the two paths differ. Keep mode never
// deletes, so retention — not clearing — is what isolates the repeat.
func TestKeepRepeatRetainsPriorLeaf(t *testing.T) {
	// Retention lasts through the assertions; the parent owns fixture cleanup.
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv(KeepExecutablesEnv, "1")
	var paths []string
	run := 0
	fn := func(child *testing.T) {
		run++
		body := "old"
		if run == 2 {
			body = "new"
		}
		paths = append(paths, WriteExecutable(child, "retained", []byte(body), 0o600))
	}
	for cycle := 1; cycle <= 2; cycle++ {
		if !runNestedProbe(t, "TestKeepRetentionProbe", fn) {
			t.Fatal("nested run failed")
		}
	}
	if len(paths) != 2 {
		t.Fatalf("got %d paths, want 2", len(paths))
	}
	if paths[0] == paths[1] {
		t.Errorf("both lifecycles reused %s; want a unique leaf per run", paths[0])
	}
	if filepath.Dir(paths[0]) == filepath.Dir(paths[1]) {
		t.Errorf("both lifecycles share dir %s; want a unique leaf per run", filepath.Dir(paths[0]))
	}
	data, err := os.ReadFile(paths[0])
	if err != nil {
		t.Errorf("cycle-1 file %s missing after cycle 2: %v; keep mode must never delete", paths[0], err)
	} else if string(data) != "old" {
		t.Errorf("cycle-1 file %s holds %q, want %q", paths[0], data, "old")
	}
	if data, err := os.ReadFile(paths[1]); err != nil || string(data) != "new" {
		t.Errorf("cycle-2 file %s holds %q (%v), want %q", paths[1], data, err, "new")
	}
}

// TestKeepCaseDistinctLeavesArePhysicallyDistinct proves case-distinct tests
// never share a keep directory. Subtests "Case" and "case" sanitize to
// leaves differing only in case, which resolve to one directory on the Mac
// mini's case-insensitive APFS volume. The allocator reserves case-folded
// names, so the second test takes a -N suffix and both files keep their own
// bytes. os.SameFile compares the physical directories, not the strings,
// so the test is meaningful where the volume folds case and passes
// trivially where it does not.
func TestKeepCaseDistinctLeavesArePhysicallyDistinct(t *testing.T) {
	// Retention lasts through the assertions; the parent owns fixture cleanup.
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv(KeepExecutablesEnv, "1")
	paths := map[string]string{}
	for _, name := range []string{"Case", "case"} {
		t.Run(name, func(t *testing.T) {
			paths[name] = WriteExecutable(t, "data", []byte("body-"+name), 0o600)
		})
	}
	if paths["Case"] == paths["case"] {
		t.Fatalf("case-distinct tests share path %s", paths["Case"])
	}
	first, err := os.Stat(filepath.Dir(paths["Case"]))
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.Stat(filepath.Dir(paths["case"]))
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(first, second) {
		t.Errorf("case-distinct tests share directory %s and %s",
			filepath.Dir(paths["Case"]), filepath.Dir(paths["case"]))
	}
	for name, want := range map[string]string{"Case": "body-Case", "case": "body-case"} {
		data, err := os.ReadFile(paths[name])
		if err != nil || string(data) != want {
			t.Errorf("subtest %s file %s holds %q (%v), want %q", name, paths[name], data, err, want)
		}
	}
}

// TestKeepOverwritePreservesInode proves a same-path rewrite truncates in
// place in both modes: the returned path is unchanged (the sessionmanager
// repaint fixture pins that), the inode survives (no unlink-and-recreate),
// and the new bytes land. An open descriptor held across the rewrite still
// refers to the same file.
func TestKeepOverwritePreservesInode(t *testing.T) {
	// Retention lasts through the assertions; the parent owns fixture cleanup.
	t.Setenv("TMPDIR", t.TempDir())
	for _, keep := range []string{"0", "1"} {
		t.Run("keep-"+keep, func(t *testing.T) {
			t.Setenv(KeepExecutablesEnv, keep)
			first := WriteExecutable(t, "rewrite", []byte("old"), 0o600)
			held, err := os.Open(first)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			before, err := held.Stat()
			if err != nil {
				t.Fatal(err)
			}
			second := WriteExecutable(t, "rewrite", []byte("new-body"), 0o600)
			if second != first {
				t.Errorf("rewrite returned %s, want the same path %s", second, first)
			}
			after, err := os.Stat(second)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(before, after) {
				t.Errorf("keep=%s: rewrite unlinked %s; keep mode must never delete", keep, first)
			}
			if data, err := os.ReadFile(second); err != nil || string(data) != "new-body" {
				t.Errorf("keep=%s: rewritten file holds %q (%v), want %q", keep, data, err, "new-body")
			}
		})
	}
}
