package sessionhost

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func goalSourceFixture(t *testing.T, data string) (*goalFileSource, string) {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	locator := filepath.Join(directory, "GOAL.md")
	if err := os.WriteFile(locator, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := newGoalFileSource(directory, locator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := source.close(); err != nil {
			t.Error(err)
		}
	})
	return source, locator
}

func requireGoalSnapshot(t *testing.T, source *goalFileSource, locator, data string) goalFileSnapshot {
	t.Helper()
	snapshot, err := source.read()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(data))
	if snapshot.locator != locator || snapshot.digest != hex.EncodeToString(digest[:]) {
		t.Fatal("snapshot did not preserve the declared locator and exact byte digest")
	}
	return snapshot
}

// Exact byte hashes include empty and non-UTF8 content; the Host has no content
// parser. The inclusive source bound is independent of a native frame budget.
func TestGoalSourceBytesAndSize(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"empty", ""},
		{"opaque", "# Goals\nA private instruction that must stay inside the source.\n"},
		{"opaque-non-utf8", string([]byte{0xff, 0x00, 0xfe})},
		{"inclusive-limit", strings.Repeat("g", goalSnapshotLimit)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, locator := goalSourceFixture(t, tc.data)
			requireGoalSnapshot(t, source, locator, tc.data)
		})
	}
	source, locator := goalSourceFixture(t, strings.Repeat("g", goalSnapshotLimit+1))
	if snapshot, err := source.read(); err != goalSourceTooLarge || snapshot != (goalFileSnapshot{}) {
		t.Fatal("oversized source must return only its intended refusal")
	}
	if err := os.WriteFile(locator, []byte("valid again"), 0600); err != nil {
		t.Fatal(err)
	}
	requireGoalSnapshot(t, source, locator, "valid again")
}

// Normal manager-authored file modes remain compatible with a private source
// directory. Read-only or Git's ordinary 0644 files are not all rejected.
func TestGoalSourceValidFileModes(t *testing.T) {
	for _, mode := range []os.FileMode{0400, 0444, 0600, 0644} {
		source, locator := goalSourceFixture(t, "valid mode")
		if err := os.Chmod(locator, mode); err != nil {
			t.Fatal(err)
		}
		requireGoalSnapshot(t, source, locator, "valid mode")
	}
}

// A writable source directory or symlinked ancestry is not authority to read
// a goal. Restoring the real safe directory gives a nearby positive control.
func TestGoalSourceDirectoryBoundary(t *testing.T) {
	source, locator := goalSourceFixture(t, "valid")
	directory := filepath.Dir(locator)
	if err := os.Chmod(directory, 0777); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := source.read(); err != goalSourceInvalid || snapshot != (goalFileSnapshot{}) {
		t.Fatal("unsafe directory must refuse without a snapshot")
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	requireGoalSnapshot(t, source, locator, "valid")
	alias := filepath.Join(directory, "alias")
	if err := os.Symlink(directory, alias); err != nil {
		t.Fatal(err)
	}
	if other, err := newGoalFileSource(directory, filepath.Join(alias, "GOAL.md")); err != goalSourceInvalid || other != nil {
		t.Fatal("symlinked ancestry must refuse")
	}
	requireGoalSnapshot(t, source, locator, "valid")
}

// Initial construction does not require a goal file; a missing optional source
// is refused on read and can become available on a later eligible read.
func TestGoalSourceMissingUntilRead(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	locator := filepath.Join(directory, "GOAL.md")
	source, err := newGoalFileSource(directory, locator)
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	if snapshot, err := source.read(); err != goalSourceUnavailable || snapshot != (goalFileSnapshot{}) {
		t.Fatal("missing file must refuse with no snapshot")
	}
	if err := os.WriteFile(locator, []byte("now present"), 0600); err != nil {
		t.Fatal(err)
	}
	requireGoalSnapshot(t, source, locator, "now present")
}

// A legitimate writer rename between snapshots is accepted without re-opening
// the reader's source. An inode change during one snapshot is a different case.
func TestGoalSourceAtomicRenameBetweenReads(t *testing.T) {
	source, locator := goalSourceFixture(t, "old")
	requireGoalSnapshot(t, source, locator, "old")
	replacement := filepath.Join(filepath.Dir(locator), "replacement.md")
	if err := os.WriteFile(replacement, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, locator); err != nil {
		t.Fatal(err)
	}
	requireGoalSnapshot(t, source, locator, "new")
}

// Each forbidden source has a nearby same-reader positive control. These are
// real isolated files; no real manager home, credential or native API is read.
func TestGoalSourceForbiddenFiles(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		source, locator := goalSourceFixture(t, "valid")
		requireGoalSnapshot(t, source, locator, "valid")
		target := filepath.Join(filepath.Dir(locator), "target.md")
		if err := os.Rename(locator, target); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, locator); err != nil {
			t.Fatal(err)
		}
		if snapshot, err := source.read(); err != goalSourceInvalid || snapshot != (goalFileSnapshot{}) {
			t.Fatal("symlink must refuse, not follow the target")
		}
		if err := os.Remove(locator); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(target, locator); err != nil {
			t.Fatal(err)
		}
		requireGoalSnapshot(t, source, locator, "valid")
	})
	t.Run("hardlink", func(t *testing.T) {
		source, locator := goalSourceFixture(t, "valid")
		alias := filepath.Join(filepath.Dir(locator), "alias.md")
		if err := os.Link(locator, alias); err != nil {
			t.Fatal(err)
		}
		if snapshot, err := source.read(); err != goalSourceInvalid || snapshot != (goalFileSnapshot{}) {
			t.Fatal("hardlinked source must refuse")
		}
		if err := os.Remove(alias); err != nil {
			t.Fatal(err)
		}
		requireGoalSnapshot(t, source, locator, "valid")
	})
	for _, tc := range []struct {
		name string
		mode os.FileMode
		want error
	}{
		{"writable-by-others", 0666, goalSourceInvalid},
		{"unreadable", 0000, goalSourceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, locator := goalSourceFixture(t, "valid")
			if err := os.Chmod(locator, tc.mode); err != nil {
				t.Fatal(err)
			}
			if snapshot, err := source.read(); err != tc.want || snapshot != (goalFileSnapshot{}) {
				t.Fatal("permission refusal must be exact and return no snapshot")
			}
			if err := os.Chmod(locator, 0600); err != nil {
				t.Fatal(err)
			}
			requireGoalSnapshot(t, source, locator, "valid")
		})
	}
	t.Run("directory", func(t *testing.T) {
		source, locator := goalSourceFixture(t, "valid")
		if err := os.Remove(locator); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(locator, 0700); err != nil {
			t.Fatal(err)
		}
		if snapshot, err := source.read(); err != goalSourceInvalid || snapshot != (goalFileSnapshot{}) {
			t.Fatal("nonregular source must refuse")
		}
		if err := os.Remove(locator); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(locator, []byte("valid"), 0600); err != nil {
			t.Fatal(err)
		}
		requireGoalSnapshot(t, source, locator, "valid")
	})
}

// The coherence comparator detects a real atomic inode replacement even with
// identical bytes. This is a deterministic boundary witness, not a concurrent
// filesystem exclusion proof or a completed source.read race test.
func TestGoalSourceSnapshotIdentity(t *testing.T) {
	_, locator := goalSourceFixture(t, "same bytes")
	before, err := os.Stat(locator)
	if err != nil {
		t.Fatal(err)
	}
	if !sameGoalFileSnapshot(before, before) {
		t.Fatal("unchanged metadata is the positive control")
	}
	replacement := filepath.Join(filepath.Dir(locator), "new.md")
	if err := os.WriteFile(replacement, []byte("same bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, locator); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(locator)
	if err != nil {
		t.Fatal(err)
	}
	if sameGoalFileSnapshot(before, after) {
		t.Fatal("an inode replacement must not be a coherent single snapshot")
	}
}

// Locator validation is a syntax bound; a syntactically valid 4096-byte path
// is not a claim that the filesystem can create it or that it is admitted.
func TestGoalSourceLocatorBoundsAndScope(t *testing.T) {
	if !validGoalLocator("/" + strings.Repeat("x", goalLocatorLimit-1)) {
		t.Fatal("inclusive locator limit must pass syntax validation")
	}
	for _, path := range []string{
		"/" + strings.Repeat("x", goalLocatorLimit), "relative.md", "/a/../GOAL.md", "/goal\n.md", "/goal\x7f.md", "/goal\xff.md",
	} {
		if validGoalLocator(path) {
			t.Fatal("forbidden locator syntax was accepted")
		}
	}
	source, locator := goalSourceFixture(t, "valid")
	if other, err := newGoalFileSource(filepath.Dir(locator), filepath.Join(filepath.Dir(filepath.Dir(locator)), "outside.md")); err != goalSourceInvalid || other != nil {
		t.Fatal("locator outside the declared source must refuse")
	}
	requireGoalSnapshot(t, source, locator, "valid")
	if err := source.close(); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := source.read(); err != goalSourceUnavailable || snapshot != (goalFileSnapshot{}) {
		t.Fatal("closed source cannot continue reading")
	}
}

// The exact fixed text contains only JSON-quoted locator and exact byte digest.
// Complete native-request frame admission remains the later adapter's duty.
func TestGoalWakeRendering(t *testing.T) {
	content := "private goal fixture that must never become prompt content"
	source, locator := goalSourceFixture(t, content)
	snapshot := requireGoalSnapshot(t, source, locator, content)
	snapshot.locator = filepath.Join(filepath.Dir(locator), "goal \"quoted\".md")
	text, err := renderGoalWake(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	quoted, err := json.Marshal(snapshot.locator)
	if err != nil {
		t.Fatal(err)
	}
	if text != "Advance your goals: "+string(quoted)+" sha256 "+snapshot.digest || strings.Contains(text, content) {
		t.Fatal("wake must preserve only the declared locator and digest")
	}
	for _, invalid := range []goalFileSnapshot{
		{locator: "relative.md", digest: snapshot.digest},
		{locator: snapshot.locator, digest: strings.Repeat("A", 64)},
		{locator: snapshot.locator, digest: snapshot.digest[:63]},
	} {
		if text, err := renderGoalWake(invalid); err != goalConfigInvalid || text != "" {
			t.Fatal("invalid rendering input must yield its intended refusal and no text")
		}
	}
}
