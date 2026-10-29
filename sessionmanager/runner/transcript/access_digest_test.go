package transcript

import (
	"bytes"
	"reflect"
	"testing"
)

// Fixed SHA256 vectors bind the original labeled byte recipes independently of
// the canonical package. Content bytes and anchors cannot be interchanged;
// digesting a caller slice must not change its contents or spare capacity.
func TestRepositoryContentDigestRecipe(t *testing.T) {
	storage := []byte{'a', 0, 'b', 255, '\n', 'x', 'y'}
	payload := storage[:5]
	before := append([]byte(nil), storage...)
	got := ContextRepositoryAccessContentDigest(" ANCHOR ", payload)
	if got != "1b6f6cf18bd597e67e2df13ff1cfb6ee85dd9759146028c502b833d379708fca" {
		t.Fatalf("content recipe changed: %s", got)
	}
	if !bytes.Equal(before, storage) {
		t.Fatal("content digest changed caller storage")
	}
	if empty := ContextRepositoryAccessContentDigest("anchor", nil); empty != "d5a0191c5baf5edef1c7bc858bdddc4ab1b84de912a34437bcbb1d51e86e2d0b" || empty == got {
		t.Fatal("empty content lost its independent digest")
	}
	if ContextRepositoryAccessContentDigest("other", payload) == got || ContextRepositoryAccessContentDigest("anchor", []byte("changed")) == got {
		t.Fatal("foreign anchor or content retained digest")
	}
}

// Enumeration describes the normalized, sorted path set. Invalid escapes add
// no members; an empty set remains an observation with its own nonempty digest.
func TestRepositoryEnumerationDigestRecipe(t *testing.T) {
	entries := []string{"src/b.go", " ./docs/a.md ", `src\b.go`, "docs/a.md", "", "/absolute", "../escape", "a/../escape", ".", "./", "docs/a.md/"}
	before := append([]string(nil), entries...)
	got := ContextRepositoryAccessEnumerationDigest(" ANCHOR ", entries)
	if got != "03b9b2f109a703a0d98dac9e2fd2536af6e02a7fce443abac95fc2a057d3f333" {
		t.Fatalf("enumeration recipe changed: %s", got)
	}
	if !reflect.DeepEqual(before, entries) {
		t.Fatal("enumeration digest changed caller entries")
	}
	if ContextRepositoryAccessEnumerationDigest("anchor", []string{"docs/a.md", "src/b.go"}) != got {
		t.Fatal("same path set changed digest")
	}
	if ContextRepositoryAccessEnumerationDigest("anchor", nil) != "a1e16a11386a2ba213cca35c30a479cc9b0de262d8304ac20d148a597ea113d0" {
		t.Fatal("empty enumeration recipe changed")
	}
	for _, foreign := range []string{
		ContextRepositoryAccessEnumerationDigest("other", entries),
		ContextRepositoryAccessEnumerationDigest("anchor", []string{"docs/a.md"}),
		ContextRepositoryAccessContentDigest("anchor", nil),
	} {
		if foreign == got {
			t.Fatal("foreign anchor, membership or operation retained digest")
		}
	}
}

// Stat binds existence, normalized kind and signed size separately. Missing
// paths are observations rather than the absence of a digest.
func TestRepositoryStatDigestRecipe(t *testing.T) {
	got := ContextRepositoryAccessStatDigest(" ANCHOR ", true, " FILE ", 17)
	if got != "72d6cfa114931dd87d8643304cde0a32b9ea00c935605415affe7d56faef1f4e" {
		t.Fatalf("stat recipe changed: %s", got)
	}
	if ContextRepositoryAccessStatDigest("anchor", false, "", 0) != "516476ed590ad4ff773b7043c023854df5e71528337db550a44303aeaf527a74" {
		t.Fatal("missing path recipe changed")
	}
	for _, foreign := range []string{
		ContextRepositoryAccessStatDigest("other", true, "file", 17),
		ContextRepositoryAccessStatDigest("anchor", false, "file", 17),
		ContextRepositoryAccessStatDigest("anchor", true, "directory", 17),
		ContextRepositoryAccessStatDigest("anchor", true, "file", -17),
	} {
		if foreign == got {
			t.Fatal("foreign anchor, existence, kind or size retained digest")
		}
	}
}
