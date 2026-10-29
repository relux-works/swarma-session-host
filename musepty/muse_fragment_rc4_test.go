package musepty

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
)

// museRC4FixtureSHA256 pins the legacy rc.4 revision fixture, including its
// trailing newline. Machine paths, environment/profile names and profile-lock
// identity are fictional; the original revision and refusal assertions remain.
const museRC4FixtureSHA256 = "72f2c60cdea21bb3a43bc729cc48a544c0222889324de56c68198316e403c004"

func museRC4Fixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/rc4-muse-fragment.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != museRC4FixtureSHA256 {
		t.Fatalf("fixture sha256 = %s, want %s", got, museRC4FixtureSHA256)
	}
	return raw
}

// TestDecodeMuseCuratorFragmentRefusesRealRC4Bytes preserves the captured legacy
// revision refusal before and after canonicalization, using fictional identity
// metadata. It does not assert byte identity with an external private capture.
func TestDecodeMuseCuratorFragmentRefusesRealRC4Bytes(t *testing.T) {
	raw := museRC4Fixture(t)
	wire, err := MuseFragmentWireBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, document := range [][]byte{raw, wire} {
		fragment, err := DecodeMuseCuratorFragment(document)
		if fragment != nil || museTestFragmentCode(t, err) != MuseFragmentVersionUnknown || !errors.Is(err, agentic.ErrCuratorFragmentRevisionUnsupported) || !strings.Contains(err.Error(), "upgrade Curator") {
			t.Fatalf("rc.4 = %v, %v", fragment, err)
		}
	}
}
