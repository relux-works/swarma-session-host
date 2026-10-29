package musepty

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
)

// museTestUUID maps an opaque test name to a deterministic
// captured-format identity (UUID v4 spelling). The closed grammar
// types every joined identity field (r14), so scripted-peer and test
// records must carry well-formed ids; relational defects (foreign,
// duplicate, mismatch, unconfigured, unresolved) are preserved
// because they never depend on the id spelling. Deterministic per
// name (SHA-256 over a versioned namespace, first 128 bits) so
// narrowing-mutant replacements can bake the same values; the pinned
// mapping lives in TestMusePTYR14TestUUIDMapping.
func museTestUUID(name string) string {
	sum := sha256.Sum256([]byte("muse-test-uuid/v1:" + name))
	b := sum[:16]
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	hexed := hex.EncodeToString(b)
	return hexed[0:8] + "-" + hexed[8:12] + "-" + hexed[12:16] + "-" + hexed[16:20] + "-" + hexed[20:32]
}

// Captured from 1.4.1-R4503.1 using the isolated delayed Echo provider, before
// r3 implementation. Values remain captured; only runtime identities/content
// change in the peer. Fault-injected failure terminals are explicitly synthetic.
//
//go:embed testdata/muse-1.4.1/records.json
var museCapturedRecords embed.FS

func museFixtureRecord(kind string) map[string]any {
	b, err := museCapturedRecords.ReadFile("testdata/muse-1.4.1/records.json")
	if err != nil {
		panic(err)
	}
	var records map[string]map[string]any
	if err = json.Unmarshal(b, &records); err != nil {
		panic(err)
	}
	r := records[kind]
	if r == nil {
		panic("no captured Muse fixture: " + kind)
	}
	return r
}
func museFixturePayload(kind string) map[string]any {
	return museFixtureRecord(kind)["payload"].(map[string]any)
}
