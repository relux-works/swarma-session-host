package historyguard

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The retained image matrix now checks the structural contract: no root file
// is ignored, regardless of its seal/history. The name is retained from fix5.
func TestProtocolTempRequiresOwnEmptyImage(t *testing.T) {
	for _, kind := range []string{"controller-epochs", "session-owner-keys"} {
		for _, variant := range []string{"empty", "history", "foreign-namespace", "foreign-location", "extra-session-field", "recovery-floor", "witness-empty", "witness-history"} {
			t.Run(kind+"/"+variant, func(t *testing.T) {
				dir := t.TempDir()
				namespace := "namespace"
				imageNamespace := namespace
				imageDir := dir
				if variant == "foreign-namespace" {
					imageNamespace = "other"
				}
				if variant == "foreign-location" {
					imageDir = filepath.Join(dir, "other")
				}
				id := StoreIdentity(kind, imageNamespace+"\x00"+imageDir)
				field := "namespace"
				if kind == "session-owner-keys" {
					field = "board_fingerprint"
				}
				doc := map[string]any{"version": 1, field: imageNamespace, "entries": []string{}}
				if variant == "history" || variant == "witness-history" {
					doc["entries"] = []string{"retired"}
				}
				if variant == "extra-session-field" {
					doc["session_id"] = "prior-session"
				}
				if variant == "recovery-floor" {
					doc["recovery_floor"] = 1
				}
				// Keep the seal first, as required by the real store's exact-byte protocol.
				body, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				seal := ImageSeal{StoreID: id, Sequence: 1}
				encode := func() []byte {
					raw, _ := json.Marshal(seal)
					return append(append(append([]byte(`{"seal":`), raw...), ','), body[1:]...)
				}
				seal.SHA256 = Digest(encode())
				image := encode()
				if _, err := VerifyImage(image, id); err != nil {
					t.Fatal(err)
				}
				name := "." + kind + "-1234567890"
				if variant == "witness-empty" || variant == "witness-history" {
					name = ".admission-witness-1234567890"
				}
				path := filepath.Join(dir, name)
				if variant == "witness-empty" || variant == "witness-history" {
					if err := writeBound(path, boundWitness{StoreID: id, Pending: image}); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(path, image, 0600); err != nil {
					t.Fatal(err)
				}
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if ignored, err := SkipTempDirectory(dir, path); err != nil || ignored {
					t.Fatalf("root image ignored: %v %t", err, ignored)
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatal("classification changed bytes")
				}
			})
		}
	}
}

// Every root filename remains prior-state evidence, including an empty or
// partially written protocol prefix. Only the owned directory is skipped.
func TestProtocolTempShortWriteClassification(t *testing.T) {
	for _, row := range []struct {
		name, body string
	}{
		{".controller-epochs-123", `{"seal":`},
		{".admission-witness-123", `{"version":`},
		{".controller-epochs-123", ""},
		{".controller-epochs-orphan", `{"seal":`},
		{".controller-epochs-123", `{"session_id":`},
		{".controller-epochs-123", `{"seal":{},"session_id":"prior"}`},
		{".admission-witness-123", `{"version":2,"session_id":"prior"}`},
		{".session-owner-keys-123", `{"unrelated":true}`},
		{".session-owner-keys-123", `arbitrary partial data`},
	} {
		t.Run(row.name+"/"+row.body, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), row.name)
			if err := os.WriteFile(path, []byte(row.body), 0600); err != nil {
				t.Fatal(err)
			}
			if ignored, err := SkipTempDirectory(filepath.Dir(path), path); err != nil || ignored {
				t.Fatalf("root short write ignored: %v %t", err, ignored)
			}
		})
	}
}
