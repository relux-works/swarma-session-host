//go:build darwin || linux

package musebuilds

// L2M pre-exec gate coverage: VerifyFrozenForExec re-verifies one
// frozen tuple against the bytes about to run. These cells drive it
// directly with a published copy and with each corruption class.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestExecVerifyRefusesCorruptTuples proves the pre-exec gate admits
// a published tuple and refuses every corruption class: a wrong
// digest (for two builds, so a single-build exemption cannot hide),
// a non-canonical path, an unparseable build, a non-hex digest, a
// missing copy, wrong leaf or binary modes, a symlinked key, and a
// wrong builds-parent mode.
func TestExecVerifyRefusesCorruptTuples(t *testing.T) {
	ctx := context.Background()
	f := newFreeze(t)
	cfg := f.config()

	published := f.publishFor("1.4.2-R4684.1")
	if err := VerifyFrozenForExec(ctx, cfg, published.Frozen); err != nil {
		t.Fatalf("verify published tuple: %v", err)
	}

	digestBuilds := []string{"1.4.2-R4684.1", "9.9.9-R0.1"}
	for _, build := range digestBuilds {
		data := []byte("exec-verify bytes for " + build)
		sum := sha256.Sum256(data)
		actual := hex.EncodeToString(sum[:])
		claimed := strings.Repeat("ab", 32)
		if claimed == actual {
			claimed = strings.Repeat("cd", 32)
		}
		key := freezeKey(build, claimed)
		f.plantFrozenBytes(t, key, build, data)
		id := FrozenIdentity{Build: build, SHA256: claimed, Path: frozenBinaryPath(cfg.FrozenRoot, key, build)}
		err := VerifyFrozenForExec(ctx, cfg, id)
		refusal, ok := AsExecError(err)
		if !ok || refusal.Code != CodeExecVerifyFailed || refusal.Stage != StageExecVerify {
			t.Fatalf("verify %s with swapped digest = %v, want the typed exec-verify refusal", build, err)
		}
	}

	if err := VerifyFrozenForExec(ctx, cfg, FrozenIdentity{Build: published.Frozen.Build, SHA256: published.Frozen.SHA256, Path: filepath.Join(t.TempDir(), "elsewhere")}); err == nil {
		t.Fatal("verify non-canonical path succeeded, want refusal")
	} else if refusal, ok := AsExecError(err); !ok || refusal.Code != CodeExecVerifyFailed {
		t.Fatalf("verify non-canonical path = %v, want the typed exec-verify refusal", err)
	}

	for name, id := range map[string]FrozenIdentity{
		"unparseable build": {Build: "bogus", SHA256: published.Frozen.SHA256, Path: published.Frozen.Path},
		"non-hex digest":    {Build: published.Frozen.Build, SHA256: "zz", Path: published.Frozen.Path},
	} {
		if err := VerifyFrozenForExec(ctx, cfg, id); err == nil {
			t.Fatalf("verify %s succeeded, want refusal", name)
		} else if refusal, ok := AsExecError(err); !ok || refusal.Code != CodeExecVerifyFailed {
			t.Fatalf("verify %s = %v, want the typed exec-verify refusal", name, err)
		}
	}

	missingKey := freezeKey("8.8.8-R0.1", strings.Repeat("1", 64))
	missing := FrozenIdentity{Build: "8.8.8-R0.1", SHA256: strings.Repeat("1", 64), Path: frozenBinaryPath(cfg.FrozenRoot, missingKey, "8.8.8-R0.1")}
	if err := VerifyFrozenForExec(ctx, cfg, missing); err == nil {
		t.Fatal("verify missing copy succeeded, want refusal")
	}

	other := f.publishFor("9.9.9-R0.1")
	otherKey := freezeKey(other.Frozen.Build, other.Frozen.SHA256)
	otherDir := frozenKeyDir(cfg.FrozenRoot, otherKey)
	otherBinary := frozenBinaryPath(cfg.FrozenRoot, otherKey, other.Frozen.Build)
	if err := os.Chmod(otherDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFrozenForExec(ctx, cfg, other.Frozen); err == nil {
		t.Fatal("verify relaxed leaf mode succeeded, want refusal")
	}
	if err := os.Chmod(otherDir, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(otherBinary, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFrozenForExec(ctx, cfg, other.Frozen); err == nil {
		t.Fatal("verify relaxed binary mode succeeded, want refusal")
	}
	if err := os.Chmod(otherBinary, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFrozenForExec(ctx, cfg, other.Frozen); err != nil {
		t.Fatalf("verify restored tuple: %v", err)
	}

	sibling := f.publishFor("1.4.0-R1000")
	siblingKey := freezeKey(sibling.Frozen.Build, sibling.Frozen.SHA256)
	siblingDir := frozenKeyDir(cfg.FrozenRoot, siblingKey)
	siblingBinary := frozenBinaryPath(cfg.FrozenRoot, siblingKey, sibling.Frozen.Build)
	stash, err := os.ReadFile(siblingBinary)
	if err != nil {
		t.Fatal(err)
	}
	// The frozen key directory is 0555, so its entries cannot be
	// unlinked until the leaf is writable. Relax it for the removal
	// only; the replanted copy below restores the frozen modes.
	if err := os.Chmod(siblingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(siblingDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(otherDir, siblingDir); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFrozenForExec(ctx, cfg, sibling.Frozen); err == nil {
		t.Fatal("verify symlinked key succeeded, want refusal")
	}
	if err := os.Remove(siblingDir); err != nil {
		t.Fatal(err)
	}
	f.plantFrozenBytes(t, siblingKey, sibling.Frozen.Build, stash)
	if err := VerifyFrozenForExec(ctx, cfg, sibling.Frozen); err != nil {
		t.Fatalf("verify restored sibling tuple: %v", err)
	}

	buildsDir := filepath.Join(cfg.FrozenRoot, "builds")
	if err := os.Chmod(buildsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFrozenForExec(ctx, cfg, published.Frozen); err == nil {
		t.Fatal("verify relaxed builds-parent mode succeeded, want refusal")
	}
	if err := os.Chmod(buildsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFrozenForExec(ctx, cfg, published.Frozen); err != nil {
		t.Fatalf("verify after parent restore: %v", err)
	}
}
