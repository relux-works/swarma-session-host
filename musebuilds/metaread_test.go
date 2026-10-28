//go:build darwin || linux

package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// TestFreezeStateUnsafeRefuses proves STATE documents that are not
// plain owner-readable files refuse instead of decoding: a FIFO
// refuses without blocking, a symlink refuses without being
// followed (even to valid content), and an oversized document
// refuses without being buffered. All refuse before admission runs.
func TestFreezeStateUnsafeRefuses(t *testing.T) {
	t.Run("fifo refuses without blocking", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		dir := filepath.Join(metaDir(f.root), key)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := unix.Mkfifo(filepath.Join(dir, "STATE.json"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeStateInconsistent, StageDedupe)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times past the fifo", acquires)
		}
	})

	t.Run("symlink refuses without being followed", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
		f.plantState(key, BuildState{
			Schema: buildStateSchema, Build: discovery.Identity.Build, SHA256: discovery.Identity.SHA256,
			Size: int64(len(raw)), Transaction: "txn-test", ProbeContract: ProbeContractID, State: StateProbeOK,
		})
		target := statePath(f.root, key)
		valid, err := os.ReadFile(target)
		if err != nil {
			t.Fatal(err)
		}
		elsewhere := filepath.Join(t.TempDir(), "STATE.json")
		if err := os.WriteFile(elsewhere, valid, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(target); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, target); err != nil {
			t.Fatal(err)
		}
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeStateInconsistent, StageDedupe)
	})

	t.Run("oversized document refuses", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		dir := filepath.Join(metaDir(f.root), key)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		f.writeFileSync(filepath.Join(dir, "STATE.json"),
			[]byte(`{"schema":1,`+strings.Repeat(" ", stateMaxBytes)+`}`), 0o600)
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeStateInconsistent, StageDedupe)
	})
}
