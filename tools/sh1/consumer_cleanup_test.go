package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Actual cleanup removes a writable owned fixture. A protected parent yields a
// surfaced removal error while preserving the original failure; no shared cache
// is used. The permission failure case requires a non-root test account.
func TestConsumerCacheCleanup(t *testing.T) {
	for _, protected := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed", true: "failure"}[protected], func(t *testing.T) {
			if protected && os.Geteuid() == 0 {
				t.Skip("permission refusal requires non-root")
			}
			parent := t.TempDir()
			cache := filepath.Join(parent, "cache")
			if err := os.Mkdir(cache, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cache, "fixture"), []byte("owned"), 0600); err != nil {
				t.Fatal(err)
			}
			if protected {
				if err := os.Chmod(parent, 0500); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.Chmod(parent, 0700); err != nil {
						t.Error(err)
					}
				})
			}
			original := errors.New("original fixture failure")
			err := removeConsumerCache(cache, original)
			if !errors.Is(err, original) {
				t.Fatalf("CONSUMER_CACHE_ORIGINAL_ERROR: %v", err)
			}
			_, statErr := os.Stat(cache)
			if protected {
				var removal *os.PathError
				if !errors.As(err, &removal) || !errors.Is(err, os.ErrPermission) || statErr != nil {
					t.Fatalf("CONSUMER_CACHE_CLEANUP_FAILURE: error=%v cache=%v", err, statErr)
				}
			} else if !os.IsNotExist(statErr) {
				t.Fatalf("CONSUMER_CACHE_REMOVE: %v", statErr)
			}
		})
	}
}

// A direct fixture call keeps this code under go test's overlay, while using real
// proxy-backed Go downloads. No cache directory survives success; the separate
// CLI end-to-end test still checks deterministic report bytes and commands.
func TestConsumerCacheRemoved(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("TMPDIR", cacheRoot)
	src, candidate, _ := candidateFixture(t)
	checkout := filepath.Join(t.TempDir(), "consumer")
	copyTree(t, src, checkout)
	if err := os.RemoveAll(filepath.Join(checkout, "internal")); err != nil {
		t.Fatal(err)
	}
	save(t, filepath.Join(checkout, "cmd/demo/main_test.go"), []byte("package main\nimport (\"testing\"; \"example.org/sh1-fixture/internal/session/child\")\nfunc TestConsumer(t *testing.T) {if !child.Allowed(){t.Fatal(\"consumer admission\")}}\n"))
	if err := consumer(candidate, checkout, "v0.0.0-20261010000000-0123456789ab", filepath.Join(t.TempDir(), "proxy"), filepath.Join(t.TempDir(), "report.json")); err != nil {
		t.Fatal(err)
	}
	dirs, err := filepath.Glob(filepath.Join(cacheRoot, "sh1-consumer-cache-*"))
	if err != nil || len(dirs) != 0 {
		t.Fatalf("CONSUMER_CACHE_LEFTOVER: count=%d err=%v", len(dirs), err)
	}
}
