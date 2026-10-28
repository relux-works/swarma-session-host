package musebuilds

import (
	"context"
	"testing"
	"time"
)

// TestDiscoveryContextCancellationRefuses proves discovery honors its
// context before doing anything: a cancelled or expired context refuses,
// and a live one proceeds.
func TestDiscoveryContextCancellationRefuses(t *testing.T) {
	seed := func(t *testing.T) *installFixture {
		t.Helper()
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.4-R5419.1")
		install.writeSidecar("1.4.4-R5419.1\n")
		return install
	}

	t.Run("cancelled context refuses", func(t *testing.T) {
		install := seed(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		entries := 0
		armInterpose(t, func(point interposePoint) {
			if point == interposeAttemptEntry {
				entries++
			}
		})
		_, err := ResolveNewest(ctx, install.env())
		refusal := requireCode(t, err, CodeCanceled)
		if refusal.Stage != StageStart {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageStart)
		}
		if entries != 0 {
			t.Fatalf("%d attempts started after cancellation", entries)
		}
	})

	t.Run("expired context refuses", func(t *testing.T) {
		install := seed(t)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
		defer cancel()
		_, err := ResolveNewest(ctx, install.env())
		if e := requireCode(t, err, CodeCanceled); e.Stage != StageStart {
			t.Fatalf("stage=%s", e.Stage)
		}
	})

	t.Run("live context proceeds", func(t *testing.T) {
		install := seed(t)
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v", discovery.Identity)
		}
	})
}

// TestDiscoveryMidAttemptCancellationRefuses proves a context cancelled
// during discovery refuses instead of reporting the success the attempt
// computed: cancellation is honored before any retry or success, and a
// cancelled attempt is never retried.
func TestDiscoveryMidAttemptCancellationRefuses(t *testing.T) {
	seed := func(t *testing.T) *installFixture {
		t.Helper()
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.4-R5419.1")
		install.writeSidecar("1.4.4-R5419.1\n")
		return install
	}

	t.Run("cancel before the recheck refuses without retry", func(t *testing.T) {
		install := seed(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rechecks := 0
		armInterpose(t, func(point interposePoint) {
			if point != interposeBeforeRecheck {
				return
			}
			rechecks++
			cancel()
		})
		_, err := ResolveNewest(ctx, install.env())
		refusal := requireCode(t, err, CodeCanceled)
		if refusal.Stage != StageSelect {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageSelect)
		}
		if rechecks != 1 {
			t.Fatalf("recheck ran %d times, want 1 (no retry after cancel)", rechecks)
		}
	})

	t.Run("cancel before hashing refuses", func(t *testing.T) {
		install := seed(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		armInterpose(t, func(point interposePoint) {
			if point != interposeAfterValidate {
				return
			}
			cancel()
		})
		_, err := ResolveNewest(ctx, install.env())
		refusal := requireCode(t, err, CodeCanceled)
		if refusal.Stage != StageSource {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageSource)
		}
	})
}
