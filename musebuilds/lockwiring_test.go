//go:build darwin || linux

package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// lockFixtureScript is a POSIX-sh stand-in for the canonical
// mini-build-lock script. It implements the script's real grammar —
// `run OWNER -- CMD...`, exit 2 on misuse, a TERM trap that logs the
// release and exits 143 — and honors FIXTURE_MODE crash (exit before
// the hold). The wait bound is client-side; there is no --wait-max
// flag and no exit 75.
const lockFixtureScript = `#!/bin/sh
set -u
log() { printf '%s\n' "$1" >> "$FIXTURE_LOG"; }
fail() { log "reject: $1"; exit 2; }
[ "${1:-}" = "run" ] || fail "no run verb"
owner="${2:-}"; [ "$owner" = "muse-freeze" ] || fail "owner=$owner"
[ "${3:-}" = "--" ] || fail "no separator"
shift 3
[ "$#" -gt 0 ] || fail "no hold command"
log "argv owner=$owner hold=$*"
log "home=$HOME lock=$HOME/.mini-build-lock queue=$HOME/.mini-build-lock.queue"
mode="${FIXTURE_MODE:-normal}"
if [ "$mode" = "crash" ]; then exit 0; fi
hold=""
trap 'if [ -n "$hold" ]; then kill "$hold" 2>/dev/null; wait "$hold" 2>/dev/null; fi; log released; exit 143' TERM INT
: > "$FIXTURE_STARTED"
"$@" &
hold=$!
wait "$hold"
log "hold-exit=$?"
`

// writeLockFixture publishes the fixture script with the temp,
// sync, close, chmod, rename discipline every executable fixture
// follows.
func writeLockFixture(t *testing.T) (script, started, log string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "mini-build-lock")
	tmp, err := os.CreateTemp(dir, ".writing-*")
	if err != nil {
		t.Fatal(err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write([]byte(lockFixtureScript)); err != nil {
		_ = tmp.Close()
		t.Fatal(err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tmpName, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		t.Fatal(err)
	}
	started = filepath.Join(dir, "started")
	log = filepath.Join(dir, "fixture.log")
	t.Setenv("FIXTURE_STARTED", started)
	t.Setenv("FIXTURE_LOG", log)
	return path, started, log
}

// TestFreezeProductionLockWiring proves PublishFrozen drives the real
// minilock client shape end to end: the owner reaches the script
// under the real `run OWNER -- CMD...` grammar, the hold covers the
// critical section, and release is observed — while a script that
// exits before the hold refuses with the lock code instead of an
// unserialized publication.
func TestFreezeProductionLockWiring(t *testing.T) {
	t.Run("hold covers the publication", func(t *testing.T) {
		script, started, logPath := writeLockFixture(t)
		t.Setenv("FIXTURE_MODE", "normal")
		f := newFreeze(t)
		cfg := f.config()
		cfg.Locker = productionLocker(script, t.TempDir())
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		result, err := PublishFrozen(ctx, cfg, f.discoveryFor("1.4.4-R5419.1"))
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomePublished {
			t.Fatalf("outcome = %s, want published", result.Outcome)
		}
		if _, err := os.Stat(started); err != nil {
			t.Fatalf("hold never started: %v", err)
		}
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"owner=muse-freeze", "released"} {
			if !strings.Contains(string(log), want) {
				t.Fatalf("fixture log %q lacks %q", log, want)
			}
		}
	})

	t.Run("script crash refuses", func(t *testing.T) {
		script, _, _ := writeLockFixture(t)
		t.Setenv("FIXTURE_MODE", "crash")
		f := newFreeze(t)
		cfg := f.config()
		cfg.Locker = productionLocker(script, t.TempDir())
		locks := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeAfterLock {
				locks++
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, err := PublishFrozen(ctx, cfg, f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeLockFailed, StageAdmit)
		if locks != 0 {
			t.Fatalf("crashed script admitted the section %d times", locks)
		}
	})
}

// Only the script path is replaced by a fixture. Account-home resolution and
// the production adapter/environment binding are driven end to end.
func TestFreezeProductionLockIgnoresHomeOverride(t *testing.T) {
	evil := t.TempDir()
	t.Setenv("HOME", evil)
	production, err := DefaultHostConfig()
	if err != nil {
		t.Fatal(err)
	}
	home, err := accountHome()
	if err != nil {
		t.Fatal(err)
	}
	script, _, logPath := writeLockFixture(t)
	t.Setenv("FIXTURE_MODE", "normal")
	adapter := production.Locker.(minilockAdapter)
	if adapter.client.AccountHome != home {
		t.Fatal("production locker lacks account binding")
	}
	adapter.client.ScriptPath = script
	f := newFreeze(t)
	cfg := f.config()
	cfg.Locker = adapter
	result, err := PublishFrozen(context.Background(), cfg, f.discoveryFor("1.4.4-R5419.1"))
	if err != nil || result.Outcome != OutcomePublished {
		t.Fatalf("PublishFrozen = %v %v", result, err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "home=" + home + " lock=" + home + "/.mini-build-lock queue=" + home + "/.mini-build-lock.queue"
	if !strings.Contains(string(raw), want) || strings.Contains(string(raw), evil) {
		t.Fatalf("global lock/queue redirected by HOME: %s", raw)
	}
}
