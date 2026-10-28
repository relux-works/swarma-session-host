package musebuilds

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestDiscoveryFinalRehashCancellationStops is the F4 repeated-class
// regression. Three chunks guarantee cancellation cannot be masked by EOF.
func TestDiscoveryFinalRehashCancellationStops(t *testing.T) {
	install := validScanWithNewest(t)
	path := filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1")
	install.writeFileSync(path, bytes.Repeat([]byte("x"), 3*hashReadChunk), 0755)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts, reads, accepted := 0, 0, 0
	armInterpose(t, func(point interposePoint) {
		switch point {
		case interposeAttemptEntry:
			attempts++
		case interposeFinalRead:
			reads++
			cancel()
		case interposeAccepted:
			accepted++
		}
	})
	_, err := ResolveNewest(ctx, install.env())
	e := requireCode(t, err, CodeCanceled)
	if e.Stage != StageRecheck || !errors.Is(err, context.Canceled) || attempts != 1 || reads != 1 || accepted != 0 {
		t.Fatalf("rehash cancellation: err=%v attempts=%d reads=%d accepted=%d", err, attempts, reads, accepted)
	}
}

// TestDiscoveryCancellationAtEveryReadBoundary checks both the reached stage
// and zero subsequent reads/attempts, not merely the eventual error code.
func TestDiscoveryCancellationAtEveryReadBoundary(t *testing.T) {
	for _, cell := range []struct {
		name, stage string
		point       interposePoint
	}{
		{"launcher", StageLauncher, interposeAfterPath},
		{"selector", StageSidecar, interposeSidecarRead},
		{"inventory", StageScan, interposeInventoryRead},
		{"source", StageSource, interposeInitialRead},
		{"acceptance", StageSelect, interposeAccepted},
	} {
		t.Run(cell.name, func(t *testing.T) {
			install := validScanWithNewest(t)
			install.writeSidecar("1.4.4-R5419.1\n")
			install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1"), bytes.Repeat([]byte("x"), 3*hashReadChunk), 0755)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hits, after, attempts := 0, 0, 0
			armInterpose(t, func(point interposePoint) {
				if ctx.Err() != nil {
					after++
				}
				if point == interposeAttemptEntry {
					attempts++
				}
				if point == cell.point {
					hits++
					cancel()
				}
			})
			_, err := ResolveNewest(ctx, install.env())
			e := requireCode(t, err, CodeCanceled)
			if e.Stage != cell.stage || hits != 1 || after != 0 || attempts != 1 {
				t.Fatalf("stage=%s hits=%d after-cancel=%d attempts=%d", e.Stage, hits, after, attempts)
			}
		})
	}
}

// TestDiscoveryCancellationPreventsRetry covers cancellation following a
// real consistency disagreement; the second attempt must never start work.
func TestDiscoveryCancellationPreventsRetry(t *testing.T) {
	install := validScanWithNewest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	armInterpose(t, func(point interposePoint) {
		if point == interposeAttemptEntry {
			attempts++
		}
		if point == interposeBeforeRecheck {
			install.writeCandidate("1.4.5-R5500")
			cancel()
		}
	})
	_, err := ResolveNewest(ctx, install.env())
	e := requireCode(t, err, CodeCanceled)
	if e.Stage != StageSelect || attempts != 1 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}
}

// TestDiscoveryClosedDescriptorsOnCancellation checks real descriptor release
// even when the caller cancels after a full snapshot was acquired.
func TestDiscoveryClosedDescriptorsOnCancellation(t *testing.T) {
	install := validScanWithNewest(t)
	var files []*os.File
	snapshotObservedForTest = func(s *discoverySnapshot) {
		for _, lease := range s.files {
			files = append(files, lease.file)
		}
	}
	t.Cleanup(func() { snapshotObservedForTest = nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	armInterpose(t, func(point interposePoint) {
		if point == interposeBeforeRecheck {
			cancel()
		}
	})
	_, err := ResolveNewest(ctx, install.env())
	e := requireCode(t, err, CodeCanceled)
	if e.Stage != StageSelect || len(files) == 0 {
		t.Fatalf("no snapshot reached: %v", err)
	}
	for _, file := range files {
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("FD remained open: %v", err)
		}
	}
}
