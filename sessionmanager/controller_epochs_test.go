package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

func epochJournalFixture(t *testing.T) *controllerEpochJournal {
	t.Helper()
	j, err := newControllerEpochJournal(t.TempDir(), "board-fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// A durable reservation survives reopening even without a corresponding
// snapshot/lease. A recovered writer refuses that generation before any I/O;
// its new lease still writes. This uses the private backend double, not MSP.
func TestControllerEpochRestartBurnsReservationBeforeLease(t *testing.T) {
	j := epochJournalFixture(t)
	epoch, err := j.reserve("session")
	if err != nil || epoch != 1 {
		t.Fatalf("reservation: %d, %v", epoch, err)
	}
	reopened, err := newControllerEpochJournal(j.dir, j.namespace)
	if err != nil {
		t.Fatal(err)
	}
	b := &inputTestBackend{}
	h, err := reopened.admitInput(context.Background(), "session", b)
	if err != nil {
		t.Fatal(err)
	}
	stale := ControllerLease{SessionID: "session", Epoch: epoch}
	if _, err := h.Submit(context.Background(), textInput(stale, "old", "old")); !errors.Is(err, ErrStaleInputController) || b.writeCount() != 0 {
		t.Fatalf("stale admitted: %v writes %d", err, b.writeCount())
	}
	lease, err := h.CurrentLease(context.Background(), "session")
	if err != nil || lease.Epoch <= epoch {
		t.Fatalf("new lease: %#v %v", lease, err)
	}
	receipt, err := h.Submit(context.Background(), textInput(lease, "new", "new"))
	if err == nil {
		_, err = h.Await(context.Background(), receipt)
	}
	if err != nil || b.writeCount() != 1 {
		t.Fatalf("live refused: %v writes %d", err, b.writeCount())
	}
}

// Another host reservation invalidates an already-issued writer before its
// provider capability is called. Durable renewal drains and admits a newer
// lease. Cancellation before an attempt preserves it; a durable attempted
// reservation is burned (covered by the commit-uncertainty test).
func TestControllerEpochNewReservationRevokesExistingWriter(t *testing.T) {
	j := epochJournalFixture(t)
	ctx := context.Background()
	b := &inputTestBackend{}
	h, err := j.admitInput(ctx, "session", b)
	if err != nil {
		t.Fatal(err)
	}
	old, err := h.CurrentLease(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	other, err := newControllerEpochJournal(j.dir, j.namespace)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.reserve("session"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Submit(ctx, textInput(old, "stale", "stale")); !errors.Is(err, ErrStaleInputController) || b.writeCount() != 0 {
		t.Fatalf("superseded writer wrote: %v %d", err, b.writeCount())
	}
	if err := j.renewInput(ctx, h); err != nil {
		t.Fatal(err)
	}
	live, err := h.CurrentLease(ctx, "session")
	if err != nil || live.Epoch != 3 {
		t.Fatalf("renewed lease: %#v %v", live, err)
	}
	receipt, err := h.Submit(ctx, textInput(live, "live", "live"))
	if err == nil {
		_, err = h.Await(ctx, receipt)
	}
	if err != nil || b.writeCount() != 1 {
		t.Fatalf("renewed positive: %v %d", err, b.writeCount())
	}
	before, err := os.ReadFile(filepath.Join(j.dir, controllerEpochFile))
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := j.renewInput(cancelled, h); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted renewal: %v", err)
	}
	// Fix3 requires cancellation before an attempt to have no partial effect.
	// An attempt already recorded in the pending witness is still burned.
	if still, err := h.CurrentLease(ctx, "session"); err != nil || still != live {
		t.Fatalf("pre-canceled renewal changed lease: %#v %v", still, err)
	}
	after, err := os.ReadFile(filepath.Join(j.dir, controllerEpochFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("pre-canceled renewal changed durable bytes: %v", err)
	}
	if epoch, err := j.reserve("session"); err != nil || epoch != 4 {
		t.Fatalf("pre-canceled renewal reserved a generation: %d %v", epoch, err)
	}
}

// Fresh empty storage is a positive control. Missing, corrupt, overflowing,
// foreign, insecure and oversized journals never create a writer or rewrite
// retained state; repairing trusted bytes followed by reconcile reopens it.
func TestControllerEpochLossAndCorruptionCloseAdmission(t *testing.T) {
	for _, variant := range []string{"deleted", "corrupt", "overflow", "foreign", "insecure", "oversized"} {
		t.Run(variant, func(t *testing.T) {
			j := epochJournalFixture(t)
			b := &inputTestBackend{}
			original, err := j.admitInput(context.Background(), "session", b)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := original.CurrentLease(context.Background(), "session")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(j.dir, controllerEpochFile)
			trusted, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "deleted":
				err = os.Remove(path)
			case "corrupt":
				err = os.WriteFile(path, []byte("{"), 0600)
			case "overflow":
				raw, _ := json.Marshal(controllerEpochDocument{Version: 1, Namespace: j.namespace, Entries: []controllerEpochEntry{{SessionID: "session", HighWater: ControllerEpoch(math.MaxUint64)}}})
				err = os.WriteFile(path, raw, 0600)
			case "foreign":
				err = os.WriteFile(path, bytes.ReplaceAll(trusted, []byte(j.namespace), []byte("other-board")), 0600)
			case "insecure":
				err = os.Chmod(path, 0644)
			case "oversized":
				err = os.WriteFile(path, append(trusted, bytes.Repeat([]byte(" "), 16<<20)...), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := original.Submit(context.Background(), textInput(lease, "lost", "must not write")); !errors.Is(err, ErrStaleInputController) || b.writeCount() != 0 {
				t.Fatalf("issued writer ignored journal loss: %v writes %d", err, b.writeCount())
			}
			if _, err := original.CurrentLease(context.Background(), "session"); !errors.Is(err, ErrStaleInputController) {
				t.Fatalf("lost journal still exposes lease: %v", err)
			}
			if h, err := j.admitInput(context.Background(), "session", b); err == nil || h != nil || b.writeCount() != 0 {
				t.Fatalf("bad state admitted: %p %v", h, err)
			}
			if _, err := newControllerEpochJournal(j.dir, j.namespace); err == nil {
				t.Fatal("reopen accepted lost/corrupt state")
			}
			if err := os.WriteFile(path, trusted, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := j.reserve("session"); !errors.Is(err, ErrControllerEpochUncertain) {
				t.Fatalf("repair bypassed reconcile: %v", err)
			}
			if err := j.reconcile(); err != nil {
				t.Fatal(err)
			}
			if h, err := j.admitInput(context.Background(), "session", b); err != nil || h == nil {
				t.Fatalf("reconciled positive: %p %v", h, err)
			}
		})
	}
	for _, artifact := range []string{"sessions/record.json", "quarantine/record.json", "notices/pending", "partial-write"} {
		t.Run(artifact, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, artifact)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := newControllerEpochJournal(dir, "board"); !errors.Is(err, ErrControllerEpochUncertain) {
				t.Fatalf("prior state recreated journal: %v", err)
			}
			if raw, err := os.ReadFile(path); err != nil || string(raw) != "retained" {
				t.Fatalf("prior state changed: %q %v", raw, err)
			}
		})
	}
}

// The second independently opened handle cannot load/commit while the first
// holds the commit boundary. Shared coordination and physical fences compose;
// the compound omission control does not prove either fence independently.
// Both completed reservations have distinct epochs.
func TestControllerEpochConcurrentReservationsSerialize(t *testing.T) {
	j := epochJournalFixture(t)
	other, err := newControllerEpochJournal(j.dir, j.namespace)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	j.syncFile = func(f *os.File) error {
		if !isEpochDirectory(t, f) {
			once.Do(func() { close(entered); <-release })
		}
		return f.Sync()
	}
	type result struct {
		epoch ControllerEpoch
		err   error
	}
	first, second := make(chan result, 1), make(chan result, 1)
	go func() { e, err := j.reserve("session"); first <- result{e, err} }()
	<-entered
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	waiting := make(chan struct{})
	var waitOnce sync.Once
	call := &nativeFixtureWaitContext{Context: context.Background(), onDone: func() { waitOnce.Do(func() { close(waiting) }) }}
	go func() { e, err := other.reserveContext(call, "session"); second <- result{e, err} }()
	select {
	case <-waiting:
	case got := <-second:
		t.Fatalf("second reservation crossed locked commit: %#v", got)
	case <-time.After(30 * time.Second):
		t.Fatal("second reservation did not enter held admission")
	}
	select {
	case got := <-second:
		t.Fatalf("second reservation crossed locked commit: %#v", got)
	default:
	}
	unblock.Do(func() { close(release) })
	a, b := <-first, <-second
	if a.err != nil || b.err != nil || a.epoch != 1 || b.epoch != 2 {
		t.Fatalf("reservations: %#v %#v", a, b)
	}
}

func isEpochDirectory(t *testing.T, f *os.File) bool {
	t.Helper()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return info.IsDir()
}

// Real commit ordering is file-sync, rename, directory-sync, mode0600. Every
// injected failure returns no writer and latches admission until reconcile;
// reconciliation preserves attempted/visible high-water instead of reusing it.
// Each JSON commit syncs the file and both directories; any boundary failure
// returns no writer and requires reconciliation before another reservation.
func TestControllerEpochCommitUncertaintyRequiresReconcile(t *testing.T) {
	for _, failure := range []string{"none", "file-sync", "rename", "directory-sync", "source-directory-sync"} {
		t.Run(failure, func(t *testing.T) {
			j := epochJournalFixture(t)
			var events []string
			fault := errors.New("epoch injected commit fault")
			j.syncFile = func(f *os.File) error {
				stage := "file-sync"
				if isEpochDirectory(t, f) {
					stage = "directory-sync"
					if filepath.Clean(f.Name()) == historyguard.TempDirectory(filepath.Join(j.dir, controllerEpochFile)) {
						stage = "source-directory-sync"
					}
				}
				events = append(events, stage)
				if failure == stage {
					return fault
				}
				return f.Sync()
			}
			j.rename = func(a, b string) error {
				events = append(events, "rename")
				if failure == "rename" {
					return fault
				}
				return os.Rename(a, b)
			}
			b := &inputTestBackend{}
			h, err := j.admitInput(context.Background(), "session", b)
			if failure == "none" {
				if err != nil || h == nil {
					t.Fatalf("successful commit refused: %v", err)
				}
				if got := stringsJoinEpoch(events); got != "file-sync,rename,directory-sync,source-directory-sync" {
					t.Fatalf("commit ordering: %s", got)
				}
			} else {
				if !errors.Is(err, fault) || h != nil || b.writeCount() != 0 {
					t.Fatalf("uncertain commit exposed writer: %p %v", h, err)
				}
				j.syncFile = (*os.File).Sync
				j.rename = os.Rename
				if h, err := j.admitInput(context.Background(), "session", b); !errors.Is(err, ErrControllerEpochUncertain) || h != nil {
					t.Fatalf("uncertain admission reopened: %p %v", h, err)
				}
				if err := j.reconcile(); err != nil {
					t.Fatal(err)
				}
				if e, err := j.reserve("session"); err != nil || e != 2 {
					t.Fatalf("attempted generation reused: %d %v", e, err)
				}
			}
			info, err := os.Stat(filepath.Join(j.dir, controllerEpochFile))
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("mode: %v %v", info, err)
			}
		})
	}
}

func stringsJoinEpoch(s []string) string {
	var result string
	for i, v := range s {
		if i != 0 {
			result += ","
		}
		result += v
	}
	return result
}
