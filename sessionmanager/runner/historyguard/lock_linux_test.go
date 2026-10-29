//go:build linux

package historyguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Cancellation returns with the unrelated holder still locked. Afterward a
// live waiter succeeds; the canceled wait neither owns nor releases that lock.
func TestAdmissionLockWaitHonorsCancellation(t *testing.T) {
	ctx, end := context.WithTimeout(context.Background(), 60*time.Second)
	defer end()
	path := filepath.Join(t.TempDir(), "state.lock")
	release, err := Lock(ctx, path, true)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	unlock := func() { once.Do(release) }
	defer unlock()
	wait, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	var worker sync.WaitGroup
	worker.Add(1)
	defer func() { end(); worker.Wait() }()
	go func() {
		defer worker.Done()
		release, err := Lock(wait, path, false)
		if release != nil {
			release()
		}
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("canceled wait remained blocked")
	}
	unlock()
	release, err = Lock(ctx, path, true)
	if err != nil {
		t.Fatal(err)
	}
	release()
	var mu sync.Mutex
	mu.Lock()
	wait, cancel = context.WithCancel(ctx)
	cancel()
	if err := LockMutex(wait, &mu); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	mu.Unlock()
	if err := LockMutex(ctx, &mu); err != nil {
		t.Fatal(err)
	}
	mu.Unlock()
}

// This test observer preserves the real context and signals only when the lock
// retry evaluates Done. It neither releases a fence nor supplies cancellation.
type inodeWaitContext struct {
	context.Context
	observe func()
}

func (c *inodeWaitContext) Done() <-chan struct{} {
	c.observe()
	return c.Context.Done()
}

// Observing retry entry preserves the exact live parent channel and its real
// cancellation result; the callback cannot substitute a timeout or progress.
func TestInodeWaitContextPreservesParent(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := 0
	call := &inodeWaitContext{Context: parent, observe: func() { observed++ }}
	if call.Done() != parent.Done() || call.Err() != nil || observed != 1 {
		t.Fatal("inode wait observer replaced parent context")
	}
	cancel()
	if call.Done() != parent.Done() || !errors.Is(call.Err(), context.Canceled) || observed != 2 {
		t.Fatal("inode wait observer hid parent cancellation")
	}
}

// Observe actual failed-flock retry entry with an extra descriptor on each
// inode. Replacing only the path cannot let the old descriptor satisfy admission;
// the actual replacement fence remains held until its own retry is observed.
func TestAdmissionLockRechecksInodeAfterWaiting(t *testing.T) {
	ctx, end := context.WithTimeout(context.Background(), 60*time.Second)
	defer end()
	path := filepath.Join(t.TempDir(), "state.lock")
	oldRelease, err := Lock(ctx, path, true)
	if err != nil {
		t.Fatal(err)
	}
	var oldOnce sync.Once
	unlockOld := func() { oldOnce.Do(oldRelease) }
	defer unlockOld()
	oldInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	count := func(target os.FileInfo) int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Error(err)
			return -1
		}
		n := 0
		for _, e := range entries {
			info, err := os.Stat(filepath.Join("/proc/self/fd", e.Name()))
			if err == nil && os.SameFile(target, info) {
				n++
			}
		}
		return n
	}
	initial := count(oldInfo)
	var observationMu sync.Mutex
	target, baseline := oldInfo, initial
	waiting := make(chan struct{})
	var observed *sync.Once = &sync.Once{}
	call := &inodeWaitContext{Context: ctx, observe: func() {
		observationMu.Lock()
		defer observationMu.Unlock()
		// Only this waiter can hold an additional descriptor on the current
		// target inode. Done is evaluated after its failed actual flock.
		if count(target) > baseline {
			observed.Do(func() { close(waiting) })
		}
	}}
	done := make(chan error, 1)
	var worker sync.WaitGroup
	worker.Add(1)
	defer func() { end(); worker.Wait() }()
	go func() {
		defer worker.Done()
		release, err := Lock(call, path, false)
		if release != nil {
			release()
		}
		done <- err
	}()
	select {
	case <-waiting:
	case err := <-done:
		t.Fatalf("waiter did not retain the old inode descriptor: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("waiter did not retain the old inode descriptor")
	}
	if count(oldInfo) <= initial {
		t.Fatal("waiter did not retain the old inode descriptor")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	newRelease, err := Lock(ctx, path, true)
	if err != nil {
		t.Fatal(err)
	}
	var newOnce sync.Once
	unlockNew := func() { newOnce.Do(newRelease) }
	defer unlockNew()
	newInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	observationMu.Lock()
	target, baseline = newInfo, count(newInfo)
	waiting = make(chan struct{})
	observed = &sync.Once{}
	nextWaiting := waiting
	observationMu.Unlock()
	unlockOld()
	select {
	case <-nextWaiting:
	case err := <-done:
		t.Fatalf("old unlinked inode admitted while new inode remained held: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("replacement lock retry was not observed")
	}
	select {
	case err := <-done:
		t.Fatalf("old unlinked inode admitted while new inode remained held: %v", err)
	default:
	}
	unlockNew()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
