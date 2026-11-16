//go:build darwin

package sessionhost

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
)

// These are detector controls for the two descriptor oracles used by the
// custody tests. They use only anonymous pipes of this process: no host, no
// runner, no child. A control that does not reproduce its known failure is an
// inconclusive control and fails; it is never a verdict about a product leak.

// debugFreshPipeAlias creates unrelated pipes in batches, each batch held open
// together and then closed, and reports whether any end of a new pipe carries
// the wanted identity, with the number of pipes created so far.
func debugFreshPipeAlias(t *testing.T, want debugInode, rounds, batch int) (int, bool) {
	t.Helper()
	made := 0
	for round := 0; round < rounds; round++ {
		var open []*os.File
		found := false
		for i := 0; i < batch && !found; i++ {
			r, w, err := os.Pipe()
			if err != nil {
				for _, f := range open {
					f.Close()
				}
				t.Fatal(err)
			}
			open = append(open, r, w)
			made++
			found = debugInodeOf(t, r) == want || debugInodeOf(t, w) == want
		}
		for _, f := range open {
			f.Close()
		}
		if found {
			return made, true
		}
	}
	return made, false
}

// Claim: in a process that holds live pipes next to closed descriptor slots,
// the listing that also stats every entry by path (os.ReadDir on /dev/fd)
// fails as a whole with EBADF, while the names-only census used by the custody
// test still reports an owned live pipe in the same state.
// Limit: it reproduces the failure class, not its cause; which descriptor the
// listing could not stat is logged, not asserted. It says nothing about
// descriptor inheritance by any child.
func TestDescriptorListingByPathFailsWhereNamesCensusWorks(t *testing.T) {
	var readers, writers []*os.File
	for i := 0; i < 8; i++ {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { w.Close() })
		readers, writers = append(readers, r), append(writers, w)
	}
	// Closing every read end after all pipes exist leaves closed slots between
	// the live write ends.
	for _, r := range readers {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	_, err := os.ReadDir("/dev/fd")
	if !errors.Is(err, syscall.EBADF) {
		t.Fatalf("known failure of the by-path listing not reproduced: got %v, want EBADF; the control is inconclusive", err)
	}
	t.Logf("by-path listing failed as expected: %v", err)
	var failed *os.PathError
	if errors.As(err, &failed) {
		if fd, convErr := strconv.Atoi(filepath.Base(failed.Path)); convErr == nil {
			var st syscall.Stat_t
			if statErr := syscall.Fstat(fd, &st); statErr != nil {
				t.Logf("after the listing closed, descriptor %d answers fstat with: %v", fd, statErr)
			} else {
				t.Logf("after the listing closed, descriptor %d is open: type %s, mode %#o", fd, debugProbeType(uint32(st.Mode)), uint32(st.Mode))
			}
		}
	}
	// Nearby positive: the names-only census finds the owned live write end,
	// matched by identity and not by a fixed descriptor number.
	want, found := debugInodeOf(t, writers[0]), false
	for _, own := range debugOwnDescriptors(t) {
		if own.Type == "fifo" && own.Inode == want {
			found = true
		}
	}
	if !found {
		t.Fatal("names-only census did not report the owned live pipe")
	}
}

// Claim: the device/inode pair saved from a pipe whose ends are all closed can
// be carried by an unrelated pipe created afterwards, so such a saved pair
// cannot show that a later descriptor was inherited.
// Limit: the search is bounded; no alias within the bound is an inconclusive
// control and fails, it is not evidence that identities are never reused.
func TestRetiredPipeIdentityIsReusedByFreshPipe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	retired := debugInodeOf(t, w)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	made, found := debugFreshPipeAlias(t, retired, 128, 32)
	if !found {
		t.Fatalf("retired pipe identity not reused within %d fresh pipes; the control is inconclusive", made)
	}
	t.Logf("retired pipe identity reused by an unrelated pipe after %d fresh pipes", made)
}

// Claim: while one end of a pipe is retained through the same helper the
// cleanup test uses, with both original ends closed, no unrelated fresh pipe
// carries its identity, and the retained end keeps its type and identity.
// Limit: a bounded search over anonymous pipes of one process; it supports the
// lifetime premise of the retained endpoint, not the subordinate leak check.
func TestRetainedPipeIdentityIsNotReused(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	held := debugHoldEndpoint(t, w)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	held.live(t)
	if made, found := debugFreshPipeAlias(t, held.inode, 16, 32); found {
		t.Fatalf("an unrelated pipe carries the identity of a retained open endpoint after %d fresh pipes", made)
	}
	held.live(t)
}
