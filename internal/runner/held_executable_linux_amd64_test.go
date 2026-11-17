package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// sparseImage creates a mostly-zero regular file of exactly size bytes with
// single-byte patches at the given offsets; it never materializes the zeros.
func sparseImage(t *testing.T, size int64, patches map[int64]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "image")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for off, b := range patches {
		if _, err := f.WriteAt([]byte{b}, off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// Prime relatime: settle the atime update of the first read before the
	// capture scan, so file metadata stays quiescent while scanning, as for
	// an installed binary whose atime already exceeds mtime. An atime-only
	// update does not change ctime, so this is not what protects the scan's
	// before/after identity comparison; it only keeps the fixture quiet.
	if size > 0 {
		r, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.Read(make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

// exeDirFD returns a directory FD whose "exe" entry resolves to target,
// reached with the same openat call the capture uses. Here "exe" is an
// ordinary symlink to the fixture file, not the procfs magic link that the
// production capture opens beneath a pinned child proc directory.
func exeDirFD(t *testing.T, target string) int {
	t.Helper()
	dir := t.TempDir()
	if err := os.Symlink(target, filepath.Join(dir, "exe")); err != nil {
		t.Fatal(err)
	}
	fd, err := syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Close(fd) })
	return fd
}

// referenceDigest is the independent full-file SHA-256, read with io.Copy
// rather than the capture loop.
func referenceDigest(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// scriptedContext reports cancellation from a chosen Err call onward and can
// run a hook inside the scan, after the pre-scan stat has already happened.
type scriptedContext struct {
	context.Context
	calls    int
	failFrom int // 1-based Err call from which to report cancellation; 0 never
	onCall   func(int)
}

func (c *scriptedContext) Err() error {
	c.calls++
	if c.onCall != nil {
		c.onCall(c.calls)
	}
	if c.failFrom > 0 && c.calls >= c.failFrom {
		return context.Canceled
	}
	return nil
}

func captureImage(t *testing.T, ctx context.Context, path string) (heldLinuxImage, error) {
	t.Helper()
	return heldLinuxExecutable(ctx, exeDirFD(t, path))
}

// Claim: a valid image above the old 128 MiB guard captures with the full
// reference digest, and the at/below-guard control is unchanged.
func TestHeldExecutableAdmitsAboveOldGuard(t *testing.T) {
	small := sparseImage(t, 1<<20, map[int64]byte{0: 0x5a})
	img, err := captureImage(t, context.Background(), small)
	if err != nil || img.digest != referenceDigest(t, small) {
		t.Fatal("valid control at/below old guard refused or misdigested", err, img.digest)
	}
	large := sparseImage(t, 129<<20, map[int64]byte{0: 0x5a, (129 << 20) - 1: 0xa5})
	img, err = captureImage(t, context.Background(), large)
	if err != nil {
		t.Fatal("valid image above old guard refused", err)
	}
	if img.digest != referenceDigest(t, large) {
		t.Fatal("above-guard digest differs from full-file reference")
	}
	if img.size != 129<<20 {
		t.Fatal("captured size wrong", img.size)
	}
}

// Claim: two images identical through the first 128 MiB but differing only in
// the tail produce different digests, each equal to its full reference.
func TestHeldExecutableTailBeyondOldGuardChangesDigest(t *testing.T) {
	const size = 192 << 20
	a := sparseImage(t, size, map[int64]byte{0: 0x11, size - 1: 0xa1})
	b := sparseImage(t, size, map[int64]byte{0: 0x11, size - 1: 0xb2})
	imgA, errA := captureImage(t, context.Background(), a)
	imgB, errB := captureImage(t, context.Background(), b)
	if errA != nil || errB != nil {
		t.Fatal("tail-differentiating controls refused", errA, errB)
	}
	if imgA.digest == imgB.digest {
		t.Fatal("tail difference beyond old guard did not change digest (prefix hash)")
	}
	if imgA.digest != referenceDigest(t, a) || imgB.digest != referenceDigest(t, b) {
		t.Fatal("tail digests differ from full-file references")
	}
}

// Claim: exactly 512 MiB is admitted (the guard is > not >=) while 512 MiB+1
// is refused before scanning, proven by refusal far faster than the scan.
func TestHeldExecutableExactBoundAdmittedOverBoundRefusedBeforeScan(t *testing.T) {
	exact := sparseImage(t, 536870912, map[int64]byte{536870911: 0x77})
	start := time.Now()
	img, err := captureImage(t, context.Background(), exact)
	scanElapsed := time.Since(start)
	if err != nil {
		t.Fatal("exactly 512 MiB refused; guard must be > not >=", err)
	}
	if img.digest != referenceDigest(t, exact) {
		t.Fatal("exact-bound digest differs from full-file reference")
	}
	over := sparseImage(t, 536870913, nil)
	start = time.Now()
	img, err = captureImage(t, context.Background(), over)
	refuseElapsed := time.Since(start)
	if err == nil || img.digest != "" {
		t.Fatal("512 MiB+1 admitted with a digest", err, img.digest)
	}
	if refuseElapsed > scanElapsed/2 || refuseElapsed > 2*time.Second {
		t.Fatalf("over-bound refusal took %v vs scan %v; not refused before scanning", refuseElapsed, scanElapsed)
	}
}

// Claim: bytes allocated by one capture stay independent of image size (fixed
// scratch, streaming hash), for a small and a large image alike.
func TestHeldExecutableAllocationsIndependentOfImageSize(t *testing.T) {
	scanAllocs := func(path string) uint64 {
		child := exeDirFD(t, path)
		if _, err := heldLinuxExecutable(context.Background(), child); err != nil {
			t.Fatal("allocation control capture refused", err)
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		if _, err := heldLinuxExecutable(context.Background(), child); err != nil {
			t.Fatal("allocation measured capture refused", err)
		}
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	small := scanAllocs(sparseImage(t, 1<<20, nil))
	large := scanAllocs(sparseImage(t, 256<<20, nil))
	if large > small+(1<<20) || large > 8<<20 {
		t.Fatalf("allocation grew with image size: small=%d large=%d (whole-file buffering)", small, large)
	}
}

func selfRchar(t *testing.T) uint64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "rchar:") {
			n, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "rchar:")), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatal("rchar counter unavailable")
	return 0
}

// Claim: the identity re-check itself reads no image content (rchar moves by metadata only), beside the first-scan and changed controls.
func TestHeldExecutableMatchesReadsNoContent(t *testing.T) {
	const size = 64 << 20
	path := sparseImage(t, size, map[int64]byte{size - 1: 0x3c})
	child := exeDirFD(t, path)
	r0 := selfRchar(t)
	img, err := heldLinuxExecutable(context.Background(), child)
	scanRead := selfRchar(t) - r0
	if err != nil || img.digest != referenceDigest(t, path) {
		t.Fatal("valid control capture failed", err)
	}
	if scanRead < size {
		t.Fatalf("first scan read only %d of %d bytes; counter not observing the scan", scanRead, size)
	}
	r0 = selfRchar(t)
	if err := heldLinuxExecutableMatches(child, img); err != nil {
		t.Fatal("unchanged-image positive refused on re-observation", err)
	}
	againRead := selfRchar(t) - r0
	if againRead > 1<<20 || againRead > scanRead/10 {
		t.Fatalf("re-observation read %d content bytes (first scan %d); re-hash on observation", againRead, scanRead)
	}
	if err := os.Truncate(path, size+1); err != nil {
		t.Fatal(err)
	}
	if err := heldLinuxExecutableMatches(child, img); err == nil {
		t.Fatal("size-changed image accepted on re-observation")
	}
}

// paddedFixtureBinary copies this test binary and sparse-extends the copy to
// at least minSize bytes; trailing zeros do not affect exec, so the copy runs
// as the same fixture child with a known, larger running image.
func paddedFixtureBinary(t *testing.T, minSize int64) (string, int64) {
	t.Helper()
	source, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	path := filepath.Join(t.TempDir(), "fixture-image")
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	written, err := io.Copy(out, in)
	if err != nil {
		t.Fatal(err)
	}
	if minSize > written {
		if err := out.Truncate(minSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	// Settle the relatime atime update before spawn, as in sparseImage.
	r, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, info.Size()
}

// startV2RunnerOnBinary starts a selected runner on binary with the ordinary
// reaper held at a gate, mirroring startV2KernelRunner for a chosen image.
func startV2RunnerOnBinary(t *testing.T, binary string) (*Server, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	cfg := testConfig(t, "echo")
	cfg.IdentifyV2 = true
	cfg.Plan.Binary = binary
	s, err := startWithReaperGate(cfg, captureChildBirth, waitOriginalChild, func() { <-gate })
	if err != nil {
		open()
		t.Fatal(err)
	}
	t.Cleanup(func() { open(); s.Close() })
	trackTestRunner(t, s)
	// Qualify on the captured identity, not the capture error alone: a runner
	// whose image capture failed still holds a valid identity, and tests of
	// that failure must reach their assertions instead of skipping.
	if !validLinuxIdentity(s.linux.identity) {
		open()
		if os.Getenv("SESSION_HOST_REQUIRE_LINUX_PIDFS") == "1" {
			t.Fatalf("REQUIRED_LINUX_PIDFS_NOT_QUALIFIED: %v", s.linux.err)
		}
		t.Skipf("LINUX_PIDFS_NOT_QUALIFIED: %v", s.linux.err)
	}
	return s, open
}

// Claim: a second ProcessEvidence observation of the unchanged running image returns the same evidence while reading no image content.
func TestProcessEvidenceSecondObservationReadsNoContent(t *testing.T) {
	binary, size := paddedFixtureBinary(t, 32<<20)
	want := referenceDigest(t, binary)
	r0 := selfRchar(t)
	s, open := startV2RunnerOnBinary(t, binary)
	defer open()
	spawnRead := selfRchar(t) - r0
	if spawnRead < uint64(size) {
		t.Fatalf("spawn capture read only %d of %d image bytes; counter not observing the capture scan", spawnRead, size)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := s.ProcessEvidence(ctx)
	if err != nil || first.Executable != want {
		t.Fatal("valid positive: first observation evidence lost or misdigested", first, err)
	}
	r0 = selfRchar(t)
	second, err := s.ProcessEvidence(ctx)
	againRead := selfRchar(t) - r0
	if err != nil || second != first {
		t.Fatal("valid positive: second observation of unchanged image refused or changed", second, err)
	}
	if againRead > 1<<20 {
		t.Fatalf("second ProcessEvidence read %d content bytes (spawn scan %d, image %d); observation re-hashed the unchanged image", againRead, spawnRead, size)
	}
	t.Logf("image=%d spawn_read=%d second_observation_read=%d", size, spawnRead, againRead)
}

// Claim: a failed image capture at spawn keeps ownership verified and the original child's ordinary service, with process evidence absent and no second spawn.
func TestImageCaptureFailureKeepsOwnershipAndOrdinaryResult(t *testing.T) {
	binary, size := paddedFixtureBinary(t, 536870913)
	if size != 536870913 {
		t.Fatal("setup: over-bound image has wrong size", size)
	}
	s, open := startV2RunnerOnBinary(t, binary)
	defer open()
	pid := s.pid
	if s.linux.process != nil || s.linux.started != (OwnedProcessEvidence{}) {
		t.Fatal("setup: over-bound image capture unexpectedly produced process evidence")
	}
	snapshot, _ := s.identifyV2Snapshot(v2RequestFor(s))
	if snapshot.Result != "verified" || snapshot.Child == nil || snapshot.Child.PID != pid {
		t.Fatal("ownership snapshot lost after image-capture failure", snapshot)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := s.ProcessEvidence(ctx)
	elapsed := time.Since(start)
	t.Logf("elapsed to absent process evidence: %v", elapsed)
	if !errors.Is(err, errChildBirthUnavailable) {
		t.Fatal("process evidence present after image-capture failure", err)
	}
	if elapsed > 5*time.Second {
		t.Fatal("absent process evidence was not prompt", elapsed)
	}
	client := testClient(t, s.cfg)
	call(t, client, Request{Op: "claim", Execution: s.cfg.Execution, Epoch: 1}, "ok")
	status := call(t, client, Request{Op: "status", Execution: s.cfg.Execution}, "ok")
	if status.PID != pid || status.Exited {
		t.Fatal("ordinary result lost or child replaced after image-capture failure", status)
	}
	if s.pid != pid || s.linux.process != nil {
		t.Fatal("failed capture was retried or a second child was spawned")
	}
	if err := s.child.Kill(); err != nil {
		t.Fatal(err)
	}
	open()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("original reaper did not join after image-capture failure")
	}
}

// Claim: expiry during the scan yields no digest and returns promptly, beside
// the same image succeeding under a live context.
func TestHeldExecutableExpiryMidScanRefuses(t *testing.T) {
	path := sparseImage(t, 64<<20, nil)
	mid := &scriptedContext{Context: context.Background(), failFrom: 50}
	start := time.Now()
	img, err := captureImage(t, mid, path)
	if err == nil || img.digest != "" {
		t.Fatal("mid-scan expiry accepted a digest", err, img.digest)
	}
	if mid.calls < 50 {
		t.Fatal("expiry fired before the scan was under way", mid.calls)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("expired capture did not return promptly")
	}
	expired, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	big := sparseImage(t, 512<<20, nil)
	if img, err := captureImage(t, expired, big); err == nil || img.digest != "" {
		t.Fatal("deadline-expired large capture accepted a digest", err, img.digest)
	}
	img, err = captureImage(t, context.Background(), path)
	if err != nil || img.digest != referenceDigest(t, path) {
		t.Fatal("valid control under live context refused", err)
	}
}

// Claim: a context that expires after the last chunk but before acceptance
// still yields no digest; the scan function itself makes the final check.
func TestHeldExecutableContextCheckedBeforeAcceptingDigest(t *testing.T) {
	// Three 32 KiB chunks: Err calls 1-3 gate the chunks, call 4 can only be
	// the final acceptance check inside the scan function.
	path := sparseImage(t, 3*32<<10, map[int64]byte{0: 0x99})
	late := &scriptedContext{Context: context.Background(), failFrom: 4}
	img, err := captureImage(t, late, path)
	if err == nil || img.digest != "" {
		t.Fatal("digest accepted after context expired at acceptance", err, img.digest)
	}
	live := &scriptedContext{Context: context.Background()}
	img, err = captureImage(t, live, path)
	if err != nil || img.digest != referenceDigest(t, path) {
		t.Fatal("valid control with live context refused", err)
	}
}

// Claim: same-size content mutated during capture with mtime restored (ctime
// moves) is refused, beside the unchanged-image positive.
func TestHeldExecutableMutationDuringCaptureRefused(t *testing.T) {
	path := sparseImage(t, 1<<20, map[int64]byte{0: 0x01})
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeIdentity, ok := heldLinuxImageIdentity(before)
	if !ok {
		t.Fatal("setup: pre-capture image identity unavailable")
	}
	mutated := false
	ctimeMoved := false
	hook := &scriptedContext{Context: context.Background(), onCall: func(n int) {
		if n != 1 || mutated {
			return
		}
		mutated = true
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := f.WriteAt([]byte{0x02}, 0); err != nil {
			t.Error(err)
		}
		f.Close()
		// Restore mtime beside a fresh atime, then confirm by stat that
		// ctime advanced past its pre-capture value, nudging it with
		// Chtimes until the filesystem clock ticks. The loop exits only on
		// an observed ctime change, so a coarse timestamp clock delays
		// setup instead of silently leaving the mutation unapplied.
		deadline := time.Now().Add(5 * time.Second)
		for {
			if err := os.Chtimes(path, time.Now(), before.ModTime()); err != nil {
				t.Error(err)
				return
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Error(err)
				return
			}
			identity, ok := heldLinuxImageIdentity(info)
			if !ok {
				t.Error("setup: mutated image identity unavailable")
				return
			}
			if identity.ctime != beforeIdentity.ctime {
				if identity.size != beforeIdentity.size || identity.mtime != beforeIdentity.mtime {
					t.Error("setup: mutation changed size or mtime beside ctime")
					return
				}
				ctimeMoved = true
				return
			}
			if time.Now().After(deadline) {
				t.Error("setup: ctime did not advance after mutation; the ctime comparison cannot be exercised")
				return
			}
			time.Sleep(time.Millisecond)
		}
	}}
	img, err := captureImage(t, hook, path)
	if !mutated || !ctimeMoved {
		t.Fatal("setup: mutation with a moved ctime never landed inside the scan", mutated, ctimeMoved)
	}
	if err == nil || img.digest != "" {
		t.Fatal("image mutated during capture accepted (ctime not compared)", err, img.digest)
	}
	control := sparseImage(t, 1<<20, map[int64]byte{0: 0x01})
	img, err = captureImage(t, context.Background(), control)
	if err != nil || img.digest != referenceDigest(t, control) {
		t.Fatal("unchanged-image positive refused", err)
	}
}

// Claim: an image truncated after the pre-scan stat (short read) yields no
// digest, beside the untruncated positive.
func TestHeldExecutableShortReadRefused(t *testing.T) {
	path := sparseImage(t, 1<<20, nil)
	truncated := false
	hook := &scriptedContext{Context: context.Background(), onCall: func(n int) {
		if n != 1 || truncated {
			return
		}
		truncated = true
		if err := os.Truncate(path, 1<<19); err != nil {
			t.Error(err)
		}
	}}
	img, err := captureImage(t, hook, path)
	if !truncated {
		t.Fatal("truncate hook never ran inside the scan")
	}
	if err == nil || img.digest != "" {
		t.Fatal("short-read image accepted a digest", err, img.digest)
	}
	control := sparseImage(t, 1<<20, nil)
	img, err = captureImage(t, context.Background(), control)
	if err != nil || img.digest != referenceDigest(t, control) {
		t.Fatal("untruncated positive refused", err)
	}
}

// Claim: zero-length and nonregular images are refused without a digest,
// beside the regular nonempty positive.
func TestHeldExecutableZeroAndNonRegularRefused(t *testing.T) {
	zero := sparseImage(t, 0, nil)
	if img, err := captureImage(t, context.Background(), zero); err == nil || img.digest != "" {
		t.Fatal("zero-length image accepted", err, img.digest)
	}
	if img, err := captureImage(t, context.Background(), t.TempDir()); err == nil || img.digest != "" {
		t.Fatal("directory image accepted", err, img.digest)
	}
	if img, err := captureImage(t, context.Background(), "/dev/null"); err == nil || img.digest != "" {
		t.Fatal("character-device image accepted", err, img.digest)
	}
	control := sparseImage(t, 4096, map[int64]byte{0: 0x42})
	img, err := captureImage(t, context.Background(), control)
	if err != nil || img.digest != referenceDigest(t, control) {
		t.Fatal("regular nonempty positive refused", err)
	}
}
