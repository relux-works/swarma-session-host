package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
	// Prime relatime: the first read after creation would move atime/ctime
	// during the capture scan itself; after this read atime exceeds mtime and
	// later reads leave the identity untouched, as for an installed binary.
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

// exeDirFD returns a directory FD whose "exe" entry resolves to target, the
// same openat route the capture uses beneath a pinned child proc directory.
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

// Claim: a later observation of the unchanged image reads no image content
// (rchar moves by metadata only), beside the first-scan and changed controls.
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
	mutated := false
	hook := &scriptedContext{Context: context.Background(), onCall: func(n int) {
		if n != 1 || mutated {
			return
		}
		mutated = true
		time.Sleep(5 * time.Millisecond) // guarantee ctime advances past capture stat
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := f.WriteAt([]byte{0x02}, 0); err != nil {
			t.Error(err)
		}
		f.Close()
		// Restore mtime beside a fresh atime; ctime stays moved by both writes.
		if err := os.Chtimes(path, time.Now(), before.ModTime()); err != nil {
			t.Error(err)
		}
	}}
	img, err := captureImage(t, hook, path)
	if !mutated {
		t.Fatal("mutation hook never ran inside the scan")
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
