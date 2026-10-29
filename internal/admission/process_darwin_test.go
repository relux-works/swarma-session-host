//go:build darwin && cgo

package admission

import (
	"context"
	"os"
	"testing"
	"time"
)

// Two kernel observations identify this owned test process with its real birth,
// effective UID, boot UUID and code image. This is no PID-reuse/exec-race proof.
func TestOwnedKernelIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := ObserveProcess(ctx, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	second, err := ObserveProcess(ctx, os.Getpid())
	if err != nil || first != second || first.UID != uint32(os.Geteuid()) || !validProcess(first) {
		t.Fatalf("unstable owned identity: %v", err)
	}
	for _, pid := range []int{0, -1, 1 << 40} {
		if _, err := ObserveProcess(ctx, pid); err == nil {
			t.Fatalf("invalid pid %d observed", pid)
		}
	}
	expired, cancel2 := context.WithCancel(ctx)
	cancel2()
	if _, err := ObserveProcess(expired, os.Getpid()); err == nil {
		t.Fatal("expired observation succeeded")
	}
}
