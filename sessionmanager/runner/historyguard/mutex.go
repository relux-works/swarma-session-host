package historyguard

import (
	"context"
	"sync"
	"time"
)

// LockMutex keeps in-process serialization under the same cancellation contract
// as the file lock; a peer waiting on disk cannot trap a canceled caller here.
func LockMutex(ctx context.Context, mu *sync.Mutex) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if mu.TryLock() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
