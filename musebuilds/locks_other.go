//go:build !unix

package musebuilds

import (
	"context"
	"errors"
)

// Freeze refuses before reaching the locks on platforms without
// advisory flock, so these stubs are unreachable in production.
func acquireMaintenanceLeaseAt(ctx context.Context, root *pinnedDir) (func() error, error) {
	return nil, errors.New("maintenance leases need unix flock")
}

func acquireStateLockAt(ctx context.Context, root *pinnedDir) (func() error, error) {
	return nil, errors.New("metadata locks need unix flock")
}
