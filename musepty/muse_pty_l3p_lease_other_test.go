//go:build !unix

package musepty

import "testing"

// The L3P r2 lease proof needs flock, so off unix the whole test
// skips at the gate: the lease proof runs where the store leases
// exist. The probe stubs below are unreachable behind the gate and
// skip defensively so the file still compiles.
func museL3PRequireLeaseProbes(t *testing.T) {
	t.Helper()
	t.Skip("the use-lease flock probe needs a unix host")
}

func requireMuseL3PUseLeaseHeld(t *testing.T, frozenRoot string) {
	t.Helper()
	t.Skip("the use-lease flock probe needs a unix host")
}

func requireMuseL3PUseLeaseReleased(t *testing.T, frozenRoot string) {
	t.Helper()
	t.Skip("the use-lease flock probe needs a unix host")
}

func holdMuseL3PUseLeaseExclusive(t *testing.T, frozenRoot string) {
	t.Helper()
	t.Skip("the use-lease flock probe needs a unix host")
}
