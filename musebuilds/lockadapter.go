package musebuilds

import (
	"context"

	"github.com/relux-works/swarma-session-host/musebuilds/minilock"
)

// minilockAdapter binds the production minilock client to the
// HostLock seam. Go interfaces match structurally but not
// covariantly, so the client's concrete hold type needs this one
// forwarding step; it adds no behaviour.
type minilockAdapter struct {
	client *minilock.Client
}

func (a minilockAdapter) Acquire(ctx context.Context) (HostLockHeld, error) {
	held, err := a.client.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return minilockHeldAdapter{held: held}, nil
}

type minilockHeldAdapter struct {
	held *minilock.Held
}

func (a minilockHeldAdapter) Check() error {
	return a.held.Check()
}

func (a minilockHeldAdapter) Release() error {
	return a.held.Release()
}

func productionLocker(scriptPath string, home string) HostLock {
	return minilockAdapter{client: &minilock.Client{ScriptPath: scriptPath, Owner: minilock.OwnerName, AccountHome: home}}
}
