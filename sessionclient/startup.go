package sessionclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultConnectOrStartTimeout = 15 * time.Second

var connectOrStartLocks sync.Map

type startupRuntime struct {
	options StartupOptions
	reason  ManagerReplacementReason
}

// ConnectOrStart transfers the original per-namespace startup arbitration.
// The supplied source owns launch/current-holder authorization and effects;
// this package neither provisions identity nor signals an arbitrary PID.
func ConnectOrStart(ctx context.Context, layout StartupLayout, options StartupOptions) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStartupLayout(layout); err != nil {
		return nil, err
	}
	r := &startupRuntime{options: options}
	return r.connectOrStart(ctx, layout)
}

func (r *startupRuntime) connectOrStart(
	ctx context.Context,
	layout StartupLayout,
) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultConnectOrStartTimeout)
		defer cancel()
	}
	var launch func() error
	if r.options.Source != nil {
		launch = func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := r.options.Source.Launch(ctx, layout); err != nil {
				return &startupSourceError{cause: err}
			}
			return nil
		}
	}
	lockValue, _ := connectOrStartLocks.LoadOrStore(layout.Namespace, &sync.Mutex{})
	launchLock := lockValue.(*sync.Mutex)
	if err := lockStartup(ctx, launchLock); err != nil {
		return nil, err
	}
	defer launchLock.Unlock()

	if client, _, err := r.dialHealthyManager(ctx, layout); err == nil {
		return client, nil
	} else {
		if startupFatal(err) {
			return nil, err
		}
		var upgrade *ProtocolUpgradeError
		if errors.As(err, &upgrade) {
			if launch == nil {
				return nil, upgrade
			}
			if !upgrade.RequiresReplacement() {
				return nil, upgrade
			}
			// The gate runs on MEETING a skewed daemon, not on reaching the
			// signal. A hosted caller has no business inspecting the lock, the
			// drain endpoint or the process table of a daemon it must not
			// replace, and the acceptance criterion is about the call, not
			// about how far into the takeover it managed to get.
			if upgrade.BuildSkew {
				r.logManagerBuildSkew(layout, upgrade)
			}
			if refusal := r.guardManagerReplacementForUpgrade(ctx, layout, upgrade); refusal != nil {
				return nil, refusal
			}
			takeover, takeoverErr := r.takeOverOutdatedManager(ctx, layout, upgrade)
			if takeoverErr != nil {
				// The replacement gate's refusal is the answer, not a failed
				// attempt at one. Wrapping it in "automatic upgrade failed;
				// stop the recorded manager and retry" would tell a hosted
				// caller to do by hand the exact thing this refusal exists to
				// prevent, so it travels unwrapped.
				var refused *ManagerReplacementRefusedError
				if errors.As(takeoverErr, &refused) {
					return nil, takeoverErr
				}
				return nil, fmt.Errorf("automatic Session Manager protocol upgrade failed; stop the recorded manager and retry: %w", takeoverErr)
			}
			if takeover == nil {
				return nil, fmt.Errorf("Session Manager protocol changed while preparing its upgrade; retry the wrapper")
			}
			return r.connectAfterManagerUpgrade(ctx, layout, launch, takeover)
		}
	}
	if launch == nil {
		return nil, ErrStartupAuthorityUnavailable
	}
	healthyClient, takeover, err := r.takeOverUnresponsiveManager(ctx, layout)
	if err != nil {
		return nil, err
	}
	if healthyClient != nil {
		return healthyClient, nil
	}
	launchErr := launch()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		client, status, dialErr := r.dialHealthyManager(ctx, layout)
		if dialErr == nil {
			if takeover != nil {
				if status.PID == takeover.old.PID &&
					status.InstanceID == takeover.old.InstanceID {
					client.Close()
					return nil, fmt.Errorf(
						"stale session manager holder pid %d remained active after takeover",
						takeover.old.PID,
					)
				}
				r.logStaleInstanceTakeover(layout, takeover.old, status)
			}
			return client, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(
				fmt.Errorf("waiting for session manager: %w", err),
				launchErr,
			)
		}
		healthyClient, nextTakeover, takeoverErr := r.takeOverUnresponsiveManager(ctx, layout)
		if takeoverErr != nil {
			return nil, takeoverErr
		}
		if healthyClient != nil {
			return healthyClient, nil
		}
		if nextTakeover != nil {
			takeover = nextTakeover
			launchErr = errors.Join(launchErr, launch())
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(
				fmt.Errorf("waiting for session manager: %w", ctx.Err()),
				launchErr,
			)
		case <-ticker.C:
		}
	}
}

func (r *startupRuntime) connectAfterManagerUpgrade(
	ctx context.Context,
	layout StartupLayout,
	launch func() error,
	takeover *protocolUpgradeTakeover,
) (*Client, error) {
	launchErr := launch()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		client, status, dialErr := r.dialHealthyManager(ctx, layout)
		if dialErr == nil {
			if status.PID == takeover.old.PID && status.InstanceID == takeover.old.InstanceID {
				client.Close()
				return nil, fmt.Errorf("outdated session manager pid %d remained active after upgrade", takeover.old.PID)
			}
			r.logProtocolUpgrade(layout, takeover, status)
			return client, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(fmt.Errorf("waiting for upgraded session manager: %w", err), launchErr)
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(fmt.Errorf("waiting for upgraded session manager: %w", ctx.Err()), launchErr)
		case <-ticker.C:
		}
	}
}

// StartupStatus reads the original startup/status identity without inventing
// launch, owner or provider authority from those response fields.
func (c *Client) StartupStatus(ctx context.Context) (StartupStatus, error) {
	var status StartupStatus
	err := c.DoRequest(ctx, "GET", "/v1/status", nil, &status, true)
	return status, err
}
