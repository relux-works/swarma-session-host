//go:build unix

package hostdefaults

import (
	"context"
	"fmt"
	"sync"

	"github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// NewGoalRuntime supplies the physical operations for sessiongoal's HTTP
// handler. That handler retains Service's independently verified caller fence;
// this internal adapter does not admit callers or derive grants from owners.
func NewGoalRuntime(host *core.SessionHost, gate core.GoalGate) (sessiongoal.Protocol8Runtime, error) {
	if host == nil {
		return nil, fmt.Errorf("goal runtime requires the actual session host")
	}
	return goalRuntime{host: host, gate: gate}, nil
}

type goalRuntime struct {
	host *core.SessionHost
	gate core.GoalGate
}

func (r goalRuntime) WithGoalSession(ctx context.Context, owner core.OwnerKey, fn func(sessiongoal.Protocol8GoalSession) error) error {
	if fn == nil {
		return sessiongoal.ErrBindingUnavailable
	}
	// This observation performs no binding, hydration or journal write. The
	// transport verifies the current exact-session holder before any closure.
	view, captured, live, err := r.host.OwnerSession(ctx, owner)
	if err != nil {
		return err
	}
	if !live || view.SessionID == "" || view.Owner != owner || captured.SessionID != view.SessionID {
		return sessiongoal.ErrBindingUnavailable
	}
	// Close and join captured operations before returning the callback scope.
	// No operation capability may be retained beyond the verified HTTP call.
	var operations sync.Mutex
	open := true
	defer func() { operations.Lock(); open = false; operations.Unlock() }()
	invoke := func(operation func() error) error {
		operations.Lock()
		defer operations.Unlock()
		if !open {
			return core.ErrInputScopeClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return operation()
	}
	refresh := func() (core.SessionView, error) {
		next, lease, running, err := r.host.OwnerSession(ctx, owner)
		if err != nil {
			return core.SessionView{}, err
		}
		if !running || lease != captured || next.SessionID != view.SessionID || next.Owner != owner {
			return core.SessionView{}, core.ErrStaleInputController
		}
		return next, nil
	}
	return fn(sessiongoal.Protocol8GoalSession{
		Session: view, Gate: r.gate, Live: live,
		Reconcile: func() (core.SessionView, error) {
			var next core.SessionView
			err := invoke(func() error {
				if err := r.host.WithOwnerGoalBinding(ctx, owner, captured, func(scope core.OwnerGoalBinding) error {
					_, err := scope.Reconcile()
					return err
				}); err != nil {
					return err
				}
				var err error
				next, err = refresh()
				return err
			})
			if err != nil {
				return core.SessionView{}, err
			}
			// WithOwnerGoalBinding has expired the binding and committed/read back
			// its applied metadata before the refreshed projection is obtained.
			return next, nil
		},
		Rollback: func(env *core.GoalEnvelope) error {
			return invoke(func() error {
				return r.host.WithOwnerGoalBinding(ctx, owner, captured, func(scope core.OwnerGoalBinding) error {
					return scope.Binding.Rollback(env)
				})
			})
		},
		BeginActivation: func() (string, bool, error) {
			var id string
			var live bool
			err := invoke(func() error {
				var err error
				id, live, err = r.host.BeginOwnerActivation(ctx, owner, captured)
				return err
			})
			return id, live, err
		},
		EndActivation: func(id string) error {
			return invoke(func() error {
				if _, err := refresh(); err != nil {
					return err
				}
				return r.host.EndOwnerActivation(owner, id)
			})
		},
	})
}
