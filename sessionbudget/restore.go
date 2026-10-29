package sessionbudget

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Restore rebinds this store's retained accounting through the actual current
// host observer. A genuinely fresh native session can start an empty account;
// an existing native objective with no retained account is NOT a fresh session.
// Legacy record hydration must use the codec's lossless accounting projection;
// it cannot be replaced by calling Restore on an empty store.
func (s *Store) Restore(ctx context.Context, lease core.ControllerLease, nativeID string, observer core.NativeGoalObserver) error {
	if nativeID == "" || missing(observer) || lease.SessionID == "" || lease.Epoch == 0 {
		return ErrAccountingUnavailable
	}
	return observer.WithNativeGoal(ctx, lease, func(o core.NativeGoalObservation, _ core.HostInputWriter) error {
		if o.SessionID != lease.SessionID || o.Epoch != lease.Epoch || o.Sequence == 0 || o.ObservedAt.IsZero() {
			return core.ErrStaleInputController
		}
		if !validUsage(o.Cumulative) {
			return ErrAccountingUnavailable
		}
		var refusal error
		err := s.transaction(ctx, true, func(j *journal) error {
			a := j.Sessions[lease.SessionID]
			if a == nil {
				if o.Objective != "" || o.Status != "" {
					return ErrAccountingUnavailable
				}
				u := *o.Cumulative
				j.Sessions[lease.SessionID] = &account{Lease: lease, NativeID: nativeID, Ledger: map[string]*allocation{}, Cumulative: &u}
				return nil
			}
			if a.NativeID != nativeID || lease.Epoch < a.Lease.Epoch {
				return core.ErrStaleInputController
			}
			if a.Uncertain {
				return ErrAccountingUncertain
			}
			if a.Lease != lease && (a.Pending != nil || a.Recovery != nil) {
				return ErrAccountingUncertain
			}
			if a.Current != nil && o.Objective != a.Current.Objective {
				return ErrAccountingUncertain
			}
			if a.Current == nil && (o.Objective != "" || o.Status != "") {
				return ErrAccountingUncertain
			}
			if e := accountUsage(a, o.Cumulative); e != nil {
				if errors.Is(e, ErrCounterRegression) {
					a.Uncertain = true
					refusal = e
					return nil
				}
				return e
			}
			a.Lease = lease
			return nil
		})
		return errors.Join(err, refusal)
	})
}
