package sessionbudget

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// LegacyImages is the host's existing provider-image transaction boundary.
// Read returns the actual runner native identity separately from its image.
// Both calls must admit the exact current controller lease; CAS compares the
// entire original image through the normal host decode/commit/recovery path.
// A raw file writer cannot substitute for that admitted transaction. This
// port grants no authority and performs no RPC.
type LegacyImages interface {
	ReadMuseImage(context.Context, core.ControllerLease) (string, []byte, error)
	CompareAndSwapMuseImage(context.Context, core.ControllerLease, []byte, []byte) error
}

// LegacySource hydrates and persists the real codec-owned accounting group.
// Runtime composition supplies actual admitted record access; absence refuses.
// Record access never occurs inside the native observer/writer callback.
type LegacySource struct {
	Codec  *core.LegacyRecordCodec
	Images LegacyImages
}

func (s *LegacySource) image(ctx context.Context, l core.ControllerLease) ([]byte, core.LegacyMuseAccountingSnapshot, error) {
	if s == nil || s.Codec == nil || missing(s.Images) {
		return nil, core.LegacyMuseAccountingSnapshot{}, ErrAccountingUnavailable
	}
	native, raw, e := s.Images.ReadMuseImage(ctx, l)
	if e != nil {
		return nil, core.LegacyMuseAccountingSnapshot{}, e
	}
	state, e := s.Codec.DecodeMuseAccounting(ctx, raw)
	if e != nil {
		return nil, state, e
	}
	if native == "" || state.SessionID != native {
		return nil, state, core.ErrInputAcknowledgementMismatch
	}
	return raw, state, nil
}
func (s *LegacySource) HydrateBudget(ctx context.Context, l core.ControllerLease, o core.NativeGoalObserver, store *Store) error {
	_, state, e := s.image(ctx, l)
	if e != nil {
		return e
	}
	return store.ImportLegacy(ctx, l, state, o)
}
func (s *LegacySource) PrepareBudget(ctx context.Context, l core.ControllerLease, r goal.Revision, store *Store) error {
	raw, base, e := s.image(ctx, l)
	if e != nil {
		return e
	}
	prior, next, e := store.legacyProjection(ctx, l)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(base, prior) {
		return ErrAccountingUncertain
	}
	if r.State == goal.Active {
		limit, e := SelectBudget(&r.Content.Budget, 0)
		if e != nil {
			return e
		}
		next.GoalRef = &core.LegacyGoalRef{ID: r.Ref.ID, Revision: r.Ref.Revision}
		next.GoalObjective = goal.RevisionToken(r.Ref) + " " + strings.TrimSpace(r.Content.Condition)
		next.GoalStatus = "active"
		next.TokenBudget = limit
		next.GoalObserved = true
	} else {
		next.GoalRef = nil
		next.GoalObjective = ""
		next.GoalStatus = ""
		next.TokenBudget = 0
		next.BudgetPaused = false
		next.ExternalPause = false
		next.BudgetPauseOutcome = nil
	}
	// The real codec validates its preserved reference/wire contract. No local
	// alias, fabricated goal ID, or replacement schema can make this pass.
	_, e = s.Codec.EncodeMuseAccounting(ctx, raw, next)
	return e
}
func (s *LegacySource) CommitBudget(ctx context.Context, l core.ControllerLease, store *Store) error {
	raw, base, e := s.image(ctx, l)
	if e != nil {
		return e
	}
	prior, next, e := store.legacyProjection(ctx, l)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(base, prior) {
		return ErrAccountingUncertain
	}
	updated, e := s.Codec.EncodeMuseAccounting(ctx, raw, next)
	if e != nil {
		return e
	}
	if e = s.Images.CompareAndSwapMuseImage(ctx, l, raw, updated); e != nil {
		return e
	}
	return store.transaction(ctx, true, func(j *journal) error {
		a := j.Sessions[l.SessionID]
		if a == nil || a.Lease != l {
			return core.ErrStaleInputController
		}
		if !reflect.DeepEqual(a.Legacy, &prior) {
			return ErrAccountingUncertain
		}
		actual, e := projectLegacy(a)
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(actual, next) {
			return ErrAccountingUncertain
		}
		copy, e := cloneLegacy(next)
		if e != nil {
			return e
		}
		a.Legacy = &copy
		a.SnapshotPending = false
		return nil
	})
}
func cloneLegacy(s core.LegacyMuseAccountingSnapshot) (core.LegacyMuseAccountingSnapshot, error) {
	var out core.LegacyMuseAccountingSnapshot
	b, e := json.Marshal(s)
	if e == nil {
		e = json.Unmarshal(b, &out)
	}
	return out, e
}
func (s *Store) ImportLegacy(ctx context.Context, l core.ControllerLease, state core.LegacyMuseAccountingSnapshot, observer core.NativeGoalObserver) error {
	if l.SessionID == "" || l.Epoch == 0 || state.SessionID == "" || state.GoalTransport != "native" || missing(observer) || state.TokenBudget < 0 || state.TokenUsageTotal < 0 || state.TokenUsageBaseline < 0 || state.TokenUsageBaseline > state.TokenUsageTotal || !state.GoalObserved {
		return ErrAccountingUnavailable
	}
	if state.GoalRef != nil && (state.GoalRef.ID == "" || state.GoalRef.Revision <= 0 || state.GoalObjective == "") {
		return ErrAccountingUnavailable
	}
	if state.GoalRef == nil && state.TokenBudget != 0 {
		return ErrAccountingUnavailable
	}
	copy, e := cloneLegacy(state)
	if e != nil {
		return e
	}
	return observer.WithNativeGoal(ctx, l, func(o core.NativeGoalObservation, _ core.HostInputWriter) error {
		if o.SessionID != l.SessionID || o.Epoch != l.Epoch || o.Sequence == 0 || o.ObservedAt.IsZero() {
			return core.ErrStaleInputController
		}
		if o.Objective != state.GoalObjective {
			return core.ErrInputAcknowledgementMismatch
		}
		if !validUsage(o.Cumulative) {
			return ErrAccountingUnavailable
		}
		if o.Cumulative.TotalTokens < state.TokenUsageTotal {
			e := s.transaction(ctx, true, func(j *journal) error {
				a := j.Sessions[l.SessionID]
				if a == nil {
					a = &account{Lease: l, NativeID: state.SessionID, Ledger: map[string]*allocation{}, Legacy: &copy}
					j.Sessions[l.SessionID] = a
				} else if a.NativeID != state.SessionID || l.Epoch < a.Lease.Epoch {
					return core.ErrStaleInputController
				}
				a.Uncertain = true
				return nil
			})
			return errors.Join(ErrCounterRegression, e)
		}
		return s.transaction(ctx, true, func(j *journal) error {
			a := j.Sessions[l.SessionID]
			if a != nil {
				if a.NativeID != state.SessionID || l.Epoch < a.Lease.Epoch {
					return core.ErrStaleInputController
				}
				if a.Uncertain {
					return ErrAccountingUncertain
				}
				if a.Lease != l && (a.Pending != nil || a.Recovery != nil) {
					return ErrAccountingUncertain
				}
				if a.Legacy == nil {
					return ErrAccountingUncertain
				}
				projected, e := projectLegacy(a)
				if e != nil {
					return e
				}
				if !reflect.DeepEqual(*a.Legacy, state) && !reflect.DeepEqual(projected, state) {
					return ErrAccountingUncertain
				}
				if a.Current != nil && a.Current.Objective != o.Objective {
					return ErrAccountingUncertain
				}
				if e = accountUsage(a, o.Cumulative); e != nil {
					return e
				}
				a.Lease = l
				a.Legacy = &copy
				return nil
			}
			u := *o.Cumulative
			a = &account{Lease: l, NativeID: state.SessionID, Cumulative: &u, Ledger: map[string]*allocation{}, Legacy: &copy}
			if state.GoalRef != nil {
				v := &allocation{Ref: core.RevisionRef{ID: state.GoalRef.ID, Revision: state.GoalRef.Revision}, Objective: state.GoalObjective, Limit: state.TokenBudget, Baseline: state.TokenUsageBaseline, BudgetPaused: state.BudgetPaused, ExternalPause: state.ExternalPause, NativeStatus: o.Status}
				if facts, ok := state.GoalLedger[v.Objective]; ok {
					v.Spend = facts.Spend
				}
				if state.BudgetPauseOutcome != nil {
					v.PauseCode = state.BudgetPauseOutcome.Code
					v.PauseStatus = state.BudgetPauseOutcome.NativeStatus
				}
				if o.Status == "active" {
					v.BudgetPaused = false
					v.ExternalPause = false
					if v.PauseCode == "budget_pause_not_applicable" {
						v.PauseCode = ""
						v.PauseStatus = ""
					}
				}
				a.Current = v
				if facts, ok := state.GoalLedger[v.Objective]; ok {
					entry := copyAllocation(v)
					entry.Baseline = facts.Baseline
					entry.Spend = facts.Spend
					entry.BudgetPaused = facts.BudgetPaused
					entry.ExternalPause = facts.ExternalPause
					entry.PauseCode = ""
					entry.PauseStatus = ""
					if facts.PauseOutcome != nil {
						entry.PauseCode = facts.PauseOutcome.Code
						entry.PauseStatus = facts.PauseOutcome.NativeStatus
					}
					a.Ledger[v.Objective] = entry
				}
			}
			if e = a.validate(); e != nil {
				return e
			}
			j.Sessions[l.SessionID] = a
			return nil
		})
	})
}
func (s *Store) legacyProjection(ctx context.Context, l core.ControllerLease) (prior, next core.LegacyMuseAccountingSnapshot, err error) {
	err = s.transaction(ctx, false, func(j *journal) error {
		a := j.Sessions[l.SessionID]
		if a == nil || a.Lease != l {
			return core.ErrStaleInputController
		}
		if a.Legacy == nil {
			return ErrAccountingUnavailable
		}
		var e error
		prior, e = cloneLegacy(*a.Legacy)
		if e != nil {
			return e
		}
		next, e = projectLegacy(a)
		return e
	})
	return
}

// LegacySnapshot returns the actual codec type, retaining the original native
// field group and untouched historical facts. It supplies no runtime admission.
func (s *Store) LegacySnapshot(ctx context.Context, l core.ControllerLease) (core.LegacyMuseAccountingSnapshot, error) {
	_, next, e := s.legacyProjection(ctx, l)
	return next, e
}
func projectLegacy(a *account) (core.LegacyMuseAccountingSnapshot, error) {
	if a.Legacy == nil {
		return core.LegacyMuseAccountingSnapshot{}, ErrAccountingUnavailable
	}
	if a.Uncertain {
		return core.LegacyMuseAccountingSnapshot{}, ErrAccountingUncertain
	}
	out, e := cloneLegacy(*a.Legacy)
	if e != nil {
		return out, e
	}
	if a.Cumulative == nil {
		return out, ErrAccountingUnavailable
	}
	out.TokenUsageTotal = a.Cumulative.TotalTokens
	if v := a.Current; v != nil {
		out.GoalRef = &core.LegacyGoalRef{ID: v.Ref.ID, Revision: v.Ref.Revision}
		out.GoalObjective = v.Objective
		out.GoalStatus = v.NativeStatus
		out.TokenBudget = v.Limit
		out.TokenUsageBaseline = v.Baseline
		out.BudgetPaused = v.BudgetPaused
		out.ExternalPause = v.ExternalPause
		out.BudgetPauseOutcome = legacyOutcome(v)
	} else if a.Legacy.GoalRef != nil {
		out.GoalRef = nil
		out.GoalObjective = ""
		out.GoalStatus = ""
		out.TokenBudget = 0
		out.TokenUsageBaseline = out.TokenUsageTotal
		out.BudgetPaused = false
		out.ExternalPause = false
		out.BudgetPauseOutcome = nil
	}
	if out.GoalLedger == nil && len(a.Ledger) > 0 {
		out.GoalLedger = map[string]core.LegacyMuseGoalRevisionFacts{}
	}
	for key := range a.Retired {
		delete(out.GoalLedger, key)
	}
	for key, v := range a.Ledger {
		out.GoalLedger[key] = core.LegacyMuseGoalRevisionFacts{Baseline: v.Baseline, Spend: v.Spend, PauseOutcome: legacyOutcome(v), BudgetPaused: v.BudgetPaused, ExternalPause: v.ExternalPause}
	}
	return out, nil
}
func legacyOutcome(v *allocation) *core.LegacyMuseBudgetPauseOutcome {
	if v.PauseCode == "" {
		return nil
	}
	return &core.LegacyMuseBudgetPauseOutcome{Code: v.PauseCode, NativeStatus: v.PauseStatus}
}

var _ AccountingSource = (*LegacySource)(nil)
var _ accountingPreparer = (*LegacySource)(nil)
var _ accountingCommitter = (*LegacySource)(nil)
