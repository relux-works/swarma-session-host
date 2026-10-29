package sessionrun

import (
	"context"
	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// AccountingSource supplies the actual local host image adapter to the budget
// factory. BindHost must precede Recover/Attach/serving, never snapshot authority.
func (r *Runtime) AccountingSource() *budget.LegacySource {
	return &budget.LegacySource{Codec: core.NewLegacyRecordCodec(), Images: r}
}

// BindBudget selects the same factory used by the host's GoalBindings option.
// No parallel accounting binding is allocated by the renderer.
func (r *Runtime) BindBudget(f *budget.Factory) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f == nil || r.budgets != nil {
		return core.ErrInvalidInput
	}
	r.budgets = f
	return nil
}
func (r *Runtime) ReadMuseImage(ctx context.Context, l core.ControllerLease) (string, []byte, error) {
	r.mu.Lock()
	h := r.host
	r.mu.Unlock()
	if h == nil {
		return "", nil, core.ErrHostClosed
	}
	c, e := h.OwnerRunState(ctx, l)
	if e != nil {
		return "", nil, e
	}
	if c.Record.Provider != "muse" {
		return "", nil, core.ErrHostCapabilityUnavailable
	}
	records, e := core.NewLegacyMuseAccountingRecords(h)
	if e != nil {
		return "", nil, e
	}
	return (&capturedMuseImages{records: records, capture: c}).ReadMuseImage(ctx, l)
}
func (r *Runtime) CompareAndSwapMuseImage(ctx context.Context, l core.ControllerLease, a, b []byte) error {
	r.mu.Lock()
	h := r.host
	r.mu.Unlock()
	if h == nil {
		return core.ErrHostClosed
	}
	records, e := core.NewLegacyMuseAccountingRecords(h)
	if e != nil {
		return e
	}
	return records.CompareAndSwap(ctx, l, a, b)
}
func (r *Runtime) checkBudget(ctx context.Context, l core.ControllerLease) error {
	r.mu.Lock()
	f := r.budgets
	r.mu.Unlock()
	if f == nil {
		return budget.ErrAccountingUnavailable
	}
	return f.CheckBudget(ctx, l)
}
