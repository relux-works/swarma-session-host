package sessionrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"

	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

var _ core.MuseAccountingRestorer = (*Runtime)(nil)

// ValidateMuseAccountingRestore validates the exact original image and its
// admitted goal journal before physical Restore. Neither a typed carrier nor a
// snapshot reference can create an assignment, receipt or holder authority.
func (r *Runtime) ValidateMuseAccountingRestore(ctx context.Context, record core.RunnerRecord, image core.LegacyRecordInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.Provider != "muse" || record.ProviderHost.Provider != "muse" || record.ProviderHost.Snapshot.Kind != "muse-msp" || record.ProviderHost.Snapshot.MuseMSP == nil || record.SessionID == "" || image.SessionID != record.SessionID {
		return core.ErrLegacyViewStale
	}
	sum := sha256.Sum256(image.Bytes)
	if image.SHA256 != hex.EncodeToString(sum[:]) {
		return core.ErrLegacyViewStale
	}
	var wire struct {
		SessionID    string          `json:"session_id"`
		Provider     string          `json:"provider"`
		ProviderHost json.RawMessage `json:"provider_host"`
	}
	if err := json.Unmarshal(image.Bytes, &wire); err != nil {
		return err
	}
	if wire.SessionID != record.SessionID || wire.Provider != "muse" {
		return core.ErrLegacyViewStale
	}
	state, err := core.NewLegacyRecordCodec().DecodeMuseAccounting(ctx, wire.ProviderHost)
	if err != nil {
		return err
	}
	typed := record.ProviderHost.Snapshot.MuseMSP
	if typed.NativeAccounting == nil || typed.GoalTransport != "native" || typed.SessionID != state.SessionID || typed.WorkspaceRoot != state.WorkspaceRoot || !reflect.DeepEqual(*typed.NativeAccounting, state.LegacyMuseNativeAccounting) {
		return core.ErrLegacyCodecMismatch
	}
	r.mu.Lock()
	f := r.budgets
	store := r.store
	r.mu.Unlock()
	if f == nil || store == nil {
		return budget.ErrAccountingUnavailable
	}
	head, err := f.AssignmentHead(record.SessionID)
	if err != nil {
		return err
	}
	if head.Attempt != nil || head.Rollback != nil || head.Acknowledged != nil {
		return budget.ErrAccountingUncertain
	}
	if state.GoalRef == nil {
		if record.AppliedGoalRef != nil || state.GoalObjective != "" || state.TokenBudget != 0 || state.BudgetPaused || state.BudgetPauseOutcome != nil {
			return budget.ErrAccountingUncertain
		}
		if head.Latest.Ref.Revision > 0 && (head.Latest.State == goal.Active || head.Applied == nil || *head.Applied != head.Latest.Ref) {
			return budget.ErrAccountingUncertain
		}
		return nil
	}
	ref := core.RevisionRef{ID: state.GoalRef.ID, Revision: state.GoalRef.Revision}
	if !state.GoalObserved || record.AppliedGoalRef == nil || *record.AppliedGoalRef != ref || head.Applied == nil || *head.Applied != ref || head.AppliedReceipt == nil || head.AppliedReceipt.SessionID != record.SessionID || head.AppliedReceipt.Epoch == 0 || head.AppliedReceipt.ID == "" || head.Latest.Ref != ref || head.Latest.State != goal.Active || head.Latest.SessionID != record.SessionID || head.Latest.Namespace != goal.Assignment {
		return budget.ErrAccountingUncertain
	}
	limit, err := budget.SelectBudget(&head.Latest.Content.Budget, 0)
	if err != nil {
		return err
	}
	objective := goal.RevisionToken(ref) + " " + strings.TrimSpace(head.Latest.Content.Condition)
	if state.TokenBudget != limit || state.GoalObjective != objective {
		return core.ErrInputAcknowledgementMismatch
	}
	return nil
}

// RestoreMuseAccounting consumes the actual current native Fold and observer
// into the durable sink. Core must call outside its map/epoch locks and before
// goal reconciliation or publishing restored attachment/input access.
func (r *Runtime) RestoreMuseAccounting(ctx context.Context, c core.CapturedOwnerRun) error {
	if err := validateCapture(c); err != nil {
		return err
	}
	if err := r.ValidateMuseAccountingRestore(ctx, c.Record, c.Committed); err != nil {
		return err
	}
	source, state, err := r.museSource(ctx, c)
	if err != nil {
		return err
	}
	observer, ok := c.Input.(core.NativeGoalObserver)
	if !ok {
		return core.ErrHostCapabilityUnavailable
	}
	subscription, ok := c.Input.(core.MuseObservationSource)
	if !ok {
		return core.ErrHostCapabilityUnavailable
	}
	feed, err := subscription.SubscribeMuse(ctx, c.Lease)
	if err != nil {
		return err
	}
	defer feed.Close()
	evidence := &museEvidence{lease: c.Lease, native: state.SessionID, feed: feed, state: state, finals: newFinals()}
	value, err := evidence.next(ctx)
	if err != nil {
		return err
	}
	if value.RestoreFold == nil {
		return core.ErrMuseObservationInvalid
	}
	// The observer is a new real read under this exact controller. It must not
	// lag the complete restore Fold or silently replace the retained objective.
	err = observer.WithNativeGoal(ctx, c.Lease, func(o core.NativeGoalObservation, _ core.HostInputWriter) error {
		if o.SessionID != c.Lease.SessionID || o.Epoch != c.Lease.Epoch || o.Sequence == 0 || o.ObservedAt.IsZero() {
			return core.ErrStaleInputController
		}
		f := value.RestoreFold.Cumulative
		if o.Cumulative == nil || o.Cumulative.TotalTokens < f.TotalTokens || o.Cumulative.PromptTokens < f.PromptTokens || o.Cumulative.OutputTokens < f.OutputTokens {
			return core.ErrMuseObservationUncertain
		}
		if o.Objective != state.GoalObjective {
			return core.ErrInputAcknowledgementMismatch
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Unknown committed canonical observation is not upgraded from native data.
	// Fresh empty-account bootstrap needs its actual core-owned observed commit.
	if !state.GoalObserved {
		return budget.ErrAccountingUnavailable
	}
	if err = r.store.ImportLegacy(ctx, c.Lease, state, observer); err != nil {
		return err
	}
	// Commit through the actual normal decoder/writer/ACK path, outside observer
	// admission. A failed commit retains the sink's pending/uncertain projection.
	return source.CommitBudget(ctx, c.Lease, r.store)
}
