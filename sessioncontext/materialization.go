package sessioncontext

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

const MaterializationContractVersion = "task-board-context-materialization-v1"

var (
	materializationDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	materializationRunIDPattern  = regexp.MustCompile(`^RUN-[A-Za-z0-9][A-Za-z0-9-]*$`)
)

// MaterializationState is the machine-local lifecycle of one provider handle.
type MaterializationState = sm.MaterializationState

const (
	MaterializationMaterializing MaterializationState = "materializing"
	MaterializationReady         MaterializationState = "ready"
	MaterializationLeased        MaterializationState = "leased"
	MaterializationDetached      MaterializationState = "detached"
	MaterializationOrphaned      MaterializationState = "orphaned"
	MaterializationQuarantined   MaterializationState = "quarantined"
)

// MaterializationSource says how this machine obtained the provider handle.
type MaterializationSource = sm.MaterializationSource

const (
	MaterializationSourceBuilt   MaterializationSource = "built"
	MaterializationSourceFetched MaterializationSource = "fetched"
	MaterializationSourceForked  MaterializationSource = "forked"
)

type MaterializationDetachReason string

const (
	MaterializationDetachUnpublished  MaterializationDetachReason = "publication_not_requested"
	MaterializationDetachTargetClosed MaterializationDetachReason = "target_closed"
	MaterializationDetachRerouted     MaterializationDetachReason = "rerouted"
	MaterializationDetachRunFailed    MaterializationDetachReason = "run_failed"
)

// materializationRecord is persisted only below Layout.MaterializationsDir.
// ProviderHost is intentionally opaque and never copied into board storage or
// public session output.
type materializationRecord struct {
	// Historical images stay private and are never projected in a control view.
	legacyImage      []byte
	legacyProjection []byte
	Contract         string                 `json:"contract"`
	CtxID            string                 `json:"ctx_id"`
	Provider         sm.ContextProvider     `json:"provider"`
	BindingScope     sm.ContextBindingScope `json:"binding_scope"`
	BindingElementID string                 `json:"binding_element_id,omitempty"`
	TargetElementID  string                 `json:"target_element_id"`
	// TargetRole is persisted because every later lease, reattach, and reroute
	// re-runs the clearance gate, and it must decide against the role that will
	// actually receive the content rather than whatever the caller passes then.
	TargetRole string `json:"target_role"`
	// ResolutionMode records how this context was chosen. It is persisted for
	// the same reason as the role: every later lease, reattach, and provider
	// launch re-runs the gate, and a denial means different things depending on
	// whether an operator named this context or a resolver ranked it.
	ResolutionMode  sm.ContextResolutionMode `json:"resolution_mode,omitempty"`
	ProviderHost    sm.ProviderHostRecord    `json:"provider_host"`
	State           MaterializationState     `json:"state"`
	Source          MaterializationSource    `json:"source"`
	PayloadSHA256   string                   `json:"payload_sha256,omitempty"`
	ActiveRunID     string                   `json:"active_run_id,omitempty"`
	LeaseAcquiredAt time.Time                `json:"lease_acquired_at,omitempty"`
	LastUsedAt      time.Time                `json:"last_used_at"`
	CreatedAt       time.Time                `json:"created_at"`
	UpdatedAt       time.Time                `json:"updated_at"`

	// UnresolvedRunID preserves writer custody when run evidence is corrupt.
	// Quarantine cannot make an unknown live writer collectable.
	UnresolvedRunID  string                      `json:"unresolved_run_id,omitempty"`
	DetachReason     MaterializationDetachReason `json:"detach_reason,omitempty"`
	RetireAfterLease bool                        `json:"retire_after_lease,omitempty"`
	QuarantineReason string                      `json:"quarantine_reason,omitempty"`
}

// MaterializationView is safe for control-plane clients.
type MaterializationView struct {
	CtxID            string                 `json:"ctx_id"`
	Provider         sm.ContextProvider     `json:"provider"`
	BindingScope     sm.ContextBindingScope `json:"binding_scope"`
	BindingElementID string                 `json:"binding_element_id,omitempty"`
	TargetElementID  string                 `json:"target_element_id"`
	TargetRole       string                 `json:"target_role,omitempty"`
	// ResolutionMode is safe to project: it says how the context was chosen, not
	// what is in it.
	ResolutionMode sm.ContextResolutionMode `json:"resolution_mode,omitempty"`
	State          MaterializationState     `json:"materialization_state"`
	Source         MaterializationSource    `json:"source"`
	ActiveRunID    string                   `json:"active_run_id,omitempty"`
	LastUsedAt     time.Time                `json:"last_used_at"`
	CreatedAt      time.Time                `json:"created_at"`
	UpdatedAt      time.Time                `json:"updated_at"`
}

type CreateMaterializationRequest struct {
	Context *sm.ContextRecord
	// RunProvider is the provider the consuming run actually executes on.
	//
	// It is stated by the caller rather than read off Context, because the gate's
	// job includes deciding whether this run may consume that record at all. A
	// provider taken from the record under test turns "does the consumer match"
	// into "does the record match itself", which is true for every record ever
	// written — including a lineage parent from the other provider's lineage.
	// It is required: an unnamed provider is a refusal, not a default.
	RunProvider     sm.ContextProvider
	TargetElementID string
	// TargetRole is the role of the run that will consume this materialization.
	// It is required: clearance is decided per role, so an unnamed role is a
	// refusal rather than a default.
	TargetRole string
	// Mode is the resolution that produced this CTX choice. Auto denials may
	// fall back to the next ranked candidate; explicit denials are fatal. An
	// unset mode is treated as explicit.
	Mode          sm.ContextResolutionMode
	Source        MaterializationSource
	ProviderHost  sm.ProviderHostRecord
	PayloadSHA256 string
}

type RerouteMaterializationRequest struct {
	OldCtxID string
	OldRunID string
	New      CreateMaterializationRequest
	NewRunID string
}

type MaterializationGCPolicy struct {
	UnpublishedTTL time.Duration
	IdleTTL        time.Duration
	OrphanedTTL    time.Duration
}

type MaterializationGCCandidate struct {
	CtxID  string `json:"ctx_id"`
	Reason string `json:"reason"`
}

// coldBuildAwaitingHandle reports the states in which a cold build legitimately
// has no provider handle.
//
// A cold build is recorded before its provider starts, so "no handle yet" is the
// accurate state rather than a gap. It stays accurate through `leased`, because
// the lease is taken before the launch — that ordering is what lets a
// context-builder launch be confined against a materialization that already
// exists — and through `orphaned`, because a build whose run died never produced
// one at all.
//
// The states a later run can read from are excluded: `ready` and `detached` are
// fork and reattach sources, and both would resume nothing without a handle;
// `quarantined` must keep whatever evidence it was quarantined over. So a
// handle-less record can never be forked, restored, or reattached.
func coldBuildAwaitingHandle(record *materializationRecord) bool {
	if record == nil || record.Source != MaterializationSourceBuilt {
		return false
	}
	switch record.State {
	case MaterializationMaterializing, MaterializationLeased, MaterializationOrphaned:
		return true
	default:
		return false
	}
}

func validateMaterializationRecord(record *materializationRecord) error {
	if record == nil {
		return fmt.Errorf("materialization record is required")
	}
	if record.Contract != MaterializationContractVersion {
		return fmt.Errorf("unsupported materialization contract %q", record.Contract)
	}
	if normalized := normalizeContextID(record.CtxID); normalized != record.CtxID {
		return fmt.Errorf("materialization ctx_id must be normalized")
	} else if err := validateContextID(normalized); err != nil {
		return err
	}
	if err := validateProvider(record.Provider); err != nil {
		return err
	}
	binding := sm.ContextBinding{
		Scope:        record.BindingScope,
		ElementID:    record.BindingElementID,
		ProducerRole: "materialization",
	}
	if err := validateBinding(binding); err != nil {
		return err
	}
	target := normalizeElementID(record.TargetElementID)
	if target != record.TargetElementID {
		return fmt.Errorf("materialization target_element_id must be normalized")
	}
	if err := validateBinding(sm.ContextBinding{
		Scope:        "element",
		ElementID:    target,
		ProducerRole: "materialization",
	}); err != nil {
		return fmt.Errorf("invalid materialization target: %w", err)
	}
	if strings.TrimSpace(record.TargetRole) == "" {
		return fmt.Errorf("materialization requires a target role")
	}
	if record.ProviderHost.Provider != string(record.Provider) {
		return fmt.Errorf("materialization provider host does not match provider")
	}
	// A cold build is the one shape that legitimately has no handle: nothing has
	// been forked and no provider process has started, so there is no snapshot to
	// record yet. The exemption is narrow on purpose — only for source=built, and
	// only while materializing, leased, or orphaned, which are exactly the states
	// no other run may read from. `leased` is in that set because the lease is
	// taken before the launch. The states a later run can read from — `ready`,
	// `detached`, `quarantined` — still require a handle, so a handle-less record
	// can never become a fork source or be restored. See coldBuildAwaitingHandle.
	if !hostPresent(record.ProviderHost) && (!coldBuildAwaitingHandle(record) || !hostEmpty(record.ProviderHost)) {
		return fmt.Errorf("materialization requires a private provider handle")
	}
	switch record.State {
	case MaterializationMaterializing, MaterializationReady, MaterializationLeased,
		MaterializationDetached, MaterializationOrphaned, MaterializationQuarantined:
	default:
		return fmt.Errorf("unsupported materialization state %q", record.State)
	}
	switch record.Source {
	case MaterializationSourceBuilt, MaterializationSourceFetched, MaterializationSourceForked:
	default:
		return fmt.Errorf("unsupported materialization source %q", record.Source)
	}
	if record.PayloadSHA256 != "" && !materializationDigestPattern.MatchString(record.PayloadSHA256) {
		return fmt.Errorf("payload_sha256 must be a lowercase sha256 digest")
	}
	if record.State == MaterializationReady && record.PayloadSHA256 == "" {
		return fmt.Errorf("ready materialization requires payload_sha256")
	}
	if record.State == MaterializationDetached &&
		record.DetachReason == MaterializationDetachUnpublished &&
		record.PayloadSHA256 == "" {
		return fmt.Errorf("unpublished detached materialization requires payload_sha256")
	}
	if record.ActiveRunID != "" && !materializationRunIDPattern.MatchString(record.ActiveRunID) {
		return fmt.Errorf("invalid active run id %q", record.ActiveRunID)
	}
	if record.State == MaterializationLeased {
		if record.ActiveRunID == "" || record.LeaseAcquiredAt.IsZero() {
			return fmt.Errorf("leased materialization requires active_run_id and lease_acquired_at")
		}
	} else if record.ActiveRunID != "" || !record.LeaseAcquiredAt.IsZero() {
		return fmt.Errorf("non-leased materialization cannot carry an active lease")
	}
	if record.CreatedAt.IsZero() || record.LastUsedAt.Before(record.CreatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return fmt.Errorf("valid ordered materialization timestamps are required")
	}
	if record.State == MaterializationQuarantined && strings.TrimSpace(record.QuarantineReason) == "" {
		return fmt.Errorf("quarantined materialization requires a reason")
	}
	return nil
}

func materializationView(record *materializationRecord) MaterializationView {
	if record == nil {
		return MaterializationView{}
	}
	return MaterializationView{
		CtxID:            record.CtxID,
		Provider:         record.Provider,
		BindingScope:     record.BindingScope,
		BindingElementID: record.BindingElementID,
		TargetElementID:  record.TargetElementID,
		TargetRole:       record.TargetRole,
		ResolutionMode:   record.ResolutionMode,
		State:            record.State,
		Source:           record.Source,
		ActiveRunID:      record.ActiveRunID,
		LastUsedAt:       record.LastUsedAt,
		CreatedAt:        record.CreatedAt,
		UpdatedAt:        record.UpdatedAt,
	}
}

func validateMaterializationMirror(record *materializationRecord, contextRecord *sm.ContextRecord) error {
	if contextRecord == nil {
		return fmt.Errorf("layer-1 context %s is unavailable", record.CtxID)
	}
	if normalizeContextID(contextRecord.ID) != record.CtxID {
		return fmt.Errorf("layer-1 context id does not match private record")
	}
	if contextRecord.Provider != record.Provider {
		return fmt.Errorf("layer-1 provider does not match private record")
	}
	if contextRecord.Binding.Scope != record.BindingScope ||
		normalizeElementID(contextRecord.Binding.ElementID) != record.BindingElementID {
		return fmt.Errorf("layer-1 binding does not match private mirror")
	}
	if digest := strings.TrimSpace(contextRecord.Payload.SHA256); digest != "" &&
		record.PayloadSHA256 != "" && digest != record.PayloadSHA256 {
		return fmt.Errorf("layer-1 payload digest does not match private materialization")
	}
	return nil
}

func validateMaterializationTarget(
	ctx context.Context,
	authority MaterializationAuthority,
	bindingScope sm.ContextBindingScope,
	bindingElementID string,
	targetElementID string,
) (*MaterializationElement, error) {
	if authority == nil {
		if bindingScope == "element" &&
			!strings.EqualFold(bindingElementID, targetElementID) {
			return nil, fmt.Errorf("target ancestry requires a materialization authority")
		}
		return nil, nil
	}
	target, err := authority.Element(ctx, targetElementID)
	if err != nil {
		return nil, materializationAuthorityError("resolving materialization target "+targetElementID, err)
	}
	if bindingScope == "board" {
		return target, nil
	}
	seen := map[string]struct{}{}
	current := target
	for current != nil {
		normalized := normalizeElementID(current.ID)
		if normalized == bindingElementID {
			return target, nil
		}
		if _, exists := seen[normalized]; exists {
			return nil, fmt.Errorf("board ancestry cycle while validating target %s", targetElementID)
		}
		seen[normalized] = struct{}{}
		if strings.TrimSpace(current.ParentID) == "" {
			break
		}
		current, err = authority.Element(ctx, current.ParentID)
		if err != nil {
			return nil, materializationAuthorityError("resolving parent of materialization target "+targetElementID, err)
		}
	}
	return nil, fmt.Errorf(
		"target %s is not the binding element %s or its descendant",
		targetElementID,
		bindingElementID,
	)
}

func sortedMaterializationViews(records map[string]*materializationRecord) []MaterializationView {
	views := make([]MaterializationView, 0, len(records))
	for _, record := range records {
		views = append(views, materializationView(record))
	}
	sort.Slice(views, func(i, j int) bool { return views[i].CtxID < views[j].CtxID })
	return views
}
