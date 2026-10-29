package sessioncontext

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

func (m *Service) loadAndReconcileMaterializationsLocked(ctx context.Context) error {
	var records []*materializationRecord
	for {
		var err error
		records, err = m.materializationRecords.load()
		if err == nil {
			break
		}
		var loadErr *materializationLoadError
		if !errors.As(err, &loadErr) {
			return err
		}
		if quarantineErr := m.materializationRecords.quarantineInvalid(
			loadErr.CtxID,
			loadErr,
			m.now(),
		); quarantineErr != nil {
			return errors.Join(err, quarantineErr)
		}
	}
	quarantinedCount, err := m.materializationRecords.quarantinedCount()
	if err != nil {
		return err
	}
	m.materializations = make(map[string]*materializationRecord, len(records))
	m.quarantinedMaterializations = quarantinedCount
	authority := m.materializationAuthority
	if authority != nil {
		var snapshotErr error
		authority, snapshotErr = materializationReadAuthority(ctx, authority)
		if snapshotErr != nil {
			wrapped := materializationAuthorityError(
				"opening materialization topology snapshot",
				snapshotErr,
			)
			m.logOperatorError(
				"materialization_reconciliation_deferred",
				wrapped,
				map[string]any{"record_count": len(records)},
			)
			for _, record := range records {
				if record.State == MaterializationQuarantined {
					m.quarantinedMaterializations++
				}
				m.materializations[record.CtxID] = record
			}
			return nil
		}
	}
	for _, record := range records {
		if record.State == MaterializationQuarantined {
			m.quarantinedMaterializations++
		}
		if authority != nil {
			if err := m.reconcileMaterializationLocked(ctx, authority, record); err != nil {
				if isTransientMaterializationAuthorityError(err) {
					m.logOperatorError(
						"materialization_reconciliation_deferred",
						err,
						map[string]any{"ctx_id": record.CtxID},
					)
				} else {
					m.quarantineMaterializationLocked(record, err)
				}
			}
		}
		m.materializations[record.CtxID] = record
		if err := m.saveMaterializationLocked(record); err != nil {
			return fmt.Errorf("persisting reconciled materialization %s: %w", record.CtxID, err)
		}
	}
	return nil
}

func isTransientMaterializationAuthorityError(err error) bool {
	var readErr *materializationAuthorityReadError
	return errors.As(err, &readErr)
}

type materializationAuthorityReadError struct {
	operation string
	err       error
}

func (e *materializationAuthorityReadError) Error() string {
	return fmt.Sprintf("%s: %v", e.operation, e.err)
}

func (e *materializationAuthorityReadError) Unwrap() error {
	return e.err
}

func materializationAuthorityError(operation string, err error) error {
	if errors.Is(err, ErrMaterializationEvidenceNotFound) ||
		isContextErrorCode(err, "context_not_found") {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return &materializationAuthorityReadError{operation: operation, err: err}
}

func (m *Service) reconcileMaterializationLocked(
	ctx context.Context,
	authority MaterializationAuthority,
	record *materializationRecord,
) error {
	contextRecord, err := authority.Context(ctx, record.CtxID)
	if err != nil {
		return materializationAuthorityError("reading layer-1 context "+record.CtxID, err)
	}
	if err := validateMaterializationMirror(record, contextRecord); err != nil {
		return err
	}
	target, err := validateMaterializationTarget(
		ctx,
		authority,
		record.BindingScope,
		record.BindingElementID,
		record.TargetElementID,
	)
	if err != nil {
		return err
	}
	if target != nil && (target.Closed || target.Status == "closed") {
		record.DetachReason = MaterializationDetachTargetClosed
		if record.State == MaterializationLeased {
			record.RetireAfterLease = true
		} else {
			record.State = MaterializationDetached
		}
		record.UpdatedAt = m.now()
	}

	if record.State == MaterializationLeased {
		evidence, evidenceErr := authority.RunEvidence(ctx, record.ActiveRunID)
		if evidenceErr != nil {
			if errors.Is(evidenceErr, ErrMaterializationEvidenceNotFound) {
				m.logOperatorInfo(
					"materialization_reconciliation_deferred",
					map[string]any{
						"ctx_id": record.CtxID,
						"run_id": record.ActiveRunID,
						"reason": "run_evidence_not_available",
					},
				)
				return nil
			}
			return materializationAuthorityError("reading run evidence for "+record.ActiveRunID, evidenceErr)
		}
		if evidence == nil ||
			evidence.ID != record.ActiveRunID ||
			normalizeContextID(evidence.ContextID) != record.CtxID ||
			normalizeElementID(evidence.Owner) != record.TargetElementID {
			return fmt.Errorf("run evidence does not match materialization lease")
		}
		if runStatusActive(evidence.Status) || evidence.Status != "completed" && evidence.Status != "failed" && evidence.Status != "cancelled" {
			return nil
		}
		heldRun := record.ActiveRunID
		record.UnresolvedRunID = heldRun
		record.ActiveRunID = ""
		record.LeaseAcquiredAt = time.Time{}
		record.LastUsedAt = m.now()
		record.UpdatedAt = record.LastUsedAt
		switch evidence.Status {
		case "completed":
			digest := strings.TrimSpace(evidence.PayloadSHA256)
			if digest == "" {
				digest = strings.TrimSpace(contextRecord.Payload.SHA256)
			}
			if digest == "" {
				digest = strings.TrimSpace(contextRecord.Integrity.TranscriptSHA256)
			}
			if !materializationDigestPattern.MatchString(digest) {
				return fmt.Errorf("completed run evidence carries no materialization digest")
			}
			record.PayloadSHA256 = digest
			switch {
			case record.RetireAfterLease:
				record.State = MaterializationDetached
			case evidence.Published && contextRecord.State == "ready":
				record.State = MaterializationReady
			case !evidence.Published &&
				contextRecord.State == "retired" &&
				contextRecord.StateReason == "publication_not_requested":
				record.State = MaterializationDetached
				record.DetachReason = MaterializationDetachUnpublished
			default:
				return fmt.Errorf("completed run evidence does not match layer-1 publication state")
			}
		case "failed", "cancelled":
			record.State = MaterializationOrphaned
			record.DetachReason = MaterializationDetachRunFailed
		default:
			return fmt.Errorf("unsupported terminal run evidence state %q", evidence.Status)
		}
		record.RetireAfterLease = false
		record.UnresolvedRunID = ""
		return nil
	}

	switch record.State {
	case MaterializationMaterializing:
		if contextRecord.State != "building" {
			return fmt.Errorf("materializing local record does not match layer-1 %s state", contextRecord.State)
		}
	case MaterializationReady:
		if contextRecord.State != "ready" {
			return fmt.Errorf("ready local record does not match layer-1 %s state", contextRecord.State)
		}
	case MaterializationDetached:
		if record.DetachReason == MaterializationDetachUnpublished &&
			(contextRecord.State != "retired" ||
				contextRecord.StateReason != "publication_not_requested") {
			return fmt.Errorf("unpublished detached record does not match layer-1 retirement")
		}
	case MaterializationOrphaned:
		if contextRecord.State == "ready" {
			return fmt.Errorf("orphaned local record cannot mirror a ready layer-1 context")
		}
	case MaterializationQuarantined:
		// A quarantine is terminal. Reconciliation never promotes it.
	default:
		return fmt.Errorf("unsupported materialization state %q", record.State)
	}
	return nil
}

func (m *Service) quarantineMaterializationLocked(record *materializationRecord, cause error) {
	if record.State != MaterializationQuarantined {
		m.quarantinedMaterializations++
	}
	if record.ActiveRunID != "" {
		record.UnresolvedRunID = record.ActiveRunID
	}
	record.State = MaterializationQuarantined
	record.ActiveRunID = ""
	record.LeaseAcquiredAt = time.Time{}
	record.RetireAfterLease = false
	record.QuarantineReason = string("context_materialization_divergent")
	if cause != nil {
		record.QuarantineReason = string("context_materialization_divergent") + ": " + cause.Error()
	}
	record.UpdatedAt = m.now()
}

// CreateMaterialization records one provider handle for one CTX. Repeating an
// identical request is idempotent; a changed target/provider/handle is a
// divergence and must allocate a different child CTX.
func (m *Service) CreateMaterialization(
	ctx context.Context,
	request CreateMaterializationRequest,
) (MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createMaterializationLocked(ctx, request)
}

func (m *Service) createMaterializationLocked(
	ctx context.Context,
	request CreateMaterializationRequest,
) (MaterializationView, error) {
	record, existing, contextRecord, err := m.prepareMaterializationLocked(ctx, request)
	if err != nil {
		return MaterializationView{}, err
	}
	if existing != nil {
		return materializationView(existing), nil
	}
	// Refuse a builder materialization immediately when this manager cannot
	// confine its future provider launch. Evidence is not sealed here: there is
	// no process yet. ensureHostLocked seals only after the provider host starts
	// under the requested boundary and reports what it enforced.
	if err := m.requireBuilderConfinementCapabilityLocked(ctx, contextRecord); err != nil {
		return MaterializationView{}, err
	}
	if err := m.saveMaterializationLocked(record); err != nil {
		return MaterializationView{}, err
	}
	m.materializations[record.CtxID] = record
	return materializationView(record), nil
}

// prepareMaterializationLocked validates the complete new side without
// persisting it. Reroute uses this before retiring the old CTX or releasing its
// lease, so an invalid or closed destination cannot strand the active run.
func (m *Service) prepareMaterializationLocked(
	ctx context.Context,
	request CreateMaterializationRequest,
) (*materializationRecord, *materializationRecord, *sm.ContextRecord, error) {
	if request.Context != nil && m.materializationRecords.quarantinedIdentity(request.Context.ID) {
		return nil, nil, nil, fmt.Errorf("materialization identity has unresolved quarantined source evidence")
	}
	if request.Context == nil {
		return nil, nil, nil, fmt.Errorf("layer-1 context is required")
	}
	if err := validateContextFacts(request.Context); err != nil {
		return nil, nil, nil, err
	}
	if m.materializationAuthority == nil {
		return nil, nil, nil, fmt.Errorf("context authority unavailable")
	}
	contextRecord := cloneContext(request.Context)
	if m.materializationAuthority != nil {
		authoritative, err := m.materializationAuthority.Context(ctx, request.Context.ID)
		if err != nil {
			return nil, nil, nil, err
		}
		if authoritative == nil || authoritative.ID != request.Context.ID || authoritative.Revision != request.Context.Revision ||
			authoritative.Provider != request.Context.Provider ||
			authoritative.Binding != request.Context.Binding {
			return nil, nil, nil, fmt.Errorf("layer-1 context changed before materialization")
		}
		contextRecord = authoritative
	}
	if contextRecord.State != "building" &&
		contextRecord.State != "ready" {
		return nil, nil, nil, fmt.Errorf(
			"context %s in state %s cannot be materialized",
			contextRecord.ID,
			contextRecord.State,
		)
	}
	targetID := normalizeElementID(request.TargetElementID)
	target, err := validateMaterializationTarget(
		ctx,
		m.materializationAuthority,
		contextRecord.Binding.Scope,
		normalizeElementID(contextRecord.Binding.ElementID),
		targetID,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	if target != nil && (target.Closed || target.Status == "closed") {
		return nil, nil, nil, fmt.Errorf("materialization target %s is closed", targetID)
	}
	// The consuming provider is validated before it is used, so a caller cannot
	// reach the gate without stating one and cannot state one that disagrees with
	// the record it is materializing. Both halves matter: the first keeps the
	// provider check from silently defaulting, the second is the check itself.
	runProvider := request.RunProvider
	if runProvider == "" {
		return nil, nil, nil, &ContextError{
			Code: "context_provider_mismatch",
			Message: fmt.Sprintf(
				"materialization of %s did not name the consuming run's provider",
				normalizeContextID(contextRecord.ID),
			),
		}
	}
	if runProvider != contextRecord.Provider {
		return nil, nil, nil, &ContextError{
			Code: "context_provider_mismatch",
			Message: fmt.Sprintf(
				"context %s belongs to the %s lineage and cannot be materialized for a %s run",
				normalizeContextID(contextRecord.ID), contextRecord.Provider, runProvider,
			),
		}
	}
	// The gate runs here, before any provider handle is persisted and before the
	// record exists, so a refused context leaves no local state behind to be
	// reattached later.
	if err := m.gateMaterializationLocked(
		ctx,
		contextRecord,
		runProvider,
		request.TargetRole,
		targetID,
		request.Source,
		"materialization",
		request.Mode,
	); err != nil {
		return nil, nil, nil, err
	}
	now := m.now()
	record := &materializationRecord{
		Contract:         MaterializationContractVersion,
		CtxID:            normalizeContextID(contextRecord.ID),
		Provider:         runProvider,
		BindingScope:     contextRecord.Binding.Scope,
		BindingElementID: normalizeElementID(contextRecord.Binding.ElementID),
		TargetElementID:  targetID,
		TargetRole:       normalizeRole(request.TargetRole),
		ResolutionMode:   normalizeContextResolutionMode(request.Mode),
		ProviderHost:     cloneHostRecord(request.ProviderHost),
		State:            MaterializationMaterializing,
		Source:           request.Source,
		PayloadSHA256:    strings.TrimSpace(request.PayloadSHA256),
		LastUsedAt:       now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if contextRecord.State == "ready" {
		record.State = MaterializationReady
	}
	if existing := m.materializations[record.CtxID]; existing != nil {
		if existing.State == MaterializationQuarantined {
			return nil, nil, nil, &ContextError{
				Code: "context_materialization_divergent",
				Message: fmt.Sprintf(
					"materialization %s is quarantined and must be collected before reuse",
					record.CtxID,
				),
			}
		}
		if sameMaterializationIdentity(existing, record) {
			return nil, existing, contextRecord, nil
		}
		return nil, nil, nil, &ContextError{
			Code:    "context_materialization_divergent",
			Message: fmt.Sprintf("materialization %s already exists with different private identity", record.CtxID),
		}
	}
	if err := validateMaterializationRecord(record); err != nil {
		return nil, nil, nil, err
	}
	return record, nil, contextRecord, nil
}

// normalizeContextResolutionMode defaults an unset mode to explicit.
//
// Explicit is the stricter of the two: it makes a refusal fatal instead of
// offering a fallback. Defaulting the other way would let a caller that simply
// forgot to record the mode turn every denial into a silent skip.
func normalizeContextResolutionMode(mode sm.ContextResolutionMode) sm.ContextResolutionMode {
	if mode == "auto" {
		return "auto"
	}
	return "explicit"
}

func sameMaterializationIdentity(left, right *materializationRecord) bool {
	return left.CtxID == right.CtxID &&
		left.Provider == right.Provider &&
		left.BindingScope == right.BindingScope &&
		left.BindingElementID == right.BindingElementID &&
		left.TargetElementID == right.TargetElementID &&
		left.TargetRole == right.TargetRole &&
		// A context chosen explicitly and the same context ranked automatically
		// are not the same materialization: the two differ in what a later
		// denial is allowed to do, so reusing one record for both would let an
		// auto record inherit an explicit refusal contract or the reverse.
		left.ResolutionMode == right.ResolutionMode &&
		left.Source == right.Source &&
		left.PayloadSHA256 == right.PayloadSHA256 &&
		left.ProviderHost.Provider == right.ProviderHost.Provider &&
		reflect.DeepEqual(left.ProviderHost.Snapshot, right.ProviderHost.Snapshot)
}

func (m *Service) AcquireMaterializationLease(
	ctx context.Context,
	ctxID string,
	runID string,
	targetElementID string,
) (MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acquireMaterializationLeaseLocked(ctx, ctxID, runID, targetElementID, false)
}

func (m *Service) acquireMaterializationLeaseLocked(
	ctx context.Context,
	ctxID string,
	runID string,
	targetElementID string,
	reattach bool,
) (MaterializationView, error) {
	record := cloneMaterialization(m.materializations[normalizeContextID(ctxID)])
	if record == nil {
		return MaterializationView{}, fmt.Errorf("materialization %s was not found", ctxID)
	}
	runID = strings.TrimSpace(runID)
	if !materializationRunIDPattern.MatchString(runID) {
		return MaterializationView{}, fmt.Errorf("invalid run id %q", runID)
	}
	targetID := normalizeElementID(targetElementID)
	if targetID != record.TargetElementID {
		return MaterializationView{}, &ContextError{
			Code:    "context_materialization_divergent",
			Message: fmt.Sprintf("materialization %s targets %s, not %s", record.CtxID, record.TargetElementID, targetID),
		}
	}
	if record.State == MaterializationLeased {
		if record.ActiveRunID == runID {
			return materializationView(record), nil
		}
		return MaterializationView{}, fmt.Errorf(
			"materialization %s is already leased by %s",
			record.CtxID,
			record.ActiveRunID,
		)
	}
	if err := m.ensureMaterializationRunAvailableLocked(runID, record.CtxID); err != nil {
		return MaterializationView{}, err
	}
	if reattach {
		if record.State != MaterializationDetached {
			return MaterializationView{}, fmt.Errorf("only a detached materialization may be reattached")
		}
	} else if record.State != MaterializationMaterializing && record.State != MaterializationDetached {
		return MaterializationView{}, fmt.Errorf(
			"materialization %s in state %s cannot acquire a writer lease",
			record.CtxID,
			record.State,
		)
	}
	if m.materializationAuthority != nil {
		contextRecord, err := m.materializationAuthority.Context(ctx, record.CtxID)
		if err != nil {
			return MaterializationView{}, err
		}
		if err := validateMaterializationMirror(record, contextRecord); err != nil {
			return MaterializationView{}, err
		}
		target, err := validateMaterializationTarget(
			ctx,
			m.materializationAuthority,
			record.BindingScope,
			record.BindingElementID,
			record.TargetElementID,
		)
		if err != nil {
			return MaterializationView{}, err
		}
		if target != nil && (target.Closed || target.Status == "closed") {
			return MaterializationView{}, fmt.Errorf("materialization target %s is closed", record.TargetElementID)
		}
		// Granting the writer lease is the moment the provider is handed the
		// transcript, so the gate runs again against the current board record
		// rather than resting on whatever was true at creation. Clearance can be
		// revoked and a context can be retired between the two.
		if err := m.gateMaterializationLocked(
			ctx,
			contextRecord,
			record.Provider,
			record.TargetRole,
			record.TargetElementID,
			record.Source,
			leaseConsumerOperation(reattach),
			record.ResolutionMode,
		); err != nil {
			return MaterializationView{}, err
		}
	}
	now := m.now()
	record.State = MaterializationLeased
	record.ActiveRunID = runID
	record.LeaseAcquiredAt = now
	record.LastUsedAt = now
	record.UpdatedAt = now
	record.DetachReason = ""
	record.RetireAfterLease = false
	if err := m.saveMaterializationLocked(record); err != nil {
		return MaterializationView{}, err
	}
	return materializationView(record), nil
}

// leaseConsumerOperation names what a lease is about to do to the provider
// session. A fresh lease forks the snapshot; a reattach resumes the same one.
func leaseConsumerOperation(reattach bool) sm.ContextConsumerOperation {
	if reattach {
		return "provider_resume"
	}
	return "provider_fork"
}

func (m *Service) ensureMaterializationRunAvailableLocked(runID string, excludingCtxID string) error {
	for _, record := range m.materializations {
		if record.CtxID != excludingCtxID &&
			record.State == MaterializationLeased &&
			record.ActiveRunID == runID {
			return fmt.Errorf(
				"run %s already leases materialization %s",
				runID,
				record.CtxID,
			)
		}
	}
	return nil
}

func (m *Service) ReattachMaterialization(
	ctx context.Context,
	ctxID string,
	runID string,
	targetElementID string,
) (MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acquireMaterializationLeaseLocked(ctx, ctxID, runID, targetElementID, true)
}

type CompleteMaterializationRequest struct {
	CtxID         string
	RunID         string
	Published     bool
	PayloadSHA256 string
	ProviderHost  sm.ProviderHostRecord
	// Usage is what this run reports about its own realized spend. It is
	// optional: a launch path that measured nothing supplies nothing, and the
	// recorded row says so rather than reporting zeros that would average into
	// the aggregate as though they had been observed.
	Usage *ContextRunUsage
}

func (m *Service) CompleteMaterialization(
	ctx context.Context,
	request CompleteMaterializationRequest,
) (MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record := cloneMaterialization(m.materializations[normalizeContextID(request.CtxID)])
	if record == nil {
		return MaterializationView{}, fmt.Errorf("materialization %s was not found", request.CtxID)
	}
	if record.State != MaterializationLeased || record.ActiveRunID != strings.TrimSpace(request.RunID) {
		return MaterializationView{}, fmt.Errorf("run %s does not hold materialization %s", request.RunID, record.CtxID)
	}
	digest := strings.TrimSpace(request.PayloadSHA256)
	if !materializationDigestPattern.MatchString(digest) {
		return MaterializationView{}, fmt.Errorf("completed materialization requires a payload sha256 digest")
	}
	if request.ProviderHost.Provider != string(record.Provider) {
		return MaterializationView{}, fmt.Errorf("completed provider host does not match materialization provider")
	}
	var contextRecord *sm.ContextRecord
	gatewayClosed := false
	if m.materializationAuthority != nil {
		var err error
		contextRecord, err = m.materializationAuthority.Context(ctx, record.CtxID)
		if err != nil {
			return MaterializationView{}, err
		}
		if !request.Published && contextRecord.State == "building" {
			contextRecord, err = m.materializationAuthority.RetireContext(
				ctx,
				record.CtxID,
				contextRecord.Revision,
				"publication_not_requested",
				m.now(),
			)
			if err != nil {
				return MaterializationView{}, err
			}
		}
		if request.Published && contextRecord.State == "building" {
			// This is where the `--register-context` opt-in is discharged. The
			// ready-state rule below is not relaxed for it: the publication runs
			// the real registry gate and the completion still refuses anything that
			// did not come out of it ready, so a base always rests on a record that
			// passed publication rather than on a caller's assertion that it did.
			//
			// The gateway closes first. Its trace describes the run whose snapshot
			// is about to become a base, and attesting it after publication would
			// publish a base whose evidence surface was still open.
			if _, closeErr := m.closeBuilderGatewayLocked(ctx, record.CtxID); closeErr != nil {
				return MaterializationView{}, m.abandonPublishedCompletionLocked(ctx, record, closeErr)
			}
			gatewayClosed = true
			published, publishErr := m.publishCompletedContextLocked(
				ctx, record, contextRecord, request.ProviderHost,
			)
			if publishErr != nil {
				return MaterializationView{}, m.abandonPublishedCompletionLocked(ctx, record, publishErr)
			}
			contextRecord = published
		}
		if request.Published && contextRecord.State != "ready" {
			return MaterializationView{}, fmt.Errorf("published completion requires layer-1 ready state")
		}
		if !request.Published &&
			(contextRecord.State != "retired" ||
				contextRecord.StateReason != "publication_not_requested") {
			return MaterializationView{}, fmt.Errorf("unpublished completion requires layer-1 publication_not_requested retirement")
		}
	}
	// The builder is finished reading, so its evidence surface closes here whether
	// or not the run published. Leaving it open past completion would keep a socket
	// listening that could still write into a trace the run has already been
	// accounted for, and a completion that never closed the gateway is how every
	// honest base ended up with no terminal attestation to present.
	//
	// A reconciliation failure fails the completion. The alternative — completing
	// and reporting the trace problem later — would leave the materialization in a
	// terminal state whose evidence nobody can reconstruct.
	//
	// A published completion already closed it above, before the base it
	// describes was published; the call is a no-op for a context with no gateway,
	// but the flag says so rather than relying on that.
	if !gatewayClosed {
		if _, err := m.closeBuilderGatewayLocked(ctx, record.CtxID); err != nil {
			return MaterializationView{}, err
		}
	}
	now := m.now()
	record.ProviderHost = cloneHostRecord(request.ProviderHost)
	record.PayloadSHA256 = digest
	record.ActiveRunID = ""
	record.LeaseAcquiredAt = time.Time{}
	record.LastUsedAt = now
	record.UpdatedAt = now
	switch {
	case record.RetireAfterLease:
		record.State = MaterializationDetached
	case request.Published:
		record.State = MaterializationReady
	default:
		record.State = MaterializationDetached
		record.DetachReason = MaterializationDetachUnpublished
	}
	record.RetireAfterLease = false
	if err := m.saveMaterializationLocked(record); err != nil {
		return MaterializationView{}, err
	}
	// CV-6 telemetry is captured after the completion is durable and can never
	// undo it. The run is over either way; what is being recorded is how much of
	// the repository the fork still had to read, and that measurement is advisory
	// by contract.
	m.recordConsumerForkTelemetryLocked(ctx, record, strings.TrimSpace(request.RunID))
	// Realized economics is captured beside it and under the same rule: the
	// completion is already durable, and measuring what a run cost can never be
	// allowed to undo the run it measured.
	m.recordRunEconomicsLocked(record, strings.TrimSpace(request.RunID), request.Usage, contextRecord)
	return materializationView(record), nil
}

func (m *Service) FailMaterialization(
	ctx context.Context,
	ctxID string,
	runID string,
) (MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record := cloneMaterialization(m.materializations[normalizeContextID(ctxID)])
	if record == nil {
		return MaterializationView{}, fmt.Errorf("materialization %s was not found", ctxID)
	}
	if record.State != MaterializationLeased || record.ActiveRunID != strings.TrimSpace(runID) {
		return MaterializationView{}, fmt.Errorf("run %s does not hold materialization %s", runID, record.CtxID)
	}
	if err := m.releaseFailedMaterializationLocked(
		ctx, record, "building_failed",
	); err != nil {
		return MaterializationView{}, err
	}
	return materializationView(record), nil
}

// releaseFailedMaterializationLocked is the terminal transition for a run that
// finished without producing a base.
//
// It is shared with the failed-publication path rather than duplicated there
// because the two failures owe the record the same thing: the lease has to come
// off, the provisional child has to be tombstoned with a reason that says which
// failure it was, and the evidence socket must not outlive the run. Only the
// reason differs.
func (m *Service) releaseFailedMaterializationLocked(
	ctx context.Context,
	record *materializationRecord,
	reason sm.StateReason,
) error {
	if m.materializationAuthority != nil {
		contextRecord, err := m.materializationAuthority.Context(ctx, record.CtxID)
		if err != nil {
			return err
		}
		if contextRecord.State == "building" {
			if _, err := m.materializationAuthority.RetireContext(
				ctx,
				record.CtxID,
				contextRecord.Revision,
				reason,
				m.now(),
			); err != nil {
				return err
			}
		}
	}
	// A failed run's gateway is closed too, and its reconciliation verdict is
	// deliberately discarded: the run already failed, and refusing to mark it
	// failed because its trace is incomplete would leave the materialization leased
	// forever. The socket must not outlive the run either way.
	_, _ = m.closeBuilderGatewayLocked(ctx, record.CtxID)
	now := m.now()
	record.State = MaterializationOrphaned
	record.ActiveRunID = ""
	record.LeaseAcquiredAt = time.Time{}
	record.RetireAfterLease = false
	record.DetachReason = MaterializationDetachRunFailed
	record.LastUsedAt = now
	record.UpdatedAt = now
	return m.saveMaterializationLocked(record)
}

func (m *Service) RerouteMaterialization(
	ctx context.Context,
	request RerouteMaterializationRequest,
) (MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := cloneMaterialization(m.materializations[normalizeContextID(request.OldCtxID)])
	if old == nil {
		return MaterializationView{}, fmt.Errorf("materialization %s was not found", request.OldCtxID)
	}
	if old.State != MaterializationLeased || old.ActiveRunID != strings.TrimSpace(request.OldRunID) {
		return MaterializationView{}, fmt.Errorf("run %s does not hold materialization %s", request.OldRunID, old.CtxID)
	}
	newCtxID := ""
	if request.New.Context != nil {
		newCtxID = normalizeContextID(request.New.Context.ID)
	}
	newTarget := normalizeElementID(request.New.TargetElementID)
	if newCtxID == old.CtxID {
		if newTarget != old.TargetElementID {
			return MaterializationView{}, fmt.Errorf("a different target requires a new child CTX")
		}
		if request.New.Context.Provider != old.Provider ||
			request.New.Context.Binding.Scope != old.BindingScope ||
			normalizeElementID(request.New.Context.Binding.ElementID) != old.BindingElementID {
			return MaterializationView{}, fmt.Errorf("same-target reroute changed provider or binding and requires a new child CTX")
		}
		newRunID := strings.TrimSpace(request.NewRunID)
		if !materializationRunIDPattern.MatchString(newRunID) {
			return MaterializationView{}, fmt.Errorf("invalid new run id %q", request.NewRunID)
		}
		if err := m.ensureMaterializationRunAvailableLocked(newRunID, old.CtxID); err != nil {
			return MaterializationView{}, err
		}
		// A same-target reroute hands the live session to a different run without
		// passing through create or acquire, so it re-gates here. Otherwise it
		// would be the one way to reach a leased provider session with no
		// authorization decision in front of it.
		if m.materializationAuthority != nil {
			contextRecord, err := m.materializationAuthority.Context(ctx, old.CtxID)
			if err != nil {
				return MaterializationView{}, err
			}
			if err := m.gateMaterializationLocked(
				ctx,
				contextRecord,
				old.Provider,
				old.TargetRole,
				old.TargetElementID,
				old.Source,
				"provider_resume",
				old.ResolutionMode,
			); err != nil {
				return MaterializationView{}, err
			}
		}
		old.ActiveRunID = newRunID
		now := m.now()
		old.LeaseAcquiredAt = now
		old.LastUsedAt = now
		old.UpdatedAt = now
		if err := m.saveMaterializationLocked(old); err != nil {
			return MaterializationView{}, err
		}
		return materializationView(old), nil
	}
	if newCtxID == "" || newCtxID == old.CtxID {
		return MaterializationView{}, fmt.Errorf("reroute requires a distinct new child CTX")
	}
	newRunID := strings.TrimSpace(request.NewRunID)
	if !materializationRunIDPattern.MatchString(newRunID) {
		return MaterializationView{}, fmt.Errorf("invalid new run id %q", request.NewRunID)
	}
	if err := m.ensureMaterializationRunAvailableLocked(newRunID, old.CtxID); err != nil {
		return MaterializationView{}, err
	}
	prepared, existing, preparedContext, err := m.prepareMaterializationLocked(ctx, request.New)
	if err != nil {
		return MaterializationView{}, err
	}
	candidate := prepared
	if existing != nil {
		candidate = existing
	}
	if candidate.State == MaterializationLeased {
		if candidate.ActiveRunID != newRunID {
			return MaterializationView{}, fmt.Errorf(
				"materialization %s is already leased by %s",
				candidate.CtxID,
				candidate.ActiveRunID,
			)
		}
	} else if candidate.State != MaterializationMaterializing &&
		candidate.State != MaterializationDetached {
		return MaterializationView{}, fmt.Errorf(
			"materialization %s in state %s cannot acquire a writer lease",
			candidate.CtxID,
			candidate.State,
		)
	}
	if m.materializationAuthority != nil {
		contextRecord, err := m.materializationAuthority.Context(ctx, old.CtxID)
		if err != nil {
			return MaterializationView{}, err
		}
		if contextRecord.State == "building" {
			if _, err := m.materializationAuthority.RetireContext(
				ctx,
				old.CtxID,
				contextRecord.Revision,
				"superseded",
				m.now(),
			); err != nil {
				return MaterializationView{}, err
			}
		}
	}
	now := m.now()
	old.State = MaterializationOrphaned
	old.ActiveRunID = ""
	old.LeaseAcquiredAt = time.Time{}
	old.DetachReason = MaterializationDetachRerouted
	old.LastUsedAt = now
	old.UpdatedAt = now
	if err := m.saveMaterializationLocked(old); err != nil {
		return MaterializationView{}, err
	}
	if existing == nil {
		// A reroute allocates a new child CTX, so its future launch needs its own
		// confinement capability and will seal its own post-launch claim.
		if err := m.requireBuilderConfinementCapabilityLocked(ctx, preparedContext); err != nil {
			return MaterializationView{}, err
		}
		if err := m.materializationRecords.save(prepared); err != nil {
			return MaterializationView{}, err
		}
		m.materializations[prepared.CtxID] = prepared
	}
	return m.acquireMaterializationLeaseLocked(
		ctx,
		candidate.CtxID,
		newRunID,
		request.New.TargetElementID,
		false,
	)
}

func (m *Service) Materialization(ctxID string) (MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record := cloneMaterialization(m.materializations[normalizeContextID(ctxID)])
	if record == nil {
		return MaterializationView{}, fmt.Errorf("materialization %s was not found", ctxID)
	}
	return materializationView(record), nil
}

func (m *Service) ListMaterializations() []MaterializationView {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sortedMaterializationViews(m.materializations)
}

// HandleElementClose detaches every materialization whose exact target is the
// closed element or one of its descendants. Active writer leases are preserved
// and become detached only when the lease ends.
func (m *Service) HandleElementClose(
	ctx context.Context,
	elementID string,
) ([]MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	closedID := normalizeElementID(elementID)
	changed := make([]MaterializationView, 0)
	authority, err := materializationReadAuthority(ctx, m.materializationAuthority)
	if err != nil {
		return nil, materializationAuthorityError(
			"opening materialization topology snapshot",
			err,
		)
	}
	for _, record := range m.materializations {
		if record.State == MaterializationQuarantined {
			continue
		}
		matches, err := m.targetDescendsFromLocked(
			ctx,
			authority,
			record.TargetElementID,
			closedID,
		)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		record.DetachReason = MaterializationDetachTargetClosed
		record.UpdatedAt = m.now()
		if record.State == MaterializationLeased {
			record.RetireAfterLease = true
		} else {
			record.State = MaterializationDetached
		}
		if err := m.saveMaterializationLocked(record); err != nil {
			return nil, err
		}
		changed = append(changed, materializationView(record))
	}
	sort.Slice(changed, func(i, j int) bool { return changed[i].CtxID < changed[j].CtxID })
	return changed, nil
}

func (m *Service) targetDescendsFromLocked(
	ctx context.Context,
	authority MaterializationAuthority,
	targetID string,
	ancestorID string,
) (bool, error) {
	if targetID == ancestorID {
		return true, nil
	}
	if authority == nil {
		return false, nil
	}
	seen := map[string]struct{}{}
	currentID := targetID
	for currentID != "" {
		if _, exists := seen[currentID]; exists {
			return false, fmt.Errorf("board ancestry cycle while checking close of %s", ancestorID)
		}
		seen[currentID] = struct{}{}
		element, err := authority.Element(ctx, currentID)
		if err != nil {
			return false, err
		}
		if element == nil {
			return false, fmt.Errorf("materialization element evidence %s is unavailable", currentID)
		}
		parentID := normalizeElementID(element.ParentID)
		if parentID == ancestorID {
			return true, nil
		}
		currentID = parentID
	}
	return false, nil
}

func (m *Service) MaterializationGCCandidates(
	now time.Time,
	policy MaterializationGCPolicy,
) []MaterializationGCCandidate {
	m.mu.Lock()
	defer m.mu.Unlock()
	if now.IsZero() {
		now = m.now()
	}
	candidates := make([]MaterializationGCCandidate, 0)
	for _, record := range m.materializations {
		if record.State == MaterializationLeased ||
			record.ActiveRunID != "" || record.UnresolvedRunID != "" ||
			m.materializationReferencedLocked(record.CtxID) {
			continue
		}
		age := now.Sub(record.UpdatedAt)
		reason := ""
		switch record.State {
		case MaterializationDetached:
			ttl := policy.IdleTTL
			reason = "detached"
			if record.DetachReason == MaterializationDetachUnpublished {
				ttl = policy.UnpublishedTTL
				reason = "unpublished"
			}
			if age < ttl {
				continue
			}
		case MaterializationOrphaned:
			if age < policy.OrphanedTTL {
				continue
			}
			reason = "orphaned"
		case MaterializationReady:
			if now.Sub(record.LastUsedAt) < policy.IdleTTL {
				continue
			}
			reason = "idle"
		case MaterializationQuarantined:
			if age < policy.OrphanedTTL {
				continue
			}
			reason = "quarantined"
		default:
			continue
		}
		candidates = append(candidates, MaterializationGCCandidate{CtxID: record.CtxID, Reason: reason})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].CtxID < candidates[j].CtxID })
	return candidates
}

func (m *Service) CollectMaterializations(candidates []MaterializationGCCandidate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, candidate := range candidates {
		ctxID := normalizeContextID(candidate.CtxID)
		record := cloneMaterialization(m.materializations[ctxID])
		if record == nil {
			continue
		}
		if record.State == MaterializationLeased ||
			record.ActiveRunID != "" || record.UnresolvedRunID != "" ||
			m.materializationReferencedLocked(ctxID) {
			return fmt.Errorf("materialization %s is live and cannot be collected", ctxID)
		}
		if _, _, err := m.releaseMaterializationBytesLocked(context.Background(), record); err != nil {
			return err
		}
		if err := m.materializationRecords.remove(ctxID); err != nil {
			return err
		}
		if record.State == MaterializationQuarantined &&
			m.quarantinedMaterializations > 0 {
			m.quarantinedMaterializations--
		}
		delete(m.materializations, ctxID)
	}
	return nil
}

func (m *Service) materializationReferencedLocked(ctxID string) bool {
	quarantined, err := m.materializationRecords.quarantinedIDs()
	if err != nil {
		return true
	}
	if _, held := quarantined[normalizeContextID(ctxID)]; held {
		return true
	}

	if m.references == nil {
		return true
	}
	live, known, err := m.references.Referenced(context.Background(), ctxID)
	return err != nil || !known || live
}
