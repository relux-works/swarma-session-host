package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

var ErrHostCapabilityUnavailable = errors.New("session host capability unavailable")
var ErrHostClosed = errors.New("session host is closed")

// SessionHost owns neutral admission and the sole legacy-record transaction.
// Fork authority, policy interpretation and private wire state remain in the
// supplied ports. Instances retain the existing lifetime manager lock.
type SessionHost struct {
	builderClose             *hostBuilderClose
	attachResults            map[*AttachmentResultReceipt]struct{}
	proxies                  map[string]*hostProviderProxy
	nativeAccountingRestores map[string]*nativeAccountingScope
	nativeAccountingReady    map[string]ControllerLease
	goalRecoveries           map[string]*goalRecoveryScope
	quarantinedCount         int
	startupRepair            RepairReport
	ownerRuns                map[OwnerKey]*ownerRunActivity
	ownerActivations         map[OwnerKey]*hostOwnerActivation
	ownerOperations          map[OwnerKey]*hostOwnerOperation
	mu                       sync.Mutex
	noticeMu                 sync.Mutex
	noticeWake               chan struct{}
	goalReconciling          map[string]bool
	options                  HostOptions
	settings                 HostSettings
	records                  *codecRecords
	lock                     *instanceLock
	epochs                   *controllerEpochJournal
	life                     context.Context
	cancel                   context.CancelFunc
	closed                   bool
	closeReleased            bool
	sessions                 map[string]RunnerRecord
	runners                  map[string]RunnerHost
	attachments              map[string]string
	inputs                   map[string]*sessionInput
	pendingInitial           map[string]pendingInitialRunner
	metadataIDs              map[ControllerLease]map[string]struct{}
	metadataLeases           map[string]ControllerLease
	preparations             map[AttachmentPreparationToken]*preparedAttachment
	attachmentOps            sync.WaitGroup

	attachmentControllers map[string]ControllerLease
	issuedAttachments     map[string]ControllerLease
	attachmentAccesses    map[*attachmentAccess]struct{}
	newAttachmentID       func() (string, error) // nil uses the production opaque generator

	// Ordinary graceful cleanup of actual owned runners, not force policy.
	gracefulJoins map[string]bool
}

// A live runner whose first record is not yet acknowledged remains a reserved
// identity. It is not a committed session, and never licenses another Launch.
type pendingInitialRunner struct {
	record RunnerRecord
	epoch  ControllerEpoch
}

func absentHostPort(port any) bool {
	if port == nil {
		return true
	}
	v := reflect.ValueOf(port)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Func, reflect.Chan, reflect.Slice:
		return v.IsNil()
	}
	return false
}

// NewHost requires an explicit codec even when canonical goals are absent.
// Structural refusal precedes config/factory calls and every filesystem effect.
func NewHost(options HostOptions) (host *SessionHost, retErr error) {
	if nilCodec(options.Codec) {
		return nil, ErrCodecRequired
	}
	if err := validateNativeAccountingCodecCoupling(options); err != nil {
		return nil, err
	}
	if absentHostPort(options.Config) || absentHostPort(options.Notices) {
		return nil, fmt.Errorf("%w: config and notices required", ErrHostCapabilityUnavailable)
	}
	goalsMissing, bindingMissing := absentHostPort(options.Goals), absentHostPort(options.Binding) && absentHostPort(options.GoalBindings)
	if goalsMissing != bindingMissing || goalsMissing && !absentHostPort(options.Gate) {
		return nil, fmt.Errorf("%w: goal source/binding/gate mismatch", ErrHostCapabilityUnavailable)
	}
	if options.Layout.Namespace == "" || !filepath.IsAbs(options.Layout.StateDir) || !filepath.IsAbs(options.Layout.SessionsDir) || !filepath.IsAbs(options.Layout.LockFile) {
		return nil, fmt.Errorf("absolute explicit host layout required")
	}
	factories := make(map[string]ProviderHostFactory, len(options.Factories))
	for _, factory := range options.Factories {
		if absentHostPort(factory) {
			return nil, fmt.Errorf("provider factory required")
		}
		name := factory.Provider()
		normalized, err := normalizeProvider(name)
		if err != nil || normalized != name {
			return nil, fmt.Errorf("exact normalized provider name required")
		}
		if _, exists := factories[name]; exists {
			return nil, fmt.Errorf("duplicate provider %q", name)
		}
		factories[name] = factory
	}
	settings, err := options.Config.Snapshot(context.Background())
	if err != nil {
		return nil, err
	}
	settings = copyHostSettings(settings)
	records, err := newCodecRecords(options.Layout.SessionsDir, options.Codec)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(options.Layout.StateDir, 0700); err != nil {
		return nil, err
	}
	lock, err := acquireInstanceLock(options.Layout.LockFile)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = lock.Close()
		}
	}()
	epochs, err := newControllerEpochJournal(options.Layout.StateDir, string(options.Layout.Namespace))
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	defer func() {
		if retErr != nil {
			cancel()
		}
	}()
	h := &SessionHost{noticeWake: make(chan struct{}, 1), options: options, settings: settings, records: records, lock: lock, epochs: epochs, life: life, cancel: cancel, sessions: map[string]RunnerRecord{}, runners: map[string]RunnerHost{}, attachments: map[string]string{}, inputs: map[string]*sessionInput{}, pendingInitial: map[string]pendingInitialRunner{}}
	h.options.Factories = append([]ProviderHostFactory(nil), options.Factories...)
	entries, err := os.ReadDir(options.Layout.SessionsDir)
	if errors.Is(err, os.ErrNotExist) {
		entries = nil
		err = nil
	}
	if err != nil {
		return nil, err
	}
	// Complete codec admission for every committed record before the first
	// provider Restore; no private state or invalid round trip reaches a factory.
	for _, entry := range entries {
		if !entry.IsDir() || !validLegacySessionID(entry.Name()) {
			continue
		}
		record, err := records.load(life, entry.Name())
		if err != nil {
			return nil, err
		}
		if record.ConditionalStop != nil {
			if _, err := recordMetadataPhase(record); err != nil {
				return nil, err
			}
		}
		if _, ok := factories[record.Provider]; !ok {
			return nil, fmt.Errorf("%w: provider %s", ErrHostCapabilityUnavailable, record.Provider)
		}
		h.sessions[record.SessionID] = record
	}

	h.quarantinedCount, err = quarantinedSessionCount(options.Layout.QuarantineDir)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func copyHostSettings(src HostSettings) HostSettings {
	out := src
	out.LaunchDefaults.RequiredEnvNames = copyRunnerStrings(src.LaunchDefaults.RequiredEnvNames)
	if src.LocalPolicy.Clearance != nil {
		out.LocalPolicy.Clearance = make(map[string][]ContextSecretClass, len(src.LocalPolicy.Clearance))
		for key, classes := range src.LocalPolicy.Clearance {
			out.LocalPolicy.Clearance[key] = append([]ContextSecretClass(nil), classes...)
		}
	}
	out.LocalPolicy.BuilderAllowlist.Paths = copyRunnerStrings(src.LocalPolicy.BuilderAllowlist.Paths)
	return out
}

func (h *SessionHost) installInput(ctx context.Context, id string, epoch ControllerEpoch, backend hostInputBackend) (*sessionInput, error) {
	input, err := newSessionInput(h.life, id, epoch, backend)
	if err != nil {
		return nil, err
	}
	input.admission = func(ctx context.Context) bool { return h.epochs.admitsContext(ctx, id, epoch) }
	input.beginIO = h.epochs.beginIO
	if err := h.epochs.registerInput(ctx, input, epoch); err != nil {
		return nil, err
	}
	h.rememberAttachmentController(id, epoch)
	if native, ok := backend.(RunnerHost); ok {
		records := h.records
		input.codexSource = func(call context.Context, lease ControllerLease) (CodexRunnerSnapshot, error) {
			return records.readCodexSource(call, lease, input, native)
		}
	}
	return input, nil
}

func (h *SessionHost) lockContext(ctx context.Context) error {
	if h == nil {
		return ErrHostClosed
	}
	if err := historyguard.LockMutex(ctx, &h.mu); err != nil {
		return err
	}
	if h.closed {
		h.mu.Unlock()
		return ErrHostClosed
	}
	return nil
}

// attachmentCloseWait observes entry to the native launch join; nil in production.
var attachmentCloseWait func(*SessionHost)

func (h *SessionHost) Close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closeReleased {
		return
	}
	h.closed = true
	// Stop actual selected observers before canceling their input feed lifetime.
	// Public host admission is already closed; callbacks run outside the map lock.
	retireCtx, retireCancel := context.WithTimeout(context.Background(), 30*time.Second)
	retireErr := h.retireGoalObserversBeforeCloseLocked(retireCtx)
	retireCancel()
	h.cancel()
	h.mu.Unlock()
	// Staging reserves under mu before adding work. Closing admission first
	// makes this Wait safe, and no operation needs mu while Close waits.
	if attachmentCloseWait != nil {
		attachmentCloseWait(h)
	}
	h.attachmentOps.Wait()
	h.mu.Lock()
	if h.closeReleased {
		return
	}
	if err := h.closeProviderProxiesLocked(); err != nil {
		if filepath.IsAbs(h.options.Layout.OperatorLogFile) {
			_ = RecordOperatorError(h.options.Layout, "provider_proxy_close_failed", err, nil)
		}
	}
	for _, input := range h.inputs {
		_ = input.revokeAndDrain(context.Background())
	}
	if retireErr != nil {
		if filepath.IsAbs(h.options.Layout.OperatorLogFile) {
			_ = RecordOperatorError(h.options.Layout, "goal_observer_retirement_failed", retireErr, nil)
		}
		// Keep the owner lock until an actual retry joins the retained observers.
		// No healthy restart handoff is authorized by a failed retirement.
		return
	}
	if err := h.closeBuilderGatewaysLocked(); err != nil {
		if filepath.IsAbs(h.options.Layout.OperatorLogFile) {
			_ = RecordOperatorError(h.options.Layout, "builder_gateway_close_failed", err, nil)
		}
		return
	}
	// Retain the lifetime lock until every optional native observer has joined.
	// A failed join leaves admission closed and the lock held; a later Close may
	// retry it. No Stop or signal is part of observation shutdown.
	for _, runner := range h.runners {
		if closer, ok := runner.(ProviderObservationCloser); ok {
			if err := closer.CloseObservation(context.Background()); err != nil {
				return
			}
		}
	}
	// Closing the control owner preserves the provider for restart, as in the
	// existing detached-host lifecycle. Stop remains an explicit operation.
	if err := h.lock.Close(); err == nil {
		h.closeReleased = true
	}
}

func providerSlotConflict(incumbent RunnerRecord, provider string) *PublicError {
	return &PublicError{
		Code:    ErrorSessionProviderConflict,
		Message: fmt.Sprintf("cannot attach provider %s: owner %s is held by session %s of provider %s; continue through that session, or stop it with: task-board session stop %s", provider, incumbent.Owner, incumbent.SessionID, incumbent.Provider, incumbent.SessionID),
	}
}

// checkExplicitResumeOwnerSlot runs under h.mu after validating the target,
// before epoch reservation or native launch. Original manager.go at 435efc55:601
// returns providerConflictError (ErrorSessionProviderConflict) for this case.
// Liveness, retirement and provider never authorize an implicit slot release.
func (h *SessionHost) checkExplicitResumeOwnerSlot(target RunnerRecord, provider string) error {
	for _, incumbent := range h.sessions {
		if incumbent.Owner == target.Owner && incumbent.SessionID != target.SessionID && incumbent.State != SessionStopped {
			return providerSlotConflict(incumbent, provider)
		}
	}
	return nil
}

func (h *SessionHost) Attach(ctx context.Context, request AttachRequest) (*Attachment, error) {
	if err := h.lockOwnerOperation(ctx, request.Owner, true); err != nil {
		return nil, err
	}
	defer h.mu.Unlock()
	return h.attachLocked(ctx, request, nil)
}

// Caller retains the original owner/map admission throughout this transaction.
func (h *SessionHost) attachLocked(ctx context.Context, request AttachRequest, result *AttachmentResultReceipt) (*Attachment, error) {
	if err := h.checkExpectedOwnerLeaseLocked(ctx, request.Owner, request.ExpectedOwnerLease); err != nil {
		return nil, err
	}
	if !absentHostPort(h.options.Goals) && absentHostPort(h.options.GoalBindings) {
		return nil, ErrHostCapabilityUnavailable
	}
	if h.attachmentReserved(request.Owner, request.ResumeSessionID) {
		return nil, ErrLegacyCommitUncertain
	}
	// Adoption creates a fresh managed owner around a provider-native identity
	// already admitted in Launch. It cannot also select a managed resume.
	if request.Adopt && (request.Resume || request.ResumeSessionID != "") {
		return nil, fmt.Errorf("%w: adoption and managed resume are mutually exclusive", ErrInvalidInput)
	}
	if request.Owner == "" || request.MaterializationCtxID != "" {
		return nil, fmt.Errorf("%w: attach authority", ErrHostCapabilityUnavailable)
	}
	if request.ProjectDir != "" && !filepath.IsAbs(request.ProjectDir) || (request.ProjectDir == "") != (request.NamespaceFingerprint == "") {
		return nil, fmt.Errorf("invalid session location")
	}
	provider, err := normalizeProvider(request.Provider)
	if err != nil || provider != request.Provider {
		return nil, fmt.Errorf("invalid provider")
	}
	if err := validateLaunchPayload(provider, request.Launch); err != nil {
		return nil, err
	}
	var factory ProviderHostFactory
	for _, candidate := range h.options.Factories {
		if candidate.Provider() == provider {
			factory = candidate
			break
		}
	}
	if factory == nil {
		return nil, fmt.Errorf("%w: provider", ErrHostCapabilityUnavailable)
	}
	// A failed initial write reserves its owner even when no record reached
	// disk. Recover must resolve that exact attempt before another Attach.
	for id, pending := range h.pendingInitial {
		if pending.record.Owner == request.Owner || id == request.ResumeSessionID {
			return nil, ErrLegacyCommitUncertain
		}
	}
	var record RunnerRecord
	if request.ResumeSessionID != "" {
		record = h.sessions[request.ResumeSessionID]
		if record.SessionID == "" || record.Owner != request.Owner || record.Provider != provider {
			return nil, ErrProviderHostNotFound
		}
		if err := h.checkExplicitResumeOwnerSlot(record, provider); err != nil {
			return nil, err
		}
	}
	if record.SessionID == "" {
		for _, candidate := range h.sessions {
			if candidate.Owner == request.Owner && candidate.State != SessionStopped {
				record = candidate
				break
			}
		}
	}
	if h.records.uncertain[record.SessionID] {
		return nil, ErrLegacyCommitUncertain
	}
	// Retirement and unresolved native effects must be refused before any
	// incumbent release, just as on the canonical Attach admission path.
	if record.GracefulRetirement && record.State != SessionStopped {
		return nil, fmt.Errorf("%w: session is retiring", ErrHostCapabilityUnavailable)
	}
	if record.SessionID != "" && request.Adopt {
		released, err := h.releaseDeadAdoptionIncumbent(ctx, record)
		if err != nil {
			return nil, err
		}
		if released {
			record = RunnerRecord{}
		}
	}
	// Reusing this record would silently ignore adoption; launching another
	// native host would duplicate the active owner. Refuse before either.
	if request.Adopt && record.SessionID != "" {
		return nil, fmt.Errorf("%w: owner already has a managed session", ErrInvalidInput)
	}
	if record.SessionID != "" && record.Provider != provider {
		return nil, providerSlotConflict(record, provider)
	}
	if record.SessionID == "" {
		if result != nil {
			result.fresh = true
		}
		id, err := opaqueID("SES")
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		record = RunnerRecord{Contract: sessionRecordContract, SessionID: id, Owner: request.Owner, Provider: provider, ProjectDir: request.ProjectDir, NamespaceFingerprint: request.NamespaceFingerprint, State: SessionRunning, Liveness: SessionLivenessLive, CreatedAt: now, UpdatedAt: now}
	}
	if h.runners[record.SessionID] == nil {
		_, seen := h.sessions[record.SessionID]
		if seen {
			base, ok := h.records.bases[record.SessionID]
			if !ok {
				return nil, ErrLegacyViewStale
			}
			path, err := h.records.path(record.SessionID)
			if err != nil {
				return nil, err
			}
			if err := h.records.matchesCurrent(path, base); err != nil {
				return nil, err
			}
		}
		if seen {
			// A nil launch selection cannot erase an archived explicit budget to
			// bypass the missing live consumer. Refuse before reserving a controller.
			if err := validateCodexBudgetRelaunch(record.ProviderHost); err != nil {
				return nil, err
			}
			if err := h.validateNativeAccountingRestoreLocked(ctx, record.SessionID); err != nil {
				return nil, err
			}
		}
		epoch, err := h.epochs.reserveContext(ctx, record.SessionID)
		if err != nil {
			return nil, err
		}
		launch := ProviderLaunchRequest{SessionID: record.SessionID, Owner: record.Owner, Launch: copyLaunchSpec(request.Launch)}
		if seen {
			prior := record.ProviderHost
			prior.Snapshot = copyRunnerSnapshot(prior.Snapshot)
			launch.Prior = &prior
			launch.PriorMustFork = record.ProviderHostPendingFork
		}
		runner, err := factory.Launch(ctx, launch)
		if err != nil {
			return nil, err
		}
		if absentHostPort(runner) {
			return nil, fmt.Errorf("nil launched runner")
		}
		snapshot, err := runner.Snapshot()
		if err != nil {
			return nil, err
		}
		snapshot, err = snapshotWithCommittedAccounting(record, snapshot)
		if err != nil {
			return nil, err
		}
		record.ProviderHost = ProviderHostRecord{Provider: provider, Snapshot: copyRunnerSnapshot(snapshot)}
		record.State = SessionRunning
		record.GracefulRetirement = false
		record.Liveness = SessionLivenessLive
		record.LivenessDetail = "loaded provider host is live"
		record.Eviction = nil
		record.UpdatedAt = time.Now().UTC()
		if seen {
			err = h.updateRecord(ctx, record)
		} else {
			h.pendingInitial[record.SessionID] = pendingInitialRunner{record: copyRunnerRecord(record), epoch: epoch}
			err = h.records.create(ctx, record)
			if err == nil {
				delete(h.pendingInitial, record.SessionID)
			}
		}
		if err != nil {
			// Retain the runner and closed admission on an uncertain write; it must
			// never be discarded or signalled using an uncommitted projection.
			h.runners[record.SessionID] = runner
			return nil, err
		}
		h.sessions[record.SessionID] = record
		h.runners[record.SessionID] = runner
		h.rememberAttachmentController(record.SessionID, epoch)
		if backend, ok := runner.(hostInputBackend); ok {
			input, err := h.installInput(ctx, record.SessionID, epoch, backend)
			if err != nil {
				return nil, err
			}
			h.inputs[record.SessionID] = input
		}
	}
	if h.sessions[record.SessionID].State == SessionRecovering && h.inputs[record.SessionID] == nil {
		if err := h.restoreRetainedInputLocked(ctx, record.SessionID); err != nil {
			return nil, err
		}
	}
	if err := h.restoreNativeAccountingInstalledLocked(ctx, record.SessionID); err != nil {
		return nil, err
	}
	if err := h.reconcileInstalledHostGoalLocked(ctx, record.SessionID); err != nil {
		return nil, err
	}
	record = h.sessions[record.SessionID]
	attachment, err := h.attachmentDetailsLocked(ctx, record.SessionID)
	if err != nil {
		return nil, err
	}
	attachmentID, err := h.issueAttachment(ctx, record.SessionID)
	if err != nil {
		return nil, err
	}
	h.nudgeSessionNoticeDelivery()
	attachment.AttachmentID = attachmentID
	attachment.Session = h.view(record)
	return attachment, nil
}

func (h *SessionHost) view(record RunnerRecord) SessionView {
	count := 0
	for _, id := range h.attachments {
		if id == record.SessionID {
			count++
		}
	}
	owned := copyRunnerRecord(record)
	return SessionView{Retiring: record.GracefulRetirement && record.State != SessionStopped, SessionID: record.SessionID, Owner: record.Owner, Provider: record.Provider, State: record.State, Liveness: record.Liveness, LivenessDetail: record.LivenessDetail, OwnerRunError: record.OwnerRunError, AttachedClients: count, AppliedGoalRef: owned.AppliedGoalRef, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt, Eviction: owned.Eviction, CtxID: record.MaterializationCtxID}
}

// A failed visible transaction revokes the actual input capability, including
// handles already returned to renderers. No failed commit can leave a live
// writer behind while the caller observes an uncertain record.
func (h *SessionHost) updateRecord(ctx context.Context, record RunnerRecord) error {
	err := h.records.update(ctx, record)
	if err != nil && h.records.uncertain[record.SessionID] {
		delete(h.metadataLeases, record.SessionID)
		if input := h.inputs[record.SessionID]; input != nil {
			err = errors.Join(err, input.revokeAndDrain(context.Background()))
			delete(h.inputs, record.SessionID)
		}
	}
	return err
}

func (h *SessionHost) Session(ctx context.Context, id string) (SessionView, error) {
	if err := h.lockContext(ctx); err != nil {
		return SessionView{}, err
	}
	defer h.mu.Unlock()
	if h.attachmentChanging(id) {
		return SessionView{}, ErrLegacyCommitUncertain
	}
	if _, pending := h.pendingInitial[id]; pending || h.records.uncertain[id] {
		return SessionView{}, ErrLegacyCommitUncertain
	}
	record, ok := h.sessions[id]
	if !ok {
		return SessionView{}, ErrProviderHostNotFound
	}
	if record.ConditionalStop != nil {
		return SessionView{}, ErrLegacyCommitUncertain
	}
	// A prior retirement poll may have ACKed stopped but failed its join. Retry
	// only the retained cleanup, without another write or native exit request.
	if record.State == SessionStopped && (record.GracefulRetirement || h.gracefulJoins[id]) {
		base, ok := h.records.bases[id]
		if !ok {
			return SessionView{}, ErrLegacyViewStale
		}
		path, err := h.records.path(id)
		if err != nil {
			return SessionView{}, err
		}
		if err := h.records.matchesCurrent(path, base); err != nil {
			return SessionView{}, err
		}
		if err := h.joinStoppedRunner(ctx, id); err != nil {
			return SessionView{}, err
		}
		if err := h.forgetStoppedRunner(ctx, id); err != nil {
			return SessionView{}, err
		}
		return h.view(record), nil
	}
	if runner := h.runners[id]; runner != nil {
		state, err := runner.State(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return SessionView{}, ctx.Err()
			}
			// Disclose the failed read without releasing or rewriting its owner.
			record.Liveness = SessionLivenessUnknown
			record.LivenessDetail = "loaded provider host state could not be read: " + err.Error()
			return h.view(record), nil
		}
		switch state {
		case ProviderHostRunning:
			if record.State == SessionRecovering {
				// Only the acknowledged recovery transaction can settle Running.
				// A query cannot replace hydration, input or goal settlement.
				record.Liveness = SessionLivenessLive
				return h.view(record), nil
			}
			record.State = SessionRunning
			record.Liveness = SessionLivenessLive
		case ProviderHostStopped:
			record.State = SessionStopped
			record.Liveness = SessionLivenessDead
			if h.gracefulJoins[id] {
				record.LivenessDetail = "provider exited after graceful request; mode=graceful"
			}
		default:
			return SessionView{}, fmt.Errorf("invalid provider state")
		}
		snapshot, err := runner.Snapshot()
		if err != nil {
			return SessionView{}, err
		}
		snapshot, err = snapshotWithCommittedAccounting(record, snapshot)
		if err != nil {
			return SessionView{}, err
		}
		record.ProviderHost = ProviderHostRecord{Provider: record.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
		record.UpdatedAt = time.Now().UTC()
		if err := h.updateRecord(ctx, record); err != nil {
			return SessionView{}, err
		}
		h.sessions[id] = record
		// Either retirement path can leave an observed runner pending. Only
		// this final stopped ACK licenses its observation/attachment cleanup.
		if record.State == SessionStopped && (record.GracefulRetirement || h.gracefulJoins[id]) {
			if err := h.joinStoppedRunner(ctx, id); err != nil {
				return SessionView{}, err
			}
			if err := h.forgetStoppedRunner(ctx, id); err != nil {
				return SessionView{}, err
			}
		}
	}
	return h.view(record), nil
}

func (h *SessionHost) List(ctx context.Context) ([]SessionView, error) {
	if err := h.lockContext(ctx); err != nil {
		return nil, err
	}
	defer h.mu.Unlock()
	out := make([]SessionView, 0, len(h.sessions))
	for _, record := range h.sessions {
		out = append(out, h.view(record))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })
	return out, nil
}
func (h *SessionHost) Detach(ctx context.Context, id, attachmentID string) (SessionView, error) {
	if err := h.lockContext(ctx); err != nil {
		return SessionView{}, err
	}
	defer h.mu.Unlock()
	record, ok := h.sessions[id]
	if !ok {
		return SessionView{}, ErrProviderHostNotFound
	}
	if attached, ok := h.attachments[attachmentID]; !ok || attached != id {
		return SessionView{}, ErrProviderHostNotFound
	}
	delete(h.attachments, attachmentID)
	h.pruneEmptyProxyRootLocked()
	return h.view(record), nil
}

func (h *SessionHost) Input(ctx context.Context, id string) (ControllerLease, HostInputWriter, error) {
	if err := h.lockContext(ctx); err != nil {
		return ControllerLease{}, nil, err
	}
	defer h.mu.Unlock()
	if h.freshAttachmentReserved(id) || !h.nativeAccountingAdmitted(ctx, id) || h.goalRecoveries[id] != nil && !h.goalRecoveryAdmittedLocked(ctx, id) {
		return ControllerLease{}, nil, ErrLegacyCommitUncertain
	}
	if _, pending := h.pendingInitial[id]; pending || h.records.uncertain[id] {
		return ControllerLease{}, nil, ErrLegacyCommitUncertain
	}
	if h.sessions[id].ConditionalStop != nil {
		return ControllerLease{}, nil, ErrLegacyCommitUncertain
	}
	input, ok := h.inputs[id]
	if !ok {
		return ControllerLease{}, nil, fmt.Errorf("%w: provider input", ErrHostCapabilityUnavailable)
	}
	lease, err := input.CurrentLease(ctx, id)
	return lease, input, err
}

func (h *SessionHost) factory(provider string) ProviderHostFactory {
	for _, factory := range h.options.Factories {
		if factory.Provider() == provider {
			return factory
		}
	}
	return nil
}

// Recover validates committed records during NewHost before restoring any
// provider. On failure the caller retains this host, its lock and prior runners.
func (h *SessionHost) Recover(ctx context.Context) error {
	if err := h.invalidateAttachmentPreparations(ctx); err != nil {
		return err
	}
	if err := h.lockContext(ctx); err != nil {
		return err
	}
	defer h.mu.Unlock()
	pendingIDs := make([]string, 0, len(h.pendingInitial))
	for id := range h.pendingInitial {
		pendingIDs = append(pendingIDs, id)
	}
	sort.Strings(pendingIDs)
	// A recovery may commit one pending initial record before another fails.
	// Keep every pending owner fenced until the entire recovery succeeds; an
	// earlier visible commit is reloaded exactly on the next attempt.
	resolved := make(map[string]RunnerRecord, len(pendingIDs))
	for _, id := range pendingIDs {
		committed, err := h.records.recoverInitial(ctx, h.pendingInitial[id].record)
		if err != nil {
			return err
		}
		resolved[id] = committed
	}
	ids := make([]string, 0, len(h.sessions))
	for id := range h.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if h.nativeAccountingRestores[id] != nil {
			return ErrLegacyCommitUncertain
		}
		record := h.sessions[id]
		committed, err := h.records.load(ctx, id)
		if err != nil {
			return err
		}
		if committed.ConditionalStop != nil {
			if _, err := recordMetadataPhase(committed); err != nil {
				return err
			}
		}
		if committed.Owner != record.Owner || committed.Provider != record.Provider {
			return ErrLegacyViewStale
		}
		record = committed
		delete(h.metadataLeases, id)
		h.sessions[id] = record
		// A conditional image is metadata recovery input, never native Restore
		// authority. The fork must durably clear its checked phase first.
		if record.ConditionalStop != nil {
			if input := h.inputs[id]; input != nil {
				if err := input.revokeAndDrain(ctx); err != nil {
					h.records.markUncertain(id)
					return err
				}
				delete(h.inputs, id)
			}
			continue
		}
		// Reload also resolves an uncertain record commit for an existing
		// runner, retaining that runner without a second Restore or signal.
		if record.State == SessionStopped {
			if err := h.joinStoppedRunner(ctx, id); err != nil {
				return err
			}
			if err := h.forgetStoppedRunner(ctx, id); err != nil {
				return err
			}
			continue
		}
		if runner := h.runners[id]; runner != nil {
			if _, ok := runner.(hostInputBackend); ok && h.inputs[id] == nil && !record.GracefulRetirement {
				if err := h.restoreRetainedInputLocked(ctx, id); err != nil {
					return err
				}
				if err := h.restoreNativeAccountingInstalledLocked(ctx, id); err != nil {
					return err
				}
			}
			if err := h.restoreProviderProxyLocked(ctx, id); err != nil {
				return err
			}
			continue
		}
		epoch, err := h.epochs.reserveContext(ctx, id)
		if err != nil {
			return err
		}
		if err := h.validateNativeAccountingRestoreLocked(ctx, id); err != nil {
			return err
		}
		request := ProviderHostRecord{Provider: record.Provider, Snapshot: copyRunnerSnapshot(record.ProviderHost.Snapshot)}
		var restored RunnerHost
		if record.GracefulRetirement {
			factory, ok := h.factory(record.Provider).(RetiringHostFactory)
			if !ok {
				return fmt.Errorf("%w: retirement observer", ErrHostCapabilityUnavailable)
			}
			restored, err = factory.RestoreRetiring(ctx, request)
		} else {
			restored, err = h.factory(record.Provider).Restore(ctx, request)
		}
		if err != nil {
			return err
		}
		if absentHostPort(restored) {
			return fmt.Errorf("nil restored runner")
		}
		h.runners[id] = restored
		h.rememberAttachmentController(id, epoch)
		if backend, ok := restored.(hostInputBackend); ok && !record.GracefulRetirement {
			input, err := h.installInput(ctx, id, epoch, backend)
			if err != nil {
				return err
			}
			h.inputs[id] = input
			if err := h.restoreNativeAccountingInstalledLocked(ctx, id); err != nil {
				return err
			}
		}
		if err := h.restoreProviderProxyLocked(ctx, id); err != nil {
			return err
		}
	}
	// Publish one committed pending record under mu, install its writer, then
	// enter its private hydration scope before releasing mu. Untouched siblings
	// keep pendingInitial and have no writer if this session fails hydration.
	for _, id := range pendingIDs {
		pending := h.pendingInitial[id]
		h.sessions[id] = resolved[id]
		delete(h.pendingInitial, id)
		h.rememberAttachmentController(id, pending.epoch)
		if backend, ok := h.runners[id].(hostInputBackend); ok && h.inputs[id] == nil {
			input, err := h.installInput(ctx, id, pending.epoch, backend)
			if err != nil {
				return err
			}
			h.inputs[id] = input
		}
		if err := h.restoreNativeAccountingInstalledLocked(ctx, id); err != nil {
			return err
		}
		if err := h.restoreProviderProxyLocked(ctx, id); err != nil {
			return err
		}
	}
	for token, staged := range h.preparations {
		if staged.phase == "invalid" {
			delete(h.preparations, token)
		}
	}

	for id, record := range h.sessions {
		if record.State != SessionStopped && !record.GracefulRetirement {
			if err := h.reconcileInstalledHostGoalLocked(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}
