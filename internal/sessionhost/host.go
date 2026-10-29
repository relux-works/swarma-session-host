// Package sessionhost implements the fixture host without harness-specific types.
package sessionhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"sync"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/admission"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

const MaxRecords = 32

var scalar = regexp.MustCompile(`^[A-Za-z0-9._:/@+\-]{1,128}$`)

// Selection is opaque data, not a model catalog or launch admission decision.
type Selection struct {
	Vendor    string `json:"vendor"`
	Model     string `json:"model"`
	Reasoning string `json:"reasoning"`
}

// Notice contains a reference, never mailbox content or an arbitrary prompt.
type Notice struct {
	ID        string `json:"id"`
	Reference string `json:"reference"`
}

// InputState is a conservative observation, not proof of real harness readiness.
// An adapter must supply idle explicitly; missing or unrecognized evidence closes input.
type InputState string

const (
	InputIdle         InputState = "idle"
	InputBusy         InputState = "busy"
	InputDraft        InputState = "draft"
	InputApproval     InputState = "approval"
	InputUnknown      InputState = "unknown"
	InputSecurityStop InputState = "security-stop"
)

func validInputState(s InputState) bool {
	switch s {
	case InputIdle, InputBusy, InputDraft, InputApproval, InputUnknown, InputSecurityStop:
		return true
	}
	return false
}

type Adapter interface {
	Name() string
	// InputState returns a bounded local snapshot; it must not block on I/O.
	InputState() InputState
	Encode(Notice, Selection) ([]byte, error)
	// Applied inspects the bounded complete output prefix. Missing evidence is
	// false; malformed evidence is an error, not permission to retry delivery.
	Applied([]byte, Notice, Selection) (bool, error)
}

type Record struct {
	Notice         Notice `json:"notice"`
	Digest         string `json:"digest"`
	Sequence       uint64 `json:"sequence"`
	State          string `json:"state"`
	NativeAccepted bool   `json:"native_accepted,omitempty"`
	NativeSession  string `json:"native_session,omitempty"`
	NativeTurn     string `json:"native_turn,omitempty"`
	NativeReceipt  string `json:"native_receipt,omitempty"`
	Outcome        string `json:"outcome,omitempty"`
}

type State struct {
	Version         int                 `json:"version"`
	Admission       *admission.Snapshot `json:"admission,omitempty"`
	Execution       string              `json:"execution"`
	RunnerInstance  string              `json:"runner_instance"`
	Adapter         string              `json:"adapter"`
	Selection       Selection           `json:"selection"`
	Epoch           uint64              `json:"epoch"`
	Sequence        uint64              `json:"sequence"`
	Fence           string              `json:"fence"`
	Records         []Record            `json:"records"`
	StopRequested   bool                `json:"stop_requested"`
	InputState      InputState          `json:"input_state"`
	SecurityStopped bool                `json:"security_stopped"`
	Protocol        *ProtocolState      `json:"protocol,omitempty"`
}

type Config struct {
	// Admission is an opt-in internal synthetic proof seam, not a hosted launcher.
	Admission       *admission.Config
	StateDir        string
	Socket          string
	Execution       string
	Selection       Selection
	Adapter         Adapter
	IOTimeout       time.Duration
	ProtocolName    string
	ProtocolFactory func([]byte) (ap.Driver, uint64, error)
	PollInterval    time.Duration
}

type Host struct {
	cfg            Config
	store          *store
	client         *runner.Client
	mu             sync.Mutex
	state          State
	err            error
	ctx            context.Context
	cancel         context.CancelFunc
	kick           chan struct{}
	wg             sync.WaitGroup
	closeOnce      sync.Once
	driver         ap.Driver
	outputOffset   uint64
	recovering     bool
	protocolFailed bool
	writeBlocked   bool
}

func validNotice(n Notice) bool { return scalar.MatchString(n.ID) && scalar.MatchString(n.Reference) }

func validSelection(s Selection) bool {
	return scalar.MatchString(s.Vendor) && scalar.MatchString(s.Model) && scalar.MatchString(s.Reasoning)
}

func digestNotice(n Notice, s Selection) string {
	data, _ := json.Marshal(struct {
		Notice    Notice
		Selection Selection
	}{n, s})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func Open(cfg Config) (*Host, error) {
	name := cfg.ProtocolName
	protocol := cfg.ProtocolFactory != nil
	if !protocol && cfg.Adapter != nil {
		name = cfg.Adapter.Name()
	}
	if (protocol && (cfg.Adapter != nil || cfg.PollInterval <= 0 || cfg.PollInterval > time.Minute)) || (!protocol && (cfg.Adapter == nil || cfg.ProtocolName != "")) || !scalar.MatchString(name) || !scalar.MatchString(cfg.Execution) || !validSelection(cfg.Selection) || cfg.IOTimeout <= 0 || cfg.IOTimeout > time.Minute {
		return nil, errors.New("invalid_host_config")
	}
	if cfg.Admission != nil {
		c := *cfg.Admission // Freeze caller-owned expectation; provider callbacks remain injected.
		cfg.Admission = &c
		b := c.Expected.Record.Binding
		if admission.ValidateConfig(c) != nil || b.Execution != cfg.Execution || b.RunnerSocket != cfg.Socket || b.Runtime != name || b.Vendor != cfg.Selection.Vendor || b.Model != cfg.Selection.Model || b.Effort != cfg.Selection.Reasoning {
			return nil, errors.New("admission_config_conflict")
		}
	}
	store, err := openStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	state, exists, err := store.read()
	if err != nil {
		store.close()
		return nil, err
	}
	if exists && (state.Execution != cfg.Execution || state.Adapter != name || state.Selection != cfg.Selection || (state.Protocol != nil) != protocol) {
		store.close()
		return nil, errors.New("stored_plan_conflict")
	}
	if !exists {
		state = State{Version: 1, Execution: cfg.Execution, Adapter: name, Selection: cfg.Selection, Fence: "held", InputState: InputUnknown}
	}
	if exists && ((state.Admission != nil) != (cfg.Admission != nil) || (state.Admission != nil && state.Admission.Failed)) {
		store.close()
		return nil, errors.New("admission_reconciliation_required")
	}
	if !exists && cfg.Admission != nil {
		marker := cfg.Admission.Expected
		marker.Failed = true
		state.Version = 2
		state.Admission = &marker // A crash before verification cannot become admission.
	}
	var driver ap.Driver
	var outputOffset uint64
	if protocol {
		var saved []byte
		if state.Protocol != nil {
			saved = state.Protocol.Checkpoint
		}
		driver, outputOffset, err = cfg.ProtocolFactory(saved)
		if err != nil || driver == nil || driver.Descriptor().Name != name || ap.Validate(driver.Descriptor(), driver.Snapshot()) != nil || outputOffset > runner.MaxOutput {
			store.close()
			return nil, errors.New("protocol_restore_refused")
		}
		if state.Protocol == nil {
			state.Protocol = &ProtocolState{BoundaryVersion: ap.Version, ObservedRelease: driver.Descriptor().ObservedRelease, Checkpoint: driver.Snapshot().Checkpoint, View: driver.Snapshot().View}
		} else if state.Protocol.ObservedRelease != driver.Descriptor().ObservedRelease {
			store.close()
			return nil, errors.New("protocol_release_conflict")
		}
		state.InputState = InputUnknown
		if cfg.Admission != nil && cfg.Admission.Expected.Record.Binding.NativeRelease != driver.Descriptor().ObservedRelease {
			store.close()
			return nil, errors.New("admission_release_conflict")
		}
	}
	state.Epoch++
	if state.Epoch == 0 {
		store.close()
		return nil, errors.New("epoch_exhausted")
	}
	// Preserve required intent with delivery closed before any runner contact.
	priorFence := state.Fence
	if state.Fence != "unknown" {
		state.Fence = "held"
	}
	if err := store.write(state); err != nil {
		store.close()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.IOTimeout)
	defer cancel()
	client, err := runner.Dial(ctx, cfg.Socket)
	if err != nil {
		store.close()
		return nil, errors.New("runner_unavailable")
	}
	claim, err := client.Call(ctx, runner.Request{Op: "claim", Execution: cfg.Execution, Epoch: state.Epoch})
	// A canceled poll may close the old connection just before its handler
	// releases ownership. Wait only for that bounded protocol handoff; never
	// raise the epoch or displace a still-live controller to make it pass.
	for protocol && err == nil && claim.Code == "controller_conflict" && ctx.Err() == nil {
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		if ctx.Err() != nil {
			break
		}
		claim, err = client.Call(ctx, runner.Request{Op: "claim", Execution: cfg.Execution, Epoch: state.Epoch})
	}
	if err != nil || claim.Code != "ok" || len(claim.Instance) != 32 || claim.PID <= 0 || claim.Epoch != state.Epoch || (exists && state.RunnerInstance != "" && state.RunnerInstance != claim.Instance) {
		client.Close()
		store.close()
		return nil, errors.New("runner_claim_refused")
	}
	state.RunnerInstance = claim.Instance
	if cfg.Admission != nil {
		var prior *admission.Snapshot
		if exists {
			prior = state.Admission
		}
		proof, proofErr := admission.Verify(ctx, *cfg.Admission, prior, claim.Instance, claim.PID)
		if proofErr != nil {
			state.Fence = "unknown"
			state.InputState = InputUnknown
			if state.Admission != nil {
				failed := *state.Admission
				failed.Failed = true
				state.Admission = &failed
			}
			persistErr := store.write(state)
			client.Close()
			store.close()
			if persistErr != nil {
				return nil, errors.New("host_storage_unavailable")
			}
			return nil, proofErr
		}
		state.Admission = &proof
	}
	if protocol && (claim.ProtocolVersion != runner.ProtocolVersion || claim.WriteReceiptVersion != runner.WriteReceiptVersion) {
		client.Close()
		store.close()
		return nil, errors.New("runner_protocol_unsupported")
	}
	state.Fence = "open"
	if priorFence == "unknown" || (exists && priorFence == "held") {
		state.Fence = priorFence
	}
	for _, record := range state.Records {
		if record.State == "unknown" {
			state.Fence = "unknown"
		}
	}
	if claim.Exited || claim.Overflow || (protocol && claim.OutputEnded) {
		state.Fence = "unknown"
	}
	if protocol && (claim.Writes > int(state.Protocol.WireSequence) || outputOffset > uint64(claim.Offset)) {
		state.Fence = "unknown"
	}
	if state.StopRequested {
		state.Fence = "held"
	}
	if err := store.write(state); err != nil {
		client.Close()
		store.close()
		return nil, err
	}
	workCtx, workCancel := context.WithCancel(context.Background())
	h := &Host{cfg: cfg, store: store, client: client, state: state, ctx: workCtx, cancel: workCancel, kick: make(chan struct{}, 1), driver: driver, outputOffset: outputOffset, recovering: exists}
	if err := h.ObserveInput(); err != nil {
		h.cancel()
		client.Close()
		store.close()
		return nil, err
	}
	if protocol {
		transition, beginErr := driver.Begin()
		h.mu.Lock()
		if beginErr == nil {
			beginErr = h.persistTransitionLocked(transition)
		}
		h.mu.Unlock()
		if beginErr != nil {
			h.cancel()
			client.Close()
			store.close()
			return nil, errors.New("protocol_begin_refused")
		}
	}
	h.wg.Add(1)
	if protocol {
		go h.dispatchProtocol()
	} else {
		go h.dispatch()
	}
	h.wake()
	return h, nil
}

func (h *Host) persistLocked(next State) error {
	if h.ctx.Err() != nil {
		return errors.New("host_unavailable")
	}
	if h.err != nil {
		return errors.New("host_storage_unavailable")
	}
	if err := h.store.write(next); err != nil {
		h.err = err
		h.state.Fence = "unknown"
		return errors.New("host_storage_unavailable")
	}
	h.state = next
	return nil
}

func (h *Host) Snapshot() State {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneState(h.state)
}

func (h *Host) Enqueue(notice Notice) (Record, error) {
	if !validNotice(notice) {
		return Record{}, errors.New("invalid_notice")
	}
	if h.driver == nil {
		data, err := h.cfg.Adapter.Encode(notice, h.cfg.Selection)
		if err != nil || len(data) == 0 || len(data) > 4096 {
			return Record{}, errors.New("adapter_input_refused")
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil || h.err != nil {
		return Record{}, errors.New("host_unavailable")
	}
	digest := digestNotice(notice, h.state.Selection)
	for _, record := range h.state.Records {
		if record.Notice.ID == notice.ID {
			if record.Digest != digest {
				return Record{}, errors.New("request_conflict")
			}
			return record, nil
		}
	}
	if h.state.StopRequested {
		return Record{}, errors.New("session_stopped")
	}
	if h.state.SecurityStopped {
		return Record{}, errors.New("security_stopped")
	}
	if len(h.state.Records) >= MaxRecords {
		return Record{}, errors.New("queue_capacity_exceeded")
	}
	next := h.state
	next.Sequence++
	record := Record{Notice: notice, Digest: digest, Sequence: next.Sequence, State: "queued"}
	next.Records = append(slices.Clone(next.Records), record)
	if err := h.persistLocked(next); err != nil {
		return Record{}, err
	}
	h.wake()
	return record, nil
}

func (h *Host) Fence(open bool) error {
	if err := h.ObserveInput(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil {
		return errors.New("host_unavailable")
	}
	if h.state.Fence == "unknown" {
		return errors.New("reconciliation_required")
	}
	if open && h.state.StopRequested {
		return errors.New("session_stopped")
	}
	if open && h.state.SecurityStopped {
		return errors.New("security_stopped")
	}
	if open && h.state.InputState != InputIdle {
		return errors.New("input_ineligible")
	}
	next := h.state
	next.Fence = "held"
	if open {
		next.Fence = "open"
	}
	if err := h.persistLocked(next); err != nil {
		return err
	}
	h.wake()
	return nil
}

// ObserveInput refreshes adapter evidence and wakes queued work. Security STOP is
// durable and cannot be cleared by idle evidence, fence release or host restart.
// Like holding the manual fence, a new observation cannot retract an issued write.
func (h *Host) ObserveInput() error {
	if err := h.observeInput(); err != nil {
		return err
	}
	h.wake()
	return nil
}

func (h *Host) observeInput() error {
	if h.driver != nil {
		return nil
	} // The ordered output worker owns protocol evidence.
	observation := h.cfg.Adapter.InputState()
	if !validInputState(observation) {
		observation = InputUnknown
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil || h.err != nil {
		return errors.New("host_unavailable")
	}
	next := h.state
	next.InputState = observation
	next.SecurityStopped = next.SecurityStopped || observation == InputSecurityStop
	if next.Admission != nil && next.Admission.Failed {
		next.InputState = InputUnknown
	}
	if next.InputState != h.state.InputState || next.SecurityStopped != h.state.SecurityStopped {
		if err := h.persistLocked(next); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) wake() {
	select {
	case h.kick <- struct{}{}:
	default:
	}
}

func (h *Host) dispatch() {
	defer h.wg.Done()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.kick:
		}
		for h.deliverNext() {
		}
	}
}

func (h *Host) deliverNext() bool {
	if h.checkAdmission() != nil {
		return false
	}
	if err := h.observeInput(); err != nil {
		return false
	}
	h.mu.Lock()
	if h.ctx.Err() != nil || h.err != nil || h.state.Fence != "open" || h.state.InputState != InputIdle || h.state.SecurityStopped {
		h.mu.Unlock()
		return false
	}
	index := -1
	for i, record := range h.state.Records {
		if record.State == "queued" {
			index = i
			break
		}
	}
	if index < 0 {
		h.mu.Unlock()
		return false
	}
	next := h.state
	next.Records = slices.Clone(next.Records)
	next.Records[index].State = "unknown"
	if err := h.persistLocked(next); err != nil {
		h.mu.Unlock()
		return false
	}
	record, epoch := next.Records[index], next.Epoch
	h.mu.Unlock()
	// No host state lock crosses adapter/runner I/O. The persisted unknown
	// record is the in-flight marker and prevents replay after a crash.
	data, err := h.cfg.Adapter.Encode(record.Notice, h.cfg.Selection)
	ctx, cancel := context.WithTimeout(h.ctx, h.cfg.IOTimeout)
	defer cancel()
	applied := false
	// Re-observe after adapter preparation: a lease can be lost while Encode runs.
	if err == nil {
		err = h.checkAdmission()
	}
	if err == nil {
		response, callErr := h.client.Call(ctx, runner.Request{Op: "write", Execution: h.cfg.Execution, Epoch: epoch, Sequence: record.Sequence, Data: data})
		if callErr == nil && response.Code == "written" {
			var output []byte
			for ctx.Err() == nil {
				read, readErr := h.client.Call(ctx, runner.Request{Op: "read", Execution: h.cfg.Execution, Epoch: epoch, Offset: len(output)})
				if readErr != nil || read.Code != "ok" || len(read.Data) == 0 {
					break
				}
				output = append(output, read.Data...)
				applied, err = h.cfg.Adapter.Applied(output, record.Notice, h.cfg.Selection)
				if applied || err != nil {
					break
				}
			}
		}
	}
	if err := h.observeInput(); err != nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil || h.state.Epoch != epoch {
		return false
	}
	next = h.state
	next.Records = slices.Clone(next.Records)
	if applied {
		next.Records[index].State = "applied"
	} else {
		next.Fence = "unknown"
	}
	if err := h.persistLocked(next); err != nil {
		return false
	}
	return applied
}

func (h *Host) Stop(ctx context.Context) (runner.Response, error) {
	if _, ok := ctx.Deadline(); !ok {
		return runner.Response{}, errors.New("deadline_required")
	}
	if err := ctx.Err(); err != nil {
		return runner.Response{}, err
	}
	h.mu.Lock()
	next := h.state
	next.Fence = "held"
	next.StopRequested = true
	if h.state.Fence == "unknown" {
		next.Fence = "unknown"
	}
	err := h.persistLocked(next)
	epoch := h.state.Epoch
	h.mu.Unlock()
	if err != nil {
		return runner.Response{}, err
	}
	return h.client.Call(ctx, runner.Request{Op: "stop", Execution: h.cfg.Execution, Epoch: epoch})
}

// Close releases only the controller; it never stops the runner child.
func (h *Host) Close() error {
	h.closeOnce.Do(func() {
		h.cancel()
		h.wg.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), h.cfg.IOTimeout)
		h.client.Call(ctx, runner.Request{Op: "release", Execution: h.cfg.Execution, Epoch: h.state.Epoch})
		cancel()
		h.client.Close()
		h.store.close()
	})
	return nil
}
