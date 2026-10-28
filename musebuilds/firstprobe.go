package musebuilds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
	"github.com/relux-works/swarma-session-host/musebuilds/probeprocess"
)

const (
	StageProbeExecute          = "probe-execute"
	StageProbeCleanup          = "probe-cleanup"
	StageRecovery              = "probe-recovery"
	CodeProbeAttemptFailed     = "muse_probe_attempt_failed"
	CodeProbeCleanupIncomplete = "muse_probe_cleanup_incomplete"
	CodeProbeBusy              = "muse_probe_busy"
	CodeRecoveryUnavailable    = "muse_recovery_unavailable"
	firstProbeCap              = 30 * time.Second
	probeCleanupCap            = 5 * time.Second
	probeStreamCap             = 64 * 1024
)

// ProbeLauncher is the process adapter seam; nil selects the production
// owned-group launcher. Tests use fake processes, never the host global lock.
type ProbeLauncher = probeprocess.Launcher

// ProbeRequest binds the first probe to one invocation and its discovery.
// Env is explicit parent input to the module-owned curated environment filter.
type ProbeRequest struct {
	AttemptID, SelectionID string
	Env                    []string
}

// ProbeExecution is structured process evidence, never a diagnostic guess.
// Zero values remain readable for receipts made through the recording API.
type ProbeExecution struct {
	Contract     string `json:"contract,omitempty"`
	Stage        string `json:"stage,omitempty"`
	Attempted    bool   `json:"attempted"`
	Started      bool   `json:"started"`
	ExitCode     int    `json:"exit_code"`
	Signal       string `json:"signal,omitempty"`
	Deadline     bool   `json:"deadline"`
	Reaped       bool   `json:"reaped"`
	GroupGone    bool   `json:"group_gone"`
	Drained      bool   `json:"drained"`
	StderrDigest string `json:"stderr_digest,omitempty"`
	StderrBytes  int64  `json:"stderr_bytes"`
}

// ProbeResult contains the state and independent failure receipt produced by
// the first probe. Success is PROBE_OK, not an actual startup or last-good.
type ProbeResult struct {
	Frozen      FrozenIdentity
	Record      ProbeRecord
	Observation ProbeObservation
}

// ProbeRunner owns one invocation's probe and recovery admission. Use the
// same runner until startup/recovery ends. An unconfirmed child latches the
// runner closed; making a fresh runner is not reconciliation of an orphan.
// The consumer must persist live/orphan supervision for process-wide recovery
// (design section 3.5); this runner never infers that a prior process is dead.
type ProbeRunner struct {
	mu                            sync.Mutex
	active, blocked, recoveryUsed bool
	launcher                      ProbeLauncher
	budget                        func(context.Context, time.Duration) (context.Context, context.CancelFunc)
	failure                       *probeFailure
}

type probeFailure struct {
	cfg    HostConfig
	result ProbeResult
	cause  error
}

func NewProbeRunner(launcher ProbeLauncher) *ProbeRunner {
	if launcher == nil {
		launcher = probeprocess.OSLauncher{}
	}
	return &ProbeRunner{launcher: launcher, budget: context.WithTimeout}
}

func (r *ProbeRunner) enter(ctx context.Context) error {
	if err := probeRefuseIfCanceled(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.blocked {
		return &ProbeError{Stage: StageProbeCleanup, Code: CodeProbeCleanupIncomplete}
	}
	if r.active {
		return &ProbeError{Stage: StageProbeExecute, Code: CodeProbeBusy}
	}
	r.active = true
	return nil
}

func (r *ProbeRunner) leave() { r.mu.Lock(); r.active = false; r.mu.Unlock() }

// Freeze orchestrates publication and the first probe within the same outer
// 300-second envelope. PublishFrozen's accepted admission/copy behavior is
// unchanged; its global admission has been released before a child can start.
func (r *ProbeRunner) Freeze(ctx context.Context, cfg HostConfig, d Discovery, req ProbeRequest) (ProbeResult, error) {
	ctx, cancel := context.WithTimeout(ctx, freezeEnvelope)
	defer cancel()
	if err := r.enter(ctx); err != nil {
		return ProbeResult{}, err
	}
	defer r.leave()
	r.failure = nil
	if req.SelectionID != d.SnapshotID {
		return ProbeResult{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid}
	}
	copy, err := PublishFrozen(ctx, cfg, d)
	if err != nil {
		return ProbeResult{}, err
	}
	return r.probeFrozen(ctx, cfg, d, copy.Frozen, req)
}

// ProbeFrozen resumes a COPIED first probe for this invocation's accepted
// newest discovery, or verifies/reuses PROBE_OK without double-probing.
// Only a verified frozen descriptor reaches the launcher, and only for
// the discovery's own tuple: a differing tuple is refused before child
// admission, and any non-newest execution requires the recorded
// failure-to-exact-last-good permit consumed by Recover.
func (r *ProbeRunner) ProbeFrozen(ctx context.Context, cfg HostConfig, d Discovery, id FrozenIdentity, req ProbeRequest) (ProbeResult, error) {
	ctx, cancel := context.WithTimeout(ctx, freezeEnvelope)
	defer cancel()
	if err := r.enter(ctx); err != nil {
		return ProbeResult{}, err
	}
	defer r.leave()
	return r.probeFrozen(ctx, cfg, d, id, req)
}

func (r *ProbeRunner) probeFrozen(ctx context.Context, cfg HostConfig, d Discovery, id FrozenIdentity, req ProbeRequest) (ProbeResult, error) {
	r.failure = nil
	if !validAttemptToken(req.AttemptID) || !validAttemptToken(req.SelectionID) {
		return ProbeResult{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid}
	}
	// Design section 5 admits exactly one execution route per tuple: the
	// invocation's selected newest runs as the primary probe, and any
	// differing tuple runs only as the exact last-good under the
	// recorded failure permit consumed by Recover. Path is not
	// compared: the discovery path names the installed source while
	// the frozen path names the published copy, and publication
	// derives the frozen Build+SHA256 from this same discovery.
	if req.SelectionID != d.SnapshotID {
		return ProbeResult{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid}
	}
	if id.Build != d.Identity.Build || id.SHA256 != d.Identity.SHA256 {
		return ProbeResult{}, &ProbeError{Stage: StageProbe, Code: CodeProbeSelectionMismatch}
	}
	if err := validateFrozenReference(cfg.FrozenRoot, id); err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeInvalid, err)
	}
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	defer store.close()
	cfg.store = store
	release, err := acquireMaintenanceLeaseAt(ctx, store.root)
	if err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeLockFailed, err)
	}
	defer func() { _ = release() }()
	key := freezeKey(id.Build, id.SHA256)
	state, present, err := readBuildStateAt(ctx, store.meta, key)
	if err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	if !present {
		return ProbeResult{}, &ProbeError{Stage: StageProbe, Code: CodeProbeUnprepared}
	}
	if err := state.agreesWith(key, id); err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	leaf, file, err := openFrozenBytes(store, key, id.Build, true)
	if err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	defer leaf.close()
	defer file.Close()
	before, err := descriptorIdentity(file)
	if err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	digest, err := hashFileDescriptor(ctx, StageProbe, file)
	if err != nil || digest != id.SHA256 {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeStateInconsistent, errors.Join(err, errors.New("frozen digest mismatch before probe")))
	}
	after, err := descriptorIdentity(file)
	if err != nil || !sameIdentity(before, after) {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeStateInconsistent, errors.New("frozen descriptor changed before probe"))
	}
	if state.State == StateProbeOK {
		return ProbeResult{Frozen: id, Record: ProbeRecord{State: state}}, nil
	}
	if state.State == StateBAD {
		return ProbeResult{Frozen: id, Record: ProbeRecord{State: state}}, &ProbeError{Stage: StageProbe, Code: CodeProbeRecordedBad, Bad: state.Verdict}
	}
	env, err := (&muse.System{}).ChildEnv(req.Env, agentic.LaunchRequest{})
	if err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeInvalid, err)
	}
	if err := ctx.Err(); err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeCanceled, err)
	}
	// The inherited descriptor's offset is not execution authority, but reset
	// it for adapters which inspect the verified file before starting.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ProbeResult{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	executionCtx, cancelExecution := r.budget(ctx, firstProbeCap)
	p, startErr := r.launcher.Start(executionCtx, file, env)
	if errors.Is(startErr, probeprocess.ErrNotAttempted) {
		cancelExecution()
		return ProbeResult{}, &ProbeError{Stage: StageProbeExecute, Code: CodeRecoveryUnavailable, Cause: startErr}
	}
	obs := ProbeObservation{AttemptID: req.AttemptID, SelectionID: req.SelectionID, Cleanup: CleanupConfirmed}
	obs.Execution = ProbeExecution{Contract: ProbeContractID, Stage: StageProbeExecute, Attempted: true, ExitCode: -1}
	if startErr != nil {
		obs.Outcome = ProbeExecFailed
		obs.Detail = "first-probe fork/exec failed"
	} else {
		obs = r.collectProbe(ctx, executionCtx, p, id, obs)
	}
	cancelExecution()
	if obs.Cleanup == CleanupIncomplete {
		r.mu.Lock()
		r.blocked = true
		r.mu.Unlock()
	}
	result := ProbeResult{Frozen: id, Observation: obs}
	if err := ctx.Err(); err != nil {
		return result, probeRefusal(ctx, CodeProbeCanceled, err)
	}
	// The same descriptor is rehashed after observation. A path replacement or
	// in-place change is an integrity refusal, never a deterministic BAD.
	digest, err = hashFileDescriptor(ctx, StageProbe, file)
	after, statErr := descriptorIdentity(file)
	if err != nil || statErr != nil || digest != id.SHA256 || !sameIdentity(before, after) {
		return result, probeRefusal(ctx, CodeProbeStateInconsistent, errors.Join(err, statErr, errors.New("frozen bytes changed during probe")))
	}
	result.Record, err = RecordProbeOutcome(ctx, cfg, id, obs)
	if obs.Cleanup == CleanupIncomplete {
		cleanupErr := &ProbeError{Stage: StageProbeCleanup, Code: CodeProbeCleanupIncomplete, Cause: err}
		r.failure = &probeFailure{cfg: cfg, result: result, cause: cleanupErr}
		return result, cleanupErr
	}
	if obs.Outcome == ProbeExitOKMatch {
		return result, err
	}
	failure := &ProbeError{Stage: StageProbeExecute, Code: CodeProbeAttemptFailed, Cause: err}
	// Persisted failure is necessary but not sufficient: integrity conflicts
	// and unavailable records must never be converted into a recovery offer.
	publicationFailure := false
	if refusal, ok := AsProbeError(err); ok {
		publicationFailure = refusal.Code == CodeProbePublicationFailed || refusal.Code == CodeProbePublicationUnknown
	}
	if (err == nil || publicationFailure) && result.Record.Receipt != nil {
		r.failure = &probeFailure{cfg: cfg, result: result, cause: failure}
	}
	return result, failure
}

type probeDrain struct {
	data []byte
	err  error
	over bool
}

func readProbeStream(pipe io.ReadCloser, overflow chan<- struct{}) <-chan probeDrain {
	ch := make(chan probeDrain, 1)
	go func() {
		// LimitReader bounds allocation DURING reading. The extra byte is the
		// refusal witness; exactly-at-cap data is admissible only with EOF.
		data, err := io.ReadAll(io.LimitReader(pipe, probeStreamCap+1))
		over := len(data) > probeStreamCap
		if over {
			data = data[:probeStreamCap]
			overflow <- struct{}{}
			// Continue draining without retaining output. The controller kills
			// the group on the notification and closes this pipe at its bound.
			_, err = io.Copy(io.Discard, pipe)
		}
		ch <- probeDrain{data: data, err: err, over: over}
	}()
	return ch
}

func outputHash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func (r *ProbeRunner) collectProbe(outer, execution context.Context, p probeprocess.Process, id FrozenIdentity, obs ProbeObservation) ProbeObservation {
	outPipe, errPipe := p.Stdout(), p.Stderr()
	defer outPipe.Close()
	defer errPipe.Close()
	overflow := make(chan struct{}, 2)
	outCh, errCh, done := readProbeStream(outPipe, overflow), readProbeStream(errPipe, overflow), p.Done()
	var out, stderr probeDrain
	exit := probeprocess.Exit{Code: -1}
	var reaped bool
	var cleanup context.Context
	var stopCleanup context.CancelFunc
	var cleanupDone <-chan struct{}
	executionDone := execution.Done()
	beginCleanup := func() {
		if cleanup != nil {
			return
		}
		cleanup, stopCleanup = r.budget(outer, probeCleanupCap)
		cleanupDone = cleanup.Done()
		// A completed direct child can still have descendants holding pipes.
		_ = p.Kill()
	}
	defer func() {
		if stopCleanup != nil {
			stopCleanup()
		}
	}()
	obs.Execution.Started = true
	obs.Outcome = ProbeExitOKMatch
	for outCh != nil || errCh != nil || done != nil {
		select {
		case <-overflow:
			obs.Outcome = ProbeOutputLimit
			beginCleanup()
		case out = <-outCh:
			outCh = nil
			if out.over {
				obs.Outcome = ProbeOutputLimit
				beginCleanup()
			} else if out.err != nil {
				obs.Outcome = ProbeDrainFailed
				beginCleanup()
			}
		case stderr = <-errCh:
			errCh = nil
			if stderr.over {
				obs.Outcome = ProbeOutputLimit
				beginCleanup()
			} else if stderr.err != nil {
				obs.Outcome = ProbeDrainFailed
				beginCleanup()
			}
		case exit = <-done:
			done = nil
			reaped = true
			beginCleanup()
		case <-executionDone:
			executionDone = nil
			obs.Outcome = ProbeTimeout
			obs.Execution.Deadline = true
			beginCleanup()
		case <-cleanupDone:
			// Close our read ends even when a descendant has escaped the group.
			// Closing bounds drainage; it does not establish escapee cleanup.
			_ = outPipe.Close()
			_ = errPipe.Close()
			_ = p.Kill()
			obs.Cleanup = CleanupIncomplete
			if obs.Outcome == ProbeExitOKMatch {
				obs.Outcome = ProbeDrainFailed
			}
			goto collected
		}
	}
collected:
	obs.Execution.Reaped = reaped
	obs.Execution.GroupGone = p.GroupGone()
	obs.Execution.Drained = outCh == nil && errCh == nil && out.err == nil && stderr.err == nil
	obs.Execution.ExitCode, obs.Execution.Signal = exit.Code, exit.Signal
	if !reaped || !obs.Execution.GroupGone {
		obs.Cleanup = CleanupIncomplete
	}
	if outCh == nil {
		obs.OutputDigest, obs.OutputBytes = outputHash(out.data), int64(len(out.data))
	}
	if errCh == nil {
		obs.Execution.StderrDigest, obs.Execution.StderrBytes = outputHash(stderr.data), int64(len(stderr.data))
	}
	if obs.Outcome == ProbeExitOKMatch {
		switch {
		case obs.Cleanup != CleanupConfirmed:
			obs.Outcome = ProbeDrainFailed
		case exit.Signal != "":
			obs.Outcome = ProbeSignaled
		case exit.Code != 0 || exit.Err != nil:
			obs.Outcome = ProbeExitNonzero
		default:
			parsed, err := muse.ParseBuildIdentity(out.data)
			if err != nil {
				obs.Outcome = ProbeExitOKUnparseable
			} else if parsed.Build != id.Build {
				obs.Outcome, obs.ObservedBuild = ProbeExitOKMismatch, parsed.Build
			} else {
				obs.ObservedBuild = parsed.Build
			}
		}
	}
	obs.Detail = fmt.Sprintf("first probe outcome=%s exit=%d signal=%s cleanup=%s", obs.Outcome, exit.Code, exit.Signal, obs.Cleanup)
	return obs
}
