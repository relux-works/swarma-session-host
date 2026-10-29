package musepty

import (
	"context"
	"errors"
	"fmt"
	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
	musebuilds "github.com/relux-works/swarma-session-host/musebuilds"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

const musePTYQuiet = 300 * time.Millisecond

var ErrMusePTYDeferred = errors.New("Muse PTY delivery deferred: idle not positively known")
var ErrMusePTYHeld = errors.New("Muse PTY delivery held: composer changed before submit")

// MusePTYLaunchPlan is opaque so a host cannot accept hand-written argv. The
// composer calls BuildMusePTYLaunchPlan before passing it to the host. Native
// arguments (including trust, and resume selection) remain plugin inputs.
type MusePTYLaunchPlan struct {
	plan agentic.Plan
	// R198 observation seam, nil in production. Tests synchronize with the
	// actual owned-prefix decision instead of treating elapsed time as proof.
	startupObserved func(*musePTYHost, *museLogFold, error)
	// L3P frozen binding. frozenSet reports the plan was composed
	// against a frozen tuple (LaunchRequest FrozenTool* or a verified
	// imported seal bound to a selection). A set plan starts only
	// through the pre-exec gate — frozen bytes plus the module seal —
	// and never probes PATH; an unset plan keeps the legacy PATH
	// probe. imported is non-nil exactly for plans built by
	// BuildMusePTYLaunchPlanFromImport.
	frozenTuple musebuilds.FrozenIdentity
	frozenStore musebuilds.HostConfig
	frozenSet   bool
	imported    *agentic.ImportedProcess
}

// ToolRelease returns the assertion the sealed plan actually carries. The
// wrapper must serialize this value after an optional native isolation fallback.
func (p *MusePTYLaunchPlan) ToolRelease() string { return p.plan.ToolRelease }

// BuildMusePTYLaunchPlan retains its optional warning argument for callers.
// The module owns v4 exclusion qualification;
// a required root-context fact is never dropped to recover a native launch.
func BuildMusePTYLaunchPlan(req agentic.LaunchRequest, warnings ...io.Writer) (*MusePTYLaunchPlan, error) {
	if req.System != "muse" {
		return nil, fmt.Errorf("Muse PTY requires the Muse system")
	}
	registry := agentic.NewRegistry()
	if err := registry.Register(muse.New()); err != nil {
		return nil, err
	}
	plan, err := agentic.BuildPlan(registry, req, agentic.LaunchModeInteractive)
	if err != nil {
		return nil, err
	}
	trusted := false
	for _, arg := range plan.Argv {
		if arg == "--trust-workspace" {
			trusted = true
		}
	}
	if !trusted {
		return nil, fmt.Errorf("Muse PTY launch requires workspace trust in the plugin plan")
	}
	launch := &MusePTYLaunchPlan{plan: plan}
	if err := launch.recordFrozenTuple(req); err != nil {
		return nil, err
	}
	return launch, nil
}

type musePTYSnapshot struct {
	Contract  string `json:"contract"`
	SessionID string `json:"session_id"`
	LogPath   string `json:"log_path"`
	PID       int    `json:"pid"`
	// L3P/M50: the completed recovery leg, when this host is one.
	// Omit-empty keeps every pre-L3P snapshot decoding identically.
	VersionFallback      bool                    `json:"versionFallback,omitempty"`
	VersionFallbackFrom  string                  `json:"versionFallbackFrom,omitempty"`
	VersionFallbackEvent *musePTYFallbackWarning `json:"versionFallbackEvent,omitempty"`
	VersionDeferralEvent *musePTYDeferralWarning `json:"versionDeferralEvent,omitempty"`
}

// StartMusePTYHost starts an already composed plan. Factory registration and
// the operator command belong to R3; this entry point has no CLI dispatch.
// Sessions are discovered under the plan's HOME/XDG_DATA_HOME, bound by PID.
// L3P/M34: the entry verifies before it starts, exactly like the resume
// entry; the shared starter below is unreachable unverified.
func StartMusePTYHost(ctx context.Context, launch *MusePTYLaunchPlan) (*musePTYHost, error) {
	if launch == nil {
		return nil, fmt.Errorf("Muse PTY plan is required")
	}
	lease, err := acquireMusePTYUseLease(ctx, launch, musePTYLegLaunch)
	if err != nil {
		return nil, err
	}
	if err := verifyMusePTYPreExec(ctx, launch, musePTYLegLaunch); err != nil {
		_ = lease.Release()
		return nil, err
	}
	return startMusePTYHostAfterVerify(ctx, launch, lease)
}

// acquireMusePTYUseLease takes the frozen-store use exclusion a frozen leg
// holds across pre-exec verification, child startup and reap (design
// section 7; the musebuilds/execverify.go contract; L2M's startMuseChild
// pattern). Legacy plans run outside the frozen store and hold nothing.
// A frozen plan without its store binding refuses here, before any lease
// is taken; a lease failure refuses typed like L2M's, with the module
// lease error chained so callers can tell contention from bad bytes.
func acquireMusePTYUseLease(ctx context.Context, launch *MusePTYLaunchPlan, leg musePTYExecLeg) (*musebuilds.UseLease, error) {
	if !launch.frozenSet {
		return nil, nil
	}
	if launch.frozenStore.FrozenRoot == "" {
		return nil, &MusePTYExecError{Leg: string(leg), Code: musePTYCodeStoreMissing, Cause: fmt.Errorf("frozen plan carries no frozen store binding")}
	}
	lease, err := musebuilds.AcquireUseLease(ctx, launch.frozenStore)
	if err != nil {
		return nil, &MusePTYExecError{Leg: string(leg), Code: musePTYCodeFrozenVerifyFailed, Cause: err}
	}
	return lease, nil
}

// verifyMusePTYPreExec is the L3P pre-exec gate: every PTY entry runs it
// after composition/import and before its child starts (design section
// 7). Legacy plans keep the PATH release probe exactly as before;
// frozen plans verify the tuple against the frozen bytes plus the
// module seal, and never execute the PATH probe (M62). The caller holds
// the frozen-use lease across this call (see acquireMusePTYUseLease).
func verifyMusePTYPreExec(ctx context.Context, launch *MusePTYLaunchPlan, leg musePTYExecLeg) error {
	plan := launch.plan
	// The probe is structural, never a release pin: the module's own
	// prober detects any well-formed version answer and refuses anything
	// else. Release admission stays with the module seal over the plan's
	// binary; this host names no release. Frozen plans skip it: the
	// frozen gate plus the seal re-verification below already probe the
	// frozen binary itself, and no earlier probe may execute an unfrozen
	// binary on the override path.
	if !launch.frozenSet {
		if _, err := muse.New().ProbeToolRelease(ctx, plan.Env); err != nil {
			return fmt.Errorf("Muse PTY requires a detected tool release: %w", err)
		}
		return nil
	}
	tuple := launch.frozenTuple
	if launch.frozenStore.FrozenRoot == "" {
		return &MusePTYExecError{Leg: string(leg), Code: musePTYCodeStoreMissing, Cause: fmt.Errorf("frozen plan carries no frozen store binding")}
	}
	if plan.Binary != tuple.Path {
		return &MusePTYExecError{Leg: string(leg), Code: musePTYCodeFrozenVerifyFailed, Cause: fmt.Errorf("plan binary %q is not the frozen tuple path %q", plan.Binary, tuple.Path)}
	}
	if err := musebuilds.VerifyFrozenForExec(ctx, launch.frozenStore, tuple); err != nil {
		return &MusePTYExecError{Leg: string(leg), Code: musePTYCodeFrozenVerifyFailed, Cause: err}
	}
	return verifyMusePTYSeal(launch, leg)
}

// verifyMusePTYSeal re-verifies the module seal immediately before exec:
// the in-process verifier for composed plans, the imported verifier for
// verified-import plans (whose bare process plan verifies nothing alone).
func verifyMusePTYSeal(launch *MusePTYLaunchPlan, leg musePTYExecLeg) error {
	var err error
	if launch.imported != nil {
		err = launch.imported.VerifyBeforeExec(launch.plan)
	} else {
		err = launch.plan.VerifyBeforeExec()
	}
	if err != nil {
		return &MusePTYExecError{Leg: string(leg), Code: musePTYCodeSealVerifyFailed, Cause: err}
	}
	return nil
}

// startMusePTYHostAfterVerify spawns the PTY and witnesses the owned
// startup prefix. Both entries verify first; this starter performs no
// checks of its own. The caller passes the frozen-use lease it acquired
// before verification (nil on legacy legs); the starter holds it across
// the child and drops it only after the child is reaped, releasing it on
// every error path that starts no child.
func startMusePTYHostAfterVerify(ctx context.Context, launch *MusePTYLaunchPlan, lease *musebuilds.UseLease) (*musePTYHost, error) {
	plan := launch.plan
	dataHome := envValue(plan.Env, "XDG_DATA_HOME")
	if dataHome == "" {
		dataHome = filepath.Join(envValue(plan.Env, "HOME"), ".local", "share")
	}
	command := exec.Command(plan.Binary, plan.Argv...)
	command.Dir = plan.WorkDir
	command.Env = plan.Env
	terminal, err := pty.StartWithSize(command, &pty.Winsize{Rows: 40, Cols: 120})
	if err != nil {
		_ = lease.Release()
		return nil, err
	}
	h := &musePTYHost{command: command, ptmx: terminal, vt: newMuseVT(40, 120), lastOutput: time.Now(), exited: make(chan struct{}), drained: make(chan struct{}), streams: map[*musePTYStream]bool{}, snapshot: musePTYSnapshot{Contract: "task-board.muse-pty-host-v1", PID: command.Process.Pid}, useLease: lease}
	go h.pump()
	go func() {
		_ = command.Wait()
		// The use lease drops only after the child is reaped, so
		// reviewed exclusive cleanup can never delete bytes a live
		// child runs. Release is idempotent and nil-safe; every
		// path below observes the reap (Stop waits for it, and the
		// exited-during-startup path fires only after it), so no
		// path releases the lease itself.
		_ = lease.Release()
		close(h.exited)
	}()
	// Bound startup even when the caller supplied an unbounded context.
	ready, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var incomplete *MuseStartupPrefixError
	for {
		path, e := findMuseLog(filepath.Join(dataHome, "muse", "sessions"), command.Process.Pid)
		if path != "" {
			var f *museLogFold
			if e == nil {
				f, e = readMuseLog(path)
			}
			observed := e
			if e == nil && !f.StartupPrefixWitnessed {
				incomplete = &MuseStartupPrefixError{Code: MuseStartupPrefixIncomplete, Msg: "owned startup prefix needs three complete records"}
				observed = incomplete
			}
			if launch.startupObserved != nil {
				launch.startupObserved(h, f, observed)
			}
			var prefix *MuseStartupPrefixError
			if errors.As(e, &prefix) && prefix.Code != MuseStartupPrefixIncomplete {
				_ = h.Stop(context.Background())
				return h, prefix
			}
			// L3G startup admission: an opened session plus the full
			// three-record witnessed prefix. A mid-startup log folds
			// but never admits here.
			if e == nil && f.Opened && f.StartupPrefixWitnessed {
				h.mu.Lock()
				h.snapshot.LogPath = path
				h.snapshot.SessionID = f.SessionID
				h.mu.Unlock()
				return h, nil
			}
		}
		select {
		case <-ready.Done():
			_ = h.Stop(context.Background())
			if incomplete != nil {
				return h, fmt.Errorf("Muse PTY startup unobserved: %w", errors.Join(ready.Err(), incomplete))
			}
			return h, fmt.Errorf("Muse PTY startup unobserved: %w", ready.Err())
		case <-h.exited:
			<-h.drained
			return h, fmt.Errorf("Muse PTY exited during startup")
		case <-ticker.C:
		}
	}
}

// ResumeMusePTYHost requires a composer-provided muse resume plan and verifies
// the restored durable session identity before exposing the host.
func ResumeMusePTYHost(ctx context.Context, launch *MusePTYLaunchPlan, record sm.ProviderHostRecord) (*musePTYHost, error) {
	prior, priorErr := museRecordSnapshot(record)
	if priorErr != nil || prior.Contract != "task-board.muse-pty-host-v1" || prior.SessionID == "" {
		return nil, fmt.Errorf("invalid Muse PTY resume snapshot")
	}
	if launch == nil {
		return nil, fmt.Errorf("Muse resume plan is required")
	}
	selected := false
	for i, arg := range launch.plan.Argv {
		if arg == "resume" && i+1 < len(launch.plan.Argv) && launch.plan.Argv[i+1] == prior.SessionID {
			selected = true
		}
	}
	if !selected {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeInvalidConfig, Cause: fmt.Errorf("Muse plan does not resume the recorded session")}
	}
	// L3P/M60: a frozen resume starts only from a verified imported
	// plan. Host-side recomposition is unauthenticated evidence on
	// this leg; the outer composer re-exports through
	// BuildMusePTYLaunchPlanFromImport instead. Legacy resumes are
	// unaffected.
	if launch.frozenSet && launch.imported == nil {
		return nil, &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeResumeNotImported, Cause: fmt.Errorf("frozen Muse resume requires a verified imported plan")}
	}
	lease, err := acquireMusePTYUseLease(ctx, launch, musePTYLegResume)
	if err != nil {
		return nil, err
	}
	if err := verifyMusePTYPreExec(ctx, launch, musePTYLegResume); err != nil {
		_ = lease.Release()
		return nil, err
	}
	h, err := startMusePTYHostAfterVerify(ctx, launch, lease)
	if err != nil {
		return h, err
	}
	if h.NativeIdentity() != prior.SessionID {
		_ = h.Stop(context.Background())
		return nil, fmt.Errorf("Muse resume returned a different native session")
	}
	return h, nil
}
func envValue(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		if v, ok := strings.CutPrefix(env[i], key+"="); ok {
			return v
		}
	}
	return ""
}

type musePTYHost struct {
	mu       sync.Mutex
	writeMu  sync.Mutex
	command  *exec.Cmd
	ptmx     *os.File
	vt       *museVT
	snapshot musePTYSnapshot
	// useLease is the frozen-store use exclusion a frozen leg holds
	// across verification, startup and reap (L3P r2, mirroring L2M's
	// museChild). Nil on legacy legs, which run outside the frozen
	// store. The starter's reap goroutine drops it after Wait; the
	// field records the ownership.
	useLease        *musebuilds.UseLease
	lastOutput      time.Time
	exited, drained chan struct{}
	streams         map[*musePTYStream]bool
	replay          []byte
	held            string
	pendingRun      bool
	stopped         bool
	// L3P/M50: pending recovery notes for L4 delivery, never an
	// assertion that a task note was delivered.
	fallbackNote string
	deferralNote string
	// tb-R198 test seams. Zero values preserve production behaviour: a
	// nil testFakeNow uses the wall clock, and nil notify channels
	// disable event signalling (non-blocking sends). Installed only
	// from _test.go setters; production allocates and sends nothing.
	testFakeNow  *time.Time
	outputNotify chan struct{}
	foldNotify   chan struct{}
	settleNotify chan struct{}
}

// nowLocked reports the clock the quiet expressions run against. Call
// with h.mu held. Production default is the wall clock (fake nil);
// scripted tests inject a fake instant and advance it past the quiet
// window, so the production comparisons below always execute.
func (h *musePTYHost) nowLocked() time.Time {
	if h.testFakeNow != nil {
		return *h.testFakeNow
	}
	return time.Now()
}

// quietLocked reports whether the PTY has been quiet for musePTYQuiet.
// Call with h.mu held.
func (h *musePTYHost) quietLocked() bool {
	return h.nowLocked().Sub(h.lastOutput) >= musePTYQuiet
}

// settleLocked returns how long deliver should wait for an observed idle
// candidate to become quiet. Call with h.mu held.
func (h *musePTYHost) settleLocked() time.Duration {
	return musePTYQuiet - h.nowLocked().Sub(h.lastOutput)
}

// readyLocked reports the idle predicate minus the quiet window: no
// stop, no pending run and a safe composer over an idle fold. Call
// with h.mu held. Scripted settle loops wait on this plus pump
// events and drive quiet separately through the test clock, so a
// frozen clock never hangs a helper.
func (h *musePTYHost) readyLocked(f *museLogFold, expected string) bool {
	return !h.stopped && !h.pendingRun && h.vt.safeComposer(expected, f)
}

func (h *musePTYHost) pump() {
	defer close(h.drained)
	buf := make([]byte, 32768)
	for {
		n, err := h.ptmx.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			h.mu.Lock()
			reply := h.vt.observe(chunk)
			h.lastOutput = time.Now()
			h.replay = append(h.replay, chunk...)
			if len(h.replay) > 65536 {
				h.replay = append([]byte(nil), h.replay[len(h.replay)-65536:]...)
			}
			for stream := range h.streams {
				select {
				case stream.output <- chunk:
				default:
					stream.closeLocked()
				}
			}
			// The notify channel is installed post-start by the
			// test setter, so capture it under the mutex; the
			// non-blocking send itself stays outside the lock.
			notify := h.outputNotify
			h.mu.Unlock()
			if notify != nil {
				select {
				case notify <- struct{}{}:
				default:
				}
			}
			// Terminal responses must bypass the human-input lock: the TUI may probe
			// during a delivery whose acknowledgement holds that lock.
			if len(reply) > 0 {
				_, _ = h.ptmx.Write(reply)
			}
		}
		if err != nil {
			return
		}
	}
}
func (h *musePTYHost) NativeIdentity() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshot.SessionID
}
func (h *musePTYHost) Snapshot() (sm.RunnerSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return sm.RunnerSnapshot{Kind: "muse-pty", MusePTY: ptrMuseCoreSnapshot(h.snapshot)}, nil
}
func (h *musePTYHost) State(context.Context) (sm.ProviderHostState, error) {
	select {
	case <-h.exited:
		return sm.ProviderHostStopped, nil
	default:
		return sm.ProviderHostRunning, nil
	}
}
func (h *musePTYHost) ProviderLaunchMetadata() sm.ProviderLaunchMetadata {
	return sm.ProviderLaunchMetadata{PID: h.command.Process.Pid, Executable: h.command.Path}
}

// FallbackWarning reports the recorded fallback warning, or nil when
// this host did not fall back. It reads the same snapshot fact the
// stderr emission and the pending note carry.
func (h *musePTYHost) FallbackWarning() *musePTYFallbackWarning {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.snapshot.VersionFallbackEvent == nil {
		return nil
	}
	reported := *h.snapshot.VersionFallbackEvent
	return &reported
}

// DeferralWarning reports the recorded deferral warning, or nil when
// this host did not defer.
func (h *musePTYHost) DeferralWarning() *musePTYDeferralWarning {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.snapshot.VersionDeferralEvent == nil {
		return nil
	}
	reported := *h.snapshot.VersionDeferralEvent
	return &reported
}

// FallbackNote reports the pending task-note text for the fallback, or
// empty when this host did not fall back. Delivery belongs to L4; this
// surface composes the single wording.
func (h *musePTYHost) FallbackNote() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fallbackNote
}

// DeferralNote reports the pending task-note text for the deferral, or
// empty when this host did not defer.
func (h *musePTYHost) DeferralNote() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.deferralNote
}
func (h *musePTYHost) fold() (*museLogFold, error) {
	h.mu.Lock()
	path := h.snapshot.LogPath
	session := h.snapshot.SessionID
	pid := h.snapshot.PID
	// The notify channel is installed post-start by the test
	// setter, so capture it under the mutex; the non-blocking
	// send itself stays outside the lock.
	notify := h.foldNotify
	h.mu.Unlock()
	f, err := readMuseLog(path)
	if notify != nil {
		select {
		case notify <- struct{}{}:
		default:
		}
	}
	if err != nil {
		return nil, err
	}
	// Bind the fold to the owned host's startup snapshot: a log that names
	// another session or process is unknown state before any paste, not at
	// a later acknowledgement.
	if f.SessionID != session {
		return nil, fmt.Errorf("Muse log names an unowned session")
	}
	if f.PID != pid {
		return nil, fmt.Errorf("Muse log names an unowned process")
	}
	return f, nil
}
func (h *musePTYHost) idle(expected string) bool {
	if state, _ := h.State(context.Background()); state != sm.ProviderHostRunning {
		return false
	}
	f, err := h.fold()
	if err != nil || !f.idle() {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.quietLocked() && h.readyLocked(f, expected)
}
func (h *musePTYHost) awaitQuiet(ctx context.Context) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		h.mu.Lock()
		quiet := h.quietLocked()
		h.mu.Unlock()
		if quiet {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.exited:
			return io.ErrClosedPipe
		case <-ticker.C:
		}
	}
}

// deliver serializes human input and transport acknowledgements. It refuses
// before paste on unknown/busy/dialog and checks the full predicate again after
// paste, immediately before Enter. A failed re-check leaves held text intact.
func (h *musePTYHost) deliver(ctx context.Context, text string) (string, error) {
	return h.deliverPhased(ctx, text, nil, nil)
}

// submitted marks the last physical write; observed receives only a native
// session ID read alongside a new run-start, never an expected/caller value.
// Nil callbacks retain the legacy delivery path and its baseline semantics.
func (h *musePTYHost) deliverPhased(ctx context.Context, text string, submitted func(), observed func(string)) (string, error) {
	if err := historyguard.LockMutex(ctx, &h.writeMu); err != nil {
		return "", err
	}
	defer h.writeMu.Unlock()
	if strings.TrimSpace(text) == "" || strings.ContainsAny(text, "\x1b\x00\r") {
		return "", fmt.Errorf("Muse PTY text must be nonempty and contain no terminal controls")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if h.held != "" && h.held != text {
		return "", ErrMusePTYHeld
	}
	// A just-drained turn can have a valid empty UI before its final redraw
	// has been quiet for 300ms. Settle that observed idle candidate once,
	// then re-read the full gate. Busy, unknown and dialog states never wait
	// here, and fresh output during settling still causes a deferral.
	if f, e := h.fold(); e == nil && f.idle() {
		h.mu.Lock()
		settle := time.Duration(0)
		// The notify channel is installed post-start by the test
		// setter, so capture it under the mutex.
		settleNotify := h.settleNotify
		if !h.stopped && !h.pendingRun && h.vt.safeComposer(h.held, f) {
			settle = h.settleLocked()
		}
		h.mu.Unlock()
		if settle > 0 {
			if settleNotify != nil {
				select {
				case settleNotify <- struct{}{}:
				default:
				}
			}
			timer := time.NewTimer(settle)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return "", ctx.Err()
			case <-h.exited:
				timer.Stop()
				return "", io.ErrClosedPipe
			}
		}
	}
	if !h.idle(h.held) {
		return "", ErrMusePTYDeferred
	}
	baseline, err := h.fold()
	if err != nil {
		return "", err
	}
	seen := map[string]bool{}
	for _, id := range baseline.Started {
		seen[id] = true
	}
	if h.held == "" {
		if _, err = h.ptmx.Write([]byte("\x1b[200~" + text + "\x1b[201~")); err != nil {
			return "", err
		}
		h.held = text
	}
	// Await a rendered composer, not merely elapsed time since the old frame.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		h.mu.Lock()
		matches := h.vt.composerMatches(text)
		h.mu.Unlock()
		if matches {
			break
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", ErrMusePTYHeld
		case <-h.exited:
			return "", io.ErrClosedPipe
		case <-tick.C:
		}
	}
	if err = h.awaitQuiet(ctx); err != nil {
		return "", err
	}
	if !h.idle(text) {
		return "", ErrMusePTYHeld
	} // mandatory pre-submit re-check
	if err = ctx.Err(); err != nil {
		return "", err
	}
	// Serialize the final frame check with the output pump; no observed dialog
	// can arrive between that check and Enter. The durable log is reread
	// for the frame too: off-frame rows are backed by committed output,
	// so the backing must be current, not the pre-paste fold. The fresh
	// fold must also still prove idle: a log change between the first
	// check and the submit defers, even when the screen never moved.
	finalFold, finalErr := h.fold()
	h.mu.Lock()
	if finalErr != nil || !finalFold.idle() || !h.vt.safeComposer(text, finalFold) || !h.quietLocked() {
		h.mu.Unlock()
		return "", ErrMusePTYHeld
	}
	if submitted != nil {
		// The neutral receipt excludes even completed runs that appeared
		// during paste: they preceded this Enter and cannot acknowledge it.
		for _, id := range finalFold.Started {
			seen[id] = true
		}
	}
	_, err = h.ptmx.Write([]byte{'\r'})
	if err == nil {
		h.pendingRun = true
		h.held = ""
	}
	h.mu.Unlock()
	if err != nil {
		return "", err
	}
	if submitted != nil {
		submitted()
	}
	ack, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		if err := ack.Err(); err != nil {
			return "", fmt.Errorf("Muse run-start transport acknowledgement unobserved: %w", err)
		}
		f, e := h.fold()
		if e == nil {
			for _, id := range f.Started {
				if !seen[id] {
					h.mu.Lock()
					h.pendingRun = false
					h.mu.Unlock()
					if observed != nil {
						observed(f.SessionID)
					}
					return id, nil
				}
			}
		}
		select {
		case <-ack.Done():
			return "", fmt.Errorf("Muse run-start transport acknowledgement unobserved: %w", ack.Err())
		case <-h.exited:
			return "", io.ErrClosedPipe
		case <-tick.C:
		}
	}
}

// DeliverGoalText submits consumer-rendered text through the same guarded PTY
// path as owner input. The caller owns canonical goal interpretation.
func (h *musePTYHost) DeliverGoalText(ctx context.Context, text string) error {
	_, err := h.deliver(ctx, text)
	return err
}
func (h *musePTYHost) DeliverSessionNotice(ctx context.Context, text string) error {
	if err := sm.ValidateSessionNoticeText(text); err != nil {
		return err
	}
	_, err := h.deliver(ctx, text)
	return err
}
func (h *musePTYHost) RunOwner(ctx context.Context, r sm.ProviderRunRequest) (*sm.ProviderRunResult, error) {
	id, err := h.deliver(ctx, r.Prompt)
	if err != nil {
		return nil, err
	}
	return h.ObserveOwnerRun(ctx, id)
}

// ObserveOwnerRun observes an already submitted, receipt-checked native run.
// It never submits input or changes transport lifetime. The caller supplies the
// actual run ID from its checked PTY observation; this method grants no authority.
func (h *musePTYHost) ObserveOwnerRun(ctx context.Context, id string) (*sm.ProviderRunResult, error) {
	if id == "" {
		return nil, sm.ErrInvalidInput
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		f, e := h.fold()
		if e == nil {
			if terminal, ok := f.Terminals[id]; ok {
				return museTextResult(terminal), nil
			}
		}
		select {
		case <-ctx.Done():
			if e != nil {
				return &sm.ProviderRunResult{ExitCode: 1, Status: sm.ProviderRunFailed, ProviderStatus: "unknown_failure", Detail: e.Error(), Recoverable: false}, nil
			}
			return &sm.ProviderRunResult{ExitCode: 1, Status: sm.ProviderRunFailed, ProviderStatus: "context_cancelled", Recoverable: true}, nil
		case <-h.exited:
			if e != nil {
				return &sm.ProviderRunResult{ExitCode: 1, Status: sm.ProviderRunFailed, ProviderStatus: "unknown_failure", Detail: e.Error(), Recoverable: false}, nil
			}
			return &sm.ProviderRunResult{ExitCode: 1, Status: sm.ProviderRunFailed, ProviderStatus: "transport_lost", Recoverable: true}, nil
		case <-ticker.C:
		}
	}
}
func (h *musePTYHost) Stop(ctx context.Context) error {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return nil
	}
	h.stopped = true
	h.mu.Unlock()
	_ = syscall.Kill(-h.command.Process.Pid, syscall.SIGTERM)
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-h.exited:
	case <-timer.C:
		_ = syscall.Kill(-h.command.Process.Pid, syscall.SIGKILL)
		<-h.exited
	case <-ctx.Done():
		_ = syscall.Kill(-h.command.Process.Pid, syscall.SIGKILL)
		<-h.exited
	}
	_ = h.ptmx.Close()
	<-h.drained
	return nil
}
func (h *musePTYHost) AttachTerminal(ctx context.Context) (io.ReadWriteCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if state, _ := h.State(ctx); state != sm.ProviderHostRunning {
		return nil, io.ErrClosedPipe
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	s := &musePTYStream{host: h, output: make(chan []byte, 256), closed: make(chan struct{}), pending: append([]byte(nil), h.replay...)}
	h.streams[s] = true
	return s, nil
}
func (h *musePTYHost) ResizeTerminal(rows, cols uint16) error {
	if rows == 0 || cols == 0 {
		return fmt.Errorf("Muse terminal dimensions must be positive")
	}
	h.mu.Lock()
	h.vt = newMuseVT(int(rows), int(cols))
	h.vt.unknown = true
	h.mu.Unlock()
	return pty.Setsize(h.ptmx, &pty.Winsize{Rows: rows, Cols: cols})
}

type musePTYStream struct {
	host    *musePTYHost
	output  chan []byte
	closed  chan struct{}
	pending []byte
}

func (s *musePTYStream) closeLocked() {
	if s.host.streams[s] {
		delete(s.host.streams, s)
		close(s.closed)
	}
}
func (s *musePTYStream) Close() error {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	s.closeLocked()
	return nil
}
func (s *musePTYStream) Read(p []byte) (int, error) {
	for len(s.pending) == 0 {
		select {
		case data := <-s.output:
			s.pending = data
		case <-s.closed:
			return 0, io.EOF
		case <-s.host.drained:
			select {
			case s.pending = <-s.output:
			default:
				return 0, io.EOF
			}
		}
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}
func (s *musePTYStream) Write(p []byte) (int, error) {
	s.host.writeMu.Lock()
	defer s.host.writeMu.Unlock()
	select {
	case <-s.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	// Interactive input owns the composer until a fresh redraw proves otherwise.
	s.host.mu.Lock()
	s.host.lastOutput = time.Now()
	s.host.mu.Unlock()
	return s.host.ptmx.Write(p)
}

var (
	_ sm.ProviderHost            = (*musePTYHost)(nil)
	_ sm.TerminalHost            = (*musePTYHost)(nil)
	_ sm.ProviderSessionNotifier = (*musePTYHost)(nil)
	_ sm.ProviderOwnerRunner     = (*musePTYHost)(nil)
	_ sm.ProviderNativeIdentity  = (*musePTYHost)(nil)
	_ sm.ProviderLaunchReporter  = (*musePTYHost)(nil)
)
