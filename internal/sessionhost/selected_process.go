package sessionhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

// The ordinary owner supplies this from its admitted execution record, never
// from an RPC or a status snapshot. Both methods must be bounded, read-only
// checks. The actual runner object retains the original child/OS handle.
type selectedProcessOwner interface {
	ordinaryExecutionOwner
	ownedHarness(context.Context, *selectedExecution) (*runner.Server, selectedProcessAdmission, error)
}

// This private frozen binding preserves the M3 predicates while the canonical
// public schema is supplied separately. No lease mutation or input authority
// follows from this record, including when its kernel tuple matches.
type selectedProcessBinding struct {
	ManagerID, RegisteredAt, ManagerExecutable string
	Operation, Session, Execution              string
	Generation, Revision                       uint64
	Profile, Home, Workspace                   string
	PlanDigest, ProfileLock, ProfileFragment   string
	Environment, Permissions, Transport        string
	Runtime, Vendor, Model, Effort             string
	NativeRelease, NativeSession               string
	RunnerInstance                             string
	RunnerPID                                  int
}

type selectedProcessAdmission struct {
	Binding       selectedProcessBinding
	Role, Handoff string
	Kernel        runner.OwnedProcessEvidence
}

type selectedStartedProcess struct {
	AdmissionDigest    string
	Operation, Session string
	Generation         uint64
	Kernel             runner.OwnedProcessEvidence
}

type selectedProcessProof struct {
	daemon    *selectedManagerDaemon
	owner     selectedProcessOwner
	held      *runner.Server
	operation string
	admission selectedProcessAdmission
	started   selectedStartedProcess
}

var selectedBootID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func selectedProcessPath(path string) bool {
	return len(path) <= 1024 && filepath.IsAbs(path) && filepath.Clean(path) == path &&
		!strings.ContainsAny(path, "\x00\r\n")
}

func selectedKernelValid(k runner.OwnedProcessEvidence) bool {
	if k.PID <= 0 || k.PID > 2147483647 || k.RealUID == 1<<32-1 || k.EffectiveUID == 1<<32-1 ||
		!selectedBootID.MatchString(k.BootID) || !notificationRef(k.Execution, 128) {
		return false
	}
	if k.Platform == "linux" {
		return notificationU64(k.StartTicks, false) && k.StartSec == "" && k.StartUsec == 0 && notificationSHA256(k.Executable)
	}
	if k.Platform == "darwin" {
		if !notificationU64(k.StartSec, false) || k.StartUsec >= 1000000 || k.StartTicks != "" || len(k.Executable) != 40 {
			return false
		}
		_, err := hex.DecodeString(k.Executable)
		return err == nil && strings.ToLower(k.Executable) == k.Executable
	}
	return false
}

func (a selectedProcessAdmission) valid(e *selectedExecution) bool {
	b := a.Binding
	if b.ManagerID != "session-host" || !selectedOperationID.MatchString(b.Operation) ||
		b.Session != e.session || b.Generation != e.generation || b.Generation == 0 ||
		b.Revision == 0 || b.Revision > 1<<53-1 || b.Execution != a.Kernel.Execution ||
		b.RunnerPID <= 0 || b.RunnerPID > 2147483647 || b.RunnerPID == a.Kernel.PID ||
		a.Role != "harness" || !notificationSHA256(a.Handoff) || !selectedKernelValid(a.Kernel) {
		return false
	}
	if len(b.RegisteredAt) > 64 {
		return false
	}
	if _, err := time.Parse(time.RFC3339Nano, b.RegisteredAt); err != nil {
		return false
	}
	for _, path := range []string{b.ManagerExecutable, b.Home, b.Workspace} {
		if !selectedProcessPath(path) {
			return false
		}
	}
	for _, digest := range []string{b.PlanDigest, b.ProfileLock, b.ProfileFragment} {
		if !notificationSHA256(digest) {
			return false
		}
	}
	for _, value := range []string{b.Session, b.Execution, b.Profile, b.Environment, b.Permissions, b.Transport,
		b.Runtime, b.Vendor, b.Model, b.Effort, b.NativeRelease, b.NativeSession, b.RunnerInstance} {
		if !notificationRef(value, 128) {
			return false
		}
	}
	return b.Transport == "pipe" && b.Runtime == "codex_cli"
}

func validSelectedStarted(entry selectedOperationEntry) bool {
	if entry.Started == nil || !notificationSHA256(entry.Started.AdmissionDigest) || !selectedKernelValid(entry.Started.Kernel) ||
		len(entry.Result) == 0 || !validSelectedResult(entry.Result) {
		return false
	}
	var result struct {
		Readable         bool
		Generation       uint64
		Operation        string `json:"operation_id"`
		Session, Manager string
	}
	return json.Unmarshal(entry.Result, &result) == nil && result.Readable &&
		result.Generation != 0 && result.Generation == entry.Started.Generation &&
		result.Operation == entry.Started.Operation && result.Session == entry.Started.Session && result.Manager == "session-host"
}

func (p *selectedProcessProof) current(ctx context.Context, e *selectedExecution) bool {
	if p == nil || p.daemon.ctx.Err() != nil || p.owner.current(ctx, e) != nil {
		return false
	}
	held, admission, err := p.owner.ownedHarness(ctx, e)
	if err != nil || held != p.held || admission != p.admission || !admission.valid(e) {
		return false
	}
	kernel, err := held.ProcessEvidence(ctx)
	if err != nil || kernel != p.started.Kernel || ctx.Err() != nil {
		return false
	}
	// The same owner must still attest the admitted exec/handoff and binding
	// after the kernel reread. Equality also refuses any revision change.
	held, admission, err = p.owner.ownedHarness(ctx, e)
	return err == nil && ctx.Err() == nil && p.daemon.ctx.Err() == nil && held == p.held && admission == p.admission
}

func (d *selectedManagerDaemon) captureHeldProcess(ctx context.Context, e *selectedExecution, operation, kind string) *selectedStartedProcess {
	e.processMu.Lock()
	defer e.processMu.Unlock()
	if e.processCaptured || kind != "launch" {
		return nil
	}
	e.processCaptured = true
	owner, ok := d.owner.(selectedProcessOwner)
	if !ok {
		return nil
	}
	held, admission, err := owner.ownedHarness(ctx, e)
	if err != nil || held == nil || !admission.valid(e) || admission.Binding.Operation != operation {
		return nil
	}
	data, _ := json.Marshal(admission)
	digest := sha256.Sum256(data)
	p := &selectedProcessProof{daemon: d, owner: owner, held: held, operation: operation, admission: admission,
		started: selectedStartedProcess{AdmissionDigest: hex.EncodeToString(digest[:]), Operation: operation,
			Session: e.session, Generation: e.generation, Kernel: admission.Kernel}}
	if !p.current(ctx, e) {
		return nil
	}
	e.processProof = p
	copy := p.started
	return &copy
}

// discardHeldProcessProof drops optional evidence that cannot be persisted
// with its mandatory result. The result itself is unaffected.
func (e *selectedExecution) discardHeldProcessProof() {
	e.processMu.Lock()
	defer e.processMu.Unlock()
	e.processProof = nil
}

// observeHeldProcess returns a value copy only for a live retained execution
// whose authoritative started snapshot was durably stored. Disk alone cannot
// recreate a proof after daemon replacement. It has no repair or input path.
func (d *selectedManagerDaemon) observeHeldProcess(ctx context.Context, session string, generation uint64) *runner.OwnedProcessEvidence {
	if ctx == nil || ctx.Err() != nil {
		return nil
	}
	d.mu.Lock()
	if d.closed || d.ctx.Err() != nil {
		d.mu.Unlock()
		return nil
	}
	var execution *selectedExecution
	for e := range d.executions {
		if e.session == session && e.generation == generation {
			if execution != nil {
				d.mu.Unlock()
				return nil // ambiguous retained owner is never chosen by map order.
			}
			execution = e
		}
	}
	d.mu.Unlock()
	if execution == nil {
		return nil
	}
	// Observation is read-only and shared: concurrent observers of one valid
	// proof must all receive it. Only capture (and proof discard) takes the
	// write side, so a second reader is never refused for contention alone.
	execution.processMu.RLock()
	defer execution.processMu.RUnlock()
	p := execution.processProof
	if p == nil || p.daemon != d {
		return nil
	}
	entry, exists, err := d.journal.read(p.operation)
	if err != nil || !exists || entry.Started == nil || *entry.Started != p.started || !p.current(ctx, execution) {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.ctx.Err() != nil || ctx.Err() != nil {
		return nil
	}
	kernel := p.started.Kernel
	return &kernel
}

// provedNoStart reads a decision made by the serialized pre-execute path.
// It never reconciles an operation or derives absence from a failed lookup.
// Wire capability negotiation and outcome projection are separate consumers.
func (d *selectedManagerDaemon) provedNoStart(ctx context.Context, operation string) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.ctx.Err() != nil {
		return false
	}
	entry, exists, err := d.journal.read(operation)
	return ctx.Err() == nil && err == nil && exists && entry.NoStart &&
		(entry.Cancelled || len(entry.Refusal) != 0) && len(entry.Result) == 0
}
