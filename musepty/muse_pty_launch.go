// Muse provider wiring for the session-manager factory (R3,
// regression): the launch payload the task-board muse wrapper
// sends, the factory the daemon registers, and the detached handle
// daemon restarts restore.
//
// The payload carries everything the daemon needs to rebuild the exact
// launch request the wrapper validated: model, effort, workdir, the raw
// parent env, posture, tool release, operator args, binary selection and
// the canonical curator fragment with its provenance. The factory
// re-validates the fragment, rebuilds the request through the shared
// builder, plans through R2's BuildMusePTYLaunchPlan, verifies the module
// seal immediately before exec, and delegates the start to R2. Resume
// selection is manager-composed from the durable record; the payload
// carries no resume identity, so an operator can never inject one.
//
// The factory lives in this package (not in the parent) because the
// parent cannot import it: the R2 host imports the parent for the
// provider seams, so a parent-side factory would be an import cycle.
// cmd/tb-sessiond registers it alongside the codex/claude factories.
package musepty

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// MuseSessionLaunchContract is the launch-envelope contract the wrapper
// sends and the factory decodes.
const MuseSessionLaunchContract = "task-board.muse-session-launch-v1"

// musePTYHostSnapshotContract pins R2's durable snapshot contract. It is
// restated here (not referenced) because R2's own occurrences are
// narrowing-mutant anchors in the tagged driver: editing that line would
// move the anchor and break the lane. The wiring test pins the literal,
// so an R2 rename fails loudly instead of drifting.
const musePTYHostSnapshotContract = "task-board.muse-pty-host-v1"

// MuseSessionLaunch is the serializable rebuild input for one managed
// Muse primary launch. Env is the wrapper's raw parent environment;
// Fragment is the exact canonical bytes the wrapper digested.
type MuseSessionLaunch = sm.MuseSessionLaunch

// DecodeMuseSessionLaunch strictly decodes one launch payload and checks
// its integrity: the contract, the fragment bytes against the recorded
// sha, the fragment itself through the shared v4 decoder, and the
// provenance fields against the decoded fragment. Launch semantics (the
// request, the posture, the release) belong to the builder and to
// BuildPlan, not to this envelope check.
func DecodeMuseSessionLaunch(data []byte) (*MuseSessionLaunch, error) {
	var launch MuseSessionLaunch
	if err := decodeMuseStrictJSON(data, &launch); err != nil {
		return nil, fmt.Errorf("decoding Muse session launch: %w", err)
	}
	if launch.Contract != MuseSessionLaunchContract {
		return nil, fmt.Errorf("Muse session launch contract %q is not %q", launch.Contract, MuseSessionLaunchContract)
	}
	if strings.TrimSpace(launch.Model) == "" {
		return nil, fmt.Errorf("Muse session launch requires a model")
	}
	if !filepath.IsAbs(launch.WorkDir) {
		return nil, fmt.Errorf("Muse session launch requires an absolute working directory")
	}
	if len(launch.Env) == 0 {
		return nil, fmt.Errorf("Muse session launch requires a parent environment")
	}
	if len(launch.Fragment) == 0 {
		return nil, fmt.Errorf("Muse session launch requires a curator fragment")
	}
	if MuseFragmentDigest(launch.Fragment) != strings.TrimSpace(launch.FragmentSHA256) {
		return nil, fmt.Errorf("Muse session launch fragment does not match the recorded sha256")
	}
	fragment, err := DecodeMuseCuratorFragment(launch.Fragment)
	if err != nil {
		return nil, err
	}
	if launch.Profile != fragment.Profile.Name || launch.ProfileLock != fragment.Profile.LockSHA256 || launch.ManagedHome != MuseFragmentManagedHome(fragment) {
		return nil, fmt.Errorf("Muse session launch provenance does not match its fragment")
	}
	if err := ValidateMuseOperatorNativeArgs(launch.OperatorArgs); err != nil {
		return nil, err
	}
	return &launch, nil
}

// MuseLaunchRequestFromPayload rebuilds the module launch request from a
// decoded payload. The factory and the wrapper-side plan tests share it,
// so the request the wrapper validated and the request the daemon plans
// are built by the same code. resumeID is the manager-composed resume
// selection from the durable record, empty on the cold path.
func MuseLaunchRequestFromPayload(launch *MuseSessionLaunch, resumeID string) (agentic.LaunchRequest, error) {
	if launch == nil {
		return agentic.LaunchRequest{}, fmt.Errorf("Muse session launch is required")
	}
	fragment, err := DecodeMuseCuratorFragment(launch.Fragment)
	if err != nil {
		return agentic.LaunchRequest{}, err
	}
	return BuildMuseLaunchRequest(MuseLaunchRequestParams{
		Model:           launch.Model,
		Effort:          launch.Effort,
		EffortRequired:  launch.EffortRequired,
		WorkDir:         launch.WorkDir,
		Env:             launch.Env,
		Fragment:        fragment,
		Yolo:            launch.Yolo,
		ToolRelease:     launch.ToolRelease,
		OperatorArgs:    launch.OperatorArgs,
		BinarySelection: launch.BinarySelection,
		ResumeSessionID: strings.TrimSpace(resumeID),
	})
}

// MusePTYHostFactory launches and restores manager-owned Muse primaries
// through R2's PTY host. It holds no state: every launch is composed
// from the payload it is given.
type MusePTYHostFactory struct {
	// bindableTimeout bounds the post-startup settle below. Tests
	// shrink it; production leaves the constructor default.
	bindableTimeout time.Duration
}

// musePTYBindableTimeout is the production post-startup settle bound.
const musePTYBindableTimeout = 45 * time.Second

// NewMusePTYHostFactory constructs the production factory registered by
// the daemon entry point.
func NewMusePTYHostFactory() *MusePTYHostFactory {
	return &MusePTYHostFactory{bindableTimeout: musePTYBindableTimeout}
}

// Provider answers the session-manager provider name.
func (f *MusePTYHostFactory) Provider() string { return "muse" }

// Launch plans through R2, verifies the module seal immediately before
// exec, and starts (cold) or resumes (prior record) the PTY host. The
// launch payload is always required: R2's durable snapshot carries no
// rebuild fields, so every attach composes fresh, including resumes.
func (f *MusePTYHostFactory) Launch(ctx context.Context, request sm.ProviderLaunchRequest) (sm.ProviderHost, error) {
	// Fail closed before the prior record is inspected at all: a
	// required fork with nothing to fork is refused, never started
	// fresh. Muse has no materialization fork plan yet.
	if request.PriorMustFork {
		return nil, fmt.Errorf("muse launches cannot fork a materialized context: refusing rather than starting a fresh session")
	}
	if request.Launch.Muse == nil {
		return nil, fmt.Errorf("Muse session launch payload is required")
	}
	raw, err := json.Marshal(request.Launch.Muse)
	if err != nil {
		return nil, err
	}
	launch, err := DecodeMuseSessionLaunch(raw)
	if err != nil {
		return nil, err
	}
	var resumeID string
	if request.Prior != nil && museHasSnapshot(request.Prior.Snapshot) {
		if request.Prior.Provider != "muse" {
			return nil, fmt.Errorf("muse launch carries a %q prior record", request.Prior.Provider)
		}
		resumeID, err = museTypedResumeSessionID(*request.Prior)
		if err != nil {
			return nil, err
		}
	}
	built, err := MuseLaunchRequestFromPayload(launch, resumeID)
	if err != nil {
		return nil, err
	}
	plan, err := BuildMusePTYLaunchPlan(built)
	if err != nil {
		return nil, err
	}
	if err := plan.plan.VerifyBeforeExec(); err != nil {
		return nil, err
	}
	var host *musePTYHost
	if resumeID != "" {
		host, err = ResumeMusePTYHost(ctx, plan, *request.Prior)
	} else {
		host, err = StartMusePTYHost(ctx, plan)
	}
	if err != nil {
		return nil, err
	}
	if err := f.waitBindable(ctx, host); err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	return withMuseProvenance(host, launch), nil
}

// waitBindable settles the fresh host to idle before Launch returns it.
// R2's start resolves at session-log open, not at a quiet composer, and
// the manager binds the goal immediately after Launch: without this
// settle every cold attach would race startup chatter against delivery's
// single quiet window and fail deferred. The wait lives here (not in
// R2's start) because R2's refusal corpus starts never-idle peers on
// purpose; a start that implied idle would break those tests by design.
// A host that never settles (a startup dialog the bind cannot cross)
// fails here, loudly, instead of failing the bind.
func (f *MusePTYHostFactory) waitBindable(ctx context.Context, host *musePTYHost) error {
	bound := f.bindableTimeout
	if bound <= 0 {
		bound = musePTYBindableTimeout
	}
	timeout, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	if host.idle("") {
		return nil
	}
	for {
		select {
		case <-timeout.Done():
			return fmt.Errorf("Muse PTY did not settle idle within %s: the goal bind needs a quiet composer\n%s", bound, host.bindableDiagnostic())
		case <-ctx.Done():
			return ctx.Err()
		case <-host.exited:
			return fmt.Errorf("Muse PTY exited before settling idle")
		case <-ticker.C:
			if host.idle("") {
				return nil
			}
		}
	}
}

// Restore never revives a PTY: the master descriptor dies with the
// manager process. It returns a detached handle that reports the host
// as stopped, so the next attach relaunches through Launch with the
// prior snapshot and resumes the same native session.
func (f *MusePTYHostFactory) Restore(_ context.Context, record sm.ProviderHostRecord) (sm.ProviderHost, error) {
	if _, err := museTypedResumeSessionID(record); err != nil {
		return nil, sm.ErrProviderHostNotFound
	}
	snapshot, err := cloneMuseRunnerSnapshot(record.Snapshot)
	if err != nil {
		return nil, sm.ErrProviderHostNotFound
	}
	return &musePTYDetachedHost{snapshot: snapshot}, nil
}

// musePTYProvenanceHost is the R2 host with the composition provenance
// the session record keeps. It embeds the concrete host, so every R2
// capability (terminal, notices, owner runs, native identity, launch
// metadata) is promoted unchanged; only the snapshot gains the four
// recorded fields. The contract stays R2's v1: R2's resume decode
// requires it and ignores the extra members.
type musePTYProvenanceHost struct {
	*musePTYHost
	profile        string
	profileLock    string
	managedHome    string
	fragmentSHA256 string
}

func withMuseProvenance(host *musePTYHost, launch *MuseSessionLaunch) *musePTYProvenanceHost {
	return &musePTYProvenanceHost{
		musePTYHost:    host,
		profile:        launch.Profile,
		profileLock:    launch.ProfileLock,
		managedHome:    launch.ManagedHome,
		fragmentSHA256: launch.FragmentSHA256,
	}
}

// musePTYProvenanceSnapshot is R2's snapshot plus the recorded
// composition provenance, marshalled flat under R2's contract.
type musePTYProvenanceSnapshot struct {
	musePTYSnapshot
	Profile        string `json:"profile"`
	ProfileLock    string `json:"profile_lock_sha256"`
	ManagedHome    string `json:"managed_home"`
	FragmentSHA256 string `json:"fragment_sha256"`
}

// Snapshot extends R2's snapshot with the composition provenance.
func (h *musePTYProvenanceHost) Snapshot() (sm.RunnerSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return sm.RunnerSnapshot{Kind: "muse-pty-provenance", MusePTYProvenance: &sm.MusePTYProvenanceSnapshot{
		Base: museSnapshotToCore(h.snapshot), Profile: h.profile, ProfileLock: h.profileLock, ManagedHome: h.managedHome, FragmentSHA256: h.fragmentSHA256,
	}}, nil
}

var (
	_ sm.ProviderHost            = (*musePTYProvenanceHost)(nil)
	_ sm.TerminalHost            = (*musePTYProvenanceHost)(nil)
	_ sm.ProviderSessionNotifier = (*musePTYProvenanceHost)(nil)
	_ sm.ProviderOwnerRunner     = (*musePTYProvenanceHost)(nil)
	_ sm.ProviderNativeIdentity  = (*musePTYProvenanceHost)(nil)
	_ sm.ProviderLaunchReporter  = (*musePTYProvenanceHost)(nil)
)

// musePTYDetachedHost is the restored-without-a-PTY handle: stopped,
// snapshotting the record it was restored from, refusing goal
// reflection. Stop marks it stopped and signals nothing: without the
// live process-group handle a kill would aim at a possibly recycled PID,
// so muse orphans are not swept in R3 (stated bound, same as the repair
// sweep, which has no muse arm either).
type musePTYDetachedHost struct {
	mu       sync.Mutex
	snapshot sm.RunnerSnapshot
	stopped  bool
}

func (h *musePTYDetachedHost) State(context.Context) (sm.ProviderHostState, error) {
	return sm.ProviderHostStopped, nil
}

func (h *musePTYDetachedHost) Snapshot() (sm.RunnerSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneMuseRunnerSnapshot(h.snapshot)
}

func (h *musePTYDetachedHost) DeliverGoalText(context.Context, string) error {
	return fmt.Errorf("detached Muse provider host cannot reflect goals")
}

func (h *musePTYDetachedHost) Stop(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopped = true
	return nil
}

var _ sm.ProviderHost = (*musePTYDetachedHost)(nil)

var _ sm.ProviderHostFactory = (*MusePTYHostFactory)(nil)
