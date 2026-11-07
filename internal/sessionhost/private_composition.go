package sessionhost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
)

var errPrivateComposition = errors.New("private_composition_unavailable")

// These are selected, trusted in-process dependencies, never a caller DTO.
// Source must own its actual provider origin. Policy is selected by the explicit
// private profile configuration below. There are no implicit defaults.
type privateCompositionDependencies struct {
	source NotificationBroker
	core   managerCoreRead
}

type privateCompositionConfig struct {
	profile                   privateRuntimeProfileConfig
	records, stateDir, socket string
	launcher                  ordinaryLauncherConfig
	selection                 Selection
	nativeRelease, permission string
	coreEnvironment           []string
	ioTimeout, poll, ready    time.Duration
}

// One object owns one fresh execution. No listener, provider discovery, caller
// authentication, restart adoption, or public manager admission is installed.
type privateComposition struct {
	profile    RuntimeProfileReport
	create     *ordinaryCreateHandler
	owner      *continuingHostOwner
	manager    *NotificationManager
	attachment *ManagerAttachment
	once       sync.Once
	done       chan struct{}
	closeErr   error // published by done
}

// openPrivateComposition constructs its policy after basic arguments are valid.
// request controls startup only; parent/expires own the continuing execution.
// Every failed startup joins all resources it acquired before returning.
func openPrivateComposition(parent, request context.Context, expires time.Time, cfg privateCompositionConfig, deps privateCompositionDependencies, kind string, body []byte) (_ *privateComposition, resultErr error) {
	if parent == nil || request == nil || parent.Err() != nil || request.Err() != nil || !time.Now().Before(expires) ||
		deps.source == nil || deps.core == nil || kind != "launch" ||
		!filepath.IsAbs(cfg.stateDir) || filepath.Clean(cfg.stateDir) != cfg.stateDir ||
		cfg.ready <= 0 || cfg.ready > time.Minute || cfg.poll <= 0 || cfg.poll > time.Minute ||
		cfg.ioTimeout <= 0 || cfg.ioTimeout > time.Minute || !validSelection(cfg.selection) || cfg.nativeRelease == "" || cfg.permission == "" {
		return nil, errPrivateComposition
	}
	var err error
	var report RuntimeProfileReport
	cfg, report, err = freezePrivateProfile(cfg)
	if err != nil {
		return nil, err
	}
	policy, _, err := newSingleTrustUnitCreatePolicy(cfg)
	if err != nil {
		return nil, err
	}
	r := &privateComposition{profile: report, done: make(chan struct{})}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, r.close(context.Background()))
		}
		policy.close()
	}()
	// An existing directory (even empty) is a restart/adoption request. Retain
	// this footprint on failure; never delete a journal or retry a possible Start.
	if err := os.Mkdir(cfg.stateDir, 0700); err != nil {
		return nil, errPrivateComposition
	}
	body = append([]byte(nil), body...)
	r.create, err = openOrdinaryCreateHandler(parent, expires, cfg.records, cfg.launcher, cfg.socket, cfg.ioTimeout, policy)
	if err != nil {
		return nil, err
	}
	outcome, err := r.create.create(request, kind, body)
	if err != nil {
		return nil, err
	}
	if outcome != createRunnerOwned {
		return nil, errPrivateComposition
	}
	actual, err := r.create.custody()
	if err != nil {
		return nil, err
	}
	hostConfig := Config{StateDir: cfg.stateDir, Socket: cfg.socket, Execution: actual.claim.id,
		Selection: cfg.selection, NoticeMode: WorkReference, ProtocolName: "codex", IOTimeout: cfg.ioTimeout, PollInterval: cfg.poll}
	hostConfig.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		return codexadapter.OpenDriverWithOptions(ap.Config{NativeRelease: cfg.nativeRelease, Lifecycle: ap.Create,
			Selection: ap.Selection{Provider: cfg.selection.Vendor, Model: cfg.selection.Model, Effort: cfg.selection.Reasoning}, Workspace: actual.claim.invocation.workspace},
			codexadapter.DriverOptions{NoticeMode: WorkReference, PermissionPolicy: cfg.permission}, saved)
	}
	r.owner, err = openContinuingHostOwner(parent, expires, hostConfig)
	if err != nil {
		return nil, err
	}
	if err = r.waitFresh(request, cfg.ready, cfg.poll); err != nil {
		return nil, err
	}
	r.manager, err = openCreateNotificationManager(r.create, r.owner, cfg.coreEnvironment, deps.source)
	if err != nil {
		return nil, err
	}
	r.manager.read = deps.core // fixed before Prepare or publication
	r.attachment, err = r.manager.Prepare(request, kind, body)
	if err != nil {
		return nil, err
	}
	if err = r.attachment.Bootstrap(request); err != nil {
		return nil, err
	}
	if request.Err() != nil || r.manager.createCorrelation.check(request) != nil {
		return nil, errPrivateComposition
	}
	go func() {
		select {
		case <-r.manager.createCorrelation.done:
		case <-r.attachment.lifecycle.done:
		case <-r.owner.ctx.Done():
		case <-r.done:
			return
		}
		_ = r.close(context.Background())
	}()
	return r, nil
}

// Waiting for real parser progress does not consume T2's single handoff slot.
// Only the subsequent retained-object acknowledgment can admit the manager.
func (r *privateComposition) waitFresh(request context.Context, bound, poll time.Duration) error {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		h := r.owner.host
		h.mu.Lock()
		d, ok := h.driver.(*codexadapter.Driver)
		fresh := false
		if ok {
			_, fresh = d.FreshThread()
		}
		failed := h.err != nil || h.protocolFailed || h.writeBlocked || h.state.StopRequested || h.recovering
		ready := !failed && fresh && h.outputOffset != 0 && h.state.Protocol != nil && len(h.state.Protocol.Pending) == 0
		h.mu.Unlock()
		if failed {
			return errPrivateComposition
		}
		if ready {
			return nil
		}
		select {
		case <-request.Done():
			return request.Err()
		case <-r.owner.ctx.Done():
			return errPrivateComposition
		case <-r.create.wire.owner.ctx.Done():
			return errPrivateComposition
		case <-timer.C:
			return errPrivateComposition
		case <-tick.C:
		}
	}
}

// Cleanup outlives canceled waiters. On either side of the T2 handoff it joins
// the actual objects, without reconstructing a process from durable metadata.
func (r *privateComposition) close(wait context.Context) error {
	if r == nil || wait == nil {
		return errPrivateComposition
	}
	r.once.Do(func() {
		if r.create != nil {
			r.create.wire.owner.cancel()
		}
		if r.owner != nil {
			r.owner.cancel()
		}
		go func() {
			defer close(r.done)
			if r.manager != nil {
				r.closeErr = errors.Join(r.closeErr, r.manager.Close(context.Background()))
			} else if r.owner != nil {
				r.closeErr = errors.Join(r.closeErr, r.owner.close(context.Background()))
			}
			if r.create != nil {
				r.closeErr = errors.Join(r.closeErr, r.create.close(context.Background()))
			}
		}()
	})
	select {
	case <-r.done:
		return r.closeErr
	default:
	}
	select {
	case <-r.done:
		return r.closeErr
	case <-wait.Done():
		return wait.Err()
	}
}
