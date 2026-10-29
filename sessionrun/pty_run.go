package sessionrun

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// This is the actual PTY provider's read-only post-delivery observer. Its run
// identity comes only from the captured writer's retained physical observation.
// The legacy ProviderOwnerRunner entry includes a write and must not be used.
type ptyOwnerObserver interface {
	ObserveOwnerRun(context.Context, string) (*core.ProviderRunResult, error)
}

func ptySnapshot(s core.RunnerSnapshot) (core.MusePTYSnapshot, bool) {
	switch s.Kind {
	case "muse-pty":
		if s.MusePTY != nil {
			return *s.MusePTY, true
		}
	case "muse-pty-provenance":
		if s.MusePTYProvenance != nil {
			return s.MusePTYProvenance.Base, true
		}
	}
	return core.MusePTYSnapshot{}, false
}
func samePTYPhysical(a, b core.RunnerSnapshot) bool {
	x, ok := ptySnapshot(a)
	if !ok {
		return false
	}
	y, ok := ptySnapshot(b)
	if !ok || a.Kind != b.Kind || x.SessionID == "" || x.PID <= 0 || x.LogPath == "" || x.Contract != y.Contract || x.SessionID != y.SessionID || x.PID != y.PID || x.LogPath != y.LogPath {
		return false
	}
	if a.Kind == "muse-pty-provenance" {
		p, q := a.MusePTYProvenance, b.MusePTYProvenance
		return p.Profile == q.Profile && p.ProfileLock == q.ProfileLock && p.ManagedHome == q.ManagedHome && p.FragmentSHA256 == q.FragmentSHA256
	}
	return true
}
func (r *Runtime) runPTY(ctx context.Context, c core.CapturedOwnerRun, q core.ProviderRunRequest) (*core.ProviderRunResult, error) {
	raw, err := committedProvider(c)
	if err != nil {
		return nil, err
	}
	prior, err := core.NewLegacyRecordCodec().DecodeProviderHost(ctx, raw)
	if err != nil {
		return nil, err
	}
	native, err := c.Native.Snapshot()
	if err != nil {
		return nil, err
	}
	id, ok := c.Native.(core.ProviderNativeIdentity)
	if prior.Provider != "muse" || !samePTYPhysical(prior.Snapshot, native) || !ok {
		return nil, core.ErrLegacyViewStale
	}
	physical, _ := ptySnapshot(native)
	if id.NativeIdentity() != physical.SessionID {
		return nil, core.ErrLegacyViewStale
	}
	observer, ok := c.Native.(ptyOwnerObserver)
	if !ok {
		return nil, core.ErrHostCapabilityUnavailable
	}
	// Per-write admission belongs to the captured host writer; no epoch or map
	// lock spans this native receipt wait or the long-running observer below.
	observed, err := submitPTY(ctx, c.Input, c.Lease, physical.SessionID, q.Prompt)
	if err != nil {
		return nil, err
	}
	result, runErr := observer.ObserveOwnerRun(ctx, observed.PTYRun.RunID)
	after, snapshotErr := c.Native.Snapshot()
	if snapshotErr == nil && (!samePTYPhysical(native, after) || id.NativeIdentity() != physical.SessionID) {
		snapshotErr = core.ErrLegacyViewStale
	}
	return result, errors.Join(runErr, snapshotErr)
}
