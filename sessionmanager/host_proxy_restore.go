//go:build unix

package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/relux-works/swarma-session-host/sessionproxy"
)

// restoreProviderProxyLocked is called by Recover after the committed record,
// restored runner and its current controller/input are installed. Failure keeps
// those recovery inputs for retry; it must propagate before the server is Ready.
// It neither stops the native runner nor changes retained accounting or goals.
func (h *SessionHost) restoreProviderProxyLocked(ctx context.Context, id string) error {
	record, ok := h.sessions[id]
	if !ok || record.SessionID != id {
		return ErrLegacyViewStale
	}
	if record.State == SessionStopped || record.ConditionalStop != nil || record.GracefulRetirement {
		return nil
	}
	if absentHostPort(h.runners[id]) {
		return ErrHostCapabilityUnavailable
	}
	if _, ok := h.runners[id].(ProviderAttachmentTarget); !ok {
		return nil
	}
	if _, ok := h.attachmentControllers[id]; !ok {
		return ErrStaleInputController
	}
	root := h.options.Layout.AttachmentsDir
	// A recoverable stale pathname is only meaningful inside our private root.
	// Never chmod/adopt a foreign root merely to make recovery succeed.
	if info, err := os.Lstat(root); err == nil {
		owner, owned := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() || info.Mode().Perm() != 0700 || !owned || owner.Uid != uint32(os.Getuid()) {
			return sessionproxy.ErrPrivatePath
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	_, err := h.ensureProviderProxyLocked(ctx, id)
	if !errors.Is(err, sessionproxy.ErrPathOccupied) {
		return err
	}
	// New already validated the actual target, private directory and session ID.
	// Only a known-absent same-owned socket may be reclaimed; live/unknown
	// probes and regular files/symlinks stay untouched (canonical socket custody).
	path := sessionproxy.SocketPathFor(root, id)
	observed, statErr := os.Lstat(path)
	if statErr != nil {
		return errors.Join(err, statErr)
	}
	if observed.Mode()&os.ModeSocket == 0 || !unixSocketKnownAbsent(probeUnixSocket(path, transportProbeTimeout)) {
		return err
	}
	removed, removeErr := removeSameOwnedSocket(path, observed)
	if removeErr != nil || !removed {
		return errors.Join(err, removeErr)
	}
	if _, err = h.ensureProviderProxyLocked(ctx, id); err != nil {
		return fmt.Errorf("restoring session %s attachment proxy: %w", id, err)
	}
	return nil
}
