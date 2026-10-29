package sessionmanager

import (
	"context"
	"fmt"
	"io"
	"sync"
)

var _ AttachmentAccessHost = (*SessionHost)(nil)

// This capability is local to one admitted operation. It deliberately exposes
// neither a runner nor a way to submit fresh work using a released acquisition.
type attachmentAccess struct {
	host  *SessionHost
	lease ControllerLease
	once  sync.Once
}

func (a *attachmentAccess) Lease() ControllerLease { return a.lease }
func (a *attachmentAccess) Release() error {
	a.once.Do(func() { a.host.mu.Lock(); delete(a.host.attachmentAccesses, a); a.host.mu.Unlock() })
	return nil
}

// Called only under mu, at the same publication point as live membership.
// Issued IDs stay reserved for this host lifetime, including after detach.
func (h *SessionHost) issueAttachment(ctx context.Context, id string) (string, error) {
	lease, ok := h.attachmentControllers[id]
	if !ok {
		return "", ErrStaleInputController
	}
	if input := h.inputs[id]; input != nil {
		current, err := input.CurrentLease(ctx, id)
		if err != nil {
			return "", err
		}
		if current != lease {
			return "", ErrStaleInputController
		}
	}
	release, err := h.epochs.beginIO(ctx, lease)
	if err != nil {
		return "", err
	}
	release()
	if h.issuedAttachments == nil {
		h.issuedAttachments = map[string]ControllerLease{}
	}
	for attempt := 0; attempt < 32; attempt++ {
		next := h.newAttachmentID
		if next == nil {
			next = func() (string, error) { return opaqueID("ATT") }
		}
		candidate, err := next()
		if err != nil {
			return "", err
		}
		if candidate == "" {
			continue
		}
		if _, seen := h.issuedAttachments[candidate]; seen {
			continue
		}
		h.issuedAttachments[candidate] = lease
		h.attachments[candidate] = id
		return candidate, nil
	}
	return "", fmt.Errorf("%w: attachment identity collision", ErrHostCapabilityUnavailable)
}

func (h *SessionHost) rememberAttachmentController(id string, epoch ControllerEpoch) {
	if h.attachmentControllers == nil {
		h.attachmentControllers = map[string]ControllerLease{}
	}
	h.attachmentControllers[id] = ControllerLease{SessionID: id, Epoch: epoch}
}

// Membership and controller capture have exactly one order with Detach. No
// lock or durable I/O fence is retained for the run's duration: existing input
// fences continue to revoke old-epoch operations on Stop/replacement.
func (h *SessionHost) acquireAttachment(ctx context.Context, id, attachmentID string) (*attachmentAccess, RunnerHost, error) {
	if err := h.lockContext(ctx); err != nil {
		return nil, nil, err
	}
	defer h.mu.Unlock()
	record, exists := h.sessions[id]
	if id == "" || attachmentID == "" || !exists || h.attachments[attachmentID] != id {
		return nil, nil, ErrProviderHostNotFound
	}
	if h.attachmentChanging(id) || h.freshAttachmentReserved(id) || h.records.uncertain[id] || record.ConditionalStop != nil {
		return nil, nil, ErrLegacyCommitUncertain
	}
	if _, pending := h.pendingInitial[id]; pending {
		return nil, nil, ErrLegacyCommitUncertain
	}
	runner := h.runners[id]
	if runner == nil || record.State != SessionRunning || record.GracefulRetirement {
		return nil, nil, ErrHostCapabilityUnavailable
	}
	lease, issued := h.issuedAttachments[attachmentID]
	if !issued || lease != h.attachmentControllers[id] {
		return nil, nil, ErrStaleInputController
	}
	if input := h.inputs[id]; input != nil {
		current, err := input.CurrentLease(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		if current != lease {
			return nil, nil, ErrStaleInputController
		}
	}
	release, err := h.epochs.beginIO(ctx, lease)
	if err != nil {
		return nil, nil, err
	}
	release()
	access := &attachmentAccess{host: h, lease: lease}
	if h.attachmentAccesses == nil {
		h.attachmentAccesses = map[*attachmentAccess]struct{}{}
	}
	h.attachmentAccesses[access] = struct{}{}
	return access, runner, nil
}
func (h *SessionHost) AcquireAttachment(ctx context.Context, id, attachmentID string) (AttachmentAccess, error) {
	access, _, err := h.acquireAttachment(ctx, id, attachmentID)
	if err != nil {
		return nil, err
	}
	return access, nil
}

func (h *SessionHost) Terminal(ctx context.Context, id, attachmentID string) (io.ReadWriteCloser, error) {
	access, runner, err := h.acquireAttachment(ctx, id, attachmentID)
	if err != nil {
		return nil, err
	}
	defer access.Release()
	terminal, ok := runner.(TerminalHost)
	if !ok {
		return nil, ErrTerminalUnsupported
	}
	call, finish := h.attachmentContext(ctx)
	defer finish()
	release, err := h.epochs.beginIO(call, access.Lease())
	if err != nil {
		return nil, err
	}
	defer release()
	// The fence covers opening only; an admitted stream keeps the provider's
	// original lifetime and may outlive Detach. The caller owns stream.Close.
	return terminal.AttachTerminal(ctx)
}
func (h *SessionHost) ResizeTerminal(ctx context.Context, id, attachmentID string, rows, cols uint16) error {
	access, runner, err := h.acquireAttachment(ctx, id, attachmentID)
	if err != nil {
		return err
	}
	defer access.Release()
	terminal, ok := runner.(TerminalHost)
	if !ok {
		return ErrTerminalUnsupported
	}
	call, finish := h.attachmentContext(ctx)
	defer finish()
	release, err := h.epochs.beginIO(call, access.Lease())
	if err != nil {
		return err
	}
	defer release()
	return terminal.ResizeTerminal(rows, cols)
}
