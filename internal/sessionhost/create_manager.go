package sessionhost

import (
	"bytes"
	"context"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

// transferTo serializes the one acknowledgment with create/close. Failed
// observation or acknowledgment leaves every T1 cleanup duty in place. Success
// releases duty, never the physical objects or their original finite lifetime.
// This handoff is used by private composition; public entry points stay closed.
func (h *ordinaryCreateHandler) transferTo(owner *continuingHostOwner) (*nativeCorrelation, error) {
	if h == nil {
		return nil, errNativeCorrelation
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.transferred != nil || h.outcome != createRunnerOwned || h.wire.owner.ctx.Err() != nil {
		return nil, errNativeCorrelation
	}
	server, ok := h.child.(*runner.Server)
	if !ok || server == nil {
		return nil, errNativeCorrelation
	}
	c, err := retainNativeCorrelation(h.operation, server, owner)
	if err != nil {
		return nil, err
	}
	if err := c.acknowledge(h.operation, server, owner); err != nil {
		return nil, err
	}
	h.transferred, h.outcome = c, createContinuingOwned
	// Policy gates the one Start; it is not a renewable post-start grant.
	if h.policy != nil {
		h.policy.close()
	}
	return c, nil
}

type createManagerAdmission struct{ correlation *nativeCorrelation }

func (a createManagerAdmission) checkManagerOperation(ctx context.Context, h *Host, kind string, data []byte) error {
	c := a.correlation
	if c == nil || c.owner.host != h || kind != c.op.claim.kind || !bytes.Equal(data, c.op.claim.data) {
		return errMemberRecordUnavailable
	}
	return c.check(ctx)
}

// openCreateNotificationManager composes retained creation and correlation with
// the existing strict manager/Core/broker checks. No provider, ACK, ordinary
// policy or Core selection is inferred here. Public Open remains closed.
func openCreateNotificationManager(h *ordinaryCreateHandler, owner *continuingHostOwner, env []string, broker NotificationBroker) (*NotificationManager, error) {
	if h == nil || owner == nil || broker == nil {
		return nil, errMemberRecordUnavailable
	}
	records, err := owner.openMemberRecordSource(h.wire.owner.records.path)
	if err != nil {
		return nil, err
	}
	c, err := h.transferTo(owner)
	if err != nil {
		_ = records.close()
		return nil, err
	}
	return &NotificationManager{owner: owner, records: records, env: append([]string(nil), env...), broker: broker,
		attachments: make(map[*ManagerAttachment]struct{}), read: readManagerCore, admission: createManagerAdmission{c}, createCorrelation: c}, nil
}
