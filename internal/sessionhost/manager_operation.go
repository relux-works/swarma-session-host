package sessionhost

import (
	"context"
	"crypto/sha256"
)

// managerNotificationOperation belongs to one actual continuing manager. It is
// a single-use effect/reconciliation record, not ordinary runtime admission.
// Even a failed preparation retains its claim: provider refusal can follow
// effects that this manager cannot safely undo or replay.
type managerNotificationOperation struct {
	kind, id   string
	digest     [32]byte
	done       chan struct{}
	attachment *ManagerAttachment // published only after the same preparation succeeds
}

// claimOperation runs only after the independent ordinary admission check. No
// caller can construct admission by storing a request here. One Host lifecycle
// has one claim, including when a different operation ID is submitted later.
// A pending/failed claim never causes an implicit retry or waits on a peer call.
func (m *NotificationManager) claimOperation(ctx context.Context, kind string, data []byte) (*managerNotificationOperation, bool, error) {
	q, err := parseMemberOperation(kind, data)
	if err != nil || ctx.Err() != nil || m.owner.checkCurrentHost(m.owner.host) != nil {
		return nil, false, errMemberRecordUnavailable
	}
	digest := sha256.Sum256(data)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || ctx.Err() != nil {
		return nil, false, errMemberRecordUnavailable
	}
	if existing := m.operation; existing != nil {
		if existing.kind != kind || existing.id != q.Operation || existing.digest != digest || existing.attachment == nil {
			return nil, false, errMemberRecordUnavailable
		}
		select {
		case <-existing.done:
			return existing, false, nil
		default:
			return nil, false, errMemberRecordUnavailable
		}
	}
	claim := &managerNotificationOperation{kind: kind, id: q.Operation, digest: digest, done: make(chan struct{})}
	m.operation = claim
	return claim, true, nil
}
