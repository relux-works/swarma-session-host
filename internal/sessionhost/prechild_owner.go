package sessionhost

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"
)

var errPreChildUnavailable = errors.New("pre_child_operation_unavailable")

// preChildManager is the actual finite resource owner before a runner/Host
// exists. Its source is opened from composition-selected configuration, never
// from the request. It is not ordinary admission and implements no broker or
// native capability. Only the selected ordinary handler may add those conjuncts.
type preChildManager struct {
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	records   *memberRecordSource
	mu        sync.Mutex
	closed    bool
	operation *preChildOperation
}

type preChildOperation struct {
	owner            *preChildManager
	kind, id         string
	data             []byte
	digest           [32]byte
	retained         *retainedMemberOperation
	invocation       protocolChildInvocation
	ready, attempted bool // guarded by owner.mu; neither is runtime admission
}

func openPreChildManager(parent context.Context, expires time.Time, directory string) (*preChildManager, error) {
	if parent == nil || parent.Err() != nil || !time.Now().Before(expires) {
		return nil, errPreChildUnavailable
	}
	ctx, cancel := context.WithDeadline(parent, expires)
	o := &preChildManager{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	s, err := openPhysicalMemberRecordSource(directory, nil, o)
	if err != nil || ctx.Err() != nil {
		cancel()
		if s != nil {
			_ = s.close()
		}
		return nil, errPreChildUnavailable
	}
	o.records = s
	go func() {
		<-ctx.Done()
		o.mu.Lock()
		o.closed = true
		_ = s.close()
		o.mu.Unlock()
		close(o.done)
	}()
	return o, nil
}

// prepare retains one exact complete request before failable physical record
// preparation. A failed valid request is consumed, not a license to retry after
// repairing the source. Identical successful requests return the same claim.
// Invalid wire input never becomes a claim or a partial invocation.
func (o *preChildManager) prepare(request context.Context, kind string, data []byte) (*preChildOperation, error) {
	if o == nil || request == nil || request.Err() != nil {
		return nil, errPreChildUnavailable
	}
	data = append([]byte(nil), data...)
	q, err := parseMemberOperation(kind, data)
	if err != nil {
		return nil, errPreChildUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.ctx.Err() != nil || request.Err() != nil || o.records == nil || o.records.preChild != o {
		return nil, errPreChildUnavailable
	}
	digest := sha256.Sum256(data)
	if op := o.operation; op != nil {
		if op.kind != kind || op.id != q.Operation || op.digest != digest || !op.ready {
			return nil, errPreChildUnavailable
		}
		return op, nil
	}
	op := &preChildOperation{owner: o, kind: kind, id: q.Operation, data: data, digest: digest}
	o.operation = op
	retained, err := o.records.retainOperation(kind, data)
	if err != nil {
		return nil, errPreChildUnavailable
	}
	fail := func() (*preChildOperation, error) { retained.close(); return nil, errPreChildUnavailable }
	n := retained.record.record.Notification
	if n == nil || retained.record.record.Identity != n.Member.Agent || retained.request.Notification.memberNotificationInput != *n {
		return fail()
	}
	invocation, err := compileProtocolChildInvocation(kind, data, protocolChildMemberSelectors{retained.record.record.Identity, n.Member.ConfigurationRoot})
	if err != nil || request.Err() != nil || o.ctx.Err() != nil || retained.recheckPhysical(o.records, kind, data) != nil {
		return fail()
	}
	op.retained, op.invocation, op.ready = retained, invocation, true
	return op, nil
}

// takeInvocation consumes the start attempt before returning its immutable
// data. Failed start/init, lost replies, canceled requests and conflicting IDs
// cannot obtain a second attempt. This does not itself invoke a runtime or
// attest ordinary policy, executable identity or native initialization.
func (op *preChildOperation) takeInvocation(request context.Context, owner *preChildManager) (protocolChildInvocation, error) {
	if op == nil || owner == nil || op.owner != owner {
		return protocolChildInvocation{}, errPreChildUnavailable
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.operation != op || !op.ready || op.attempted {
		return protocolChildInvocation{}, errPreChildUnavailable
	}
	op.attempted = true
	if request == nil || request.Err() != nil || owner.closed || owner.ctx.Err() != nil ||
		owner.records == nil || owner.records.preChild != owner ||
		op.retained.recheckPhysical(owner.records, op.kind, op.data) != nil {
		return protocolChildInvocation{}, errPreChildUnavailable
	}
	result := op.invocation
	result.args = append([]string(nil), result.args...)
	return result, nil
}

// Cancellation of a close waiter never abandons the owner's source cleanup.
// Only its own continuing lifetime, not an individual request, closes it.
func (o *preChildManager) close(wait context.Context) error {
	if o == nil || o.cancel == nil || o.done == nil || wait == nil {
		return errPreChildUnavailable
	}
	o.cancel()
	select {
	case <-o.done:
		return nil
	default:
	}
	select {
	case <-o.done:
		return nil
	case <-wait.Done():
		return wait.Err()
	}
}
