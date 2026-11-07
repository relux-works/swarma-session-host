package sessionhost

import (
	"bytes"
	"context"
	"encoding/hex"
	"sync"
)

// selectedOperationAdmission is minted only after durable ordinary admission
// and execution. The original bytes, registration, execution object and actual
// controller identities remain retained; none are decoded proof constructors.
type selectedOperationAdmission struct {
	daemon                    *selectedManagerDaemon
	plugin                    *registeredHostPlugin
	execution                 *selectedExecution
	manager                   *NotificationManager
	kind, id, session, runner string
	generation, epoch         uint64
	original                  []byte
	digest                    [32]byte
	mu                        sync.Mutex
	lost                      bool
}

func newSelectedOperationAdmission(d *selectedManagerDaemon, e *selectedExecution, kind, id string, data []byte, digest [32]byte) *selectedOperationAdmission {
	h := e.manager.owner.host
	h.mu.Lock()
	defer h.mu.Unlock()
	return &selectedOperationAdmission{daemon: d, plugin: d.plugin, execution: e, manager: e.manager, kind: kind, id: id, session: e.session, generation: e.generation, runner: h.state.RunnerInstance, epoch: h.state.Epoch, original: append([]byte(nil), data...), digest: digest}
}

func (a *selectedOperationAdmission) checkManagerOperation(ctx context.Context, h *Host, kind string, data []byte) error {
	if a == nil || ctx == nil || ctx.Err() != nil || a.daemon == nil || a.execution == nil || a.manager == nil || a.manager.owner == nil || h != a.manager.owner.host || kind != a.kind || !bytes.Equal(data, a.original) {
		return errMemberRecordUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lost {
		return errMemberRecordUnavailable
	}
	d := a.daemon
	e := a.execution
	d.mu.Lock()
	_, retained := d.executions[e]
	current := !d.closed && d.ctx.Err() == nil && d.plugin == a.plugin
	d.mu.Unlock()
	if !retained || !current || e.manager != a.manager || e.session != a.session || e.generation != a.generation || d.owner.current(ctx, e) != nil || a.manager.owner.checkCurrentHost(h) != nil {
		a.lost = true
		return errMemberRecordUnavailable
	}
	h.mu.Lock()
	physical := h.state.Execution == a.manager.owner.execution && h.state.RunnerInstance == a.runner && h.state.Epoch == a.epoch
	h.mu.Unlock()
	entry, exists, err := d.journal.read(a.id)
	if !physical || err != nil || !exists || entry.Kind != kind || entry.Digest != hex.EncodeToString(a.digest[:]) || len(entry.Result) == 0 {
		a.lost = true
		return errMemberRecordUnavailable
	}
	return nil
}
