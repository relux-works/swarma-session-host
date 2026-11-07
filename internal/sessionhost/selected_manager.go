package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

const selectedManagerProtocol = "curator-session-manager/1"

// ordinaryExecutionOwner is the daemon's ordinary execution path, not a
// notification verifier. admit must independently resolve the complete plan
// (including profile, policy, tail and resume selector) without start effects.
// execute is called only after the durable reservation. A nil execution never
// proves that nothing started. Reconciliation must not resubmit an operation.
// status observes the ordinary owner's current session index/record without
// admission or execution effects. It returns the ordinary session envelope,
// including the fixed unreadable projection, or an authoritative refusal.
// No public constructor accepts an implementation of this private interface.
type ordinaryExecutionOwner interface {
	notificationAvailable() bool
	admit(context.Context, string, []byte) error
	execute(context.Context, string, []byte) (*selectedExecution, []byte, error)
	current(context.Context, *selectedExecution) error
	status(context.Context, string) ([]byte, error)
}

// selectedExecution is retained by the ordinary owner. Generation is the
// ordinary session generation, not the independently captured Host epoch.
// Resume of a live execution must return this same object and manager.
type selectedExecution struct {
	notificationMu sync.Mutex
	manager        *NotificationManager
	session        string
	generation     uint64
	attachment     *ManagerAttachment
	claim          *selectedOperationAdmission
}

// registeredHostPlugin is the selected plugin's in-process daemon endpoint.
// Its registration is retained as object identity, never accepted on the wire.
// The plugin owns transport; Serve and the launcher fake manager are fixtures.
// The ordinary owner is supplied only by the daemon composition, not requests.
type registeredHostPlugin struct{ daemon *selectedManagerDaemon }

type selectedPendingOperation struct {
	kind   string
	digest [32]byte
}

type selectedManagerDaemon struct {
	ctx        context.Context
	cancel     context.CancelFunc
	owner      ordinaryExecutionOwner
	journal    *selectedOperationJournal
	plugin     *registeredHostPlugin
	mu         sync.Mutex
	pending    map[string]selectedPendingOperation
	executions map[*selectedExecution]struct{}
	// Each manager has one independently started cleanup and one retained result.
	// Entries may arrive from already admitted execute callbacks during shutdown.
	managerCleanup map[*NotificationManager]<-chan error
	wg             sync.WaitGroup
	closed         bool
	closeOnce      sync.Once
	done           chan struct{}
	closeErr       error
}

func registerHostPlugin(parent context.Context, expires time.Time, directory string, owner ordinaryExecutionOwner) (*registeredHostPlugin, error) {
	if parent == nil || parent.Err() != nil || owner == nil || !time.Now().Before(expires) {
		return nil, errHostOwnerUnavailable
	}
	j, err := openSelectedOperationJournal(directory)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(parent, expires)
	d := &selectedManagerDaemon{ctx: ctx, cancel: cancel, owner: owner, journal: j, pending: make(map[string]selectedPendingOperation), executions: make(map[*selectedExecution]struct{}), done: make(chan struct{})}
	p := &registeredHostPlugin{daemon: d}
	d.plugin = p
	go func() { <-ctx.Done(); d.cleanup() }()
	return p, nil
}

// Call handles exactly one bounded launch/resume RPC. Cancellation belongs to
// this request; successful execution and attachment custody belong to the daemon.
func (p *registeredHostPlugin) Call(ctx context.Context, protocol, kind string, data []byte) ([]byte, int) {
	if p == nil || p.daemon == nil || p.daemon.plugin != p || protocol != selectedManagerProtocol || ctx == nil {
		return selectedRefusal("session_host_unavailable", nil)
	}
	d := p.daemon
	d.mu.Lock()
	if d.closed || d.ctx.Err() != nil || ctx.Err() != nil {
		d.mu.Unlock()
		return selectedRefusal("session_host_unavailable", nil)
	}
	// Retain every callback route, including describe and durable replay. Close
	// freezes this admission under the same lock before waiting for these calls.
	d.wg.Add(1)
	d.mu.Unlock()
	defer d.wg.Done()
	budget := time.Minute
	if kind == "status" || kind == "cancel" || kind == "describe" {
		budget = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	stop := context.AfterFunc(p.daemon.ctx, cancel)
	defer stop()
	if kind == "status" || kind == "cancel" || kind == "describe" {
		return p.daemon.observe(ctx, kind, data)
	}
	return p.daemon.handle(ctx, kind, append([]byte(nil), data...))
}

func selectedRefusal(code string, details map[string]string) ([]byte, int) {
	if details == nil {
		details = map[string]string{}
	}
	b, _ := json.Marshal(map[string]any{"protocol": selectedManagerProtocol, "ok": false, "code": code, "details": details})
	exit := 1
	if code == "launch_plan_invalid" || code == "session_resume_invalid" || strings.HasPrefix(code, "credential_") || code == "environment_credential_store_unsupported" {
		exit = 2
	}
	if code == "session_host_provider_unsupported" || code == "session_host_capability_missing" {
		exit = 6
	}
	if code == "policy_refused" || code == "secret_policy_violation" {
		exit = 16
	}
	return b, exit
}

type ordinaryManagerRefusal struct {
	code    string
	details map[string]string
}

func (r ordinaryManagerRefusal) Error() string { return r.code }
func ordinaryRefusal(err error) ([]byte, int) {
	var refusal ordinaryManagerRefusal
	if errors.As(err, &refusal) {
		data, exit := selectedRefusal(refusal.code, refusal.details)
		if schema.Validate("refusal.schema.json", data) == nil {
			return data, exit
		}
	}
	return selectedRefusal("policy_refused", nil)
}

func (d *selectedManagerDaemon) handle(ctx context.Context, kind string, data []byte) ([]byte, int) {
	if (kind != "launch" && kind != "resume") || schema.Validate(kind+".schema.json", data) != nil {
		return selectedRefusal("launch_plan_invalid", nil)
	}
	canonical, err := schema.CanonicalJSON(data)
	if err != nil {
		return selectedRefusal("launch_plan_invalid", nil)
	}
	var q struct {
		Operation    string          `json:"operation_id"`
		Submitted    string          `json:"submitted_at"`
		Runtime      string          `json:"runtime"`
		Notification json.RawMessage `json:"notification"`
	}
	if json.Unmarshal(data, &q) != nil || q.Runtime != "codex_cli" {
		return selectedRefusal("launch_plan_invalid", nil)
	}
	if q.Notification != nil {
		if _, err := parseMemberOperation(kind, data); err != nil {
			return selectedRefusal("launch_plan_invalid", nil)
		}
	}
	digest := sha256.Sum256(canonical)
	d.mu.Lock()
	if d.closed || d.ctx.Err() != nil || ctx.Err() != nil {
		d.mu.Unlock()
		return selectedRefusal("session_host_unavailable", nil)
	}
	if pending, exists := d.pending[q.Operation]; exists {
		d.mu.Unlock()
		if pending.kind != kind || pending.digest != digest {
			return selectedRefusal("session_operation_conflict", nil)
		}
		return selectedRefusal("session_liveness_unknown", map[string]string{"operation": q.Operation})
	}
	old, exists, err := d.journal.read(q.Operation)
	if err != nil {
		d.mu.Unlock()
		return selectedRefusal("session_host_unavailable", nil)
	}
	if exists {
		d.mu.Unlock()
		if old.Cancelled {
			return selectedRefusal("session_host_unavailable", map[string]string{"reason": "cancelled"})
		}
		if old.Kind != kind || old.Digest != hex.EncodeToString(digest[:]) {
			return selectedRefusal("session_operation_conflict", nil)
		}
		if len(old.Refusal) != 0 {
			return storedSelectedRefusal(old.Refusal)
		}
		if len(old.Result) == 0 {
			return selectedRefusal("session_liveness_unknown", map[string]string{"operation": q.Operation})
		}
		return d.replay(old)
	}
	submitted, err := time.Parse(time.RFC3339, q.Submitted)
	if err != nil || time.Since(submitted) > 7*24*time.Hour {
		d.mu.Unlock()
		return selectedRefusal("session_operation_expired", nil)
	}
	d.pending[q.Operation] = selectedPendingOperation{kind, digest}
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.pending, q.Operation); d.mu.Unlock() }()
	entry := selectedOperationEntry{Digest: hex.EncodeToString(digest[:]), Kind: kind, Submitted: q.Submitted}
	// Admission is deliberately separate from both schema validation and effects.
	if q.Notification != nil && !d.owner.notificationAvailable() {
		result, exit := selectedRefusal("session_host_capability_missing", map[string]string{"capability": NotificationContract})
		return d.recordRefusal(q.Operation, entry, result, exit)
	}
	if err := d.owner.admit(ctx, kind, data); err != nil {
		result, exit := ordinaryRefusal(err)
		return d.recordRefusal(q.Operation, entry, result, exit)
	}
	if ctx.Err() != nil || d.ctx.Err() != nil {
		return selectedRefusal("session_host_unavailable", nil)
	}
	d.mu.Lock()
	old, exists, err = d.journal.read(q.Operation)
	if err == nil && exists && old.Cancelled {
		d.mu.Unlock()
		return selectedRefusal("session_host_unavailable", map[string]string{"reason": "cancelled"})
	}
	if err == nil {
		err = d.journal.write(q.Operation, entry)
	}
	d.mu.Unlock()
	if err != nil {
		return selectedRefusal("session_host_unavailable", nil)
	}
	// A persisted reservation is an unknown-outcome obligation even if execute
	// fails, loses its reply or the daemon crashes. It is never automatically retried.
	execution, result, err := d.owner.execute(ctx, kind, data)
	if execution != nil && execution.manager != nil {
		d.mu.Lock()
		d.executions[execution] = struct{}{}
		if d.closed {
			d.startManagerCleanupLocked(execution.manager)
		}
		d.mu.Unlock()
	}
	if err != nil || execution == nil || !validSelectedResult(result) {
		return selectedRefusal("session_liveness_unknown", map[string]string{"operation": q.Operation})
	}
	var actual struct {
		Session    string `json:"session"`
		Generation uint64 `json:"generation"`
		Operation  string `json:"operation_id"`
		Runtime    string `json:"runtime"`
	}
	if json.Unmarshal(result, &actual) != nil || actual.Session != execution.session || actual.Generation != execution.generation || actual.Operation != q.Operation || actual.Runtime != q.Runtime {
		return selectedRefusal("session_liveness_unknown", map[string]string{"operation": q.Operation})
	}
	// Ordinary results do not supply notification authority or references.
	var ordinary map[string]json.RawMessage
	_ = json.Unmarshal(result, &ordinary)
	delete(ordinary, "notification")
	result, _ = json.Marshal(ordinary)
	entry.Result = append([]byte(nil), result...)
	if d.journal.write(q.Operation, entry) != nil {
		return selectedRefusal("session_liveness_unknown", map[string]string{"operation": q.Operation})
	}
	if execution.manager == nil || execution.session == "" || execution.generation == 0 || d.owner.current(ctx, execution) != nil {
		return result, 0
	}
	if q.Notification != nil {
		// Another operation may already be preparing this execution. Do not
		// block an RPC on its caller or retry its claim; absence stays unresolved.
		if !execution.notificationMu.TryLock() {
			return result, 0
		}
		defer execution.notificationMu.Unlock()
		m := execution.manager
		if m.owner == nil || m.owner.host == nil {
			return result, 0
		}
		firstClaim := execution.claim == nil
		// An existing execution never spends a second claim with the resume ID.
		// The attachment rechecks its original admission and provider independently.
		if execution.claim == nil {
			a := newSelectedOperationAdmission(d, execution, kind, q.Operation, data, digest)
			execution.claim = a
			m.mu.Lock()
			if m.admission == nil {
				m.admission = a
			} else {
				a.lost = true
			}
			m.mu.Unlock()
			if a.checkManagerOperation(ctx, m.owner.host, kind, data) == nil {
				execution.attachment, _ = m.Prepare(ctx, kind, data)
				if execution.attachment != nil {
					_ = execution.attachment.Bootstrap(ctx)
					d.continueBootstrap(execution.attachment)
				}
			}
		}
		if execution.attachment != nil {
			if firstClaim || m.recheckSelectedReuse(ctx, kind, data, execution.attachment) == nil {
				result = withSelectedProjection(result, execution.attachment)
			} else {
				projection, _ := execution.attachment.Projection()
				if projection.Binding != nil {
					projection.WorkflowReference = projection.Binding.WorkflowReference
					projection.WorkflowSHA256 = projection.Binding.WorkflowSHA256
				}
				projection.State = "unresolved"
				projection.Binding = nil
				projection.Reason = "observation-unavailable"
				result = withSelectedProjectionValue(result, projection)
			}
		}
	}
	entry.Result = result
	if d.journal.write(q.Operation, entry) != nil {
		return selectedRefusal("session_liveness_unknown", map[string]string{"operation": q.Operation})
	}
	return result, 0
}

// Delayed native identity may make bounded progress after the short caller has
// returned. A transferred endpoint is never retried and request cancellation
// does not own this work. Exhaustion leaves preparing evidence, never success.
func (d *selectedManagerDaemon) continueBootstrap(a *ManagerAttachment) {
	if projection, _ := a.Projection(); projection.State != "preparing" {
		return
	}
	d.mu.Lock()
	if d.closed || d.ctx.Err() != nil {
		d.mu.Unlock()
		return
	}
	d.wg.Add(1)
	d.mu.Unlock()
	go func() {
		defer d.wg.Done()
		ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
		defer cancel()
		for attempt := 0; attempt < 3; attempt++ {
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			a.lifecycle.host.mu.Lock()
			transferred := a.lifecycle.transferred
			a.lifecycle.host.mu.Unlock()
			if transferred {
				return
			}
			_ = a.Bootstrap(ctx)
			if projection, _ := a.Projection(); projection.State != "preparing" {
				return
			}
		}
	}()
}

func validSelectedResult(data []byte) bool {
	return schema.Validate("session.schema.json", data) == nil
}

func withSelectedProjection(result []byte, a *ManagerAttachment) []byte {
	if a == nil {
		return result
	} // Absence is unresolved to a requesting consumer.
	projection, err := a.Projection()
	if err != nil {
		return result
	}
	return withSelectedProjectionValue(result, projection)
}
func withSelectedProjectionValue(result []byte, projection notificationProjection) []byte {
	var response map[string]json.RawMessage
	if json.Unmarshal(result, &response) != nil {
		return result
	}
	projectionBytes, _ := json.Marshal(projection)
	response["notification"] = projectionBytes
	out, _ := json.Marshal(response)
	if !validSelectedResult(out) {
		return result
	}
	return out
}

func (d *selectedManagerDaemon) replay(entry selectedOperationEntry) ([]byte, int) {
	// A stored ready projection is not live enrollment authority after restart.
	// Live objects are checked again; otherwise retain actual references but fence.
	var r map[string]json.RawMessage
	_ = json.Unmarshal(entry.Result, &r)
	if raw := r["notification"]; raw != nil {
		var n notificationProjection
		if json.Unmarshal(raw, &n) == nil && n.AttachmentRef != "" {
			d.mu.Lock()
			all := make([]*selectedExecution, 0, len(d.executions))
			for e := range d.executions {
				all = append(all, e)
			}
			d.mu.Unlock()
			var attachment *ManagerAttachment
			for _, e := range all {
				if !e.notificationMu.TryLock() {
					continue
				}
				a := e.attachment
				e.notificationMu.Unlock()
				if a != nil && a.lifecycle.attachment == n.AttachmentRef {
					attachment = a
					break
				}
			}
			if attachment != nil {
				return withSelectedProjection(entry.Result, attachment), 0
			}
			if n.Binding != nil {
				n.WorkflowReference = n.Binding.WorkflowReference
				n.WorkflowSHA256 = n.Binding.WorkflowSHA256
			}
			n.State = "unresolved"
			n.Binding = nil
			n.Reason = "observation-unavailable"
			r["notification"], _ = json.Marshal(n)
			out, _ := json.Marshal(r)
			if validSelectedResult(out) {
				return out, 0
			}
		}
	}
	return append([]byte(nil), entry.Result...), 0
}

func (p *registeredHostPlugin) Close(ctx context.Context) error {
	if p == nil || p.daemon == nil || ctx == nil {
		return errHostOwnerUnavailable
	}
	d := p.daemon
	d.cancel()
	select {
	case <-d.done:
		return d.closeErr
	default:
	}
	select {
	case <-d.done:
		return d.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Called with d.mu held. Never wait for a manager here: a stalled provider in
// one execution must not postpone cancellation/join of any other Host owner.
func (d *selectedManagerDaemon) startManagerCleanupLocked(m *NotificationManager) {
	if m == nil {
		return
	}
	if d.managerCleanup == nil {
		d.managerCleanup = make(map[*NotificationManager]<-chan error)
	}
	if _, started := d.managerCleanup[m]; started {
		return
	}
	result := make(chan error, 1)
	d.managerCleanup[m] = result
	go func() { result <- m.Close(context.Background()) }()
}

func (d *selectedManagerDaemon) cleanup() {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.cancel()
		for e := range d.executions {
			d.startManagerCleanupLocked(e.manager)
		}
		d.mu.Unlock()
		// Start all currently retained owners before joining any request/provider
		// callback. Late execute results start their own cleanup under d.mu above.
		// A caller timeout never releases these callbacks or their unknown outcomes.
		d.wg.Wait()
		// Admission is frozen and every callback capable of retaining a manager
		// has joined, so no more cleanup results can be added to this map.
		for _, result := range d.managerCleanup {
			d.closeErr = errors.Join(d.closeErr, <-result)
		}
		d.closeErr = errors.Join(d.closeErr, d.journal.close())
		close(d.done)
	})
}

// A resume is admitted as its own ordinary operation and independently checked
// against the configured record and Core before reusing an existing attachment.
// No changed selection may borrow the launch operation's successful projection.
func (m *NotificationManager) recheckSelectedReuse(ctx context.Context, kind string, data []byte, a *ManagerAttachment) error {
	if m == nil || m.records == nil || a == nil || a.custody == nil || a.manager != m {
		return errMemberRecordUnavailable
	}
	op, err := m.records.retainOperation(kind, data)
	if err != nil {
		return err
	}
	custody, err := admitManagerCustody(ctx, op, data, m.env, m.read)
	if err != nil {
		op.close()
		return err
	}
	defer custody.close()
	if custody.selection != a.custody.selection || custody.selected != a.custody.selected {
		return errMemberRecordUnavailable
	}
	return a.lifecycle.custody.checkNotificationOwner(m.owner.host, a.lifecycle.selection)
}

// ServeRequest is the plugin's bounded one-request adapter, not Host.Serve.
func (p *registeredHostPlugin) ServeRequest(ctx context.Context, protocol, kind string, input io.Reader, output io.Writer) int {
	data, err := io.ReadAll(io.LimitReader(input, schema.Limit+1))
	var result []byte
	var exit int
	if err != nil || len(data) > schema.Limit {
		result, exit = selectedRefusal("launch_plan_invalid", nil)
	} else {
		result, exit = p.Call(ctx, protocol, kind, data)
	}
	return writeSelectedReply(output, bytes.TrimSpace(result), exit)
}
