package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// All adapters here are owned, inert models. The real Host controls a local Go
// child; neither native Codex nor Core nor a member/provider service is invoked.
type selectedOrdinaryFixture struct {
	t           *testing.T
	execution   *selectedExecution
	plugin      *registeredHostPlugin
	available   bool
	deny        atomic.Bool
	starts      atomic.Int32
	admissions  atomic.Int32
	admitHook   func(context.Context, string, []byte) error
	executeHook func(context.Context, string, []byte) (*selectedExecution, []byte, error)
}

func (o *selectedOrdinaryFixture) notificationAvailable() bool { return o.available }
func (o *selectedOrdinaryFixture) admit(ctx context.Context, kind string, data []byte) error {
	o.admissions.Add(1)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if o.admitHook != nil {
		return o.admitHook(ctx, kind, data)
	}
	return nil // Explicit inert ordinary admission; never a production default.
}
func (o *selectedOrdinaryFixture) execute(ctx context.Context, kind string, data []byte) (*selectedExecution, []byte, error) {
	var q struct {
		ID string `json:"operation_id"`
	}
	_ = json.Unmarshal(data, &q)
	entry, exists, err := o.plugin.daemon.journal.read(q.ID)
	if err != nil || !exists || len(entry.Result) != 0 {
		o.t.Fatal("ordinary effects preceded durable reservation", err)
	}
	o.starts.Add(1)
	if o.executeHook != nil {
		return o.executeHook(ctx, kind, data)
	}
	return o.execution, selectedSessionBytes(o.execution, q.ID), nil
}
func (o *selectedOrdinaryFixture) current(ctx context.Context, e *selectedExecution) error {
	if ctx.Err() != nil || o.deny.Load() || e != o.execution {
		return errHostOwnerUnavailable
	}
	return e.manager.owner.checkCurrentHost(e.manager.owner.host)
}
func selectedSessionBytes(e *selectedExecution, id string) []byte {
	b, _ := json.Marshal(map[string]any{"protocol": selectedManagerProtocol, "ok": true, "session": e.session, "operation_id": id, "manager": "session-host", "runtime": "codex_cli", "native_id": "thread-1", "readable": true, "state": "idle", "labels": map[string]string{}, "generation": e.generation, "started_at": time.Now().UTC().Format("2006-01-02T15:04:05Z"), "last_activity_at": nil, "doorbell": map[string]any{"state": "off", "uncertain": false, "generation": nil, "reason": "not_requested"}})
	return b
}

type selectedBrokerFixture struct {
	prepares        atomic.Int32
	bootstraps      atomic.Int32
	acks            atomic.Int32
	closes          atomic.Int32
	mutate          func(*ownedSelectedEnrollment)
	beforePrepare   func(context.Context)
	beforeBootstrap func(context.Context)
	completion      *bridgeCompletionFixture
	mu              sync.Mutex
}
type ownedSelectedEnrollment struct {
	bridgeEnrollmentFixture
	expected           NotificationBinding
	expectedAttachment string
	deny               bool
}

func (e ownedSelectedEnrollment) Recheck() error {
	if e.deny || e.b != e.expected || e.ref != e.expectedAttachment || e.ctx.Err() != nil || !time.Now().Before(e.expires) {
		return ErrNotificationUnresolved
	}
	return nil
}
func (b *selectedBrokerFixture) Prepare(ctx context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
	b.prepares.Add(1)
	if b.beforePrepare != nil {
		b.beforePrepare(ctx)
	}
	if o.Recheck(ctx) != nil {
		return nil, errMemberRecordUnavailable
	}
	return &bridgePreparedFixture{origin: o, bootstrap: func(ctx context.Context, h NotificationBootstrap) (NotificationCompletion, error) {
		b.bootstraps.Add(1)
		if b.beforeBootstrap != nil {
			b.beforeBootstrap(ctx)
		}
		cctx, cancel := context.WithCancel(ctx)
		completion := &bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: h.MethodEndpoint}
		b.mu.Lock()
		b.completion = completion
		b.mu.Unlock()
		binding := h.Binding
		binding.ConsumerGeneration = "owned-generation"
		binding.EnrollmentRef = "owned-enrollment"
		e := ownedSelectedEnrollment{bridgeEnrollmentFixture: bridgeEnrollmentFixture{binding, h.AttachmentReference, cctx, time.Now().Add(10 * time.Second)}, expected: binding, expectedAttachment: h.AttachmentReference}
		if b.mutate != nil {
			b.mutate(&e)
		}
		err := h.OnEnrolled(ctx, e)
		if err == nil {
			b.acks.Add(1)
		} // Installed ACK follows callback return.
		return completion, err
	}, close: func() error {
		b.closes.Add(1)
		b.mu.Lock()
		c := b.completion
		b.mu.Unlock()
		if c != nil {
			return c.Close()
		}
		return nil
	}}, nil
}
func selectedFixture(t *testing.T) (*registeredHostPlugin, *selectedOrdinaryFixture, *selectedBrokerFixture, map[string]any) {
	t.Helper()
	m, q, _ := managerBridgeFixture(t)
	m.admission = nil
	m.owner.host.mu.Lock()
	m.owner.host.state.Protocol.ObservedRelease = "0.167.0"
	m.owner.host.mu.Unlock()
	q["submitted_at"] = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	b := &selectedBrokerFixture{}
	m.broker = b
	o := &selectedOrdinaryFixture{t: t, execution: &selectedExecution{manager: m, session: "SES-fixture", generation: 1}, available: true}
	directory := filepath.Join(t.TempDir(), "operations")
	p, err := registerHostPlugin(context.Background(), time.Now().Add(20*time.Second), directory, o)
	if err != nil {
		t.Fatal(err)
	}
	o.plugin = p
	t.Cleanup(func() { closeSelected(t, p) })
	return p, o, b, q
}
func closeSelected(t *testing.T, p *registeredHostPlugin) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Error("finite selected-manager cleanup", err)
	}
}
func selectedCall(t *testing.T, p *registeredHostPlugin, kind string, q map[string]any) []byte {
	t.Helper()
	result, exit := p.Call(context.Background(), selectedManagerProtocol, kind, memberOperationBytes(t, q))
	if exit != 0 || schema.Validate("session.schema.json", result) != nil {
		t.Fatalf("session result/exit disagree: %s exit=%d", result, exit)
	}
	return result
}
func selectedNotification(t *testing.T, data []byte) notificationProjection {
	t.Helper()
	var response struct {
		Notification notificationProjection `json:"notification"`
	}
	if json.Unmarshal(data, &response) != nil {
		t.Fatal("invalid result")
	}
	return response.Notification
}
func selectedCode(t *testing.T, data []byte, exit int, want string) {
	t.Helper()
	var r struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(data, &r)
	if exit == 0 || schema.Validate("refusal.schema.json", data) != nil || r.Code != want {
		t.Fatalf("refusal %s exit=%d want %s", data, exit, want)
	}
}
func selectedResume(t *testing.T, launch map[string]any) map[string]any {
	t.Helper()
	data, _ := schema.Documents.ReadFile("vectors/resume.json")
	var rows []struct {
		Document map[string]any `json:"document"`
	}
	_ = json.Unmarshal(data, &rows)
	q := rows[0].Document
	q["operation_id"] = "44444444-4444-4444-8444-444444444444"
	q["submitted_at"] = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	q["profile"] = launch["profile"]
	q["notification"] = launch["notification"]
	q["scope"].(map[string]any)["cwd"] = launch["cwd"]
	q["selector"] = map[string]any{"kind": "provider_id", "id": "thread-1"}
	return q
}

func TestSelectedAcceptance1(t *testing.T) {
	t.Run("legacy-omission", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		delete(q, "notification")
		o.available = false
		data := selectedCall(t, p, "launch", q)
		if n := selectedNotification(t, data); n.State != "" || b.prepares.Load() != 0 || o.starts.Load() != 1 {
			t.Fatal("legacy omission activated notification")
		}
	})
	t.Run("launch-transport-and-same-execution-resume", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		dir, err := os.MkdirTemp("/tmp", "selected-transport-")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		socket := filepath.Join(dir, "manager.sock")
		listener, err := localipc.Listen(socket)
		if err != nil {
			t.Fatal(err)
		}
		served := make(chan error, 1)
		go func() { served <- p.serveSelectedListener(listener) }()
		var output bytes.Buffer
		exit := SelectedManagerRPC(context.Background(), selectedManagerProtocol, "launch", socket, bytes.NewReader(memberOperationBytes(t, q)), &output)
		if exit != 0 || !validSelectedResult(output.Bytes()) {
			t.Fatalf("actual plugin transport: %s %d", output.Bytes(), exit)
		}
		first := selectedNotification(t, output.Bytes())
		if first.State != "ready" || first.Binding == nil || first.Binding.ConnectionRef == q["notification"].(map[string]any)["member"].(map[string]any)["connection"] {
			t.Fatal("missing actual opaque ready binding", first)
		}
		resume := selectedResume(t, q)
		next := selectedNotification(t, selectedCall(t, p, "resume", resume))
		if next.AttachmentRef != first.AttachmentRef || next.Binding == nil || next.Binding.NativeThread != "thread-1" || b.prepares.Load() != 1 || b.bootstraps.Load() != 1 || b.acks.Load() != 1 || o.starts.Load() != 2 {
			t.Fatal("resume failed to reuse actual attachment", next, b.prepares.Load())
		}
		closeSelected(t, p)
		select {
		case err := <-served:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("plugin transport not joined")
		}
	})
	t.Run("distinct-execution-first-resume", func(t *testing.T) {
		p, _, b, q := selectedFixture(t)
		resume := selectedResume(t, q)
		projection := selectedNotification(t, selectedCall(t, p, "resume", resume))
		if projection.State != "ready" || projection.Binding == nil || projection.Binding.NativeThread != "thread-1" || b.prepares.Load() != 1 {
			t.Fatal("admitted resume did not own first claim", projection)
		}
	})
}

func TestSelectedAcceptance2(t *testing.T) {
	t.Run("ordinary-refusal-exits", func(t *testing.T) {
		for _, row := range []struct {
			code string
			exit int
		}{{"session_resume_invalid", 2}, {"credential_injection_unavailable", 2}, {"session_host_provider_unsupported", 6}, {"session_host_capability_missing", 6}, {"policy_refused", 16}, {"session_generation_mismatch", 1}} {
			data, exit := ordinaryRefusal(ordinaryManagerRefusal{code: row.code})
			selectedCode(t, data, exit, row.code)
			if exit != row.exit {
				t.Fatal("published refusal exit changed", row.code, exit)
			}
		}
	})
	t.Run("decoded-private-admission-forgery", func(t *testing.T) {
		_, o, b, q := selectedFixture(t)
		m := o.execution.manager
		reads := 0
		read := m.read
		m.read = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
			reads++
			return read(ctx, dir, env, args...)
		}
		m.admission = &selectedOperationAdmission{}
		if a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q)); a != nil || err == nil || reads != 0 || b.prepares.Load() != 0 {
			t.Fatal("forged private admission caused earlier effects")
		}
		recordNoAttachment(t, m.owner)
	})
	t.Run("capability-before-ordinary-effects", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		o.available = false
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "session_host_capability_missing")
		if o.starts.Load() != 0 || b.prepares.Load() != 0 {
			t.Fatal("missing capability caused effects")
		}
	})
	t.Run("ordinary-profile-refusal", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		o.admitHook = func(context.Context, string, []byte) error { return ordinaryManagerRefusal{code: "policy_refused"} }
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "policy_refused")
		if exit != 16 || o.starts.Load() != 0 || b.prepares.Load() != 0 {
			t.Fatal("ordinary refusal lost semantics")
		}
	})
	t.Run("unsupported-runtime", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		q["runtime"] = "claude_code"
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "launch_plan_invalid")
		if exit != 2 || o.starts.Load() != 0 || b.prepares.Load() != 0 {
			t.Fatal("unsupported runtime had effects")
		}
	})
	for _, plant := range []string{"missing-admission", "foreign-host", "epoch", "runner", "generation", "registration"} {
		t.Run(plant, func(t *testing.T) {
			p, o, b, q := selectedFixture(t)
			_ = selectedCall(t, p, "launch", q)
			a := o.execution.claim
			m := o.execution.manager
			h := m.owner.host
			data := memberOperationBytes(t, q)
			if a == nil || a.checkManagerOperation(context.Background(), h, "launch", data) != nil {
				t.Fatal("valid nearby admission missing")
			}
			switch plant {
			case "missing-admission":
				m.admission = nil
			case "foreign-host":
				h = &Host{}
			case "epoch":
				h.mu.Lock()
				h.state.Epoch++
				h.mu.Unlock()
			case "runner":
				h.mu.Lock()
				h.state.RunnerInstance = "replacement"
				h.mu.Unlock()
			case "generation":
				o.execution.generation++
			case "registration":
				a.plugin = &registeredHostPlugin{daemon: p.daemon}
			}
			if plant == "missing-admission" {
				if got, err := m.Prepare(context.Background(), "launch", data); got != nil || err == nil {
					t.Fatal("missing admission accepted")
				}
			} else if a.checkManagerOperation(context.Background(), h, "launch", data) == nil {
				t.Fatal("forged/stale admission accepted")
			}
			if b.prepares.Load() != 1 {
				t.Fatal("refusal reprepared provider")
			}
		})
	}
	// These reused candidate checks cover retained profile/workflow/record and
	// physical workspace drift before broker preparation and pair allocation.
	TestManagerBridgePreAttachmentRefusal(t)
	TestMemberOperationRefusals(t)
	TestMemberRecordCurrentOrigin(t)
}

func TestSelectedAcceptance3(t *testing.T) {
	t.Run("failed-admission-replay", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		o.admitHook = func(context.Context, string, []byte) error {
			return ordinaryManagerRefusal{code: "session_generation_mismatch"}
		}
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "session_generation_mismatch")
		o.admitHook = nil
		data, exit = p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "session_generation_mismatch")
		status, exit := p.Call(context.Background(), selectedManagerProtocol, "status", []byte(`{"operation_id":"11111111-1111-4111-8111-111111111111"}`))
		if exit != 0 || !bytes.Contains(status, []byte(`"failed"`)) || o.starts.Load() != 0 || b.prepares.Load() != 0 {
			t.Fatal("failed admission was forgotten", string(status))
		}
	})
	t.Run("pending-wire-conflict", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		b.beforePrepare = func(context.Context) { close(entered); <-release }
		done := make(chan struct{})
		original := memberOperationBytes(t, q)
		go func() { p.Call(context.Background(), selectedManagerProtocol, "launch", original); close(done) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("pending provider not entered")
		}
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", original)
		selectedCode(t, data, exit, "session_liveness_unknown")
		q["detach"] = !q["detach"].(bool)
		data, exit = p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		close(release)
		selectedCode(t, data, exit, "session_operation_conflict")
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("pending operation not joined")
		}
		if o.starts.Load() != 1 || b.prepares.Load() != 1 || b.bootstraps.Load() != 1 {
			t.Fatal("pending request repeated effects")
		}
	})
	p, o, b, q := selectedFixture(t)
	original := memberOperationBytes(t, q)
	first := selectedNotification(t, selectedCall(t, p, "launch", q))
	for _, retry := range [][]byte{original, append([]byte(" \n"), original...)} {
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", retry)
		if exit != 0 || selectedNotification(t, data).AttachmentRef != first.AttachmentRef {
			t.Fatal("equivalent wire replay failed")
		}
	}
	if o.starts.Load() != 1 || b.prepares.Load() != 1 || b.bootstraps.Load() != 1 {
		t.Fatal("wire replay repeated effects")
	}
	if err := o.execution.claim.checkManagerOperation(context.Background(), o.execution.manager.owner.host, "launch", append(original, ' ')); err == nil {
		t.Fatal("private byte substitution admitted")
	}
	if err := o.execution.claim.checkManagerOperation(context.Background(), o.execution.manager.owner.host, "resume", original); err == nil {
		t.Fatal("private kind substitution admitted")
	}
	q["detach"] = !q["detach"].(bool)
	data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	selectedCode(t, data, exit, "session_operation_conflict")
	_ = o.execution.attachment.Close()
	data, exit = p.Call(context.Background(), selectedManagerProtocol, "launch", original)
	if exit != 0 || selectedNotification(t, data).State != "unresolved" || b.prepares.Load() != 1 {
		t.Fatal("terminal replay revived attachment")
	}
	TestManagerOperationPendingAndRefused(t)
}

func TestSelectedAcceptance4(t *testing.T) {
	for field := 0; field < reflect.TypeOf(NotificationBinding{}).NumField(); field++ {
		name := reflect.TypeOf(NotificationBinding{}).Field(field).Name
		t.Run(name, func(t *testing.T) {
			p, o, b, q := selectedFixture(t)
			b.mutate = func(e *ownedSelectedEnrollment) {
				v := reflect.ValueOf(&e.b).Elem().Field(field)
				switch name {
				case "WorkflowSHA256":
					v.SetString(strings.Repeat("e", 64))
				case "ControllerEpoch":
					v.SetString("2")
				default:
					v.SetString("other-valid-reference")
				}
			}
			projection := selectedNotification(t, selectedCall(t, p, "launch", q))
			if projection.State != "unresolved" || projection.Binding != nil || b.acks.Load() != 0 || o.execution.manager.owner.host.Snapshot().Notification != nil {
				t.Fatal("invalid independently checked enrollment installed service", projection)
			}
		})
	}
	for _, mode := range []string{"attachment", "lease", "live-check"} {
		t.Run(mode, func(t *testing.T) {
			p, _, b, q := selectedFixture(t)
			b.mutate = func(e *ownedSelectedEnrollment) {
				switch mode {
				case "attachment":
					e.ref = "other-attachment"
				case "lease":
					e.expires = time.Now().Add(-time.Second)
				case "live-check":
					e.deny = true
				}
			}
			projection := selectedNotification(t, selectedCall(t, p, "launch", q))
			if projection.State != "unresolved" || b.acks.Load() != 0 {
				t.Fatal("bad live enrollment admitted", projection)
			}
		})
	}
	TestManagerBridgeCompletionRefusals(t)
	TestManagerBridgeLifecycle(t)
	TestManagerBridgeOriginLoss(t)
	TestManagerBridgeOpaqueScope(t)
	TestManagedNotificationEnrollmentGates(t)
}

func TestSelectedAcceptance5(t *testing.T) {
	t.Run("lost-reply-status-reconciliation", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		input := bytes.NewReader(memberOperationBytes(t, q))
		if exit := p.ServeRequest(context.Background(), selectedManagerProtocol, "launch", input, selectedLostWriter{}); exit == 0 {
			t.Fatal("lost transport reply reported success")
		}
		request := []byte(`{"operation_id":"11111111-1111-4111-8111-111111111111"}`)
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "status", request)
		if exit != 0 || !bytes.Contains(data, []byte(`"started"`)) || !bytes.Contains(data, []byte(`"ready"`)) || o.starts.Load() != 1 || b.prepares.Load() != 1 {
			t.Fatal("lost reply erased actual outcome", string(data))
		}
	})
	t.Run("lost-snapshot-never-becomes-new", func(t *testing.T) {
		p, o, _, q := selectedFixture(t)
		_ = selectedCall(t, p, "launch", q)
		directory := p.daemon.journal.store.dir
		closeSelected(t, p)
		if err := os.Remove(filepath.Join(directory, q["operation_id"].(string)+".json")); err != nil {
			t.Fatal(err)
		}
		restarted, err := registerHostPlugin(context.Background(), time.Now().Add(10*time.Second), directory, o)
		if err != nil {
			t.Fatal(err)
		}
		defer closeSelected(t, restarted)
		data, exit := restarted.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "session_host_unavailable")
		if o.starts.Load() != 1 {
			t.Fatal("missing snapshot resubmitted")
		}
	})
	t.Run("delayed-native-identity", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		h := o.execution.manager.owner.host
		h.mu.Lock()
		h.state.Protocol.View.NativeSession = ""
		h.mu.Unlock()
		first := selectedNotification(t, selectedCall(t, p, "launch", q))
		if first.State != "preparing" || first.Binding != nil || b.bootstraps.Load() != 0 {
			t.Fatal("invented delayed identity", first)
		}
		h.mu.Lock()
		h.state.Protocol.View.NativeSession = "thread-1"
		h.mu.Unlock()
		deadline := time.Now().Add(2 * time.Second)
		for {
			data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
			if exit != 0 {
				t.Fatal("replay failed")
			}
			if selectedNotification(t, data).State == "ready" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("bounded bootstrap made no progress")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if b.prepares.Load() != 1 || b.bootstraps.Load() != 1 {
			t.Fatal("delayed bootstrap repeated provider effects")
		}
	})
	t.Run("pre-allocation-failure-preserves-start", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		b.beforePrepare = func(context.Context) { o.deny.Store(true) }
		data := selectedCall(t, p, "launch", q)
		if n := selectedNotification(t, data); n.AttachmentRef != "" || n.State != "" {
			t.Fatal("invented missing-attachment reference", n)
		}
		if o.starts.Load() != 1 || b.prepares.Load() != 1 {
			t.Fatal("actual start was lost")
		}
	})
	t.Run("unknown-start-and-restart", func(t *testing.T) {
		p, o, _, q := selectedFixture(t)
		directory := p.daemon.journal.store.dir
		o.executeHook = func(context.Context, string, []byte) (*selectedExecution, []byte, error) {
			return nil, nil, errors.New("inert lost start reply")
		}
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "session_liveness_unknown")
		closeSelected(t, p)
		restarted, err := registerHostPlugin(context.Background(), time.Now().Add(10*time.Second), directory, o)
		if err != nil {
			t.Fatal(err)
		}
		defer closeSelected(t, restarted)
		o.plugin = restarted
		data, exit = restarted.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "session_liveness_unknown")
		if o.starts.Load() != 1 {
			t.Fatal("unknown start resubmitted")
		}
	})
	t.Run("started-result-restart", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		directory := p.daemon.journal.store.dir
		first := selectedNotification(t, selectedCall(t, p, "launch", q))
		closeSelected(t, p)
		restarted, err := registerHostPlugin(context.Background(), time.Now().Add(10*time.Second), directory, o)
		if err != nil {
			t.Fatal(err)
		}
		defer closeSelected(t, restarted)
		o.plugin = restarted
		next := selectedNotification(t, selectedCall(t, restarted, "launch", q))
		if next.State != "unresolved" || next.AttachmentRef != first.AttachmentRef || o.starts.Load() != 1 || b.prepares.Load() != 1 {
			t.Fatal("restart revived or repeated effects", next)
		}
	})
	TestManagerBridgeCompletionEndsService(t)
}

func TestSelectedAcceptance6(t *testing.T) {
	t.Run("noncooperating-provider-remains-owned", func(t *testing.T) {
		p, _, b, q := selectedFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		b.beforePrepare = func(context.Context) { close(entered); <-release }
		done := make(chan struct{})
		go func() {
			p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
			close(done)
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("noncooperating model not entered")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := p.Close(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			close(release)
			t.Fatal("noncooperating adapter did not expose cleanup failure", err)
		}
		select {
		case <-p.daemon.done:
			close(release)
			t.Fatal("cleanup abandoned noncooperating provider")
		default:
		}
		t.Log("negative adapter: finite cleanup failed with deadline exceeded; owned callback remains tracked until released")
		close(release)
		closeSelected(t, p)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("released adapter was not joined")
		}
	})
	t.Run("operation-status-and-cancel-tombstone", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		request := []byte(`{"operation_id":"11111111-1111-4111-8111-111111111111"}`)
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "status", request)
		if exit != 0 || schema.Validate("outcome.schema.json", data) != nil || !bytes.Contains(data, []byte(`"not_found"`)) {
			t.Fatalf("initial status: %s %d", data, exit)
		}
		data, exit = p.Call(context.Background(), selectedManagerProtocol, "cancel", request)
		if exit != 0 || !bytes.Contains(data, []byte(`"cancelled"`)) {
			t.Fatalf("tombstone: %s %d", data, exit)
		}
		data, exit = p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		selectedCode(t, data, exit, "session_host_unavailable")
		if o.starts.Load() != 0 || b.prepares.Load() != 0 {
			t.Fatal("cancelled late delivery caused effects")
		}
	})
	t.Run("cancel-prepared-admission", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		o.admitHook = func(context.Context, string, []byte) error { close(entered); <-release; return nil }
		done := make(chan struct{})
		go func() {
			p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
			close(done)
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("ordinary admission not entered")
		}
		request := []byte(`{"operation_id":"11111111-1111-4111-8111-111111111111"}`)
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "cancel", request)
		close(release)
		if exit != 0 || !bytes.Contains(data, []byte(`"cancelled"`)) {
			t.Fatal("prepared cancel failed", string(data))
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled admission not joined")
		}
		if o.starts.Load() != 0 || b.prepares.Load() != 0 {
			t.Fatal("prepared cancellation failed to fence")
		}
	})
	t.Run("started-status-preserves-attachment", func(t *testing.T) {
		p, _, b, q := selectedFixture(t)
		first := selectedNotification(t, selectedCall(t, p, "launch", q))
		request := []byte(`{"operation_id":"11111111-1111-4111-8111-111111111111"}`)
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "status", request)
		if exit != 0 || schema.Validate("outcome.schema.json", data) != nil || !bytes.Contains(data, []byte(first.AttachmentRef)) {
			t.Fatal("started outcome lost actual evidence", string(data))
		}
		data, exit = p.Call(context.Background(), selectedManagerProtocol, "cancel", request)
		if exit != 0 || !bytes.Contains(data, []byte(`"started"`)) || b.completion.ctx.Err() != nil {
			t.Fatal("cancel stopped started execution")
		}
	})
	t.Run("caller-cancellation-does-not-own-custody", func(t *testing.T) {
		p, o, b, q := selectedFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		data, exit := p.Call(ctx, selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		cancel()
		if exit != 0 || selectedNotification(t, data).State != "ready" {
			t.Fatal("valid control missing")
		}
		if _, err := o.execution.manager.Snapshot(ctx); err == nil {
			t.Fatal("canceled status accepted")
		}
		if o.execution.manager.owner.checkCurrentHost(o.execution.manager.owner.host) != nil || b.completion.ctx.Err() != nil {
			t.Fatal("caller canceled owner")
		}
	})
	t.Run("close-during-prepare", func(t *testing.T) {
		p, _, b, q := selectedFixture(t)
		entered := make(chan struct{})
		b.beforePrepare = func(ctx context.Context) { close(entered); <-ctx.Done() }
		result := make(chan struct{})
		go func() {
			p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
			close(result)
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("prepare not entered")
		}
		closeSelected(t, p)
		select {
		case <-result:
		case <-time.After(5 * time.Second):
			t.Fatal("owned preparation not joined")
		}
	})
	t.Run("custody-expiry", func(t *testing.T) {
		p, o, _, q := selectedFixture(t)
		_ = selectedCall(t, p, "launch", q)
		p.daemon.cancel()
		select {
		case <-p.daemon.done:
		case <-time.After(5 * time.Second):
			t.Fatal("expiry cleanup did not join")
		}
		if o.execution.manager.owner.ctx.Err() == nil {
			t.Fatal("expired daemon retained owner")
		}
	})
	TestManagerBridgeCloseBootstrap(t)
	TestManagedNotificationExpiryAndConcurrency(t)
	TestContinuingOwnerTerminal(t)
	t.Log(fmt.Sprint("all cleanup assertions bounded by five seconds; no native or provider qualification"))
}

type selectedLostWriter struct{}

func (selectedLostWriter) Write([]byte) (int, error) { return 0, errors.New("inert lost reply") }
