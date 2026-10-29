//go:build unix

package sessiongoal

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// These are explicitly trusted in-process reservation/projection fixtures. They
// exercise real Service + receipt binding, not OS owner lookup or bridge grants.
type protocolRuntimeFixture struct {
	session      core.SessionView
	binding      core.GoalBinding
	begins, ends int
	err          error
	gate         core.GoalGate
}

func (p *protocolRuntimeFixture) WithGoalSession(ctx context.Context, owner core.OwnerKey, fn func(Protocol8GoalSession) error) error {
	if p.err != nil {
		return p.err
	}
	if owner != p.session.Owner {
		return ErrBindingUnavailable
	}
	// This original transport fixture is supplied a real concrete receipt
	// binding by its caller. Resolve that same service for the new operation
	// callback; no production scoped binding is unwrapped or replaced here.
	receipt, ok := p.binding.(*receiptBinding)
	if !ok || receipt == nil || receipt.service == nil {
		return ErrBindingUnavailable
	}
	return fn(Protocol8GoalSession{Session: p.session, Gate: p.gate, Live: true,
		Reconcile: func() (core.SessionView, error) {
			_, e := receipt.service.Reconcile(Owner(p.session.SessionID, Assignment), p.binding)
			return p.session, e
		},
		Rollback: func(env *core.GoalEnvelope) error {
			if p.binding == nil {
				return ErrBindingUnavailable
			}
			return p.binding.Rollback(env)
		},
		BeginActivation: func() (string, bool, error) { return p.BeginOwnerActivation(ctx, owner) },
		EndActivation:   func(id string) error { return p.EndOwnerActivation(ctx, owner, id) },
	})
}
func (p *protocolRuntimeFixture) BeginOwnerActivation(context.Context, core.OwnerKey) (string, bool, error) {
	p.begins++
	return "activation", true, nil
}
func (p *protocolRuntimeFixture) EndOwnerActivation(context.Context, core.OwnerKey, string) error {
	p.ends++
	return nil
}

type projectionFixture struct {
	raw json.RawMessage
	err error
}

func (p *projectionFixture) VerifyLegacyRevision(_ context.Context, raw json.RawMessage, revision Revision) error {
	p.raw = append([]byte(nil), raw...)
	if p.err != nil {
		return p.err
	}
	var fields struct {
		ID        string `json:"goal_id"`
		Revision  int    `json:"revision"`
		Objective string `json:"objective"`
	}
	if e := json.Unmarshal(raw, &fields); e != nil {
		return e
	}
	if fields.ID != revision.Ref.ID || fields.Revision != revision.Ref.Revision || fields.Objective != revision.Content.Objective {
		return ErrRevisionConflict
	}
	return nil
}
func protocolFixture(t *testing.T) (*Service, *authorityFixture, string, *writerFixture, *protocolRuntimeFixture, *projectionFixture, http.Handler, string) {
	t.Helper()
	s, a, root := fixture(t)
	r, e := s.Handle(caller("launcher"), command("http-source", 0))
	if e != nil {
		t.Fatal(e)
	}
	w := newWriter()
	binding, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	runtime := &protocolRuntimeFixture{session: core.SessionView{SessionID: "session-a", Owner: core.OwnerKey("parent/primary"), Provider: "codex"}, binding: binding}
	projection := &projectionFixture{}
	h, e := NewProtocol8Handler(s, runtime, projection)
	if e != nil {
		t.Fatal(e)
	}
	raw := `{"goal_id":"` + r.Ref.ID + `","revision":1,"owner":{"kind":"parent","id":"primary"},"objective":"Preserve the session","provider_condition":"holder accepts the outcome","reason":"retain all opaque fields","budget":{"tokens":100}}`
	return s, a, root, w, runtime, projection, h, raw
}
func protocolCall(h http.Handler, path, body, principal string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", path, strings.NewReader(body)).WithContext(caller(principal))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// Published route/status/reply fields are retained while apply invokes the real
// stored goal binding. Requester PID is carried but never grants admission or
// fabricates deferred acknowledgement; all legacy revision bytes reach proof.
func TestGoalProtocol8StoredApplyAndOwnerRoutes(t *testing.T) {
	_, _, _, writer, runtime, projection, h, raw := protocolFixture(t)
	w := protocolCall(h, "/v1/goals/apply", `{"revision":`+raw+`,"requester":{"pid":999999}}`, "launcher")
	if w.Code != 200 || w.Body.String() != "{\"applied\":true,\"outcome\":\"acknowledged\"}\n" || writer.submits != 1 || string(projection.raw) != raw {
		t.Fatalf("actual apply/wire lost: %d %s submits=%d raw=%s", w.Code, w.Body, writer.submits, projection.raw)
	}
	for _, route := range []string{"reconcile", "activation/begin", "activation/end"} {
		w = protocolCall(h, "/v1/owners/"+route, `{"owner":{"kind":"parent","id":"primary"}`+map[string]string{"activation/end": `,"lease_id":"activation"`}[route]+`}`, "launcher")
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", route, w.Code, w.Body)
		}
		if route == "reconcile" && (!strings.Contains(w.Body.String(), `"reconciled":true`) || !strings.Contains(w.Body.String(), `"session_id":"session-a"`)) {
			t.Fatal(w.Body)
		}
	}
	if runtime.begins != 1 || runtime.ends != 1 {
		t.Fatal("activation calls missing")
	}
}

// Missing authority/runtime/projection, stale reference/proof, wrong session,
// agent/old holder, and working-namespace refs cannot write journal or input.
// The nearby entitled apply above prevents a reject-all implementation passing.
func TestGoalProtocol8RefusalsHaveNoEffects(t *testing.T) {
	for _, name := range []string{"session_agent", "old_holder", "wrong_session", "missing_authority", "missing_runtime", "missing_projection", "stale_ref", "projection_refusal", "wrong_namespace"} {
		t.Run(name, func(t *testing.T) {
			s, a, root, writer, runtime, projection, h, raw := protocolFixture(t)
			principal := "launcher"
			switch name {
			case "session_agent":
				principal = "agent"
			case "old_holder":
				a.facts.Holder = "new-holder"
				a.facts.Generation = 2
			case "wrong_session":
				a.facts.SessionID = "other"
			case "missing_authority":
				s.authority = nil
			case "missing_runtime":
				h, _ = NewProtocol8Handler(s, nil, projection)
			case "missing_projection":
				h, _ = NewProtocol8Handler(s, runtime, nil)
			case "stale_ref":
				raw = strings.Replace(raw, `"revision":1`, `"revision":2`, 1)
			case "projection_refusal":
				projection.err = ErrForbidden
			case "wrong_namespace":
				raw = strings.Replace(raw, referenceID("session-a", Assignment), referenceID("session-a", Working), 1)
			}
			before := bytesAt(root)
			w := protocolCall(h, "/v1/goals/apply", `{"revision":`+raw+`,"requester":{"pid":1}}`, principal)
			if w.Code != 409 || !strings.Contains(w.Body.String(), `"code":"goal_apply_failed"`) || bytesAt(root) != before || writer.submits != 0 || runtime.begins != 0 || runtime.ends != 0 {
				t.Fatalf("refused effect: %d %s inputs=%d", w.Code, w.Body, writer.submits)
			}
		})
	}
}

// The original bounded decoder rejects unknown outer fields and oversized or
// malformed values, and retains its existing single-value/trailing-value rule.
func TestGoalProtocol8DecoderAndPrivateErrorBoundary(t *testing.T) {
	_, _, root, writer, runtime, _, h, raw := protocolFixture(t)
	before := bytesAt(root)
	for _, body := range []string{`{"revision":` + raw + `,"unknown":true}`, `{"revision":`, `{"revision":{"objective":"` + strings.Repeat("x", 1<<20) + `"}}`, `{"revision":` + raw + `,"requester":{"pid":"bad"}}`, `{"revision":{"goal_id":12,"revision":1,"owner":{"kind":"parent","id":"primary"}}}`} {
		w := protocolCall(h, "/v1/goals/apply", body, "launcher")
		if w.Code != 400 || !strings.Contains(w.Body.String(), `"code":"invalid_request"`) || bytesAt(root) != before || writer.submits != 0 {
			t.Fatalf("decoder refusal: %d %s", w.Code, w.Body)
		}
	}
	runtime.err = errors.Join(&core.PublicError{Code: "public", Message: "public message"}, ErrJournalInvalid, errors.New("private fixture path"))
	w := protocolCall(h, "/v1/goals/apply", `{"revision":`+raw+`}`, "launcher")
	if w.Code != 409 || strings.Contains(w.Body.String(), "private") || strings.Contains(w.Body.String(), "public message") || !strings.Contains(w.Body.String(), `"code":"goal_apply_failed"`) {
		t.Fatal(w.Body)
	}
	runtime.err = errors.Join(&core.PublicError{Code: "public", Message: "public message"}, &fs.PathError{Op: "read", Path: "private fixture path", Err: errors.New("isolated storage refusal")})
	w = protocolCall(h, "/v1/goals/apply", `{"revision":`+raw+`}`, "launcher")
	if w.Code != 409 || strings.Contains(w.Body.String(), "private") || strings.Contains(w.Body.String(), "public message") {
		t.Fatal(w.Body)
	}
	runtime.err = nil
	w = protocolCall(h, "/v1/goals/apply", `{"revision":`+raw+`} {"trailing":true}`, "launcher")
	if w.Code != 200 {
		t.Fatalf("published decoder behavior changed: %d %s", w.Code, w.Body)
	}
}

// Actual receipt-checked rollback restores a stored historical revision, or
// clears only the native plane. Neither route accepts/completes/deletes intent.
func TestGoalProtocol8RollbackRetainsStoredIntentAndReceiptErrors(t *testing.T) {
	s, _, _, writer, _, _, h, raw := protocolFixture(t)
	w := protocolCall(h, "/v1/goals/apply", `{"revision":`+raw+`}`, "launcher")
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	next := command("second-http", 1)
	next.Content.Objective = "new objective"
	r, e := s.Handle(caller("launcher"), next)
	if e != nil {
		t.Fatal(e)
	}
	w = protocolCall(h, "/v1/goals/rollback", `{"owner":{"kind":"parent","id":"primary"},"previous":`+raw+`}`, "launcher")
	head, e := s.Snapshot("session-a", Assignment)
	if w.Code != 200 || w.Body.String() != "{\"rolled_back\":true}\n" || e != nil || head.Latest.Ref != r.Ref || head.Applied == nil || head.Applied.Revision != 1 || head.Latest.State != Active || len(head.History) != 2 {
		t.Fatalf("rollback lost history: %d %s %+v %v", w.Code, w.Body, head, e)
	}
	w = protocolCall(h, "/v1/goals/rollback", `{"owner":{"kind":"parent","id":"primary"}}`, "launcher")
	head, e = s.Snapshot("session-a", Assignment)
	if w.Code != 200 || e != nil || head.Applied != nil || head.Latest.Ref != r.Ref || head.Latest.State != Active || len(head.History) != 2 {
		t.Fatalf("native clear changed intent: %s %+v %v", w.Body, head, e)
	}
	writer.awaitErr = errors.New("isolated acknowledgement unavailable")
	secondRaw := strings.ReplaceAll(strings.Replace(raw, `"revision":1`, `"revision":2`, 1), "Preserve the session", "new objective")
	w = protocolCall(h, "/v1/goals/apply", `{"revision":`+secondRaw+`}`, "launcher")
	head, e = s.Snapshot("session-a", Assignment)
	if w.Code != 409 || e != nil || head.Applied != nil || head.Latest.State != Active || head.Attempt == nil {
		t.Fatalf("receipt error forged ACK: %d %s %+v %v", w.Code, w.Body, head, e)
	}
}

// Activation cannot precede exact-session admission. An absent runtime and
// unauthorized holder cannot acquire/release reservations or write native input.
func TestGoalProtocol8ActivationRefusesBeforeEffects(t *testing.T) {
	for _, route := range []string{"begin", "end"} {
		t.Run(route, func(t *testing.T) {
			_, _, root, writer, runtime, _, h, _ := protocolFixture(t)
			before := bytesAt(root)
			w := protocolCall(h, "/v1/owners/activation/"+route, `{"owner":{"kind":"parent","id":"primary"},"lease_id":"activation"}`, "agent")
			// Begin has no lease_id in the original request; its outer decoder rejects it.
			if route == "begin" {
				w = protocolCall(h, "/v1/owners/activation/begin", `{"owner":{"kind":"parent","id":"primary"}}`, "agent")
			}
			if w.Code != 409 || runtime.begins != 0 || runtime.ends != 0 || writer.submits != 0 || bytesAt(root) != before {
				t.Fatalf("unauthorized activation effect: %d %s", w.Code, w.Body)
			}
		})
	}
}

// The handler invokes the actual core GoalGate port before reconciliation/input;
// disabling or a gate read error leaves durable intent and native input intact.
type protocolGateFixture struct {
	reason string
	err    error
	owner  core.OwnerKey
}

func (g *protocolGateFixture) DisabledGoalPlane(owner core.OwnerKey) (string, error) {
	g.owner = owner
	return g.reason, g.err
}
func TestGoalProtocol8ConfiguredGateRefusesBeforeInput(t *testing.T) {
	for _, name := range []string{"disabled", "read_error"} {
		t.Run(name, func(t *testing.T) {
			_, _, root, writer, runtime, _, h, raw := protocolFixture(t)
			gate := &protocolGateFixture{reason: "disabled"}
			if name == "read_error" {
				gate.reason = ""
				gate.err = errors.New("isolated gate read failure")
			}
			runtime.gate = gate
			before := bytesAt(root)
			w := protocolCall(h, "/v1/goals/apply", `{"revision":`+raw+`}`, "launcher")
			if w.Code != 409 || writer.submits != 0 || bytesAt(root) != before || gate.owner != runtime.session.Owner {
				t.Fatalf("gate bypass: %d %s inputs=%d", w.Code, w.Body, writer.submits)
			}
		})
	}
}

// Preserved original server assertion: an explicitly public upstream validator
// refusal retains its code/message. Canonical role validation is not implemented
// here; the private projection supplies its public error through this boundary.
func TestControlErrorPreservesGoalRoleIncompatible(t *testing.T) {
	recorder := httptest.NewRecorder()
	goalProtocol8Error(recorder, "goal_apply_failed", "provider goal could not be applied", &core.PublicError{Code: "goal_role_incompatible", Message: "parent/primary requires alignment/alignment_handled"})
	body := recorder.Body.String()
	for _, text := range []string{"goal_role_incompatible", "parent/primary requires alignment/alignment_handled"} {
		if !strings.Contains(body, text) {
			t.Errorf("daemon response %q does not contain %q", body, text)
		}
	}
}
