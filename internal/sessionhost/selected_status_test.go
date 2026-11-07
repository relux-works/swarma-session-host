package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// The older ordinary fixtures do not supply session observations. Tests of
// status below explicitly compose an owner with its own current record store.
func (o *selectedOrdinaryFixture) status(context.Context, string) ([]byte, error) {
	return nil, errHostOwnerUnavailable
}

type selectedStatusOwner struct {
	*selectedOrdinaryFixture
	mu           sync.Mutex
	records      map[string][]byte
	observations atomic.Int32
	hook         func(context.Context, string) ([]byte, error)
}

func (o *selectedStatusOwner) execute(ctx context.Context, kind string, data []byte) (*selectedExecution, []byte, error) {
	e, result, err := o.selectedOrdinaryFixture.execute(ctx, kind, data)
	if err == nil && e != nil {
		o.mu.Lock()
		o.records[e.session] = append([]byte(nil), result...)
		o.mu.Unlock()
	}
	return e, result, err
}
func (o *selectedStatusOwner) status(ctx context.Context, session string) ([]byte, error) {
	o.observations.Add(1)
	if o.hook != nil {
		return o.hook(ctx, session)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	result, ok := o.records[session]
	if !ok {
		return nil, ordinaryManagerRefusal{code: "session_host_unavailable", details: map[string]string{"reason": "not_found"}}
	}
	return append([]byte(nil), result...), nil
}
func selectedStatusFixture(t *testing.T) (*registeredHostPlugin, *selectedStatusOwner, *selectedBrokerFixture, map[string]any) {
	p, ordinary, broker, q := selectedFixture(t)
	o := &selectedStatusOwner{selectedOrdinaryFixture: ordinary, records: map[string][]byte{}}
	p.daemon.owner = o
	return p, o, broker, q
}
func selectedStatusRPC(t *testing.T, p *registeredHostPlugin) func(string, []byte) ([]byte, int) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "selected-status-")
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "manager.sock")
	listener, err := localipc.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- p.serveSelectedListener(listener) }()
	t.Cleanup(func() {
		closeSelected(t, p)
		select {
		case err := <-served:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("RPC cleanup exceeded bound")
		}
		os.RemoveAll(dir)
	})
	return func(kind string, input []byte) ([]byte, int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var output bytes.Buffer
		exit := SelectedManagerRPC(ctx, selectedManagerProtocol, kind, socket, bytes.NewReader(input), &output)
		return bytes.TrimSpace(output.Bytes()), exit
	}
}
func unreadableSelectedSession(session string) []byte {
	data, _ := json.Marshal(map[string]any{"protocol": selectedManagerProtocol, "ok": true, "session": session, "operation_id": nil, "manager": "session-host", "runtime": nil, "native_id": nil, "readable": false, "state": "unknown", "labels": map[string]string{"project": "fixture"}, "generation": nil, "started_at": nil, "last_activity_at": nil, "doorbell": map[string]any{"state": "off", "uncertain": false, "generation": nil, "reason": nil}})
	return data
}

func TestSelectedStatusForms(t *testing.T) {
	for _, transport := range []string{"direct", "rpc"} {
		t.Run(transport, func(t *testing.T) {
			p, o, b, q := selectedStatusFixture(t)
			delete(q, "notification")
			call := func(kind string, input []byte) ([]byte, int) {
				return p.Call(context.Background(), selectedManagerProtocol, kind, input)
			}
			if transport == "rpc" {
				call = selectedStatusRPC(t, p)
			}
			started, exit := call("launch", memberOperationBytes(t, q))
			if exit != 0 || !validSelectedResult(started) {
				t.Fatalf("start control: %s exit=%d", started, exit)
			}
			description, exit := call("describe", []byte(`{}`))
			if exit != 0 || !bytes.Contains(description, []byte(`"status/1"`)) {
				t.Fatal("status not advertised", string(description), exit)
			}
			operationRequest, _ := json.Marshal(map[string]any{"operation_id": q["operation_id"]})
			observed, exit := call("status", operationRequest)
			if exit != 0 || schema.Validate("outcome.schema.json", observed) != nil || !bytes.Contains(observed, []byte(`"outcome":"started"`)) {
				t.Fatalf("operation control: %s exit=%d", observed, exit)
			}
			// Current ordinary state has advanced independently of the launch snapshot.
			var current map[string]any
			_ = json.Unmarshal(started, &current)
			current["state"] = "busy"
			current["generation"] = 2
			current["last_activity_at"] = "2026-11-07T00:00:01Z"
			fresh, _ := json.Marshal(current)
			o.mu.Lock()
			o.records[o.execution.session] = fresh
			o.mu.Unlock()
			request := []byte(`{"session":"SES-fixture"}`)
			if err := schema.Validate("status-request.schema.json", request); err != nil {
				t.Fatal(err)
			}
			observed, exit = call("status", request)
			if exit != 0 || !bytes.Equal(observed, fresh) {
				t.Fatalf("current ordinary session observation: %s exit=%d want=%s", observed, exit, fresh)
			}
			observed, exit = call("status", operationRequest)
			if exit != 0 || !bytes.Contains(observed, []byte(`"generation":1`)) || !bytes.Contains(observed, []byte(`"outcome":"started"`)) {
				t.Fatalf("operation reconciliation changed: %s exit=%d", observed, exit)
			}
			// An unreadable indexed row is successful unknown evidence, never down.
			unreadable := unreadableSelectedSession("SES-fixture")
			o.mu.Lock()
			o.records[o.execution.session] = unreadable
			o.mu.Unlock()
			observed, exit = call("status", request)
			if exit != 0 || !bytes.Equal(observed, unreadable) || !validSelectedResult(observed) {
				t.Fatalf("unreadable: %s exit=%d", observed, exit)
			}
			observed, exit = call("status", []byte(`{"session":"SES-absent"}`))
			selectedCode(t, observed, exit, "session_host_unavailable")
			if exit != 1 || !bytes.Contains(observed, []byte(`"reason":"not_found"`)) {
				t.Fatal("missing session lost authoritative absence", string(observed), exit)
			}
			before := o.observations.Load()
			for _, malformed := range [][]byte{[]byte(`{}`), []byte(`{"session":"bad"}`), []byte(`{"session":"SES-fixture","operation_id":"11111111-1111-4111-8111-111111111111"}`), []byte(`{"session":"SES-fixture","extra":true}`), []byte(`{`)} {
				observed, exit = call("status", malformed)
				selectedCode(t, observed, exit, "launch_plan_invalid")
				if exit != 2 {
					t.Fatal("malformed request exit", exit)
				}
			}
			if o.observations.Load() != before || o.starts.Load() != 1 || o.admissions.Load() != 1 || b.prepares.Load() != 0 {
				t.Fatalf("observation effects calls=%d starts=%d admissions=%d prepares=%d", o.observations.Load(), o.starts.Load(), o.admissions.Load(), b.prepares.Load())
			}
			t.Logf("both selectors, current busy generation 2, historical operation generation 1, unreadable unknown, absent refusal and malformed controls; starts=%d preparations=%d", o.starts.Load(), b.prepares.Load())
		})
	}
}

func TestSelectedStatusOwnerFailures(t *testing.T) {
	for _, name := range []string{"error", "malformed", "wrong-session", "wrong-manager", "outcome", "refusal", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			p, o, b, _ := selectedStatusFixture(t)
			good := selectedSessionBytes(o.execution, "11111111-1111-4111-8111-111111111111")
			o.hook = func(ctx context.Context, session string) ([]byte, error) {
				switch name {
				case "error":
					return nil, errHostOwnerUnavailable
				case "malformed":
					return []byte(`{`), nil
				case "outcome":
					return []byte(`{"protocol":"curator-session-manager/1","ok":true,"outcome":"not_found"}`), nil
				case "refusal":
					return nil, ordinaryManagerRefusal{code: "session_host_unavailable", details: map[string]string{"reason": "index_unreadable"}}
				case "cancelled":
					<-ctx.Done()
					return good, nil
				case "wrong-session":
					return bytes.ReplaceAll(good, []byte("SES-fixture"), []byte("SES-other")), nil
				case "wrong-manager":
					return bytes.ReplaceAll(good, []byte(`"manager":"session-host"`), []byte(`"manager":"other"`)), nil
				}
				return good, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			if name == "cancelled" {
				time.AfterFunc(10*time.Millisecond, cancel)
			}
			defer cancel()
			result, exit := p.Call(ctx, selectedManagerProtocol, "status", []byte(`{"session":"SES-fixture"}`))
			selectedCode(t, result, exit, "session_host_unavailable")
			if exit != 1 || o.starts.Load() != 0 || o.admissions.Load() != 0 || b.prepares.Load() != 0 {
				t.Fatal("observation failure caused effects", string(result), exit)
			}
			if name == "refusal" && !bytes.Contains(result, []byte(`"reason":"index_unreadable"`)) {
				t.Fatal("authoritative refusal lost", string(result))
			}
			if p.daemon.ctx.Err() != nil {
				t.Fatal("observation ended daemon custody")
			}
		})
	}
}

func TestSelectedStatusReplyClassification(t *testing.T) {
	sessionRequest := []byte(`{"session":"SES-fixture"}`)
	operationRequest := []byte(`{"operation_id":"11111111-1111-4111-8111-111111111111"}`)
	session := unreadableSelectedSession("SES-fixture")
	outcome := []byte(`{"protocol":"curator-session-manager/1","ok":true,"outcome":"not_found"}`)
	refusal, _ := selectedRefusal("session_host_capability_missing", nil)
	for _, tc := range []struct {
		name              string
		request, response []byte
		exit              int
	}{
		{"session", sessionRequest, session, 0}, {"operation", operationRequest, outcome, 0},
		{"session-rejects-outcome", sessionRequest, outcome, 1}, {"operation-rejects-session", operationRequest, session, 1},
		{"wrong-session", sessionRequest, unreadableSelectedSession("SES-other"), 1},
		{"malformed", sessionRequest, []byte(`{`), 1}, {"session-refusal", sessionRequest, refusal, 6}, {"operation-refusal", operationRequest, refusal, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "selected-status-shape-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			socket := filepath.Join(dir, "manager.sock")
			listener, err := localipc.Listen(socket)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				conn, err := listener.AcceptUnix()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				_, err = io.ReadAll(conn)
				if err == nil {
					_, err = conn.Write(tc.response)
				}
				done <- err
			}()
			var output bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			exit := SelectedManagerRPC(ctx, selectedManagerProtocol, "status", socket, bytes.NewReader(tc.request), &output)
			_ = listener.Close()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("fixture server cleanup exceeded bound")
			}
			if exit != tc.exit {
				t.Fatalf("reply classification: %s exit=%d want=%d", output.Bytes(), exit, tc.exit)
			}
			if exit == 0 && !bytes.Equal(bytes.TrimSpace(output.Bytes()), tc.response) {
				t.Fatal("valid response modified")
			}
			if exit == 1 {
				selectedCode(t, bytes.TrimSpace(output.Bytes()), exit, "session_host_unavailable")
			}
			if exit == 6 {
				selectedCode(t, bytes.TrimSpace(output.Bytes()), exit, "session_host_capability_missing")
			}
		})
	}
}

func TestSelectedStatusIndependentOfJournal(t *testing.T) {
	p, o, b, q := selectedStatusFixture(t)
	started := selectedCall(t, p, "launch", q)
	// Even a schema-valid ordinary record cannot confer notification authority.
	if selectedNotification(t, started).State != "ready" {
		t.Fatal("notification control not ready")
	}
	o.mu.Lock()
	o.records[o.execution.session] = started
	o.mu.Unlock()
	status, exit := p.Call(context.Background(), selectedManagerProtocol, "status", []byte(`{"session":"SES-fixture"}`))
	if exit != 0 || !validSelectedResult(status) || selectedNotification(t, status).State != "" {
		t.Fatalf("ordinary status granted notification authority: %s exit=%d", status, exit)
	}
	// An unreadable operation ledger must not turn a readable ordinary session
	// into historical or missing evidence. The operation control still refuses.
	file := filepath.Join(p.daemon.journal.store.dir, q["operation_id"].(string)+".json")
	if err := os.WriteFile(file, []byte(`{`), 0600); err != nil {
		t.Fatal(err)
	}
	status, exit = p.Call(context.Background(), selectedManagerProtocol, "status", []byte(`{"session":"SES-fixture"}`))
	if exit != 0 || !validSelectedResult(status) {
		t.Fatalf("session depended on operation ledger: %s exit=%d", status, exit)
	}
	request, _ := json.Marshal(map[string]any{"operation_id": q["operation_id"]})
	operation, exit := p.Call(context.Background(), selectedManagerProtocol, "status", request)
	selectedCode(t, operation, exit, "session_host_unavailable")
	if o.starts.Load() != 1 || b.prepares.Load() != 1 {
		t.Fatal("status repeated effects")
	}
}

func TestSelectedStatusCleanup(t *testing.T) {
	p, o, b, _ := selectedStatusFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	o.hook = func(ctx context.Context, session string) ([]byte, error) {
		close(entered)
		<-ctx.Done()
		<-release // Deliberately retain the callback to test daemon join accounting.
		return unreadableSelectedSession(session), nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		data, exit := p.Call(context.Background(), selectedManagerProtocol, "status", []byte(`{"session":"SES-fixture"}`))
		if exit != 1 || !bytes.Contains(data, []byte(`"session_host_unavailable"`)) {
			t.Errorf("cancelled status: %s exit=%d", data, exit)
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("ordinary status not entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := p.Close(ctx)
	cancel()
	if err == nil {
		t.Error("cleanup abandoned the ordinary status callback")
	}
	select {
	case <-p.daemon.done:
		t.Error("daemon closed while ordinary observation was held")
	default:
	}
	unblock()
	closeSelected(t, p)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ordinary status did not finish")
	}
	if o.starts.Load() != 0 || o.admissions.Load() != 0 || b.prepares.Load() != 0 {
		t.Fatal("cleanup caused effects")
	}
}
