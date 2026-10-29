//go:build unix

package sessionmanager

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func protocolProbe(t *testing.T) (*SessionHost, *Attachment, *attachmentTerminalProbe, http.Handler) {
	t.Helper()
	h, _, _, q, _ := attachmentProbe(t)
	q.Owner = "parent/primary"
	a, e := h.Attach(context.Background(), q)
	if e != nil {
		t.Fatal(e)
	}
	native := &attachmentTerminalProbe{RunnerHost: h.runners[a.Session.SessionID], stream: &attachmentStream{}}
	h.runners[a.Session.SessionID] = native
	handler, e := NewProtocol8Handler(h, func() ControlStatus { return ControlStatus{Running: true, ProtocolVersion: 8} }, nil)
	if e != nil {
		t.Fatal(e)
	}
	return h, a, native, handler
}
func protocolCall(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w
}

// Real host records become the original safe wire projection. Provider handles,
// record bytes and endpoint credentials have no public session counterpart.
func TestProtocol8SessionWireAndRoutes(t *testing.T) {
	_, a, _, handler := protocolProbe(t)
	for _, path := range []string{"/v1/sessions", "/v1/sessions/" + a.Session.SessionID} {
		w := protocolCall(handler, "GET", path, "")
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body)
		}
		var body map[string]json.RawMessage
		if e := json.Unmarshal(w.Body.Bytes(), &body); e != nil {
			t.Fatal(e)
		}
		raw := w.Body.String()
		for _, forbidden := range []string{"ProviderHost", "provider_host", "Snapshot", "codex_home", "ClientHome"} {
			if strings.Contains(raw, forbidden) {
				t.Fatalf("private field %s leaked", forbidden)
			}
		}
		if !strings.Contains(raw, `"owner":{"kind":"parent","id":"primary"}`) || !strings.Contains(raw, `"session_id"`) {
			t.Fatalf("legacy session wire lost: %s", raw)
		}
	}
	for _, path := range []string{"/unknown", "/v1/sessions/" + a.Session.SessionID + "/unknown"} {
		if w := protocolCall(handler, "GET", path, ""); w.Code != 404 {
			t.Fatalf("unknown route accepted: %d", w.Code)
		}
	}
}

// Strict legacy JSON and exact SID/attachment membership are enforced before
// touching terminal transport; nearby valid resize and detach reach real host.
func TestProtocol8AttachmentRefusalsAndDetach(t *testing.T) {
	h, a, native, handler := protocolProbe(t)
	base := "/v1/sessions/" + a.Session.SessionID
	for _, body := range []string{`{"attachment_id":"foreign","rows":24,"cols":80}`, `{"attachment_id":"` + a.AttachmentID + `","rows":24,"cols":80,"unknown":true}`} {
		w := protocolCall(handler, "POST", base+"/resize", body)
		if w.Code != 409 && w.Code != 400 {
			t.Fatalf("resize refusal: %d %s", w.Code, w.Body)
		}
	}
	if native.resizes != 0 {
		t.Fatal("refused resize touched native transport")
	}
	valid := `{"attachment_id":"` + a.AttachmentID + `","rows":24,"cols":80}`
	if w := protocolCall(handler, "POST", base+"/resize", valid); w.Code != 200 || native.resizes != 1 || native.rows != 24 || native.cols != 80 {
		t.Fatalf("valid resize: %d %s", w.Code, w.Body)
	}
	if w := protocolCall(handler, "POST", base+"/detach", `{"attachment_id":"foreign"}`); w.Code != 409 {
		t.Fatalf("foreign detach: %d", w.Code)
	}
	if got, _ := h.Session(context.Background(), a.Session.SessionID); got.AttachedClients != 1 {
		t.Fatal("foreign detach changed membership")
	}
	if w := protocolCall(handler, "POST", base+"/detach", `{"attachment_id":"`+a.AttachmentID+`"}`); w.Code != 200 {
		t.Fatalf("valid detach: %d %s", w.Code, w.Body)
	}
	if w := protocolCall(handler, "POST", base+"/resize", valid); w.Code != 409 || native.resizes != 1 {
		t.Fatal("detached attachment reused")
	}
}

// Missing owned services never turn a legacy goal/owner/run/attach request into
// authority or a native effect. This bound is explicit, not full route support.
func TestProtocol8MissingServicesRefuseBeforeEffect(t *testing.T) {
	_, a, native, handler := protocolProbe(t)
	paths := []string{"/v1/sessions/attach", "/v1/owners/reconcile", "/v1/owners/activation/begin", "/v1/owners/activation/end", "/v1/goals/apply", "/v1/goals/rollback", "/v1/doctor", "/v1/sessions/" + a.Session.SessionID + "/run", "/v1/sessions/" + a.Session.SessionID + "/takeover"}
	for _, path := range paths {
		payload := `{"owner":{"kind":"parent","id":"primary"}}`
		if strings.HasSuffix(path, "/run") {
			payload = `{"attachment_id":"` + a.AttachmentID + `","prompt":"hello"}`
		}
		w := protocolCall(handler, "POST", path, payload)
		if w.Code != 409 || !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatalf("missing route supplier %s: %d %s", path, w.Code, w.Body)
		}
	}
	if native.opens != 0 || native.resizes != 0 {
		t.Fatal("unsupported service reached native transport")
	}
}

type protocolHijack struct{ conn net.Conn }

func (w protocolHijack) Header() http.Header         { return http.Header{} }
func (w protocolHijack) Write(p []byte) (int, error) { return w.conn.Write(p) }
func (w protocolHijack) WriteHeader(int)             {}
func (w protocolHijack) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

// The actual legacy terminal handshake and geometry precede streaming. The
// fixture is an in-memory transport; it does not qualify native terminal I/O.
func TestProtocol8TerminalHandshakeAndCleanup(t *testing.T) {
	_, a, native, handler := protocolProbe(t)
	server, client := net.Pipe()
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		handler.ServeHTTP(protocolHijack{server}, httptest.NewRequest("POST", "/v1/sessions/"+a.Session.SessionID+"/terminal", strings.NewReader(`{"attachment_id":"`+a.AttachmentID+`","rows":25,"cols":90}`)))
	}()
	raw, e := io.ReadAll(client)
	if e != nil {
		t.Fatal(e)
	}
	<-joined
	if !strings.HasSuffix(string(raw), terminalStreamPreamble) || !strings.HasPrefix(string(raw), "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream") || native.opens != 1 || native.resizes != 1 || !native.stream.closed {
		t.Fatalf("terminal contract lost: %q opens=%d resizes=%d closed=%v", raw, native.opens, native.resizes, native.stream.closed)
	}
}

// Joined persistence failures outrank public diagnostics; raw paths and native
// details remain private while an independently public refusal stays actionable.
func TestProtocol8ErrorsKeepPersistencePrivate(t *testing.T) {
	w := httptest.NewRecorder()
	protocol8Error(w, 409, "resize_failed", "provider terminal could not be resized", errors.Join(fmt.Errorf("%w: private-record-path", ErrLegacyCommitUncertain), &PublicError{Code: "joined", Message: "public joined"}))
	if strings.Contains(w.Body.String(), "private-record-path") || strings.Contains(w.Body.String(), "public joined") {
		t.Fatalf("persistence detail leaked: %s", w.Body)
	}
	w = httptest.NewRecorder()
	protocol8Error(w, 409, "resize_failed", "fixed", &PublicError{Code: "public_refusal", Message: "public message"})
	if !strings.Contains(w.Body.String(), "public_refusal") || !strings.Contains(w.Body.String(), "public message") {
		t.Fatalf("public refusal lost: %s", w.Body)
	}
}

// Ordinary stop reaches the actual durable host transaction, carries the original
// detail, and publishes the stopped projection only after the write is acknowledged.
func TestProtocol8StopUsesActualHostTransaction(t *testing.T) {
	h, a, native, handler := protocolProbe(t)
	id := a.Session.SessionID
	runner := &stopProbeRunner{hostCommitProbeRunner: native.RunnerHost.(*hostCommitProbeRunner)}
	h.runners[id] = runner
	w := protocolCall(handler, "POST", "/v1/sessions/"+id+"/stop", `{"detail":"operator reason"}`)
	if w.Code != 200 || !runner.stopped || !strings.Contains(w.Body.String(), `"state":"stopped"`) {
		t.Fatalf("actual stop: %d %s", w.Code, w.Body)
	}
	_, stored := stopProbeRead(t, h, id)
	if stored.State != SessionStopped || !strings.Contains(stored.LivenessDetail, "operator reason") {
		t.Fatalf("stop projection preceded durable detail: %+v", stored)
	}
}

// Owning service transports reuse the exact public legacy slot/session wire;
// data conversion refuses invalid/opaque foreign keys and grants no authority.
func TestProtocol8SharedWireProjection(t *testing.T) {
	for _, owner := range []Protocol8SessionOwner{{Kind: "parent", ID: "primary"}, {Kind: "run", ID: "RUN-valid-123"}} {
		key, e := DecodeProtocol8Owner(owner)
		if e != nil {
			t.Fatal(e)
		}
		back, e := EncodeProtocol8Owner(key)
		if e != nil || back.Kind != owner.Kind {
			t.Fatalf("original slot projection: %v", e)
		}
		v, e := ProjectProtocol8Session(SessionView{Owner: key, SessionID: "SES-fixture-projection-0001", BindingScope: "element", BindingElementID: "element", TargetElementID: "target"})
		if e != nil || v.BindingElementID != "element" || v.TargetElementID != "target" {
			t.Fatalf("safe binding projection: %v", e)
		}
	}
	for _, owner := range []Protocol8SessionOwner{{Kind: "parent", ID: "foreign"}, {Kind: "foreign", ID: "primary"}, {Kind: "run", ID: "invalid"}} {
		if _, e := DecodeProtocol8Owner(owner); e == nil {
			t.Fatalf("invalid original owner accepted: %+v", owner)
		}
	}
	if _, e := ProjectProtocol8Session(SessionView{Owner: "not-an-original-slot"}); e == nil {
		t.Fatal("opaque owner silently fabricated legacy authority")
	}
}
