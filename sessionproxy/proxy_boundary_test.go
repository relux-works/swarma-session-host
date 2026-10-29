package sessionproxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Invalid targets/paths must not alter a private fixture; occupied/replaced
// leaves survive. Valid loopback/Unix transport is paired in the original test.
func TestProxyRefusesForeignPathsAndRetainsReplacedSocket(t *testing.T) {
	root := privateProxyRoot(t)
	for _, target := range []string{"ws://example.invalid:80", "http://127.0.0.1:9", "ws://127.0.0.1", "ws://%"} {
		if p, e := New(root, "SES-test", target); p != nil || e == nil {
			t.Fatalf("invalid target admitted: %v %v", p, e)
		}
	}
	for _, id := range []string{"", "..", "../outside", "a/b", "a\\b"} {
		if p, e := New(root, id, "ws://127.0.0.1:9"); p != nil || !errors.Is(e, ErrPrivatePath) {
			t.Fatalf("unsafe ID admitted: %v %v", p, e)
		}
	}
	path := SocketPathFor(root, "SES-test")
	if e := os.WriteFile(path, []byte("foreign"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := New(root, "SES-test", "ws://127.0.0.1:9"); !errors.Is(e, ErrPathOccupied) {
		t.Fatalf("occupied path: %v", e)
	}
	b, _ := os.ReadFile(path)
	if string(b) != "foreign" {
		t.Fatal("foreign file changed")
	}
	if e := os.Remove(path); e != nil {
		t.Fatal(e)
	} // Explicitly owned control fixture.
	p, e := New(root, "SES-test", "ws://127.0.0.1:9")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(path); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, []byte("replacement"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = p.Close(); !errors.Is(e, ErrPathChanged) {
		t.Fatalf("changed socket not refused: %v", e)
	}
	b, _ = os.ReadFile(path)
	if string(b) != "replacement" {
		t.Fatal("replacement path removed")
	}
	if _, e = NewGated(root, "SES-other", "ws://127.0.0.1:9", nil); e == nil {
		t.Fatal("missing gate admitted")
	}
	link := filepath.Join(root, "link")
	if e = os.Symlink(root, link); e != nil {
		t.Fatal(e)
	}
	if _, e = New(link, "SES-other", "ws://127.0.0.1:9"); !errors.Is(e, ErrPrivatePath) {
		t.Fatalf("symlink root admitted: %v", e)
	}
}

// The fixture gate deliberately exercises only relay dispatch: it encloses the
// real upstream write and refuses it when fenced. It does not replace or claim
// coverage of the runner's production admission/classification state machine.
type relayFixtureGate struct {
	mu                              sync.Mutex
	fenced                          bool
	writes, observed, unknown, lost int
}

func (g *relayFixtureGate) Client() RelayClient { return g }
func (g *relayFixtureGate) Observe([]byte)      { g.mu.Lock(); g.observed++; g.mu.Unlock() }
func (g *relayFixtureGate) Unknown()            { g.mu.Lock(); g.unknown++; g.mu.Unlock() }
func (g *relayFixtureGate) Lost()               { g.mu.Lock(); g.lost++; g.mu.Unlock() }
func (g *relayFixtureGate) Write(ctx context.Context, _ []byte, _ bool, send func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := ctx.Err(); e != nil {
		return e
	}
	if g.fenced {
		return ErrTurnFenced
	}
	g.writes++
	return send()
}

// Real WebSocket hops preserve headers/path/query/subprotocol, text/binary
// bytes and JSON-RPC refusal IDs; a fenced frame never reaches the upstream.
func TestProxyRelayPreservesWireAndFencedWriteHasNoUpstreamEffect(t *testing.T) {
	headers := make(chan *http.Request, 1)
	received := make(chan []byte, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Clone(context.Background())
		c, e := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true, Subprotocols: []string{"fixture"}})
		if e != nil {
			return
		}
		defer c.CloseNow()
		for {
			kind, b, e := c.Read(r.Context())
			if e != nil {
				return
			}
			received <- b
			if c.Write(r.Context(), kind, b) != nil {
				return
			}
		}
	}))
	defer server.Close()
	gate := &relayFixtureGate{}
	p, e := NewGated(privateProxyRoot(t), "SES-relay", "ws"+strings.TrimPrefix(server.URL, "http"), gate)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", strings.TrimPrefix(p.Endpoint(), "unix://"))
	}}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, e := websocket.Dial(ctx, "ws://fixture/actual/path?mode=fixture", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, HTTPHeader: http.Header{"Origin": []string{"http://fixture"}, "Authorization": []string{"Bearer fixture-token"}}, Subprotocols: []string{"fixture"}})
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	h := <-headers
	if h.URL.Path != "/actual/path" || h.URL.RawQuery != "mode=fixture" || h.Header.Get("Authorization") != "Bearer fixture-token" || h.Header.Get("Origin") != "http://fixture" || c.Subprotocol() != "fixture" {
		t.Fatalf("relay changed negotiation: %s", h.URL)
	}
	for _, kind := range []websocket.MessageType{websocket.MessageText, websocket.MessageBinary} {
		data := []byte("exact-frame")
		if e = c.Write(ctx, kind, data); e != nil {
			t.Fatal(e)
		}
		gotKind, b, e := c.Read(ctx)
		if e != nil || gotKind != kind || string(b) != string(data) {
			t.Fatalf("frame: %d %s %v", gotKind, b, e)
		}
		if string(<-received) != string(data) {
			t.Fatal("upstream changed bytes")
		}
	}
	gate.mu.Lock()
	gate.fenced = true
	gate.mu.Unlock()
	if e = c.Write(ctx, websocket.MessageText, []byte(`{"jsonrpc":"2.0","id":"exact-id","method":"turn/start","params":{}}`)); e != nil {
		t.Fatal(e)
	}
	_, b, e := c.Read(ctx)
	if e != nil {
		t.Fatal(e)
	}
	var reply struct {
		ID    string
		Error struct {
			Code    int
			Message string
		}
	}
	if json.Unmarshal(b, &reply) != nil || reply.ID != "exact-id" || reply.Error.Code != -32001 || reply.Error.Message != "session is fenced against new turns" {
		t.Fatalf("refusal bytes: %s", b)
	}
	gate.mu.Lock()
	writes, unknown := gate.writes, gate.unknown
	gate.mu.Unlock()
	if writes != 2 || unknown != 1 {
		t.Fatalf("wrong gate dispatch: writes%d unknown%d", writes, unknown)
	}
	select {
	case <-received:
		t.Fatal("fenced frame forwarded")
	default:
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	gate.mu.Lock()
	lost := gate.lost
	gate.mu.Unlock()
	if lost != 1 {
		t.Fatalf("relay not joined/lost: %d", lost)
	}
}

// Close terminates already accepted byte-stream readers, so owned transports
// cannot outlive the session proxy even when neither peer sends another byte.
func TestProxyCloseJoinsIdleOwnedStreams(t *testing.T) {
	provider, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer provider.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, e := provider.Accept()
		if e == nil {
			accepted <- c
		}
	}()
	p, e := New(privateProxyRoot(t), "SES-idle", "ws://"+provider.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	c, e := net.Dial("unix", strings.TrimPrefix(p.Endpoint(), "unix://"))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	var upstream net.Conn
	select {
	case upstream = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("provider not connected")
	}
	defer upstream.Close()
	done := make(chan error, 1)
	go func() { done <- p.Close() }()
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy Close did not join")
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, e = c.Read(make([]byte, 1))
	if e == nil {
		t.Fatal("client stream survived")
	}
	_ = upstream.SetReadDeadline(time.Now().Add(time.Second))
	_, e = upstream.Read(make([]byte, 1))
	if e == nil || !errors.Is(e, io.EOF) {
		t.Fatalf("provider stream survived: %v", e)
	}
}

// Fixture sockets use an owned short0700 root, as required by native Unix
// sockaddr_un limits; no product path or permission requirement is relaxed.
func privateProxyRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "sh1-proxy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}
