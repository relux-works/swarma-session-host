package sessionproxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Bound each attached write so an unresponsive provider cannot hold admission
// indefinitely. The control operation has its own context-bounded lock wait.
const relayWriteTimeout = 5 * time.Second

// relayUpgrade terminates each WebSocket hop independently. Codex 0.162.1
// reads Origin (rejects it on TCP) and Authorization (websocket-auth). Forward
// those verbatim, plus the path/query and requested subprotocols. The library
// owns Upgrade, Connection, Sec-WebSocket-Key/Version/Extensions on each hop.
func (p *Proxy) relayUpgrade(w http.ResponseWriter, r *http.Request) {
	if !p.beginRelay() {
		http.Error(w, "provider proxy closed", http.StatusServiceUnavailable)
		return
	}
	defer p.wg.Done()
	ctx, cancel := context.WithCancel(p.context)
	defer cancel()
	upstreamURL := url.URL{Scheme: "ws", Host: p.target, Path: r.URL.Path, RawPath: r.URL.RawPath, RawQuery: r.URL.RawQuery}
	headers := http.Header{}
	for _, name := range []string{"Origin", "Authorization"} {
		for _, value := range r.Header.Values(name) {
			headers.Add(name, value)
		}
	}
	upstream, response, err := websocket.Dial(ctx, upstreamURL.String(), &websocket.DialOptions{
		HTTPHeader: headers, Subprotocols: relaySubprotocols(r), CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		status := http.StatusBadGateway
		if response != nil && response.StatusCode >= 400 && response.StatusCode <= 599 {
			status = response.StatusCode
		}
		http.Error(w, "provider upgrade refused", status)
		return
	}
	defer upstream.CloseNow()
	protocols := []string(nil)
	if sub := upstream.Subprotocol(); sub != "" {
		protocols = []string{sub}
	}
	downstream, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: protocols, InsecureSkipVerify: true, CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		return
	}
	defer downstream.CloseNow()
	downstream.SetReadLimit(relayReadLimit)
	upstream.SetReadLimit(relayReadLimit)
	client := p.gate.Client()
	if client == nil {
		return
	}
	defer client.Lost()
	// One serialized application writer for relayed data and injected refusals.
	// Ping/pong and framing/compression are managed independently by each hop.
	var writer sync.Mutex
	writeClient := func(kind websocket.MessageType, b []byte) error {
		writer.Lock()
		defer writer.Unlock()
		return downstream.Write(ctx, kind, b)
	}
	type ended struct {
		source *websocket.Conn
		target *websocket.Conn
		err    error
	}
	done := make(chan ended, 2)
	go func() {
		for {
			kind, b, err := upstream.Read(ctx)
			if err != nil {
				done <- ended{upstream, downstream, err}
				return
			}
			if kind == websocket.MessageText {
				client.Observe(b)
			} else {
				client.Unknown()
			}
			if err = writeClient(kind, b); err != nil {
				done <- ended{downstream, upstream, err}
				return
			}
		}
	}()
	go func() {
		for {
			kind, b, err := downstream.Read(ctx)
			if err != nil {
				done <- ended{downstream, upstream, err}
				return
			}
			writeCtx, writeCancel := context.WithTimeout(ctx, relayWriteTimeout)
			err = client.Write(writeCtx, b, kind == websocket.MessageText, func() error { return upstream.Write(writeCtx, kind, b) })
			writeCancel()
			if errors.Is(err, ErrTurnFenced) {
				var f struct{ ID json.RawMessage }
				_ = json.Unmarshal(b, &f)
				if !usableID(f.ID) {
					done <- ended{downstream, upstream, &relayPolicyClose{}}
					return
				}
				reply, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": f.ID, "error": map[string]any{"code": -32001, "message": "session is fenced against new turns"}})
				err = writeClient(websocket.MessageText, reply)
			}
			if err != nil {
				done <- ended{upstream, downstream, err}
				return
			}
		}
	}()
	first := <-done
	code, reason := relayClose(first.err)
	// Close emits the peer's code/reason on the other hop; abnormal I/O uses 1011.
	// CloseNow plus cancellation joins both readers even if a peer never replies.
	_ = first.target.Close(code, reason)
	_ = first.source.Close(code, reason)
	cancel()
	_ = upstream.CloseNow()
	_ = downstream.CloseNow()
	<-done
}

type relayPolicyClose struct{}

func (*relayPolicyClose) Error() string { return "fenced message has no usable JSON-RPC id" }
func relayClose(err error) (websocket.StatusCode, string) {
	var policy *relayPolicyClose
	if errors.As(err, &policy) {
		return websocket.StatusPolicyViolation, policy.Error()
	}
	var closeError websocket.CloseError
	if errors.As(err, &closeError) {
		if closeError.Code == websocket.StatusNoStatusRcvd {
			return websocket.StatusNormalClosure, ""
		}
		if closeError.Code >= 1000 && closeError.Code != websocket.StatusAbnormalClosure && closeError.Code != websocket.StatusTLSHandshake {
			return closeError.Code, closeError.Reason
		}
	}
	return websocket.StatusInternalError, "provider relay transport failed"
}

func relaySubprotocols(r *http.Request) []string {
	var out []string
	for _, value := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, s := range strings.Split(value, ",") {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

// usableID is the original codexUsableID predicate; gate classification stays
// with the runner, while the relay only renders its established refusal wire.
func usableID(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	var id any
	if json.Unmarshal(raw, &id) != nil {
		return false
	}
	switch id.(type) {
	case string, float64:
		return true
	}
	return false
}
