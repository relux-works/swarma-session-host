package sessionproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const relayReadLimit = 64 << 20
const relayHeaderTimeout = 10 * time.Second

var ErrPathOccupied = errors.New("attachment proxy path is occupied")
var ErrPathChanged = errors.New("attachment proxy path changed")
var ErrPrivatePath = errors.New("attachment proxy requires a private directory and safe session ID")

// Proxy owns the original session-scoped attachment transport, not an
// attachment. Detach does not close it; confirmed session Stop does.
type Proxy struct {
	gate        RelayGate
	context     context.Context
	cancel      context.CancelFunc
	server      *http.Server
	listener    net.Listener
	path        string
	target      string
	file        os.FileInfo
	once        sync.Once
	closed      chan struct{}
	serveDone   chan struct{}
	closeErr    error
	mu          sync.Mutex
	closing     bool
	connections map[net.Conn]struct{}
	wg          sync.WaitGroup
}

// SocketPathFor is the original providerProxySocketPathFor spelling. New checks
// input before using it; callers use only the runner's canonical session ID.
func SocketPathFor(root, sessionID string) string { return filepath.Join(root, sessionID+".sock") }
func EndpointFor(root, sessionID string) string {
	return (&url.URL{Scheme: "unix", Path: SocketPathFor(root, sessionID)}).String()
}

// New carries the original ungated byte-stream transport, for runners that do
// not require turn admission. A Codex runner must use NewGated with its real gate.
func New(root, sessionID, target string) (*Proxy, error) {
	return newProxy(root, sessionID, target, nil)
}
func NewGated(root, sessionID, target string, gate RelayGate) (*Proxy, error) {
	if gate == nil {
		return nil, errors.New("provider relay admission gate is unavailable")
	}
	return newProxy(root, sessionID, target, gate)
}

func newProxy(root, sessionID, target string, gate RelayGate) (*Proxy, error) {
	parsed, err := url.Parse(strings.TrimSpace(target))
	if err != nil || parsed.Scheme != "ws" || parsed.Host == "" {
		return nil, errors.New("provider attachment target must be a loopback ws URL")
	}
	host, _, err := net.SplitHostPort(parsed.Host)
	if err != nil || (host != "127.0.0.1" && host != "::1" && host != "localhost") {
		return nil, errors.New("provider attachment target must be loopback-only")
	}
	if !filepath.IsAbs(root) || sessionID == "" || sessionID == "." || sessionID == ".." || filepath.Base(sessionID) != sessionID || strings.ContainsAny(sessionID, "/\\\x00") {
		return nil, ErrPrivatePath
	}
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(root, 0700); err != nil {
			return nil, ErrPrivatePath
		}
		info, err = os.Lstat(root)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return nil, ErrPrivatePath
	}
	path := SocketPathFor(root, sessionID)
	if _, err = os.Lstat(path); err == nil {
		return nil, ErrPathOccupied
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, ErrPrivatePath
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, errors.New("attachment proxy listener unavailable")
	}
	if unix, ok := listener.(*net.UnixListener); ok {
		unix.SetUnlinkOnClose(false)
	}
	file, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return nil, ErrPrivatePath
	}
	if err = os.Chmod(path, 0600); err != nil {
		_ = listener.Close()
		if current, e := os.Lstat(path); e == nil && os.SameFile(file, current) {
			_ = os.Remove(path)
		}
		return nil, ErrPrivatePath
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{gate: gate, context: ctx, cancel: cancel, listener: listener, path: path, target: parsed.Host, file: file, closed: make(chan struct{}), serveDone: make(chan struct{}), connections: map[net.Conn]struct{}{}}
	if gate != nil {
		p.server = &http.Server{Handler: http.HandlerFunc(p.relayUpgrade), ReadHeaderTimeout: relayHeaderTimeout}
		go func() { defer close(p.serveDone); _ = p.server.Serve(listener) }()
	} else {
		go func() { defer close(p.serveDone); p.serve() }()
	}
	return p, nil
}

func (p *Proxy) Endpoint() string {
	if p == nil {
		return ""
	}
	return (&url.URL{Scheme: "unix", Path: p.path}).String()
}

// Close cancels/joins only this proxy's streams and removes only its own socket
// inode. An occupied replacement is retained, never unlinked as stale.
func (p *Proxy) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		defer close(p.closed)
		p.mu.Lock()
		p.closing = true
		p.mu.Unlock()
		p.cancel()
		err := p.listener.Close()
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
		if p.server != nil {
			_ = p.server.Close()
		}
		<-p.serveDone
		p.mu.Lock()
		for c := range p.connections {
			_ = c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
		current, e := os.Lstat(p.path)
		switch {
		case errors.Is(e, os.ErrNotExist):
		case e != nil:
			err = errors.Join(err, ErrPrivatePath)
		case !os.SameFile(p.file, current):
			err = errors.Join(err, ErrPathChanged)
		default:
			err = errors.Join(err, os.Remove(p.path))
		}
		p.closeErr = err
	})
	<-p.closed
	return p.closeErr
}
func (p *Proxy) beginRelay() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return false
	}
	p.wg.Add(1)
	return true
}
func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return false
	}
	p.connections[c] = struct{}{}
	return true
}
func (p *Proxy) untrack(c net.Conn) { p.mu.Lock(); delete(p.connections, c); p.mu.Unlock() }
func (p *Proxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		if !p.beginRelay() {
			_ = client.Close()
			continue
		}
		go func() { defer p.wg.Done(); p.forward(client) }()
	}
}
func (p *Proxy) forward(client net.Conn) {
	defer client.Close()
	if !p.track(client) {
		return
	}
	defer p.untrack(client)
	provider, err := (&net.Dialer{}).DialContext(p.context, "tcp", p.target)
	if err != nil {
		return
	}
	defer provider.Close()
	if !p.track(provider) {
		return
	}
	defer p.untrack(provider)
	done := make(chan struct{}, 2)
	copyStream := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if tcp, ok := dst.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		done <- struct{}{}
	}
	go copyStream(provider, client)
	go copyStream(client, provider)
	<-done
	_ = provider.Close()
	_ = client.Close()
	<-done
}
