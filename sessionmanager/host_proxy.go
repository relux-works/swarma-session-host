package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"

	"github.com/relux-works/swarma-session-host/sessionproxy"
)

type hostProviderProxy struct {
	proxy  *sessionproxy.Proxy
	target string
	lease  ControllerLease
	runner RunnerHost
}
type providerRelayGate interface{ attachmentRelayGate() sessionproxy.RelayGate }

// The runner's real gate remains shared with its RPC clients. Each physical
// relay write additionally holds this exact durable controller fence, never a
// whole WebSocket lifetime or native wait. Callbacks do not acquire host.mu.
type hostLeasedRelayGate struct {
	gate   sessionproxy.RelayGate
	epochs *controllerEpochJournal
	lease  ControllerLease
}
type hostLeasedRelayClient struct {
	sessionproxy.RelayClient
	epochs *controllerEpochJournal
	lease  ControllerLease
}

func (g hostLeasedRelayGate) Client() sessionproxy.RelayClient {
	client := g.gate.Client()
	if client == nil {
		return nil
	}
	return hostLeasedRelayClient{client, g.epochs, g.lease}
}
func (c hostLeasedRelayClient) Write(ctx context.Context, data []byte, text bool, send func() error) error {
	release, e := c.epochs.beginIO(ctx, c.lease)
	if e != nil {
		return e
	}
	defer release()
	return c.RelayClient.Write(ctx, data, text, send)
}
func sameProxyRunner(a, b RunnerHost) bool {
	return reflect.TypeOf(a) == reflect.TypeOf(b) && reflect.TypeOf(a) != nil && reflect.TypeOf(a).Comparable() && a == b
}

// Called under the existing host lock, before issuing any attachment ID. Proxy
// creation is lazy: a host without an attachment target creates no directory.
func (h *SessionHost) ensureProviderProxyLocked(ctx context.Context, id string) (string, error) {
	runner := h.runners[id]
	targetHost, ok := runner.(ProviderAttachmentTarget)
	if !ok {
		return "", nil
	}
	target, e := targetHost.AttachmentTarget(ctx)
	if e != nil {
		return "", fmt.Errorf("resolving provider attachment target: %w", e)
	}
	lease, ok := h.attachmentControllers[id]
	if !ok {
		return "", ErrStaleInputController
	}
	parsed, parseErr := url.Parse(strings.TrimSpace(target))
	if old := h.proxies[id]; old != nil {
		if parseErr == nil && parsed.Scheme == "ws" && parsed.Host == old.target && old.lease == lease && sameProxyRunner(old.runner, runner) {
			return old.proxy.Endpoint(), nil
		}
		if e = h.closeProviderProxyLocked(id); e != nil {
			return "", e
		}
	}
	var proxy *sessionproxy.Proxy
	if actual, ok := runner.(providerRelayGate); ok {
		gate := actual.attachmentRelayGate()
		if absentHostPort(gate) {
			return "", ErrHostCapabilityUnavailable
		}
		proxy, e = sessionproxy.NewGated(h.options.Layout.AttachmentsDir, id, target, hostLeasedRelayGate{gate, h.epochs, lease})
	} else {
		proxy, e = sessionproxy.New(h.options.Layout.AttachmentsDir, id, target)
	}
	if e != nil {
		return "", fmt.Errorf("opening provider attachment proxy: %w", e)
	}
	if h.proxies == nil {
		h.proxies = map[string]*hostProviderProxy{}
	}
	h.proxies[id] = &hostProviderProxy{proxy: proxy, target: parsed.Host, lease: lease, runner: runner}
	return proxy.Endpoint(), nil
}
func (h *SessionHost) closeProviderProxyLocked(id string) error {
	var e error
	if owned := h.proxies[id]; owned != nil {
		e = owned.proxy.Close()
		delete(h.proxies, id)
	}
	// Proxy.Close alone owns inode-checked unlink; never remove a foreign or
	// replaced path merely because its filename matches the session ID.
	h.pruneEmptyProxyRootLocked()
	return e
}
func (h *SessionHost) pruneEmptyProxyRootLocked() {
	root := h.options.Layout.AttachmentsDir
	if strings.TrimSpace(root) == "" || len(h.proxies) > 0 {
		return
	}
	entries, e := os.ReadDir(root)
	if e != nil || len(entries) > 0 {
		return
	}
	_ = os.Remove(root)
}
func (h *SessionHost) closeProviderProxiesLocked() error {
	var result error
	for id := range h.proxies {
		result = errors.Join(result, h.closeProviderProxyLocked(id))
	}
	return result
}
func (h *SessionHost) attachmentDetailsLocked(ctx context.Context, id string) (*Attachment, error) {
	endpoint, e := h.ensureProviderProxyLocked(ctx, id)
	if e != nil {
		return nil, e
	}
	out := &Attachment{ManagerEndpoint: endpoint}
	runner := h.runners[id]
	if home, ok := runner.(ProviderClientHome); ok {
		out.ClientHome = strings.TrimSpace(home.ClientHome())
	}
	if h.sessions[id].Provider == "codex" {
		if native, ok := runner.(ProviderNativeIdentity); ok {
			out.CodexThreadID = native.NativeIdentity()
		}
	}
	if policy, ok := runner.(ProviderClientToolPolicy); ok {
		out.ClientToolPolicy = policy.ClientToolPolicy()
	}
	return out, nil
}
