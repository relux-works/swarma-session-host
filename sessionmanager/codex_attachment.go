package sessionmanager

import (
	"context"
	"errors"
	"fmt"

	"github.com/relux-works/swarma-session-host/sessionproxy"
)

// Original runner projections share its actual snapshot and turn-admission
// gate. Neither the proxy nor a caller supplies native identity/client policy.
func (r *codexRunner) ClientHome() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshot.CodexHome
}
func (r *codexRunner) ClientToolPolicy() ClientToolPolicy {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshot.ToolPolicy
}
func (r *codexRunner) NativeIdentity() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshot.ThreadID
}
func (r *codexRunner) AttachmentTarget(ctx context.Context) (string, error) {
	state, e := r.State(ctx)
	if e != nil {
		return "", e
	}
	if state != ProviderHostRunning {
		return "", fmt.Errorf("Codex provider host is %s", state)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshot.URL, nil
}
func (r *codexRunner) attachmentRelayGate() sessionproxy.RelayGate {
	if r.gate == nil {
		return nil
	}
	return codexAttachmentGate{r.gate}
}

type codexAttachmentGate struct{ gate *codexTurnGate }

func (g codexAttachmentGate) Client() sessionproxy.RelayClient {
	return codexAttachmentClient{g.gate.client()}
}

type codexAttachmentClient struct{ client *codexGateClient }

func (c codexAttachmentClient) Observe(data []byte) { c.client.observe(data) }
func (c codexAttachmentClient) Unknown() {
	g := c.client.gate
	g.mu.Lock()
	g.state = ProviderActivityUnknown
	g.mu.Unlock()
}
func (c codexAttachmentClient) Lost() { c.client.lost() }
func (c codexAttachmentClient) Write(ctx context.Context, data []byte, text bool, send func() error) error {
	e := c.client.write(ctx, data, text, send)
	if errors.Is(e, errCodexTurnFenced) {
		return errors.Join(e, sessionproxy.ErrTurnFenced)
	}
	return e
}
