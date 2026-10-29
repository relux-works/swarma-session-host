//go:build unix

package sessionmanager_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	defaults "github.com/relux-works/swarma-session-host/hostdefaults"
	client "github.com/relux-works/swarma-session-host/sessionclient"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type legacyPhysicalTakeoverFixture struct {
	manager       *core.SessionHost
	client        *legacyPhysicalTakeoverClient
	goals         *goal.Service
	codex, claude *core.LegacyPhysicalTestFactory
	incumbent     *core.Attachment
	layout        core.Layout
}

// The goal supplier is a controlled exact-session authority fixture. The real
// journal enforces its own command/revision/pending semantics. It grants nothing
// in production and cannot authorize another session.
type legacyPhysicalGoalAuthority struct{ session string }

func (a *legacyPhysicalGoalAuthority) WithVerifiedCaller(ctx context.Context, sid string, fn func(goal.VerifiedCaller) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sid == "" || sid != a.session {
		return goal.ErrForbidden
	}
	return fn(goal.VerifiedCaller{SessionID: sid, Principal: "fixture-operator", Holder: "fixture-holder", Generation: 1, Operator: true, Assignment: true})
}

// This remains Client -> authenticated Unix HTTP -> the actual production
// handler. Missing composition is a fixture refusal/unqualified test, never
// replaced by a forwarding handler, direct client-to-host call or canned grant.
func legacyPhysicalNewTakeoverFixture(t *testing.T, name string) *legacyPhysicalTakeoverFixture {
	t.Helper()
	short := core.LegacyPhysicalTestLayout(t, name)
	layout, err := defaults.LayoutAt(t.TempDir(), short.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	layout.SocketPath, layout.AttachmentsDir = short.SocketPath, short.AttachmentsDir
	authority := &legacyPhysicalGoalAuthority{}
	goals, err := goal.Open(filepath.Join(t.TempDir(), "goal-journal"), authority)
	if err != nil {
		t.Fatal(err)
	}
	notices, err := core.NewSessionNoticeStore(layout)
	if err != nil {
		t.Fatal(err)
	}
	codex := core.LegacyPhysicalTestFactoryFor(t, "codex")
	claude := core.LegacyPhysicalTestFactoryFor(t, "claude")
	h, err := core.NewHost(core.HostOptions{Layout: layout, Config: core.LegacyPhysicalTestConfig{}, Codec: core.NewLegacyRecordCodec(), Notices: notices, Goals: goals, GoalBindings: goal.BindingFactory{Service: goals}, Factories: []core.ProviderHostFactory{core.LegacyPhysicalTestGoalFactory(codex), core.LegacyPhysicalTestGoalFactory(claude)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	if err := h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	source := legacyPhysicalNewAttachSource(t, h)
	attach, err := defaults.NewLegacyAttachHandler(h, source)
	if err != nil {
		t.Fatal(err)
	}
	server, err := legacyPhysicalHTTPServer(h, attach)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := core.LegacyPhysicalTestStartServer(t, server)
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("control server shutdown: %v", err)
		}
	})
	c, err := client.Dial(client.EndpointFiles{EndpointFile: layout.EndpointFile, TokenFile: layout.TokenFile, Namespace: string(layout.Namespace)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	wire := &legacyPhysicalTakeoverClient{Client: c, source: source}
	incumbent, err := wire.Attach(context.Background(), core.AttachRequest{Owner: "parent/primary", Provider: "codex"})
	if err != nil {
		t.Fatalf("original fixture requires real authenticated attach route: %v", err)
	}
	authority.session = incumbent.Session.SessionID
	return &legacyPhysicalTakeoverFixture{manager: h, client: wire, goals: goals, codex: codex, claude: claude, incumbent: incumbent, layout: layout}
}
func (f *legacyPhysicalTakeoverFixture) conflictingClaudeAttach(t *testing.T) core.SessionView {
	t.Helper()
	_, err := f.client.Attach(context.Background(), core.AttachRequest{Owner: "parent/primary", Provider: "claude"})
	var public *client.PublicError
	if !errors.As(err, &public) || public.Code != core.ErrorSessionProviderConflict {
		t.Fatalf("cross-provider attach = %v, want %s", err, core.ErrorSessionProviderConflict)
	}
	view, err := f.client.Session(context.Background(), f.incumbent.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	return view
}
func (f *legacyPhysicalTakeoverFixture) storedOpaque(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.layout.SessionsDir, f.incumbent.Session.SessionID, "record.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		ProviderHost struct {
			Opaque json.RawMessage `json:"opaque"`
		} `json:"provider_host"`
	}
	if err = json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), record.ProviderHost.Opaque...)
}
func (f *legacyPhysicalTakeoverFixture) ownerSlotHolder() string {
	// The neutral host derives the owner slot from committed active records;
	// an unreadable provider observation must not look like a released slot.
	return core.LegacyPhysicalTestOwnerSlot(f.manager, "parent/primary")
}

// These adapters only encode/decode the moved typed public wire. All route
// execution, authentication, errors and effects come from production handlers.
type legacyPhysicalTakeoverClient struct {
	*client.Client
	source *legacyPhysicalAttachSource
}

func legacyPhysicalDecodeView(w core.Protocol8Session) (core.SessionView, error) {
	owner, err := core.DecodeProtocol8Owner(w.Owner)
	if err != nil {
		return core.SessionView{}, err
	}
	return core.SessionView{SessionID: w.SessionID, Owner: owner, Provider: w.Provider, State: w.State, Liveness: w.Liveness, LivenessDetail: w.LivenessDetail, AttachedClients: w.AttachedClients, Eviction: w.Eviction, CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt}, nil
}
func (c *legacyPhysicalTakeoverClient) Session(ctx context.Context, id string) (core.SessionView, error) {
	var wire core.Protocol8Session
	if e := c.DoRequest(ctx, "GET", "/v1/sessions/"+id, nil, &wire, true); e != nil {
		return core.SessionView{}, e
	}
	return legacyPhysicalDecodeView(wire)
}
func (c *legacyPhysicalTakeoverClient) Stop(ctx context.Context, id string) (core.SessionView, error) {
	var wire core.Protocol8Session
	if e := c.DoRequest(ctx, "POST", "/v1/sessions/"+id+"/stop", struct{}{}, &wire, true); e != nil {
		return core.SessionView{}, e
	}
	return legacyPhysicalDecodeView(wire)
}
func (c *legacyPhysicalTakeoverClient) Attach(ctx context.Context, q core.AttachRequest) (*core.Attachment, error) {
	owner, e := core.EncodeProtocol8Owner(q.Owner)
	if e != nil {
		return nil, e
	}
	request := defaults.LegacyAttachRequest{
		Owner: owner, Provider: q.Provider, Launch: append(json.RawMessage(nil), c.source.launches[q.Provider]...),
		ProjectDir: q.ProjectDir, BoardFingerprint: q.NamespaceFingerprint,
		Resume: q.Resume, ResumeSessionID: q.ResumeSessionID, Adopt: q.Adopt, MaterializationCtxID: q.MaterializationCtxID,
	}
	if request.ProjectDir == "" {
		request.ProjectDir = c.source.project
	}
	if request.BoardFingerprint == "" {
		request.BoardFingerprint = c.source.namespace
	}
	var wire defaults.LegacyAttachment
	if e = c.DoRequest(ctx, "POST", "/v1/sessions/attach", request, &wire, true); e != nil {
		return nil, e
	}
	view, e := legacyPhysicalDecodeView(wire.Session)
	if e != nil {
		return nil, e
	}
	return &core.Attachment{AttachmentID: wire.AttachmentID, ManagerEndpoint: wire.ManagerEndpoint, ClientHome: wire.ClientHome, CodexThreadID: wire.CodexThreadID, ClientToolPolicy: wire.ClientToolPolicy, Session: view}, nil
}
func (c *legacyPhysicalTakeoverClient) TakeOverOwnerSlot(ctx context.Context, q core.TakeoverRequest) (core.SessionView, error) {
	owner, e := core.EncodeProtocol8Owner(q.Owner)
	if e != nil {
		return core.SessionView{}, e
	}
	request := struct {
		Owner     core.Protocol8SessionOwner `json:"owner"`
		Provider  string                     `json:"provider"`
		SessionID string                     `json:"session_id"`
		Observed  core.SessionObservation    `json:"observed"`
	}{owner, q.Provider, q.SessionID, q.Observed}
	var wire core.Protocol8Session
	if e = c.DoRequest(ctx, "POST", "/v1/sessions/"+q.SessionID+"/takeover", request, &wire, true); e != nil {
		return core.SessionView{}, e
	}
	return legacyPhysicalDecodeView(wire)
}
