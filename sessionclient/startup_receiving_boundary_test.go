//go:build linux

package sessionclient_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	client "github.com/relux-works/swarma-session-host/sessionclient"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// New real handler/transport neighbors retain the complete original launch at
// the controlled provider, use actual native identity/current controller lease,
// preserve proxy on Detach, and confirm removal through the actual HTTP Stop.
// This independently qualifies Stop even if the original restart witness fails
// before reaching its later Stop assertion; it does not replace that assertion.
func TestStartupReceivingCompleteWireActualLeaseDetachAndStop(t *testing.T) {
	layout := receivingTestLayout(t, "full-wire-stop")
	target, _ := receivingEcho(t)
	factory := newReceivingFactory(layout, target)
	m := newReceivingManager(t, layout, factory)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := m.LoadAndReconcile(ctx); err != nil {
		t.Fatal(err)
	}
	q := factory.request(false)
	a, err := m.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if a.ManagerEndpoint == "" || a.CodexThreadID != factory.native.NativeIdentity() || a.ClientHome != factory.native.ClientHome() || a.Session.Owner != q.Owner || factory.launchCount() != 1 {
		t.Fatalf("actual complete attachment projection lost: %+v", a)
	}
	_, lease, live, err := m.host.OwnerSession(ctx, "parent/primary")
	if err != nil || !live || lease.SessionID != a.Session.SessionID || lease.Epoch == 0 {
		t.Fatalf("actual lease unavailable: %+v %v", lease, err)
	}
	path := providerProxySocketPath(t, a.ManagerEndpoint)
	assertProviderProxyRoundTrip(t, path, "full-wire")
	if _, err = m.Detach(a.Session.SessionID, a.AttachmentID); err != nil {
		t.Fatal(err)
	}
	again, err := m.Attach(ctx, q)
	if err != nil || again.ManagerEndpoint != a.ManagerEndpoint || again.Session.SessionID != a.Session.SessionID || factory.launchCount() != 1 {
		t.Fatalf("actual reattach changed session/proxy: %+v %v", again, err)
	}
	m.source.mu.Lock()
	pins := append([]core.ControllerLease(nil), m.source.leases...)
	m.source.mu.Unlock()
	if len(pins) != 2 || pins[0] != (core.ControllerLease{}) || pins[1] != lease {
		t.Fatalf("verified callback did not observe absence then exact live lease: %+v", pins)
	}
	if _, err = m.Stop(ctx, a.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("actual HTTP stopped-final ACK retained proxy: %v", err)
	}
	factory.native.mu.Lock()
	stops := factory.native.stops
	factory.native.mu.Unlock()
	if stops != 1 {
		t.Fatalf("actual Stop count=%d", stops)
	}
}

// Missing/denied configured source, wrong bearer and an otherwise valid foreign
// owner cannot launch or publish an attachment. Source denial is checked before
// callback effects; nearby full-wire/original tests exercise valid admission.
func TestStartupReceivingSourceAndForeignOwnerRefuseBeforeEffects(t *testing.T) {
	for _, kind := range []string{"missing-source", "denied-source", "wrong-bearer", "foreign-owner"} {
		t.Run(kind, func(t *testing.T) {
			layout := receivingTestLayout(t, kind)
			target, _ := receivingEcho(t)
			f := newReceivingFactory(layout, target)
			m := newReceivingManager(t, layout, f)
			m.missingSource = kind == "missing-source"
			m.source.denied = kind == "denied-source"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := m.LoadAndReconcile(ctx); err != nil {
				t.Fatal(err)
			}
			q := f.request(false)
			if kind == "foreign-owner" {
				q.Owner = core.Protocol8SessionOwner{Kind: "run", ID: "RUN-fixture-foreign"}
			}
			transport := m.client
			if kind == "wrong-bearer" {
				transport = client.New(client.Config{HTTPClient: m.client.HTTPClient, SocketPath: layout.SocketPath, Token: "fixture-wrong"})
			}
			err := transport.DoRequest(ctx, http.MethodPost, "/v1/sessions/attach", q, nil, true)
			var refusal *client.PublicError
			wantCode, wantStatus := "attach_failed", http.StatusConflict
			if kind == "wrong-bearer" {
				wantCode, wantStatus = "unauthorized", http.StatusUnauthorized
			}
			if !errors.As(err, &refusal) || refusal.Code != wantCode || refusal.StatusCode != wantStatus {
				t.Fatalf("wrong typed refusal: %v", err)
			}
			if err == nil || f.launchCount() != 0 {
				t.Fatalf("refused source/owner reached provider: %v launches%d", err, f.launchCount())
			}
			m.source.mu.Lock()
			calls := m.source.calls
			pins := len(m.source.leases)
			m.source.mu.Unlock()
			want := 1
			if kind == "missing-source" || kind == "wrong-bearer" {
				want = 0
			}
			if calls != want || pins != 0 {
				t.Fatalf("wrong refusal stage: source%d want%d callbacks%d error%v", calls, want, pins, err)
			}
			views, e := m.host.List(ctx)
			if e != nil || len(views) != 0 {
				t.Fatalf("refusal created session: %+v %v", views, e)
			}
			if _, e = os.Lstat(layout.AttachmentsDir); !errors.Is(e, os.ErrNotExist) {
				t.Fatalf("refusal created proxy: %v", e)
			}
			entries, e := os.ReadDir(layout.SessionsDir)
			if e != nil && !errors.Is(e, os.ErrNotExist) {
				t.Fatal(e)
			}
			if len(entries) != 0 {
				t.Fatal("refusal wrote durable session record")
			}
		})
	}
}

// A foreign-owner request cannot stop, replace or detach the already-live
// primary. Its actual current lease, native state and working proxy remain;
// the source's rejected callback creates no second session or provider effect.
func TestStartupReceivingForeignOwnerCannotAffectLivePrimary(t *testing.T) {
	layout := receivingTestLayout(t, "foreign-live-primary")
	target, _ := receivingEcho(t)
	factory := newReceivingFactory(layout, target)
	m := newReceivingManager(t, layout, factory)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.LoadAndReconcile(ctx); err != nil {
		t.Fatal(err)
	}
	q := factory.request(false)
	primary, err := m.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	before, lease, live, err := m.host.OwnerSession(ctx, "parent/primary")
	if err != nil || !live {
		t.Fatalf("primary setup not live: %+v %v", lease, err)
	}
	q.Owner = core.Protocol8SessionOwner{Kind: "run", ID: "RUN-fixture-foreign"}
	if got, err := m.Attach(ctx, q); err == nil || got != nil {
		t.Fatalf("foreign owner admitted: %+v %v", got, err)
	} else {
		var refusal *client.PublicError
		if !errors.As(err, &refusal) || refusal.Code != "attach_failed" || refusal.StatusCode != http.StatusConflict {
			t.Fatalf("wrong foreign-owner refusal: %v", err)
		}
	}
	after, current, stillLive, err := m.host.OwnerSession(ctx, "parent/primary")
	if err != nil || !stillLive || current != lease || after.SessionID != before.SessionID || after.State != before.State || after.AttachedClients != before.AttachedClients {
		t.Fatalf("foreign request changed live owner: before%+v after%+v lease%+v error%v", before, after, current, err)
	}
	factory.native.mu.Lock()
	stops, state := factory.native.stops, factory.native.state
	factory.native.mu.Unlock()
	if stops != 0 || state != core.ProviderHostRunning || factory.launchCount() != 1 {
		t.Fatalf("foreign request changed native boundary: stops%d state%s launches%d", stops, state, factory.launchCount())
	}
	views, err := m.host.List(ctx)
	if err != nil || len(views) != 1 || views[0].SessionID != primary.Session.SessionID {
		t.Fatalf("foreign request created/replaced session: %+v %v", views, err)
	}
	assertProviderProxyRoundTrip(t, providerProxySocketPath(t, primary.ManagerEndpoint), "foreign-refusal-preserved")
}
