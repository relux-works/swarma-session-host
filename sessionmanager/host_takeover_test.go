package sessionmanager

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
)

type takeoverProbeFactory struct{ hostCommitProbeFactory }

func (takeoverProbeFactory) Provider() string { return "codex" }

// Takeover pins the disclosed provider/state/liveness/client count, observes
// the actual incumbent again, and preserves native identity in resumable Stop.
// These tests use a physical boundary fixture, never a live provider/process.
func TestHostTakeoverObservationAndResumableStop(t *testing.T) {
	h, _, native, id := hostCommitProbeHost(t)
	ctx := context.Background()
	h.options.Factories = append(h.options.Factories, takeoverProbeFactory{})
	nativeHost := h.runners[id]
	before := native.snapshot
	// Original re-observation updates this specific detail before comparing.
	r := h.sessions[id]
	r.LivenessDetail = "loaded provider host is live"
	h.sessions[id] = r
	observed := ObservationOf(h.view(r))
	v, e := h.TakeOverOwnerSlot(ctx, TakeoverRequest{Owner: r.Owner, Provider: "codex", SessionID: id, Observed: observed})
	if e != nil || v.State != SessionStopped || v.Eviction == nil || v.Eviction.ByProvider != "codex" || native.forces != 1 {
		t.Fatalf("takeover %+v %v forces%d", v, e, native.forces)
	}
	if h.runners[id] != nil || h.sessions[id].ProviderHost.Snapshot.MusePTY.SessionID != before.MusePTY.SessionID || nativeHost == nil {
		t.Fatal("native identity not resumable/runner not retired")
	}
}

// A changed disclosure, different owner/session, same-provider request or
// unreadable current host refuses without any Stop. Consent is not liveness.
func TestHostTakeoverRefusalsDoNotStop(t *testing.T) {
	for _, mode := range []string{"clients", "provider", "state", "liveness", "detail", "moved-owner", "moved-session", "same-provider", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			h, _, native, id := hostCommitProbeHost(t)
			ctx := context.Background()
			h.options.Factories = append(h.options.Factories, takeoverProbeFactory{})
			r := h.sessions[id]
			r.LivenessDetail = "loaded provider host is live"
			h.sessions[id] = r
			q := TakeoverRequest{Owner: r.Owner, Provider: "codex", SessionID: id, Observed: ObservationOf(h.view(r))}
			switch mode {
			case "clients":
				q.Observed.AttachedClients++
			case "provider":
				q.Observed.Provider = "claude"
			case "state":
				q.Observed.State = SessionRecovering
			case "liveness":
				q.Observed.Liveness = SessionLivenessDead
			case "detail":
				q.Observed.LivenessDetail = "old disclosure"
			case "moved-owner":
				q.Owner = "another-owner"
			case "moved-session":
				q.SessionID = "another-session"
			case "same-provider":
				q.Provider = "muse"
			case "unknown":
				h.runners[id] = &takeoverUnreadableRunner{RunnerHost: native}
			}
			_, e := h.TakeOverOwnerSlot(ctx, q)
			var p *PublicError
			want := ErrorSessionTakeoverObservationChanged
			if mode == "same-provider" {
				want = ErrorSessionTakeoverNotApplicable
			}
			if mode == "unknown" {
				want = ErrorSessionLivenessUnknown
			}
			if !errors.As(e, &p) || p.Code != want || native.forces != 0 || h.sessions[id].Eviction != nil {
				t.Fatalf("refusal lost/no-effect: %v forces%d", e, native.forces)
			}
		})
	}
}

type takeoverUnreadableRunner struct{ RunnerHost }

func (*takeoverUnreadableRunner) State(context.Context) (ProviderHostState, error) {
	return "", errors.New("unreadable state")
}

// A genuine cold dead observation releases only after its durable ACK, with
// no Stop. A failed liveness write or foreign record cut never licenses Stop.
func TestHostTakeoverColdDeadAndFailedObservation(t *testing.T) {
	for _, mode := range []string{"dead", "failed-write"} {
		t.Run(mode, func(t *testing.T) {
			h, _, native, id := hostCommitProbeHost(t)
			ctx := context.Background()
			h.options.Factories = append(h.options.Factories, takeoverProbeFactory{})
			if mode == "failed-write" {
				h.records.commit = func(string, []byte) error { return errors.New("owned write fault") }
				_, e := h.TakeOverOwnerSlot(ctx, TakeoverRequest{Owner: h.sessions[id].Owner, Provider: "codex", SessionID: id, Observed: ObservationOf(h.view(h.sessions[id]))})
				if e == nil || native.forces != 0 {
					t.Fatalf("failed observation allowed Stop: %v", e)
				}
				return
			}
			sid, e := opaqueID("SES")
			if e != nil {
				t.Fatal(e)
			}
			r := copyRunnerRecord(h.sessions[id])
			r.SessionID = sid
			r.Owner = "cold-owner"
			r.Provider = "codex"
			r.ProviderHost = ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{PID: 123, URL: "ws://127.0.0.1:9/cold-fixture"}}}
			if e = h.records.create(ctx, r); e != nil {
				t.Fatal(e)
			}
			h.sessions[sid] = r
			previousProbe, previousDial := probeProviderProcess, dialProviderTransport
			defer func() { probeProviderProcess = previousProbe; dialProviderTransport = previousDial }()
			probeProviderProcess = func(int) (bool, error) { return false, nil }
			dialProviderTransport = func(context.Context, string, string) (net.Conn, error) { return nil, syscall.ECONNREFUSED }
			_, e = h.TakeOverOwnerSlot(ctx, TakeoverRequest{Owner: r.Owner, Provider: "muse", SessionID: sid, Observed: ObservationOf(h.view(r))})
			var p *PublicError
			if !errors.As(e, &p) || p.Code != ErrorSessionTakeoverObservationChanged || h.sessions[sid].State != SessionStopped || native.forces != 0 {
				t.Fatalf("cold release %+v %v forces%d", h.sessions[sid], e, native.forces)
			}
		})
	}
}
