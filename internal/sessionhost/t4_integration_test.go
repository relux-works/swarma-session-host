package sessionhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// This fixture composes actual T1 Start/operator-file admission and T2 parser /
// controller retention. The only executed child is this Go test executable.
// The selected profile trusts this real shared UID. Core and Source stay synthetic.
func t4CompositionFixture(t *testing.T) (privateCompositionConfig, createPolicy, []byte, []byte, []byte) {
	t.Helper()
	pre, q := preChildFixture(t)
	selected, fragment := coreDocuments(t, q, 4)
	dir, err := os.MkdirTemp("/tmp", "t4-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	// Keep the short socket path, but use the physical spelling required by
	// the launcher and policy guards (Darwin's /tmp is a symlink).
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := []byte("#!/bin/sh\nSESSION_HOST_RPC_FIXTURE=plain GORACE=atexit_sleep_ms=0 exec '" + strings.ReplaceAll(binary, "'", "'\\''") + "' -test.run='^TestProtocolProcess$'\n")
	launcher := filepath.Join(dir, "curator-run")
	if err := os.WriteFile(launcher, script, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(script)
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	cfg := privateCompositionConfig{profile: privateRuntimeProfileConfig{SingleTrustUnitProfile, PlainPathnameBackend, filepath.Join(dir, "policy.json")}, records: pre.records.path, stateDir: filepath.Join(dir, "state"), socket: filepath.Join(dir, "r.sock"),
		launcher:      ordinaryLauncherConfig{binary: launcher, sha256: hex.EncodeToString(sum[:]), environment: []string{"HOME=" + u.HomeDir, "PATH=/usr/bin:/bin"}},
		selection:     Selection{Vendor: "unfamiliar-provider", Model: "unfamiliar-model", Reasoning: "unfamiliar-effort"},
		nativeRelease: codexadapter.ObservedRelease, permission: codexadapter.YoloPolicy, ioTimeout: time.Second, poll: 10 * time.Millisecond, ready: 5 * time.Second}
	p := singleTrustPolicyFixture(t, cfg, fixtureGrant())
	return cfg, p, memberOperationBytes(t, q), selected, fragment
}

func t4CreateFixture(t *testing.T) (*ordinaryCreateHandler, *continuingHostOwner, []byte, []byte, []byte) {
	t.Helper()
	composition, p, body, selected, fragment := t4CompositionFixture(t)
	h, err := openOrdinaryCreateHandler(context.Background(), time.Now().Add(time.Minute), composition.records,
		composition.launcher, composition.socket, composition.ioTimeout, p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if got, err := h.create(context.Background(), "launch", body); err != nil || got != createRunnerOwned {
		t.Fatal(got, err)
	}
	custody, err := h.custody()
	if err != nil {
		t.Fatal(err)
	}
	if custody.operation != h.operation || custody.claim != h.operation.claim || custody.retained != h.operation.claim.retained || custody.source != h.wire.owner.records {
		t.Fatal("T1 physical chain changed")
	}
	ready := make(chan struct{}, 1)
	cfg := Config{StateDir: composition.stateDir, Socket: h.socket, Execution: h.operation.claim.id, ProtocolName: "codex", NoticeMode: WorkReference, IOTimeout: time.Second, PollInterval: 10 * time.Millisecond,
		Selection: Selection{Vendor: "unfamiliar-provider", Model: "unfamiliar-model", Reasoning: "unfamiliar-effort"}}
	cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		d, off, err := codexadapter.OpenDriverWithOptions(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: cfg.Selection.Vendor, Model: cfg.Selection.Model, Effort: cfg.Selection.Reasoning}, Workspace: h.operation.claim.invocation.workspace}, codexadapter.DriverOptions{NoticeMode: WorkReference, PermissionPolicy: codexadapter.YoloPolicy}, saved)
		if err == nil && len(saved) > 0 && d.Snapshot().View.NativeSession != "" {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		return d, off, err
	}
	o := continuingOwnerOpen(t, context.Background(), time.Now().Add(time.Minute), cfg)
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("actual parser never established thread", o.host.Snapshot())
	}
	return h, o, body, selected, fragment
}

func t4Manager(t *testing.T, h *ordinaryCreateHandler, o *continuingHostOwner, selected, fragment []byte, broker NotificationBroker) *NotificationManager {
	t.Helper()
	m, err := openCreateNotificationManager(h, o, nil, broker)
	if err != nil {
		t.Fatal("compose", err)
	}
	// The emitted Core fixtures still traverse the existing strict correlator.
	m.read = t4CoreReader(selected, fragment)
	t.Cleanup(func() {
		if err := m.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return m
}

func t4CoreReader(selected, fragment []byte) managerCoreRead {
	return func(ctx context.Context, _ string, _ []string, args ...string) ([]byte, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if strings.Join(args, " ") == "config launcher" {
			return append([]byte(nil), selected...), nil
		}
		if strings.Join(args, " ") == "env resolve codex_cli --profile swarm-orch --format json" {
			return append([]byte(nil), fragment...), nil
		}
		return nil, errMemberRecordUnavailable
	}
}

func TestT4AcknowledgedCreateHandoff(t *testing.T) {
	h, o, body, selected, fragment := t4CreateFixture(t)
	b := bridgeBrokerFixture{prepare: func(context.Context, ManagerOrigin) (PreparedNotificationBroker, error) {
		return nil, errMemberRecordUnavailable
	}}
	m := t4Manager(t, h, o, selected, fragment, b)
	c := h.transferred
	if c == nil || !c.adopted || c.op != h.operation || c.server != h.child || c.owner != o {
		t.Fatal("handoff replaced physical objects")
	}
	if h.outcome != createContinuingOwned || h.wire.owner.ctx.Err() != nil {
		t.Fatal("duty release canceled physical owner")
	}
	if err := m.admission.checkManagerOperation(context.Background(), o.host, "launch", body); err != nil {
		t.Fatal(err)
	}
	if err := m.admission.checkManagerOperation(context.Background(), o.host, "launch", append(body, ' ')); err == nil {
		t.Fatal("copied DTO admitted")
	}
	if _, err := h.transferTo(o); err == nil {
		t.Fatal("second acknowledgment")
	}
	if p := h.policy.(*operatorCreatePolicy); p.file != nil || p.root != nil {
		t.Fatal("policy handles leaked")
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("T1 watcher did not join T2")
	}
	if !h.wire.closed || c.check(context.Background()) == nil {
		t.Fatal("cleanup resurrected custody")
	}
}

func TestT4FailedTransferKeepsT1Duty(t *testing.T) {
	h, o, _, _, _ := t4CreateFixture(t)
	o.host.mu.Lock()
	original := o.host.state.Protocol.View.NativeSession
	o.host.state.Protocol.View.NativeSession = "foreign"
	o.host.mu.Unlock()
	if _, err := h.transferTo(o); err == nil || h.transferred != nil || h.outcome != createRunnerOwned {
		t.Fatal("failed acknowledgment released duty")
	}
	// M1 is intentional: restoring evidence cannot reuse a consumed slot.
	o.host.mu.Lock()
	o.host.state.Protocol.View.NativeSession = original
	o.host.mu.Unlock()
	if _, err := h.transferTo(o); err == nil || h.transferred != nil || h.operation.t2 == nil {
		t.Fatal("failed retain slot was reused")
	}
	if err := h.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !h.wire.closed {
		t.Fatal("T1 abandoned resources")
	}
}

func TestT4TransferSerializesWithCleanup(t *testing.T) {
	for range 4 {
		h, o, _, _, _ := t4CreateFixture(t)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, _ = h.transferTo(o) }()
		go func() { defer wg.Done(); <-start; _ = h.close(context.Background()) }()
		close(start)
		wg.Wait()
		select {
		case <-h.done:
		case <-time.After(3 * time.Second):
			t.Fatal("racing handoff abandoned cleanup")
		}
		if !h.wire.closed {
			t.Fatal("pre-child resources survived race")
		}
		if h.transferred != nil {
			select {
			case <-h.transferred.done:
			default:
				t.Fatal("T1 completed before T2 cleanup")
			}
		}
	}
}

func TestT4UnselectedBrokerKeepsCreateCustody(t *testing.T) {
	h, o, _, _, _ := t4CreateFixture(t)
	if m, err := openCreateNotificationManager(h, o, nil, nil); err == nil || m != nil {
		t.Fatal("missing broker selected")
	}
	if h.transferred != nil || h.outcome != createRunnerOwned {
		t.Fatal("unselected composition released T1 duty")
	}
}

// Fixture-only control protocol; this does NOT select an upstream provider ACK
// or serialize production enrollment authority. The Source test holds and
// rechecks the real enrollment callback throughout this bounded subprocess.
type t4Control struct {
	Kind       string              `json:"kind"`
	Mode       string              `json:"mode,omitempty"`
	Binding    NotificationBinding `json:"binding"`
	Attachment string              `json:"attachment,omitempty"`
	Intents    int                 `json:"intents,omitempty"`
	Closed     bool                `json:"closed,omitempty"`
	NoticeID   string              `json:"notice_id,omitempty"`
	Sequence   string              `json:"sequence,omitempty"`
}

func TestT4HostHelper(t *testing.T) {
	if os.Getenv("T4_HOST_HELPER") != "1" {
		t.Skip("explicit synthetic cross-repository invocation required")
	}
	wire, err := inheritedDockStream(3)
	if err != nil {
		t.Fatal(err)
	}
	defer wire.Close()
	control, err := inheritedDockStream(4)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	control.SetDeadline(time.Now().Add(15 * time.Second))
	dec, enc := json.NewDecoder(io.LimitReader(control, 32768)), json.NewEncoder(control)
	var setup t4Control
	if dec.Decode(&setup) != nil || setup.Kind != "setup" {
		t.Fatal("fixture setup")
	}
	cfg, _, body, selected, fragment := t4CompositionFixture(t)
	var pumps sync.WaitGroup
	var full NotificationBinding
	broker := bridgeBrokerFixture{prepare: func(ctx context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
		if origin.Recheck(ctx) != nil {
			return nil, errMemberRecordUnavailable
		}
		return &bridgePreparedFixture{origin: origin, bootstrap: func(ctx context.Context, b NotificationBootstrap) (NotificationCompletion, error) {
			cctx, cancel := context.WithCancel(ctx)
			completion := &bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: b.MethodEndpoint}
			if enc.Encode(t4Control{Kind: "ready", Binding: b.Binding, Attachment: b.AttachmentReference}) != nil {
				return completion, ErrNotificationUnresolved
			}
			var enrolled t4Control
			if dec.Decode(&enrolled) != nil || enrolled.Kind != "enrolled" {
				return completion, ErrNotificationUnresolved
			}
			full = enrolled.Binding
			if err := b.OnEnrolled(ctx, bridgeEnrollmentFixture{full, b.AttachmentReference, cctx, time.Now().Add(15 * time.Second)}); err != nil {
				return completion, err
			}
			pumps.Add(2)
			go func() { defer pumps.Done(); io.Copy(b.MethodEndpoint, wire); b.MethodEndpoint.CloseWrite() }()
			go func() { defer pumps.Done(); io.Copy(wire, b.MethodEndpoint); wire.CloseWrite() }()
			if enc.Encode(t4Control{Kind: "installed", Binding: full}) != nil {
				return completion, ErrNotificationUnresolved
			}
			return completion, nil
		}}, nil
	}}
	runtime, err := openPrivateComposition(context.Background(), context.Background(), time.Now().Add(time.Minute), cfg,
		privateCompositionDependencies{source: broker, core: t4CoreReader(selected, fragment)}, "launch", body)
	if err != nil {
		t.Fatal("private runtime", err)
	}
	t.Cleanup(func() {
		if err := runtime.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	h, o, m := runtime.create, runtime.owner, runtime.manager
	var ack t4Control
	if dec.Decode(&ack) != nil || ack.Kind != "installed-ack" || ack.Binding != full {
		t.Fatal("fixture installed acknowledgment")
	}
	if err := m.createCorrelation.check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if setup.Mode == "refused" {
		if err := o.host.Fence(false); err != nil {
			t.Fatal(err)
		}
	}
	if setup.Mode == "ack-refused" {
		if enc.Encode(t4Control{Kind: "ack-refused"}) != nil {
			t.Fatal("fixture ack refusal")
		}
	} else if enc.Encode(t4Control{Kind: "acknowledged", Binding: full}) != nil {
		t.Fatal("fixture ack")
	}
	var finish t4Control
	if dec.Decode(&finish) != nil {
		t.Fatal("fixture command")
	}
	if finish.Kind == "revoke-host" {
		if err := m.createCorrelation.server.Close(); err != nil {
			t.Fatal(err)
		}
		if m.createCorrelation.check(context.Background()) == nil {
			t.Fatal("changed controller remained admitted")
		}
		if enc.Encode(t4Control{Kind: "host-revoked"}) != nil {
			t.Fatal("fixture revocation reply")
		}
		if dec.Decode(&finish) != nil {
			t.Fatal("fixture finish after revocation")
		}
	}
	if finish.Kind != "finish" {
		t.Fatal("fixture finish")
	}
	wire.Close()
	if err := runtime.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	pumps.Wait()
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("T1 did not join cleanup")
	}
	state, exists, err := o.host.store.read()
	if err != nil || !exists || state.Notification == nil {
		t.Fatal("Host durable journal")
	}
	result := t4Control{Kind: "result", Binding: state.Notification.Binding, Intents: len(state.Notification.Attempts), Closed: h.closed && h.wire.closed && m.createCorrelation.check(context.Background()) != nil}
	if len(state.Notification.Attempts) == 1 {
		intent := state.Notification.Attempts[0]
		record := state.Records[intent.Sequence-1]
		attempt, ok := notificationAttempt(record, state.Notification.Binding)
		if !ok {
			t.Fatal("retained Host attempt invalid")
		}
		result.NoticeID, result.Sequence = attempt.NoticeID, attempt.Sequence
	}
	if enc.Encode(result) != nil {
		t.Fatal("fixture result")
	}
}
