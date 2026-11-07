//go:build t4fixture

package sessionhost

// This entire file is excluded from ordinary product builds. All provider,
// Core selections below are explicit inert qualification fakes. The selected
// single-trust-unit profile trusts the real current UID without agent isolation.
import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	managerschema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
)

type privateFixtureSource struct {
	mode       string
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	endpoint   *net.UnixConn
	binding    NotificationBinding
	attachment string
	prepares   atomic.Int32
	bootstraps atomic.Int32
	closes     atomic.Int32
	once       sync.Once
	origin     ManagerOrigin
}

func (s *privateFixtureSource) Prepare(ctx context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
	s.prepares.Add(1)
	if s.mode == "source-refused" || origin.Recheck(ctx) != nil || s.ctx.Err() != nil {
		return nil, ErrNotificationUnresolved
	}
	s.origin = origin
	return s, nil
}
func (s *privateFixtureSource) References() NotificationReferences {
	return NotificationReferences{Member: s.origin.Member, Connection: "explicit-fake-provider-scope", Configuration: s.origin.ConfigurationRoot, ProfileLock: s.origin.ProfileLock}
}
func (s *privateFixtureSource) Recheck(ctx context.Context) error {
	if ctx.Err() != nil || s.ctx.Err() != nil {
		return ErrNotificationUnresolved
	}
	return s.origin.Recheck(ctx)
}
func (s *privateFixtureSource) Bootstrap(ctx context.Context, b NotificationBootstrap) (NotificationCompletion, error) {
	s.bootstraps.Add(1)
	s.mu.Lock()
	s.endpoint = b.MethodEndpoint
	s.binding = b.Binding
	s.binding.ConsumerGeneration = "explicit-fake-generation"
	s.binding.EnrollmentRef = "explicit-fake-receipt"
	s.attachment = b.AttachmentReference
	s.mu.Unlock()
	if s.mode == "wrong-binding" {
		s.mu.Lock()
		s.binding.RunnerInstance = "foreign-runner"
		s.mu.Unlock()
	}
	if err := b.OnEnrolled(ctx, privateFixtureEnrollment{s}); err != nil {
		return s, err
	}
	// This return models a LOCAL installed acknowledgment failure. It is not a
	// Source production ACK format, and no upstream ACK contract is implemented.
	if s.mode == "ack-refused" {
		return s, ErrNotificationUnresolved
	}
	return s, nil
}
func (s *privateFixtureSource) Binding() NotificationBinding {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binding
}
func (s *privateFixtureSource) AttachmentReference() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attachment
}
func (s *privateFixtureSource) Lifetime() (context.Context, time.Time) {
	d, _ := s.ctx.Deadline()
	return s.ctx, d
}

// Separate adapter avoids conflating provider's contextual Recheck with the
// live enrollment callback's no-argument Recheck method.
type privateFixtureEnrollment struct{ source *privateFixtureSource }

func (e privateFixtureEnrollment) Binding() NotificationBinding           { return e.source.Binding() }
func (e privateFixtureEnrollment) AttachmentReference() string            { return e.source.AttachmentReference() }
func (e privateFixtureEnrollment) Lifetime() (context.Context, time.Time) { return e.source.Lifetime() }
func (e privateFixtureEnrollment) Recheck() error                         { return e.source.Recheck(e.source.ctx) }
func (s *privateFixtureSource) Done() <-chan struct{}                     { return s.ctx.Done() }
func (s *privateFixtureSource) Err() error                                { return s.ctx.Err() }
func (s *privateFixtureSource) Close() error {
	s.once.Do(func() {
		s.closes.Add(1)
		s.cancel()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.endpoint != nil {
			_ = s.endpoint.Close()
		}
	})
	return nil
}

// RunPrivateCompositionFixture is only available with -tags t4fixture. It owns
// a pinned self-executable child, fake Core output and fake Source enrollment.
// It accepts no launcher, caller UID, credentials or native payload from argv.
func RunPrivateCompositionFixture(ctx context.Context, root, mode string) (map[string]any, error) {
	switch mode {
	case "accepted", "policy-refused", "source-refused", "ack-refused", "wrong-binding", "native-never-ready", "source-revoked", "parent-canceled", "restart-refused":
	default:
		return nil, errors.New("unknown_fixture_mode")
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	for _, d := range []string{"records", "workspace"} {
		if err = os.Mkdir(filepath.Join(root, d), 0700); err != nil {
			return nil, err
		}
	}
	workspace := filepath.Join(root, "workspace")
	record := map[string]any{"schema": "werkstatt-record-v1", "id": "member-1", "project": "project", "role": "orchestrator", "runtime": "codex_cli", "model": nil, "effort": nil, "profile": "swarm-orch", "profile_lock_sha256": strings.Repeat("a", 64), "clone": workspace, "identity": "fixture.orch", "room": "!fixture:example.test", "orchestrator": "fixture.parent", "sandbox": "none", "trust_class": "operator", "doorbell": nil, "notification": map[string]any{"member": map[string]any{"mxid": "@fixture.orch:example.test", "agent_selector": "fixture.orch", "connection": "fixture-gr", "configuration_root": "/configured/source"}, "admission_reference": "opaque-assignment-v1", "call_timeout_ms": 1000}}
	encoded, _ := json.Marshal(record)
	if err = os.WriteFile(filepath.Join(root, "records/member-1.json"), encoded, 0600); err != nil {
		return nil, err
	}
	vectors, err := managerschema.Documents.ReadFile("vectors/launch.json")
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Document map[string]any `json:"document"`
	}
	if err = json.Unmarshal(vectors, &rows); err != nil || len(rows) == 0 {
		return nil, errors.New("fixture_vector_unavailable")
	}
	q := rows[0].Document
	q["cwd"] = workspace
	fragment, _ := json.Marshal(map[string]any{"fragment": "launch-env-fragment-v4", "environment": "codex_cli", "profile": map[string]any{"name": "swarm-orch", "lock_sha256": strings.Repeat("a", 64)}, "precedence": map[string]any{"winner": "higher-weight", "placement": "winner-last"}, "env": map[string]any{}, "permissions": map[string]any{"mode": "native", "locked": false, "source": "default"}})
	canonical, err := managerschema.CanonicalJSON(fragment)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(canonical)
	q["profile"] = map[string]any{"name": "swarm-orch", "lock_sha256": strings.Repeat("a", 64), "fragment_digest": "sha256:" + hex.EncodeToString(digest[:])}
	n := record["notification"].(map[string]any)
	n["contract"] = NotificationContract
	n["backend"] = "host"
	n["workflow"] = map[string]any{"reference": "workflow-1", "sha256": strings.Repeat("c", 64)}
	q["notification"] = n
	body, _ := json.Marshal(q)
	selected, _ := json.Marshal(map[string]any{"schema": "curator.launcher-config/2", "config_schema": 8, "config_path": "explicit-fake-selected.json", "extensions": map[string]any{"notification": map[string]any{"backend": "host", "contract": NotificationContract, "workflow_reference": "workflow-1", "workflow_sha256": strings.Repeat("c", 64)}}})
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	script := []byte("#!/bin/sh\nexec '" + strings.ReplaceAll(binary, "'", "'\\''") + "' --protocol-fixture-child '" + mode + "'\n")
	launcher := filepath.Join(root, "curator-run")
	if err = os.WriteFile(launcher, script, 0700); err != nil {
		return nil, err
	}
	pin := sha256.Sum256(script)
	grant := createPolicyGrant{"swarm-orch", strings.Repeat("a", 64), PlainPathnameBackend}
	if mode == "policy-refused" {
		grant.Profile = "ungranted"
	}
	policyPath := filepath.Join(root, "policy.json")
	parent, parentCancel := context.WithCancel(ctx)
	defer parentCancel()
	sourceCtx, sourceCancel := context.WithTimeout(parent, 20*time.Second)
	defer sourceCancel()
	source := &privateFixtureSource{mode: mode, ctx: sourceCtx, cancel: sourceCancel}
	defer source.Close()
	operator, err := user.Current()
	if err != nil {
		return nil, err
	}
	cfg := privateCompositionConfig{profile: privateRuntimeProfileConfig{SingleTrustUnitProfile, PlainPathnameBackend, policyPath}, records: filepath.Join(root, "records"), stateDir: filepath.Join(root, "state"), socket: filepath.Join(root, "runner.sock"), launcher: ordinaryLauncherConfig{binary: launcher, sha256: hex.EncodeToString(pin[:]), environment: []string{"HOME=" + operator.HomeDir, "PATH=/usr/bin:/bin"}}, selection: Selection{Vendor: "unfamiliar-provider", Model: "unfamiliar-model", Reasoning: "unfamiliar-effort"}, nativeRelease: codexadapter.ObservedRelease, permission: codexadapter.YoloPolicy, ioTimeout: time.Second, poll: 10 * time.Millisecond, ready: 2 * time.Second}
	_, profileReport, err := freezePrivateProfile(cfg)
	if err != nil {
		return nil, err
	}
	policyData, _ := json.Marshal(singleTrustUnitPolicyDocument{2, SingleTrustUnitProfile, PlainPathnameBackend, profileReport.SelectedConfigurationHash, []createPolicyGrant{grant}})
	if err = os.WriteFile(policyPath, append(policyData, '\n'), 0600); err != nil {
		return nil, err
	}
	deps := privateCompositionDependencies{source: source, core: func(ctx context.Context, _ string, _ []string, args ...string) ([]byte, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		switch strings.Join(args, " ") {
		case "config launcher":
			return selected, nil
		case "env resolve codex_cli --profile swarm-orch --format json":
			return fragment, nil
		}
		return nil, errPrivateComposition
	}}
	request, requestCancel := context.WithCancel(parent)
	r, startErr := openPrivateComposition(parent, request, time.Now().Add(15*time.Second), cfg, deps, "launch", body)
	requestCancel() // A completed startup must survive its request's cancellation.
	result := map[string]any{"mode": mode, "synthetic": true, "startup": "refused", "caller_uid_is_fake": false, "runtime_profile": profileReport, "core_is_fake": true, "source_is_fake": true}
	refusal := mode == "policy-refused" || mode == "source-refused" || mode == "ack-refused" || mode == "wrong-binding" || mode == "native-never-ready"
	if refusal {
		if startErr == nil {
			_ = r.close(context.Background())
			return nil, errors.New("fixture_expected_refusal")
		}
	} else {
		if startErr != nil {
			return nil, startErr
		}
		defer r.close(context.Background())
		result["startup"] = "ready"
		result["runtime_profile"] = r.profile
		if r.manager.createCorrelation.check(parent) != nil {
			return nil, errors.New("request_cancel_ended_runtime")
		}
		b := source.Binding()
		source.mu.Lock()
		endpoint := source.endpoint
		source.mu.Unlock()
		exchange := func(method string) (NotificationResponse, error) {
			req := NotificationRequest{Contract: NotificationContract, RequestID: "fixture-" + method, Method: method, Binding: b}
			if method == "trigger" {
				req.NoticeID = "fixture-notice"
			}
			data, e := EncodeNotificationRequest(req)
			if e != nil {
				return NotificationResponse{}, e
			}
			endpoint.SetDeadline(time.Now().Add(2 * time.Second))
			if _, e = endpoint.Write(data); e != nil {
				return NotificationResponse{}, e
			}
			return ReadNotificationResponse(bufio.NewReader(endpoint), req)
		}
		observed, e := exchange("observe_binding")
		if e != nil || observed.Result != "observed" {
			return nil, errors.New("fixture_binding_not_observed")
		}
		result["binding_observed"] = true
		response, e := exchange("trigger")
		if e != nil || response.Result != "accepted" {
			return nil, fmt.Errorf("fixture_trigger_failed: %s", response.Result)
		}
		result["trigger_result"] = response.Result
		if mode == "source-revoked" {
			sourceCancel()
		}
		if mode == "parent-canceled" {
			parentCancel()
		}
		if mode == "source-revoked" || mode == "parent-canceled" {
			select {
			case <-r.done:
			case <-time.After(4 * time.Second):
				return nil, errors.New("fixture_automatic_cleanup_missing")
			}
		}
		if err = r.close(context.Background()); err != nil {
			return nil, err
		}
		select {
		case <-r.create.done:
			result["create_joined"] = true
		default:
			return nil, errors.New("fixture_create_not_joined")
		}
		select {
		case <-r.manager.createCorrelation.done:
			result["handoff_joined"] = true
		default:
			return nil, errors.New("fixture_handoff_not_joined")
		}
		state, exists, e := r.owner.host.store.read()
		if e != nil || !exists || state.Notification == nil || len(state.Notification.Attempts) != 1 {
			return nil, errors.New("fixture_journal_missing")
		}
		result["durable_attempts"] = len(state.Notification.Attempts)
		if mode == "restart-refused" {
			before, e := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			if e != nil {
				return nil, e
			}
			retryDeps := deps
			retry, e := openPrivateComposition(context.Background(), context.Background(), time.Now().Add(time.Second), cfg, retryDeps, "launch", body)
			if e == nil {
				_ = retry.close(context.Background())
				return nil, errors.New("fixture_restart_admitted")
			}
			after, e := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			if e != nil || string(before) != string(after) || source.prepares.Load() != 1 {
				return nil, errors.New("fixture_restart_mutated_journal")
			}
			result["restart_refused_without_replay"] = true
		}
	}
	if _, err = os.Lstat(cfg.socket); !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("fixture_runner_socket_survived")
	}
	result["runner_socket_removed"] = true
	source.Close()
	result["source_prepare_calls"] = source.prepares.Load()
	result["source_bootstrap_calls"] = source.bootstraps.Load()
	result["source_close_calls"] = source.closes.Load()
	return result, nil
}

// RunPrivateProtocolFixtureChild implements only inert local RPC examples.
func RunPrivateProtocolFixtureChild(mode string) error {
	if err := os.WriteFile("fixture-child.pid", []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		return err
	}
	scan := bufio.NewScanner(os.Stdin)
	write := json.NewEncoder(os.Stdout)
	for scan.Scan() {
		var q struct {
			ID     uint64         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if json.Unmarshal(scan.Bytes(), &q) != nil {
			return errors.New("fixture_bad_rpc")
		}
		switch q.Method {
		case "initialize":
			if err := write.Encode(map[string]any{"id": q.ID, "result": map[string]any{"userAgent": "explicit-inert-fixture", "platformFamily": "unix", "platformOs": "linux"}}); err != nil {
				return err
			}
		case "initialized":
		case "thread/start":
			if mode == "native-never-ready" {
				_, err := io.Copy(io.Discard, os.Stdin)
				return err
			}
			result := map[string]any{"thread": map[string]any{"id": "synthetic-thread"}, "model": q.Params["model"], "modelProvider": q.Params["modelProvider"], "approvalPolicy": q.Params["approvalPolicy"], "cwd": q.Params["cwd"], "sandbox": map[string]string{"type": "dangerFullAccess"}}
			if err := write.Encode(map[string]any{"id": q.ID, "result": result}); err != nil {
				return err
			}
		case "turn/start":
			turn := fmt.Sprintf("turn-%d", q.ID)
			if err := write.Encode(map[string]any{"id": q.ID, "result": map[string]any{"turn": map[string]any{"id": turn, "status": "inProgress"}}}); err != nil {
				return err
			}
			if err := write.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "synthetic-thread", "turn": map[string]any{"id": turn, "status": "completed"}}}); err != nil {
				return err
			}
		default:
			return errors.New("fixture_unexpected_rpc")
		}
	}
	return scan.Err()
}
