//go:build linux && amd64 && t4fixture

// host-dogfood-fixture is a standalone inert lifecycle witness, never a launch
// admission route. It accepts no provider, model, launcher, identity or payload.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--inert-child=term" || os.Args[1] == "--inert-child=ignore-term") {
		child(os.Args[1] == "--inert-child=ignore-term")
		return
	}
	root := flag.String("root", "", "new private directory for preserved synthetic evidence")
	flag.Parse()
	if flag.NArg() != 0 || !filepath.IsAbs(*root) || os.Mkdir(*root, 0700) != nil {
		fmt.Fprintln(os.Stderr, "fresh_absolute_fixture_root_required")
		os.Exit(2)
	}
	if err := run(*root); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func child(ignore bool) {
	term := make(chan os.Signal, 1)
	if ignore {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(term, syscall.SIGTERM)
	}
	// Readiness follows signal registration. No descendants or provider calls.
	if os.WriteFile("child-ready", []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600) != nil {
		os.Exit(3)
	}
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			var envelope fixtureadapters.Envelope
			if json.Unmarshal(scanner.Bytes(), &envelope) != nil || envelope.Kind != "notice" {
				os.Exit(4)
			}
			envelope.Kind = "applied"
			if json.NewEncoder(os.Stdout).Encode(envelope) != nil {
				os.Exit(5)
			}
		}
		// EOF deliberately does not complete this child. Stop must use TERM/KILL.
	}()
	select {
	case <-term:
		if os.WriteFile("term-received", []byte("SIGTERM\n"), 0600) != nil {
			os.Exit(6)
		}
	case <-time.After(15 * time.Second):
		os.Exit(7)
	}
}

func run(root string) error {
	// A held directory gives short socket names while every byte remains in the
	// caller's fresh root. This proc alias is fixture plumbing, not image proof.
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	alias := fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), dir.Fd())
	profile, err := sessionhost.DescribeRuntimeProfile(sessionhost.SingleTrustUnitProfile, sessionhost.PlainPathnameBackend)
	if err != nil {
		return err
	}
	results := []map[string]any{}
	for _, mode := range []string{"term", "ignore-term"} {
		result, err := lifecycle(root, alias, mode)
		if err != nil {
			return fmt.Errorf("%s: %w", mode, err)
		}
		results = append(results, result)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"schema": "host-dogfood-synthetic-lifecycle/1", "synthetic": true,
		"real_provider": false, "model_provider_calls": 0,
		"source_factory_exercised": false, "source_ack_exercised": false,
		"private_create_admission_exercised": false, "native_admission": false,
		"runtime_cutover": false, "identify_v2_selected": true, "profile_description_only": profile,
		"runner_pid": os.Getpid(), "cases": results,
	})
}

func lifecycle(root, alias, mode string) (map[string]any, error) {
	workspace := filepath.Join(root, mode)
	if err := os.Mkdir(workspace, 0700); err != nil {
		return nil, err
	}
	socket := filepath.Join(alias, mode, "r.sock")
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	execution := "synthetic-dogfood-" + mode
	s, err := runner.Start(runner.Config{IdentifyV2: true, Socket: socket, Execution: execution, IOTimeout: 200 * time.Millisecond, StopSignal: "term",
		Plan: runner.Plan{Binary: binary, Args: []string{"--inert-child=" + mode}, Cwd: workspace, Env: []string{"GOMAXPROCS=2", "GORACE=atexit_sleep_ms=0"}}})
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if err := await(func() bool { _, err := os.Stat(filepath.Join(workspace, "child-ready")); return err == nil }); err != nil {
		return nil, err
	}
	h, err := sessionhost.Open(sessionhost.Config{Socket: socket, StateDir: filepath.Join(workspace, "state"), Execution: execution,
		Selection: sessionhost.Selection{Vendor: "synthetic-provider", Model: "inert-model", Reasoning: "inert-effort"},
		Adapter:   fixtureadapters.JSON{}, IOTimeout: time.Second})
	if err != nil {
		return nil, err
	}
	defer h.Close()
	state := h.Snapshot()
	conn, err := net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	uid, pid, err := localipc.PeerIdentity(conn.(*net.UnixConn))
	if err != nil || uid != uint32(os.Geteuid()) || pid != os.Getpid() {
		return nil, errors.New("fixture_connected_peer_mismatch")
	}
	req := runner.IdentifyV2Request{Schema: "runner-identify-request-v2", Execution: execution, ExpectedInstance: state.RunnerInstance}
	if err := localipc.Write(conn, req); err != nil {
		return nil, err
	}
	var identity runner.IdentifyV2Response
	if err := localipc.Read(bufio.NewReader(conn), &identity); err != nil {
		return nil, err
	}
	if identity.Result != "verified" || identity.Child == nil || identity.Child.PID != s.ChildPID() {
		return nil, fmt.Errorf("fixture_owned_identity_unavailable: %+v", identity)
	}
	if _, err := h.Enqueue(sessionhost.Notice{ID: "inert-notice", Reference: "synthetic/no-provider"}); err != nil {
		return nil, err
	}
	if err := await(func() bool { st := h.Snapshot(); return len(st.Records) == 1 && st.Records[0].State == "applied" }); err != nil {
		return nil, err
	}
	beforeStop := h.Snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	first, err := h.Stop(ctx)
	if err != nil {
		return nil, err
	}
	second, err := h.Stop(ctx)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(started)
	want, wantExit, wantClose := "stopped", 0, ""
	if mode == "ignore-term" {
		want, wantExit, wantClose = "stop_unknown", -1, "runner_stop_unknown"
	}
	if first.Code != want || second.Code != want || !first.Exited || first.PID != s.ChildPID() || first.ExitCode != wantExit || first.Writes != 1 {
		return nil, fmt.Errorf("incorrect_stop_receipts: %+v %+v", first, second)
	}
	if err := h.Close(); err != nil {
		return nil, err
	}
	closeCode := ""
	if err := s.Close(); err != nil {
		closeCode = err.Error()
	}
	if closeCode != wantClose {
		return nil, fmt.Errorf("incorrect_close_receipt: %s", closeCode)
	}
	// Only an immediate absence observation, never authority to signal by PID.
	absent := syscall.Kill(s.ChildPID(), 0) == syscall.ESRCH
	_, socketErr := os.Lstat(socket)
	termData, _ := os.ReadFile(filepath.Join(workspace, "term-received"))
	termObserved := string(termData) == "SIGTERM\n"
	if !absent || !os.IsNotExist(socketErr) || termObserved != (mode == "term") {
		return nil, errors.New("incomplete_fixture_cleanup")
	}
	return map[string]any{"mode": mode, "synthetic": true, "child_pid": s.ChildPID(), "owned_identity_audit_only": identity,
		"connected_peer": map[string]any{"pid": pid, "euid": uid, "incarnation_image_admission": false},
		"notice_applied": beforeStop, "stop": first, "retry": second, "close_code": closeCode,
		"stop_elapsed_ns": elapsed.Nanoseconds(), "term_received": termObserved,
		"child_absent_after_join": absent, "socket_removed": true}, nil
}

func await(ready func() bool) error {
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			return errors.New("fixture_observation_deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}
