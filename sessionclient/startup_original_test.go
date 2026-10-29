//go:build linux

package sessionclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// These six original roots retain their status/token/PID/instance/log,
// cancellation and launch-count assertions. Fixture setup is ported from the
// board-coupled manager to an owned Unix/HTTP endpoint and verified helper source;
// it proves startup transport/arbitration, not a production provider launch.
func TestConnectOrStartTakesOverWedgedLockHolder(t *testing.T) {
	declareOutsideShell(t)
	oldHealthTimeout := staleInstanceHealthCheckTimeout
	oldTerminateGrace := staleInstanceTerminateGrace
	oldForceGrace := staleInstanceForceGrace
	oldPollInterval := staleInstancePollInterval
	staleInstanceHealthCheckTimeout = 75 * time.Millisecond
	staleInstanceTerminateGrace = 100 * time.Millisecond
	staleInstanceForceGrace = time.Second
	staleInstancePollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		staleInstanceHealthCheckTimeout = oldHealthTimeout
		staleInstanceTerminateGrace = oldTerminateGrace
		staleInstanceForceGrace = oldForceGrace
		staleInstancePollInterval = oldPollInterval
	})

	layout := staleTakeoverTestLayout(t, "wedged-holder")
	holder := startWedgedHolderProcess(t, layout)

	daemon, err := newStartupTestDaemon(layout, "takeover-test")
	if err != nil {
		t.Fatal(err)
	}
	daemonCtx, stopDaemon := context.WithCancel(context.Background())
	daemonDone := make(chan struct{})
	var launchMu sync.Mutex
	launched := false
	var daemonServeErr error
	launch := func() error {
		launchMu.Lock()
		launched = true
		launchMu.Unlock()
		go func() {
			daemonServeErr = daemon.Serve(daemonCtx)
			close(daemonDone)
		}()
		select {
		case <-daemon.Ready():
			return nil
		case <-daemonDone:
			return daemonServeErr
		case <-time.After(3 * time.Second):
			return errors.New("replacement daemon readiness timeout")
		}
	}
	t.Cleanup(func() {
		stopDaemon()
		launchMu.Lock()
		wasLaunched := launched
		launchMu.Unlock()
		if wasLaunched {
			<-daemonDone
			if daemonServeErr != nil && !errors.Is(daemonServeErr, errStartupFixtureAlreadyRunning) {
				t.Errorf("replacement daemon shutdown: %v", daemonServeErr)
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	client, err := connectOriginalStartup(ctx, layout, launch)
	if err != nil {
		t.Fatalf("ConnectOrStart wedged holder: %v", err)
	}
	defer client.Close()

	status, err := client.StartupStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.PID == holder.pid || status.PID != os.Getpid() {
		t.Fatalf("replacement status pid = %d, old=%d current=%d", status.PID, holder.pid, os.Getpid())
	}
	if status.InstanceID == holder.instanceID {
		t.Fatalf("replacement retained wedged instance ID %q", status.InstanceID)
	}
	holder.waitForExit(t, 2*time.Second)

	tokenBytes, err := ReadPrivateFile(layout.TokenFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(tokenBytes)) == wedgedHolderToken {
		t.Fatal("replacement daemon did not refresh the bearer token")
	}
	metadataBytes, err := ReadPrivateFile(layout.EndpointFile)
	if err != nil {
		t.Fatal(err)
	}
	var metadata Endpoint
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.PID != status.PID || metadata.InstanceID != status.InstanceID {
		t.Fatalf("replacement endpoint metadata = %#v, status=%#v", metadata, status)
	}

	entries, raw := readOperatorLogEntries(t, layout.OperatorLogFile)
	if !operatorLogContains(entries, "stale_instance_takeover", map[string]string{
		"old_pid": fmt.Sprint(holder.pid),
		"new_pid": fmt.Sprint(status.PID),
	}) {
		t.Fatalf("operator log missing takeover old/new pid:\n%s", raw)
	}
}

func TestConnectOrStartKeepsHealthyLockHolder(t *testing.T) {
	declareOutsideShell(t)
	layout := staleTakeoverTestLayout(t, "healthy-holder")
	daemon, err := newStartupTestDaemon(layout, "healthy-test")
	if err != nil {
		t.Fatal(err)
	}
	cancelDaemon, daemonErr := startTestDaemon(t, daemon)
	defer func() {
		cancelDaemon()
		if err := <-daemonErr; err != nil {
			t.Errorf("daemon shutdown: %v", err)
		}
	}()

	launches := 0
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := connectOriginalStartup(ctx, layout, func() error {
		launches++
		return errors.New("healthy holder must not launch a replacement")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	status, err := client.StartupStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.InstanceID != daemon.instance || status.PID != os.Getpid() {
		t.Fatalf("connected status = %#v, want instance=%s pid=%d", status, daemon.instance, os.Getpid())
	}
	if launches != 0 {
		t.Fatalf("replacement launches = %d, want 0", launches)
	}
	entries, raw := readOperatorLogEntries(t, layout.OperatorLogFile)
	if operatorLogContains(entries, "stale_instance_takeover", nil) {
		t.Fatalf("healthy holder was logged as taken over:\n%s", raw)
	}
}

func TestConnectOrStartUpgradesHealthyOutdatedProtocolHolder(t *testing.T) {
	declareOutsideShell(t)
	oldHealthTimeout := staleInstanceHealthCheckTimeout
	oldTerminateGrace := staleInstanceTerminateGrace
	oldForceGrace := staleInstanceForceGrace
	oldPollInterval := staleInstancePollInterval
	staleInstanceHealthCheckTimeout = 75 * time.Millisecond
	staleInstanceTerminateGrace = 100 * time.Millisecond
	staleInstanceForceGrace = time.Second
	staleInstancePollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		staleInstanceHealthCheckTimeout = oldHealthTimeout
		staleInstanceTerminateGrace = oldTerminateGrace
		staleInstanceForceGrace = oldForceGrace
		staleInstancePollInterval = oldPollInterval
	})

	layout := staleTakeoverTestLayout(t, "outdated-protocol-holder")
	holder := startSessionManagerHolderProcess(t, layout, "outdated-protocol")
	daemon, err := newStartupTestDaemon(layout, "0.22.2-48")
	if err != nil {
		t.Fatal(err)
	}
	var (
		cancelDaemon context.CancelFunc
		daemonErr    <-chan error
		launches     int
	)
	t.Cleanup(func() {
		if cancelDaemon != nil {
			cancelDaemon()
			if err := <-daemonErr; err != nil {
				t.Errorf("replacement daemon shutdown: %v", err)
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	client, err := connectOriginalStartup(ctx, layout, func() error {
		launches++
		if launches > 1 {
			return errors.New("replacement daemon launched more than once")
		}
		cancelDaemon, daemonErr = startTestDaemon(t, daemon)
		return nil
	})
	if err != nil {
		t.Fatalf("ConnectOrStart protocol upgrade: %v", err)
	}
	defer client.Close()

	status, err := client.StartupStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.ProtocolVersion != ProtocolVersion ||
		status.DaemonVersion != "0.22.2-48" ||
		status.PID == holder.pid {
		t.Fatalf("replacement status = %#v, old pid=%d", status, holder.pid)
	}
	metadata, err := startupEndpoint(layout.StartupLayout)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.ProtocolVersion != ProtocolVersion ||
		metadata.DaemonVersion != "0.22.2-48" ||
		metadata.PID != status.PID {
		t.Fatalf("replacement metadata = %#v, status=%#v", metadata, status)
	}
	holder.waitForExit(t, 2*time.Second)

	entries, raw := readOperatorLogEntries(t, layout.OperatorLogFile)
	if !operatorLogContains(entries, "protocol_upgrade_takeover", map[string]string{
		"old_pid": fmt.Sprint(holder.pid),
		"new_pid": fmt.Sprint(status.PID),
	}) {
		t.Fatalf("operator log missing protocol upgrade takeover:\n%s", raw)
	}
}

func TestConnectOrStartTakesOverHolderWithoutStartupHealthy(t *testing.T) {
	declareOutsideShell(t)
	oldHealthTimeout := staleInstanceHealthCheckTimeout
	oldStartupDeadline := startupHealthyDeadline
	oldTerminateGrace := staleInstanceTerminateGrace
	oldForceGrace := staleInstanceForceGrace
	oldPollInterval := staleInstancePollInterval
	staleInstanceHealthCheckTimeout = 75 * time.Millisecond
	startupHealthyDeadline = 50 * time.Millisecond
	staleInstanceTerminateGrace = 100 * time.Millisecond
	staleInstanceForceGrace = time.Second
	staleInstancePollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		staleInstanceHealthCheckTimeout = oldHealthTimeout
		startupHealthyDeadline = oldStartupDeadline
		staleInstanceTerminateGrace = oldTerminateGrace
		staleInstanceForceGrace = oldForceGrace
		staleInstancePollInterval = oldPollInterval
	})

	layout := staleTakeoverTestLayout(t, "startup-unhealthy-holder")
	holder := startSessionManagerHolderProcess(t, layout, "startup-unhealthy")
	daemon, err := newStartupTestDaemon(layout, "startup-health-takeover-test")
	if err != nil {
		t.Fatal(err)
	}
	var (
		cancelDaemon context.CancelFunc
		daemonErr    <-chan error
		launches     int
	)
	t.Cleanup(func() {
		if cancelDaemon != nil {
			cancelDaemon()
			if err := <-daemonErr; err != nil {
				t.Errorf("replacement daemon shutdown: %v", err)
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	client, err := connectOriginalStartup(ctx, layout, func() error {
		launches++
		if launches > 1 {
			return errors.New("replacement daemon launched more than once")
		}
		cancelDaemon, daemonErr = startTestDaemon(t, daemon)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	status, err := client.StartupStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !status.StartupHealthy || status.PID == holder.pid {
		t.Fatalf("replacement status = %#v, old pid=%d", status, holder.pid)
	}
	holder.waitForExit(t, 2*time.Second)
}

func TestConnectOrStartDoesNotTakeOverAfterCallerCancellation(t *testing.T) {
	layout := staleTakeoverTestLayout(t, "cancelled-health-check")
	holder := startWedgedHolderProcess(t, layout)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	launches := 0
	_, err := connectOriginalStartup(ctx, layout, func() error {
		launches++
		return nil
	})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("ConnectOrStart error = %v, want context.Canceled", err)
	}
	if launches != 0 {
		t.Fatalf("replacement launches = %d after cancellation, want 0", launches)
	}
	alive, err := fixtureProcessAlive(holder.pid)
	if err != nil {
		t.Fatal(err)
	}
	if !alive {
		t.Fatalf("caller cancellation terminated healthy pid boundary %d", holder.pid)
	}
}

func TestConnectOrStartSerializesLaunchPerBoardKey(t *testing.T) {
	layout := testLayout(t, "connect-or-start")
	daemon, err := newStartupTestDaemon(layout, "")
	if err != nil {
		t.Fatal(err)
	}
	daemonCtx, stopDaemon := context.WithCancel(context.Background())
	defer stopDaemon()
	serveErr := make(chan error, 1)
	var launchMu sync.Mutex
	launches := 0
	launch := func() error {
		launchMu.Lock()
		launches++
		launchMu.Unlock()
		go func() { serveErr <- daemon.Serve(daemonCtx) }()
		select {
		case <-daemon.Ready():
			return nil
		case err := <-serveErr:
			return err
		case <-time.After(5 * time.Second):
			return errors.New("daemon readiness timeout")
		}
	}

	var wait sync.WaitGroup
	errorsByClient := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, connectErr := connectOriginalStartup(ctx, layout, launch)
			if connectErr == nil {
				client.Close()
			}
			errorsByClient <- connectErr
		}()
	}
	wait.Wait()
	close(errorsByClient)
	for connectErr := range errorsByClient {
		if connectErr != nil {
			t.Fatal(connectErr)
		}
	}
	launchMu.Lock()
	gotLaunches := launches
	launchMu.Unlock()
	if gotLaunches != 1 {
		t.Fatalf("launches = %d, want 1", gotLaunches)
	}
	stopDaemon()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}
