package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// StartCodexRunner starts the resolved native executable in its own process
// group. The factory prepares the private home, tool isolation and rendered text;
// this constructor receives no canonical owner, goal or private record.
func StartCodexRunner(ctx context.Context, launch LaunchSpec, snapshot CodexRunnerSnapshot, model string) (host RunnerHost, resultErr error) {
	return startCodexRunner(ctx, launch, snapshot, model, nil)
}
func startCodexRunner(ctx context.Context, launch LaunchSpec, snapshot CodexRunnerSnapshot, model string, builder *BuilderConfinementRequest) (host RunnerHost, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if snapshot.Contract != codexHostSnapshotContract || !filepath.IsAbs(launch.Executable) || !filepath.IsAbs(launch.Cwd) || snapshot.ProjectDir != launch.Cwd || !filepath.IsAbs(snapshot.CodexHome) || !filepath.IsAbs(snapshot.LogPath) || snapshot.PID != 0 || snapshot.URL != "" || snapshot.ProcessIdentity != nil || snapshot.Stopped || launch.Variants.ManagedHost.Kind != "codex-app-server" {
		return nil, ErrInvalidInput
	}
	appServer := false
	for _, arg := range launch.Variants.ManagedHost.Argv {
		if arg == "--listen" || strings.HasPrefix(arg, "--listen=") {
			return nil, ErrInvalidInput
		}
		appServer = appServer || arg == codexAppServerSubcommand
	}
	if !appServer {
		return nil, ErrInvalidInput
	}
	if home, err := os.Stat(snapshot.CodexHome); err != nil || !home.IsDir() {
		return nil, ErrInvalidInput
	}
	owned := copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &snapshot})
	snapshot = *owned.Codex
	listenURL, err := reserveLoopbackWebsocketURL()
	if err != nil {
		return nil, err
	}
	args := codexHostArgvWithNativeGoalsDisabled(launch.Variants.ManagedHost.Argv)
	args = append(args, "--listen", listenURL)
	log, err := os.OpenFile(snapshot.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	isolatedArgs, isolation, err := builderIsolatedLaunchArgv("codex", args, builder)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	executable, commandArgs, confinement, err := confinedProviderCommand("codex", launch.Cwd, launch.Executable, isolatedArgs, codexBuilderRuntimeReadRoots(snapshot.CodexHome), builder)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	command := exec.Command(executable, commandArgs...)
	command.Dir = launch.Cwd
	command.Env = replaceEnvironmentValue(append([]string{}, launch.Environment...), "CODEX_HOME", snapshot.CodexHome)
	if confinement != nil {
		runtimeTemp, tempErr := prepareBuilderRuntimeTemp(filepath.Dir(snapshot.LogPath))
		if tempErr != nil {
			_ = log.Close()
			return nil, tempErr
		}
		command.Env = replaceEnvironmentValue(command.Env, "TMPDIR", runtimeTemp)
	}
	command.Stdout, command.Stderr = log, log
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	group := prepareCodexLaunchGroup(command.SysProcAttr)
	defer group.close()
	if err := command.Start(); err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("starting native Codex app-server: %w", err)
	}
	_ = log.Close()
	exited := make(chan struct{})
	join := sync.OnceFunc(func() {
		go func() {
			_ = command.Wait()
			close(exited)
		}()
	})
	identity, identityErr := readProviderProcessIdentity(command.Process.Pid)
	// Capture birth identity before reaping is possible. Where the kernel can
	// retain the original group object, Wait may run throughout startup. On
	// other kernels the unreaped child reserves the identity until cleanup or
	// handoff, so a numerical group lookup cannot follow PID reuse.
	if group.retainsOriginalGroup() {
		join()
	}
	published := false
	defer func() {
		if published {
			join()
			return
		}
		// The launch-owned group handle survives the leader's early exit. The
		// persisted-identity fallback is used only while our child is unreaped.
		groupErr := group.signal(command.Process.Pid, &identity, syscall.SIGKILL)
		_ = command.Process.Kill()
		join()
		<-exited
		until := time.Now().Add(500 * time.Millisecond)
		for !group.gone(command.Process.Pid) {
			if time.Now().After(until) {
				resultErr = errors.Join(resultErr, groupErr, &PublicError{Code: "session_host_unavailable", Message: fmt.Sprintf("native Codex startup cleanup remains pending for PID/process group %d: group disappearance was not confirmed", command.Process.Pid)})
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	if identityErr != nil {
		return nil, &PublicError{Code: ErrorSessionProcessIdentityUnverified, Message: "cannot record launched provider process identity"}
	}
	if err := waitForCodexHostReady(ctx, listenURL, codexHostReadyTimeout); err != nil {
		return nil, fmt.Errorf("waiting for native Codex app-server: %w", err)
	}
	client, err := dialCodexRPC(ctx, listenURL)
	if err != nil {
		return nil, err
	}
	defer client.close()
	if err := initializeCodexRunnerRPC(ctx, client); err != nil {
		return nil, err
	}
	threadID, instructionSources, err := openCodexRunnerThread(ctx, client, snapshot, launch, model)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshot.PID, snapshot.URL, snapshot.ThreadID = command.Process.Pid, listenURL, threadID
	snapshot.ProcessIdentity = &ProviderProcessIdentity{PID: identity.PID, UID: identity.UID, Start: identity.Start, PGID: identity.PGID}
	snapshot.Executable, snapshot.HostArgv = launch.Executable, args
	snapshot.ManagedClient = append([]string(nil), launch.Variants.ManagedClient.Argv...)
	snapshot.ToolPolicy = launch.ToolPolicy
	snapshot.Sources = append([]LaunchPlanSource(nil), launch.Sources...)
	r, err := newCodexRunner(snapshot)
	if err != nil {
		return nil, err
	}
	r.command, r.exited = command, exited
	r.builderConfinement, r.builderToolIsolation = confinement, isolation
	r.instructionSources = append([]string(nil), instructionSources...)
	r.startAutoContinue()
	published = true
	return r, nil
}

func (r *codexRunner) ProviderLaunchMetadata() ProviderLaunchMetadata {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ProviderLaunchMetadata{PID: r.snapshot.PID, Executable: r.snapshot.Executable}
}
