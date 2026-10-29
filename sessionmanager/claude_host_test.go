package sessionmanager

// These original factory roots run the external factory over a scripted local
// executable, exercising readiness/refusal/argv/resume mechanics. They do not
// execute a native provider or establish canonical board goal binding.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeClaudeScript emulates the Claude Code surfaces the host depends on:
// --version, the -p /goal capability probe, the interactive banner, the
// workspace trust dialog, and /goal set/clear/status acknowledgements.
const fakeClaudeScript = `#!/bin/sh
log="${FAKE_CLAUDE_LOG:?}"
printf 'argv:%s\n' "$*" >> "$log"
version="${FAKE_CLAUDE_VERSION:-2.1.200}"
case "$1" in
--version) printf '%s (fake-claude)\n' "$version"; exit 0 ;;
esac
if [ "$1" = "-p" ]; then
  printf '{"result":"No goal set. Usage: /goal <condition>"}\n'
  exit 0
fi
if [ -n "${FAKE_CLAUDE_ALLOWED_READ:-}" ]; then
  if content="$(cat "$FAKE_CLAUDE_ALLOWED_READ")"; then
    printf 'allowed-read:%s\n' "$content" >> "$log"
  else
    printf 'allowed-read:blocked\n' >> "$log"
  fi
fi
if [ -n "${FAKE_CLAUDE_DENIED_READ:-}" ]; then
  if content="$(cat "$FAKE_CLAUDE_DENIED_READ")"; then
    printf 'denied-read:leaked:%s\n' "$content" >> "$log"
  else
    printf 'denied-read:blocked\n' >> "$log"
  fi
fi
if [ "${FAKE_CLAUDE_TRUST:-}" = "untrusted" ]; then
  printf 'Do you trust the files in this folder?\n'
  while :; do sleep 1; done
fi
if [ "${FAKE_CLAUDE_BYPASS_WARNING:-}" = "1" ]; then
  # Real Claude Code 2.1.268 renders this screen before its banner when
  # launched with --dangerously-skip-permissions, with "No, exit"
  # default-highlighted. Capture the exact raw bytes the host writes in
  # response so the test can assert on the real key sequence, not just that
  # something unblocked the fake.
  printf 'WARNING: Claude Code running in Bypass Permissions mode\n'
  printf '> No, exit\n'
  printf '  Yes, I accept\n'
  answer="$(dd bs=1 count=4 2>/dev/null | od -An -tx1 | tr -d ' \n')"
  printf 'bypass-answer:%s\n' "$answer" >> "$log"
fi
# Session selectors, read the way the provider reads them: the resumed source and
# the named writable child are separate identities.
resume=""
sid=""
prev=""
for arg in "$@"; do
  case "$prev" in
    --resume) resume="$arg" ;;
    --session-id) sid="$arg" ;;
  esac
  prev="$arg"
done
printf 'Claude Code v%s (fake)\n' "$version"
if [ -n "${FAKE_CLAUDE_MISSING_SESSION_DELAY:-}" ]; then
  # The delayed variant of the resume failure: the banner comes first and the
  # provider only says the session is missing afterwards.
  sleep "${FAKE_CLAUDE_MISSING_SESSION_DELAY}"
  printf 'No conversation found with session ID %s\n' "$resume"
  exit 1
fi
cond=""
while IFS= read -r line; do
  # Everything the provider is actually submitted, recorded where a test can
  # read it. The interactive surface only ever echoes to stdout, which a test
  # that is not capturing the PTY stream cannot observe.
  printf 'stdin:%s\n' "$line" >> "$log"
  case "$line" in
    *"/context"*)
      # The provider commits its forked transcript on its first submission, and a
      # fork copies the resumed records through while rewriting only sessionId.
      forkmode="${FAKE_CLAUDE_FORK:-copy}"
      dir="${FAKE_CLAUDE_PROJECTS_DIR:-}"
      if [ -n "$dir" ] && [ -n "$sid" ]; then
        case "$forkmode" in
          copy)
            sed "s/$resume/$sid/g" "$dir/$resume.jsonl" > "$dir/$sid.jsonl"
            ;;
          cold)
            # A session that started without the inherited history: its own
            # records only, none of the copy's identities.
            printf '{"type":"user","uuid":"cold-1","parentUuid":null,"sessionId":"%s","cwd":"%s","version":"%s","message":{"role":"user"}}\n' "$sid" "$PWD" "$version" > "$dir/$sid.jsonl"
            ;;
          append)
            sed "s/$resume/$sid/g" "$dir/$resume.jsonl" > "$dir/$sid.jsonl"
            printf '{"type":"user","uuid":"leak-1","parentUuid":null,"sessionId":"%s","cwd":"%s","version":"%s","message":{"role":"user"}}\n' "$resume" "$PWD" "$version" >> "$dir/$resume.jsonl"
            ;;
          foreign)
            # The provider honoured the resume but minted its own identity.
            sed "s/$resume/ffffffff-9999-4999-8999-ffffffffffff/g" "$dir/$resume.jsonl" > "$dir/ffffffff-9999-4999-8999-ffffffffffff.jsonl"
            ;;
          basemutate)
            # A textbook-correct fork that nonetheless appended into the shared
            # base. Only a post-launch read of the base can see this.
            if [ -n "${FAKE_CLAUDE_BASE_PATH:-}" ]; then
              printf '{"type":"user","uuid":"base-leak-1"}\n' >> "$FAKE_CLAUDE_BASE_PATH"
            fi
            # The optional files form a deterministic test barrier: expose the
            # forbidden base mutation while withholding the otherwise valid
            # fork transcript until the test releases the provider.
            if [ -n "${FAKE_CLAUDE_BASE_MUTATED_SIGNAL:-}" ]; then
              : > "$FAKE_CLAUDE_BASE_MUTATED_SIGNAL"
            fi
            if [ -n "${FAKE_CLAUDE_BASE_MUTATE_RELEASE:-}" ]; then
              while [ ! -e "$FAKE_CLAUDE_BASE_MUTATE_RELEASE" ]; do sleep 0.01; done
            fi
            sed "s/$resume/$sid/g" "$dir/$resume.jsonl" > "$dir/$sid.jsonl"
            ;;
          none) ;;
        esac
      fi
      printf 'Context usage reported\n'
      ;;
    "/goal clear") cond=""; printf 'Goal cleared\n' ;;
    "/goal") if [ -n "$cond" ]; then printf 'Goal active: %s\n' "$cond"; else printf 'No goal set. Usage: /goal <condition>\n'; fi ;;
    "/goal "*)
      cond="${line#/goal }"
      printf 'goal-received\n' >> "$log"
      # FAKE_CLAUDE_GOAL_SILENT models a provider that accepted the directive
      # but never rendered an acknowledgement, which is what keeps the host's
      # goal ack waiter armed for its whole timeout.
      if [ "${FAKE_CLAUDE_GOAL_SILENT:-}" != "1" ]; then
		if [ -n "${FAKE_CLAUDE_GOAL_ACK_DELAY:-}" ]; then
		  sleep "$FAKE_CLAUDE_GOAL_ACK_DELAY"
		fi
        printf 'Goal set: %s\n' "$cond"
      fi
      ;;
    "/exit") exit 0 ;;
    *)
      printf 'input:%s\n' "$line"
      if [ "${FAKE_CLAUDE_OWNER_RESULT:-}" = "achieved" ]; then
        cond=""
        printf 'Goal achieved\n'
      elif [ "${FAKE_CLAUDE_OWNER_RESULT:-}" = "limited" ]; then
        printf "You've hit your limit\n"
      fi
      ;;
  esac
done
`

type fakeClaudeFixture struct {
	executable string
	projectDir string
	logPath    string
}

func newFakeClaudeFixture(t *testing.T) fakeClaudeFixture {
	t.Helper()
	root := t.TempDir()
	executable := claudeWriteExecutable(t, "claude", []byte(fakeClaudeScript), 0o755)
	projectDir := filepath.Join(root, "project")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "fake-claude.log")
	t.Setenv("FAKE_CLAUDE_LOG", logPath)
	return fakeClaudeFixture{executable: executable, projectDir: projectDir, logPath: logPath}
}

func (f fakeClaudeFixture) launchPayload(t *testing.T) LaunchSpec {
	t.Helper()
	p := f.plan()
	return LaunchSpec{Executable: p.Executable, Cwd: p.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &ClaudePreparedLaunch{Contract: ClaudeSessionLaunchContract, Plan: p, Rows: 32, Cols: 100}}
}

func (f fakeClaudeFixture) plan() PrimarySessionLaunchPlan {
	return PrimarySessionLaunchPlan{
		Contract:      PrimarySessionLaunchPlanContract,
		SchemaVersion: 1,
		Status:        "ok",
		Provider:      "claude",
		ProjectDir:    f.projectDir,
		Executable:    f.executable,
		LaunchVariants: LaunchVariants{
			Interactive: LaunchArgv{Argv: []string{}},
			ManagedHost: LaunchArgv{Kind: "claude-pty", Argv: []string{}},
		},
		RequiredEnvNames: []string{},
	}
}

func (f fakeClaudeFixture) argvLines(t *testing.T) []string {
	t.Helper()
	file, err := os.Open(f.logPath)
	if err != nil {
		return nil
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "argv:") {
			lines = append(lines, strings.TrimPrefix(scanner.Text(), "argv:"))
		}
	}
	return lines
}

func newTestClaudeFactory(t *testing.T) *ClaudeHostFactory {
	t.Helper()
	factory := NewClaudeHostFactory(claudeTestLayout(t, "claude-host"))
	factory.readyTimeout = 15 * time.Second
	factory.ackTimeout = 10 * time.Second
	factory.submitDelay = 10 * time.Millisecond
	factory.postReadySettle = 10 * time.Millisecond
	return factory
}

func stopClaudeHost(t *testing.T, host ProviderHost) {
	t.Helper()
	if host != nil {
		_ = host.Stop(context.Background())
	}
}

func TestClaudeHostArmsReadinessBeforeStartingProviderOutputPump(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	factory := newTestClaudeFactory(t)
	armedBeforePump := false
	factory.beforePump = func(readyWaiters int) {
		armedBeforePump = readyWaiters == 1
	}
	host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-claude-ready-ordering",
		Launch:    fixture.launchPayload(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClaudeHost(t, host)
	if !armedBeforePump {
		t.Fatal("Claude readiness was not armed before the provider output pump started")
	}
	concrete, ok := host.(*claudeHost)
	if !ok {
		t.Fatalf("host type = %T, want *claudeHost", host)
	}
	if output := concrete.scanner.text(); !strings.Contains(output, "claude code v") {
		t.Fatalf("the pre-armed readiness path did not consume the provider banner: %q", output)
	}
}

func TestClaudeHostFailsClosedBelowMinimumGoalVersion(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	t.Setenv("FAKE_CLAUDE_VERSION", "2.0.9")
	factory := newTestClaudeFactory(t)
	_, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-claude-unit-000000000001",
		Launch:    fixture.launchPayload(t),
	})
	var public *PublicError
	if !errors.As(err, &public) || public.Code != ClaudeErrorUnsupportedVersion {
		t.Fatalf("launch error = %v, want public %s", err, ClaudeErrorUnsupportedVersion)
	}
	for _, line := range fixture.argvLines(t) {
		if !strings.HasPrefix(line, "--version") && !strings.HasPrefix(line, "-p ") {
			t.Fatalf("interactive Claude was launched despite failed capability gate: %q", line)
		}
	}
}

func TestClaudeHostFailsClosedOnWorkspaceTrustDialog(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	t.Setenv("FAKE_CLAUDE_TRUST", "untrusted")
	factory := newTestClaudeFactory(t)
	_, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-claude-unit-000000000002",
		Launch:    fixture.launchPayload(t),
	})
	var public *PublicError
	if !errors.As(err, &public) || public.Code != ClaudeErrorWorkspaceUntrusted {
		t.Fatalf("launch error = %v, want public %s", err, ClaudeErrorWorkspaceUntrusted)
	}
	if public.Message != "Claude requires an accepted workspace trust dialog; run claude once in this project and accept trust first" {
		t.Fatalf("workspace trust guidance = %q", public.Message)
	}
}

func TestClaudeHostAnswersBypassPermissionsWarningAndBecomesReady(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	t.Setenv("FAKE_CLAUDE_BYPASS_WARNING", "1")
	factory := newTestClaudeFactory(t)
	host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-claude-bypass-warning",
		Launch:    fixture.launchPayload(t),
	})
	if err != nil {
		t.Fatalf("Launch did not get past the bypass-permissions warning: %v", err)
	}
	defer stopClaudeHost(t, host)

	concrete, ok := host.(*claudeHost)
	if !ok {
		t.Fatalf("host type = %T, want *claudeHost", host)
	}
	if output := concrete.scanner.text(); !strings.Contains(output, "claude code v") {
		t.Fatalf("readiness resolved without ever observing the real banner: %q", output)
	}

	// Precision control: the answer must be exactly Down then Enter
	// (\x1b[B\r on the wire; a PTY's standard ICRNL input translation turns
	// the \r the host writes into \n by the time the child reads it, so the
	// observed bytes are 1b5b42 0a). "No, exit" is the default-highlighted
	// option, so a simplification that just sends Enter alone would select
	// and confirm the WRONG option and exit Claude instead of accepting.
	data, readErr := os.ReadFile(fixture.logPath)
	if readErr != nil {
		t.Fatalf("read fake claude log: %v", readErr)
	}
	if !strings.Contains(string(data), "bypass-answer:1b5b420a") {
		t.Fatalf("bypass-permissions answer bytes = %q, want the Down+Enter sequence 1b5b420a", data)
	}
}

func TestClaudeHostStillRespectsDeadlineIfBannerNeverFollowsBypassWarning(t *testing.T) {
	host := &claudeHost{
		scanner:     newClaudeOutputScanner(),
		exited:      make(chan struct{}),
		submitDelay: 10 * time.Millisecond,
	}
	// A pipe stands in for the PTY: acceptBypassPermissionsWarning only needs
	// something that accepts writes. Nothing ever feeds the scanner a banner,
	// so the only way out of awaitReady's loop is the deadline.
	_, writeEnd, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatalf("os.Pipe: %v", pipeErr)
	}
	defer writeEnd.Close()
	host.ptmx = writeEnd

	waiter := host.scanner.watch([]string{"claude code v", claudeBypassPermissionsWarningMarker}, nil)
	host.scanner.observe([]byte("warning: claude code running in bypass permissions mode\r\nyes, i accept\r\n"))

	start := time.Now()
	timeout := 500 * time.Millisecond
	err := host.awaitReady(context.Background(), timeout, false, waiter)
	elapsed := time.Since(start)

	var public *PublicError
	if !errors.As(err, &public) || public.Code != ClaudeErrorPreflightFailed {
		t.Fatalf("error = %v, want public %s (the warning was answered but no banner ever followed)", err, ClaudeErrorPreflightFailed)
	}
	if elapsed < timeout {
		t.Fatalf("awaitReady returned after %s, before its %s deadline elapsed", elapsed, timeout)
	}
	if elapsed > timeout+2*time.Second {
		t.Fatalf("awaitReady took %s to honor a %s deadline; answering the warning must not reset or extend it", elapsed, timeout)
	}
}

func TestClaudeHostLaunchWaitsOutThePostReadySettleDelayBeforeReturning(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	factory := newTestClaudeFactory(t)
	settle := 300 * time.Millisecond
	factory.postReadySettle = settle

	start := time.Now()
	host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-claude-settle-000000000001",
		Launch:    fixture.launchPayload(t),
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	defer stopClaudeHost(t, host)

	if elapsed < settle {
		t.Fatalf("Launch returned after %s, before its %s post-ready settle delay elapsed", elapsed, settle)
	}
	if elapsed > settle+2*time.Second {
		t.Fatalf("Launch took %s to honor a %s settle delay", elapsed, settle)
	}
}

func TestClaudeHostEnvironmentPreservesUserConfigAndTrustState(t *testing.T) {
	configDir := filepath.Join(t.TempDir(), "claude-config")
	t.Setenv("CLAUDE_CONFIG_DIR", configDir)
	t.Setenv("CLAUDECODE", "nested-parent")

	values := map[string]string{}
	for _, entry := range claudeHostEnvironment("", os.Environ()) {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	if got := values["CLAUDE_CONFIG_DIR"]; got != configDir {
		t.Fatalf("CLAUDE_CONFIG_DIR = %q, want %q", got, configDir)
	}
	if _, ok := values["CLAUDECODE"]; ok {
		t.Fatal("nested Claude session marker was inherited")
	}
}

func TestClaudeHostResumesSameSessionAfterProcessLoss(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	factory := newTestClaudeFactory(t)
	host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-claude-unit-000000000003",
		Launch:    fixture.launchPayload(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClaudeHost(t, host)

	snapshotRaw, err := host.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeClaudeHostSnapshot(snapshotRaw)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SessionUUID == "" || snapshot.PID <= 0 {
		t.Fatalf("snapshot lacks identity: %#v", snapshot)
	}

	// Simulate recoverable provider loss.
	_ = syscall.Kill(snapshot.PID, syscall.SIGKILL)
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, stateErr := host.State(context.Background())
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		if state == ProviderHostStopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("host did not observe provider process exit")
		}
		time.Sleep(20 * time.Millisecond)
	}

	record := ProviderHostRecord{Provider: "claude", Snapshot: snapshotRaw}
	restored, err := factory.Restore(context.Background(), record)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if state, _ := restored.State(context.Background()); state != ProviderHostStopped {
		t.Fatalf("restored detached host state = %s, want stopped", state)
	}

	relaunched, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-claude-unit-000000000003",
		Prior:     &record,
	})
	if err != nil {
		t.Fatalf("relaunch with prior snapshot: %v", err)
	}
	defer stopClaudeHost(t, relaunched)

	resumeLine := ""
	for _, line := range fixture.argvLines(t) {
		if strings.Contains(line, "--resume ") {
			resumeLine = line
		}
	}
	if !strings.Contains(resumeLine, snapshot.SessionUUID) {
		t.Fatalf("relaunch did not resume session %s: argv %q", snapshot.SessionUUID, fixture.argvLines(t))
	}
}

func TestClaudeManagedArgvFailsClosedOnSessionLifecycleFlags(t *testing.T) {
	blocked := [][]string{
		// Session identity and selection.
		{"--continue"},
		{"-c"},
		{"--resume", "0f9e8d7c"},
		{"--resume=0f9e8d7c"},
		{"-r"},
		{"--session-id", "0f9e8d7c"},
		{"--session-id=0f9e8d7c"},
		{"--fork-session"},
		{"--from-pr"},
		{"--from-pr=123"},
		{"--no-session-persistence"},
		// Non-interactive modes.
		{"--print"},
		{"-p"},
		{"--output-format", "json"},
		{"--input-format=stream-json"},
		{"--fallback-model", "haiku"},
		// Lifecycle and terminal ownership.
		{"--bg"},
		{"--background"},
		{"--worktree"},
		{"-w"},
		{"--tmux"},
		{"--tmux=classic"},
		{"--remote-control"},
		{"--ide"},
		{"--chrome"},
		// Capability surface and one-shot invocations.
		{"--disable-slash-commands"},
		{"--version"},
		{"--help"},
		// Positional prompt text, subcommands, and the prompt separator.
		{"start ordinary work now"},
		{"start", "ordinary", "work"},
		{"mcp"},
		{"agents"},
		{"--", "start ordinary work now"},
		{"--"},
		// Unknown tokens and malformed allowlisted flags.
		{"--totally-unknown-flag"},
		{"--totally-unknown=value"},
		{"--verbose=1"},
		{"--model"},
		{"--model", "--continue"},
		{"--add-dir"},
		// A bare token after an attached-value variadic is positional prompt
		// text in the provider's parser (the attached form ends the variadic
		// list), not another variadic value.
		{"--mcp-config=one.json", "two.json"},
		{"--add-dir=/tmp/extra", "/tmp/more"},
		{"--model", "sonnet", "--mcp-config=one.json", "two.json"},
		// Optional-value flags consume exactly one following token; anything
		// after that is positional prompt text.
		{"--debug", "api", "start ordinary work now"},
		{"--prompt-suggestions", "false", "extra"},
		// A blocked flag after valid policy flags still fails closed.
		{"--model", "sonnet", "--continue"},
	}
	for _, argv := range blocked {
		err := ValidateClaudeManagedArgv(argv)
		var public *PublicError
		if !errors.As(err, &public) || public.Code != ClaudeErrorLaunchArgsBlocked {
			t.Fatalf("argv %v error = %v, want public %s", argv, err, ClaudeErrorLaunchArgsBlocked)
		}
	}
	allowed := [][]string{
		nil,
		{},
		{"--model", "sonnet", "--permission-mode", "plan"},
		{"--model=sonnet", "--effort", "high"},
		{"--mcp-config", "servers.json", "--add-dir", "/tmp/extra", "/tmp/more"},
		{"--settings", "settings.json", "--append-system-prompt", "stay focused"},
		{"--allowedTools", "Bash", "Edit", "--strict-mcp-config"},
		{"--dangerously-skip-permissions", "--verbose", "-d", "--debug=api"},
		{"--name", "primary-session", "--no-chrome", "--safe-mode"},
		// Optional-value flags consume one following non-dash token, exactly
		// like the provider's parser (probe-verified on claude 2.1.219).
		{"--debug", "api"},
		{"-d", "api,hooks"},
		{"--prompt-suggestions", "false"},
		{"--prompt-suggestions"},
		{"--debug", "--verbose"},
		{"--debug"},
		// An attached-value variadic is complete on its own; only a bare
		// token after it fails closed.
		{"--mcp-config=one.json", "--verbose"},
		{"--mcp-config=one.json"},
		{"--mcp-config", "one.json", "two.json"},
	}
	for _, argv := range allowed {
		if err := ValidateClaudeManagedArgv(argv); err != nil {
			t.Fatalf("argv %v unexpectedly rejected: %v", argv, err)
		}
	}
}

func TestClaudeLaunchRejectsEscapingArgvBeforeAnyProviderWork(t *testing.T) {
	probes := [][]string{
		{"--background"},
		{"--bg"},
		{"--from-pr=123"},
		{"--worktree"},
		{"--tmux"},
		{"start ordinary work now"},
		{"--model", "sonnet", "--continue"},
	}
	for _, argv := range probes {
		fixture := newFakeClaudeFixture(t)
		factory := newTestClaudeFactory(t)
		plan := fixture.plan()
		plan.LaunchVariants.ManagedHost.Argv = argv
		launch := ClaudePreparedLaunch{
			Contract: ClaudeSessionLaunchContract,
			Plan:     plan,
			Rows:     32,
			Cols:     100,
		}
		payload := LaunchSpec{Executable: plan.Executable, Cwd: plan.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &launch}
		_, err := factory.Launch(context.Background(), ProviderLaunchRequest{
			SessionID: "SES-blocked-args",
			Launch:    payload,
		})
		var public *PublicError
		if !errors.As(err, &public) || public.Code != ClaudeErrorLaunchArgsBlocked {
			t.Fatalf("argv %v Launch error = %v, want public %s", argv, err, ClaudeErrorLaunchArgsBlocked)
		}
		if lines := fixture.argvLines(t); len(lines) != 0 {
			t.Fatalf("provider executable ran despite blocked argv %v: %v", argv, lines)
		}
	}
}

func TestClaudeLaunchForwardsAcceptedComposedArgvUnchanged(t *testing.T) {
	forms := [][]string{
		{"--model", "claude-fable-5", "--dangerously-skip-permissions", "--debug", "api"},
		{"--model", "claude-fable-5", "--dangerously-skip-permissions", "--prompt-suggestions", "false"},
		{"--model", "claude-fable-5", "--mcp-config=one.json", "--strict-mcp-config"},
		{"--mcp-config", "one.json", "two.json", "-d"},
	}
	for formIndex, argv := range forms {
		fixture := newFakeClaudeFixture(t)
		factory := newTestClaudeFactory(t)
		plan := fixture.plan()
		plan.LaunchVariants.ManagedHost.Argv = argv
		launch := ClaudePreparedLaunch{
			Contract: ClaudeSessionLaunchContract,
			Plan:     plan,
			Rows:     32,
			Cols:     100,
		}
		payload := LaunchSpec{Executable: plan.Executable, Cwd: plan.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &launch}
		host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
			SessionID: fmt.Sprintf("SES-accepted-args-%012d", formIndex),
			Launch:    payload,
		})
		if err != nil {
			t.Fatalf("argv %v Launch: %v", argv, err)
		}
		interactive := ""
		for _, line := range fixture.argvLines(t) {
			if !strings.HasPrefix(line, "--version") && !strings.HasPrefix(line, "-p ") {
				interactive = line
			}
		}
		wantPrefix := strings.Join(argv, " ") + " --session-id "
		if !strings.HasPrefix(interactive, wantPrefix) {
			t.Fatalf("interactive argv = %q, want prefix %q", interactive, wantPrefix)
		}
		stopClaudeHost(t, host)
	}
}
