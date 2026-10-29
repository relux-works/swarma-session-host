package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
)

// lockedClaudeDaemonProvenance builds a valid locked claude_code v3
// provenance snapshot for daemon-gate tests: the unlocked native profile
// shape relocked to native/global, consistent so extract validates it.
func lockedClaudeDaemonProvenance() agentic.CuratorContextProvenance {
	lock := strings.Repeat("a", 64)
	return agentic.CuratorContextProvenance{
		ProfileName:         "test",
		LockSHA256:          lock,
		ManagedHomeVariable: "CLAUDE_CONFIG_DIR",
		ManagedHome:         "/managed/home",
		Fragment: agentic.CuratorFragmentIdentity{
			Revision:    agentic.CuratorLaunchFragmentV3,
			Environment: "claude_code",
			Profile:     agentic.CuratorProfilePin{Name: "test", LockSHA256: lock},
			Precedence:  agentic.CuratorPrecedence{Winner: "higher-weight", Placement: "winner-last"},
			Env:         map[string]string{"CLAUDE_CONFIG_DIR": "/managed/home"},
			Permissions: &agentic.CuratorPermissions{
				Mode: agentic.CuratorPermissionNative, Locked: true, Source: agentic.CuratorPermissionSourceGlobal,
			},
		},
	}
}

func lockedClaudeDaemonSources(t *testing.T) []LaunchPlanSource {
	t.Helper()
	p := lockedClaudeDaemonProvenance()
	p.ManagedHome = t.TempDir()
	p.Fragment.Env["CLAUDE_CONFIG_DIR"] = p.ManagedHome
	encoded, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return []LaunchPlanSource{{Kind: CuratorProvenanceSourceKind, Path: string(encoded)}}
}

func requireClaudeDaemonLockRefusal(t *testing.T, err error) {
	t.Helper()
	var public *PublicError
	if !errors.As(err, &public) || public.Code != ClaudeFragmentPermissionsLocked {
		t.Fatalf("Launch = %v; want the %s public refusal", err, ClaudeFragmentPermissionsLocked)
	}
	for _, want := range []string{
		`permissions locked by source "global" to mode "native"`,
		`(runtime "claude_code")`,
		"unlock the fragment or run the interactive session without the bypass flags",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal = %q, want it to name %q", err, want)
		}
	}
}

func requireClaudeDaemonProvenanceRefusal(t *testing.T, err error) {
	t.Helper()
	var public *PublicError
	if !errors.As(err, &public) || public.Code != ClaudeErrorCuratorProvenanceUnreadable {
		t.Fatalf("Launch = %v; want the %s public refusal", err, ClaudeErrorCuratorProvenanceUnreadable)
	}
	for _, want := range []string{
		"provenance snapshot cannot be verified",
		"unknown lock state",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal = %q, want it to name %q", err, want)
		}
	}
}

// TestClaudeHostLaunchRefusesLockedWiderPlan pins the daemon-side
// defense-in-depth negative: a hand-built plan whose provenance
// snapshot says locked and whose argv carries any wider form refuses with
// the lock code before the probe or any host work — the provider
// executable never runs. The composer gate should have refused first; the
// host refuses anyway.
func TestClaudeHostLaunchRefusesLockedWiderPlan(t *testing.T) {
	for _, argv := range [][]string{
		{"--dangerously-skip-permissions"},
		{"--allow-dangerously-skip-permissions"},
		{"--permission-mode", "bypassPermissions"},
		{"--permission-mode=bypassPermissions"},
		{"--settings", `{"permissions":{"defaultMode":"bypassPermissions"}}`},
		{"--permission-mode", "ultra"},
		{"--permission-mode", "plan"},
		// r3 allow-list: the r2 tool-grant bypass plus every other
		// managed flag outside the locked-safe set.
		{"--allowedTools", "Bash(*)", "Edit", "Write"},
		{"--allowed-tools", "mcp__x__y"},
		{"--tools", "Bash"},
		{"--add-dir", "/tmp/work"},
		{"--mcp-config", "/tmp/evil.json"},
		{"--strict-mcp-config"},
		{"--agent", "explore"},
		{"--agents", `{"a":{}}`},
		{"--plugin-dir", "/tmp/p"},
		{"--plugin-url", "https://x/y"},
		{"--betas", "x"},
		{"--bare"},
		{"--safe-mode"},
		// r4 narrowing: startup provisioning and tool enablement.
		{"--file", "file_abc:.claude/settings.json"},
		{"--file", "notes.md"},
		{"--brief"},
		{"--verbose"},
		{"--no-chrome"},
	} {
		t.Run(strings.Join(argv, " "), func(t *testing.T) {
			fixture := newFakeClaudeFixture(t)
			factory := newTestClaudeFactory(t)
			plan := fixture.plan()
			plan.LaunchVariants.ManagedHost.Argv = argv
			plan.Sources = lockedClaudeDaemonSources(t)
			launch := ClaudePreparedLaunch{
				Contract: ClaudeSessionLaunchContract,
				Plan:     plan,
				Rows:     32,
				Cols:     100,
			}
			payload := LaunchSpec{Executable: plan.Executable, Cwd: plan.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &launch}
			host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
				SessionID: "SES-locked-plan",
				Launch:    payload,
			})
			if host != nil {
				stopClaudeHost(t, host)
			}
			requireClaudeDaemonLockRefusal(t, err)
			if lines := fixture.argvLines(t); len(lines) != 0 {
				t.Fatalf("provider executable ran despite locked refusal for %v: %v", argv, lines)
			}
		})
	}
}

// TestClaudeHostLaunchPassesLockedNativeToAllowlist is the positive half:
// a locked plan whose argv carries no locked-refused form passes the lock
// gate and reaches the argv allowlist. The argv stays inside the
// locked-safe set but is malformed for the allowlist (a dangling
// --model, admitted by the lock walker as malformed-not-widening),
// which refuses with its own code — proving the lock gate admitted
// rather than fired.
func TestClaudeHostLaunchPassesLockedNativeToAllowlist(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	factory := newTestClaudeFactory(t)
	plan := fixture.plan()
	plan.LaunchVariants.ManagedHost.Argv = []string{"--model"}
	plan.Sources = lockedClaudeDaemonSources(t)
	launch := ClaudePreparedLaunch{
		Contract: ClaudeSessionLaunchContract,
		Plan:     plan,
		Rows:     32,
		Cols:     100,
	}
	payload := LaunchSpec{Executable: plan.Executable, Cwd: plan.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &launch}
	host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-locked-native",
		Launch:    payload,
	})
	if host != nil {
		stopClaudeHost(t, host)
	}
	var public *PublicError
	if !errors.As(err, &public) || public.Code != ClaudeErrorLaunchArgsBlocked {
		t.Fatalf("Launch = %v; want the allowlist refusal, proving the lock gate passed", err)
	}
	if lines := fixture.argvLines(t); len(lines) != 0 {
		t.Fatalf("provider executable ran despite allowlist refusal: %v", lines)
	}
}

// TestRefuseLockedClaudeLaunch pins the daemon gate's decision matrix:
// only a decoded locked claude_code snapshot with a refused tail flag
// refuses with the lock code — naming the flag — and any Curator
// evidence that cannot be verified refuses fail-closed with the
// unreadable code, before the allowlist and the probe. Legacy plans
// (no Curator sources at all), permissionless fragments, unlocked
// grants, foreign environments, and native argv all pass through to the
// caller's next gate.
func TestRefuseLockedClaudeLaunch(t *testing.T) {
	locked := lockedClaudeDaemonProvenance()
	encode := func(p agentic.CuratorContextProvenance) []LaunchPlanSource {
		raw, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		return []LaunchPlanSource{{Kind: CuratorProvenanceSourceKind, Path: string(raw)}}
	}
	encodeRaw := func(raw string) []LaunchPlanSource {
		return []LaunchPlanSource{{Kind: CuratorProvenanceSourceKind, Path: raw}}
	}
	lockedRaw, err := json.Marshal(locked)
	if err != nil {
		t.Fatal(err)
	}
	conflictingPermissionsRaw := strings.Replace(
		string(lockedRaw),
		`"permissions":{"mode":"native","locked":true,"source":"global"}`,
		`"permissions":{"mode":"native","locked":true,"source":"global","locked":false,"source":"profile"}`,
		1,
	)
	if conflictingPermissionsRaw == string(lockedRaw) {
		t.Fatal("permissions anchor missing: cannot splice the conflicting duplicate")
	}
	if strings.Count(string(lockedRaw), `"managed_home":"/managed/home"`) != 1 {
		t.Fatal("managed_home anchor missing or ambiguous: cannot splice the repeated member")
	}
	repeatedTopMemberRaw := strings.Replace(
		string(lockedRaw),
		`"managed_home":"/managed/home"`,
		`"managed_home":"/managed/home","managed_home":"/managed/home"`,
		1,
	)
	unlocked := locked
	unlocked.Fragment.Permissions = &agentic.CuratorPermissions{
		Mode: agentic.CuratorPermissionNative, Locked: false, Source: agentic.CuratorPermissionSourceProfile,
	}
	permissionless := locked
	permissionless.Fragment.Permissions = nil
	permissionless.Fragment.Revision = agentic.CuratorLaunchFragmentV1
	foreign := locked
	foreign.Fragment.Environment = "codex_cli"
	foreign.Fragment.Env = map[string]string{"CODEX_HOME": "/managed/home"}
	foreign.ManagedHomeVariable = "CODEX_HOME"
	inconsistent := locked
	inconsistent.LockSHA256 = strings.Repeat("b", 64)
	spoofedMCP := lockedClaudeDaemonProvenanceWithForeignMCP("/managed/home/extra.json")
	curatorSources := []LaunchPlanSource{
		{Kind: "curator-fragment", Path: "/managed/home", Scope: "profile=test"},
		{Kind: "curator-fragment-pins", Path: "lock=" + strings.Repeat("a", 64), Scope: "digest=d"},
	}
	for _, row := range []struct {
		name    string
		sources []LaunchPlanSource
		argv    []string
		want    string
		flag    string
	}{
		{"locked bypass refuses", encode(locked), []string{"--dangerously-skip-permissions"}, ClaudeFragmentPermissionsLocked, "--dangerously-skip-permissions"},
		{"locked enabling refuses", encode(locked), []string{"--allow-dangerously-skip-permissions"}, ClaudeFragmentPermissionsLocked, "--allow-dangerously-skip-permissions"},
		{"locked bypass mode refuses", encode(locked), []string{"--permission-mode", "bypassPermissions"}, ClaudeFragmentPermissionsLocked, "--permission-mode"},
		{"locked tool grant refuses", encode(locked), []string{"--allowedTools", "Bash(*)", "Edit"}, ClaudeFragmentPermissionsLocked, "--allowedTools"},
		{"locked settings refuses", encode(locked), []string{"--settings", "/tmp/s.json"}, ClaudeFragmentPermissionsLocked, "--settings"},
		{"locked setting sources refuses", encode(locked), []string{"--setting-sources="}, ClaudeFragmentPermissionsLocked, "--setting-sources"},
		{"locked mcp config refuses", encode(locked), []string{"--mcp-config", "/tmp/evil.json"}, ClaudeFragmentPermissionsLocked, "--mcp-config"},
		{"spoofed mcp emission refuses", encode(spoofedMCP), []string{"--model", "x", "--mcp-config", "/managed/home/extra.json", "--strict-mcp-config"}, ClaudeFragmentPermissionsLocked, "--mcp-config"},
		{"locked add dir refuses", encode(locked), []string{"--add-dir", "/tmp/w"}, ClaudeFragmentPermissionsLocked, "--add-dir"},
		{"locked agent refuses", encode(locked), []string{"--agent", "explore"}, ClaudeFragmentPermissionsLocked, "--agent"},
		{"locked unknown mode refuses", encode(locked), []string{"--permission-mode=ultra"}, ClaudeFragmentPermissionsLocked, "--permission-mode"},
		{"locked plan mode refuses", encode(locked), []string{"--permission-mode", "plan"}, ClaudeFragmentPermissionsLocked, "--permission-mode"},
		{"locked dangling mode refuses", encode(locked), []string{"--permission-mode"}, ClaudeFragmentPermissionsLocked, "--permission-mode"},
		{"locked positional refuses", encode(locked), []string{"--model", "x", "mcp"}, ClaudeFragmentPermissionsLocked, "mcp"},
		{"locked file refuses", encode(locked), []string{"--file", "file_abc:.claude/settings.json"}, ClaudeFragmentPermissionsLocked, "--file"},
		{"locked brief refuses", encode(locked), []string{"--brief"}, ClaudeFragmentPermissionsLocked, "--brief"},
		{"locked verbose refuses", encode(locked), []string{"--verbose"}, ClaudeFragmentPermissionsLocked, "--verbose"},
		{"locked manual admits", encode(locked), []string{"--permission-mode", "manual"}, "", ""},
		{"locked native admits", encode(locked), []string{"--model", "x"}, "", ""},
		{"locked empty admits", encode(locked), nil, "", ""},
		{"unlocked bypass passes", encode(unlocked), []string{"--dangerously-skip-permissions"}, "", ""},
		{"unlocked settings passes", encode(unlocked), []string{"--settings", "/tmp/s.json"}, "", ""},
		{"permissionless bypass passes", encode(permissionless), []string{"--dangerously-skip-permissions"}, "", ""},
		{"foreign bypass passes", encode(foreign), []string{"--dangerously-skip-permissions"}, "", ""},
		{"legacy bypass passes", nil, []string{"--dangerously-skip-permissions"}, "", ""},
		{"legacy empty sources pass", []LaunchPlanSource{}, []string{"--dangerously-skip-permissions"}, "", ""},
		{"malformed JSON refuses", encodeRaw("{bad"), []string{"--dangerously-skip-permissions"}, ClaudeErrorCuratorProvenanceUnreadable, ""},
		{"malformed JSON refuses native argv", encodeRaw("{bad"), []string{"--model", "x"}, ClaudeErrorCuratorProvenanceUnreadable, ""},
		{"trailing data refuses", encodeRaw(string(lockedRaw) + " {}"), []string{"--model", "x"}, ClaudeErrorCuratorProvenanceUnreadable, ""},
		{"duplicate provenance refuses", append(encode(locked), encode(locked)...), []string{"--model", "x"}, ClaudeErrorCuratorProvenanceUnreadable, ""},
		{"curator sources without provenance refuse", curatorSources, []string{"--model", "x"}, ClaudeErrorCuratorProvenanceUnreadable, ""},
		{"inconsistent snapshot refuses", encode(inconsistent), []string{"--model", "x"}, ClaudeErrorCuratorProvenanceUnreadable, ""},
		{"conflicting permissions duplicate refuses", encodeRaw(conflictingPermissionsRaw), []string{"--model", "x"}, ClaudeErrorCuratorProvenanceUnreadable, ""},
		{"repeated top-level member refuses", encodeRaw(repeatedTopMemberRaw), []string{"--model", "x"}, ClaudeErrorCuratorProvenanceUnreadable, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			plan := &PrimarySessionLaunchPlan{
				Sources:        row.sources,
				LaunchVariants: LaunchVariants{ManagedHost: LaunchArgv{Kind: "claude-pty", Argv: row.argv}},
			}
			err := refuseLockedClaudeLaunch(plan)
			if row.want == "" {
				if err != nil {
					t.Fatalf("refuseLockedClaudeLaunch = %v, want nil", err)
				}
				return
			}
			var public *PublicError
			if !errors.As(err, &public) || public.Code != row.want {
				t.Fatalf("refuseLockedClaudeLaunch = %v, want the %s refusal", err, row.want)
			}
			if row.flag != "" {
				want := `locked-tail flag "` + row.flag + `"`
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("refusal = %q, want it to name %q", err, want)
				}
			}
			if row.want == ClaudeErrorCuratorProvenanceUnreadable {
				for _, want := range []string{"provenance snapshot cannot be verified", "unknown lock state"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("refusal = %q, want it to name %q", err, want)
					}
				}
				if strings.Contains(err.Error(), "/managed/home") {
					t.Fatalf("refusal = %q, want no snapshot contents in the public message", err)
				}
			}
		})
	}
	if err := refuseLockedClaudeLaunch(nil); err != nil {
		t.Fatalf("refuseLockedClaudeLaunch(nil) = %v, want nil", err)
	}
}

// lockedClaudeDaemonProvenanceWithMCP is the locked snapshot carrying the
// fragment's own curator-MCP channel, so the test argv can carry the
// module's deterministic emission for it.
func lockedClaudeDaemonProvenanceWithMCP(mcpPath string) agentic.CuratorContextProvenance {
	provenance := lockedClaudeDaemonProvenance()
	provenance.Fragment.MCP = &agentic.CuratorMCPContext{
		Path: mcpPath,
		Channels: []agentic.CuratorChannelDescriptor{{
			Kind:     agentic.CuratorDescriptorFlag,
			Flag:     "--mcp-config",
			Argument: agentic.CuratorArgumentPath,
			With:     []string{"--strict-mcp-config"},
		}},
	}
	return provenance
}

// TestClaudeHostLaunchPassesLockedCuratorMCPEmission proves the daemon
// gate skips the module's own curator-MCP emission: a locked plan whose
// argv carries exactly "--mcp-config <provenance path>
// --strict-mcp-config" plus safe flags passes the lock gate and reaches
// the argv allowlist, which refuses the dangling trailing --model with
// its own code.
func TestClaudeHostLaunchPassesLockedCuratorMCPEmission(t *testing.T) {
	home := t.TempDir()
	mcpPath := filepath.Join(home, "mcp.json")
	provenance := lockedClaudeDaemonProvenanceWithMCP(mcpPath)
	provenance.ManagedHome = home
	provenance.Fragment.Env["CLAUDE_CONFIG_DIR"] = home
	encoded, err := json.Marshal(provenance)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newFakeClaudeFixture(t)
	factory := newTestClaudeFactory(t)
	plan := fixture.plan()
	plan.LaunchVariants.ManagedHost.Argv = []string{
		"--model", "x", "--mcp-config", mcpPath, "--strict-mcp-config",
		"--disallowedTools=AskUserQuestion", "--model",
	}
	plan.Sources = []LaunchPlanSource{{Kind: CuratorProvenanceSourceKind, Path: string(encoded)}}
	launch := ClaudePreparedLaunch{
		Contract: ClaudeSessionLaunchContract,
		Plan:     plan,
		Rows:     32,
		Cols:     100,
	}
	payload := LaunchSpec{Executable: plan.Executable, Cwd: plan.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &launch}
	host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-locked-mcp-emission",
		Launch:    payload,
	})
	if host != nil {
		stopClaudeHost(t, host)
	}
	var public *PublicError
	if !errors.As(err, &public) || public.Code != ClaudeErrorLaunchArgsBlocked {
		t.Fatalf("Launch = %v; want the allowlist refusal, proving the lock gate skipped the curator emission", err)
	}
	if lines := fixture.argvLines(t); len(lines) != 0 {
		t.Fatalf("provider executable ran despite allowlist refusal: %v", lines)
	}
}

// lockedClaudeDaemonProvenanceWithForeignMCP is the r4 spoof shape: a
// locked snapshot whose MCP path matches the argv triple but whose
// channel descriptor is NOT the module's declared claude emission —
// the argument kind is contents, which the generic descriptor grammar
// accepts and the vendored claude plugin refuses (it requires a path
// argument beside --mcp-config with the single --strict-mcp-config
// companion). The module would never emit the triple for this
// descriptor, so a plan carrying it is forged or stale.
func lockedClaudeDaemonProvenanceWithForeignMCP(mcpPath string) agentic.CuratorContextProvenance {
	provenance := lockedClaudeDaemonProvenance()
	provenance.Fragment.MCP = &agentic.CuratorMCPContext{
		Path: mcpPath,
		Channels: []agentic.CuratorChannelDescriptor{{
			Kind:     agentic.CuratorDescriptorFlag,
			Flag:     "--mcp-config",
			Argument: agentic.CuratorArgumentContents,
			With:     []string{"--strict-mcp-config"},
		}},
	}
	return provenance
}

// TestClaudeHostLaunchRefusesSpoofedCuratorMCPEmission is the r4
// daemon-mcp-emission-spoof negative: a matching MCP path alone never
// enables the emission skip — the snapshot's descriptor must be exactly
// the module's own declared claude emission. The argv triple over a
// non-module descriptor refuses with the lock code naming --mcp-config,
// before the allowlist and the probe.
func TestClaudeHostLaunchRefusesSpoofedCuratorMCPEmission(t *testing.T) {
	const mcpPath = "/managed/home/extra.json"
	provenance := lockedClaudeDaemonProvenanceWithForeignMCP(mcpPath)
	encoded, err := json.Marshal(provenance)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newFakeClaudeFixture(t)
	factory := newTestClaudeFactory(t)
	plan := fixture.plan()
	plan.LaunchVariants.ManagedHost.Argv = []string{
		"--model", "x", "--mcp-config", mcpPath, "--strict-mcp-config",
		"--disallowedTools=AskUserQuestion",
	}
	plan.Sources = []LaunchPlanSource{{Kind: CuratorProvenanceSourceKind, Path: string(encoded)}}
	launch := ClaudePreparedLaunch{
		Contract: ClaudeSessionLaunchContract,
		Plan:     plan,
		Rows:     32,
		Cols:     100,
	}
	payload := LaunchSpec{Executable: plan.Executable, Cwd: plan.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &launch}
	host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
		SessionID: "SES-spoofed-mcp-emission",
		Launch:    payload,
	})
	if host != nil {
		stopClaudeHost(t, host)
	}
	requireClaudeDaemonLockRefusal(t, err)
	if want := `locked-tail flag "--mcp-config"`; !strings.Contains(err.Error(), want) {
		t.Fatalf("refusal = %q, want it to name %q", err, want)
	}
	if lines := fixture.argvLines(t); len(lines) != 0 {
		t.Fatalf("provider executable ran despite spoofed-emission refusal: %v", lines)
	}
}

// TestClaudeHostLaunchRefusesUnverifiableProvenance is the r3 F2
// fresh-launch negative: every malformed provenance shape refuses with
// the unreadable code before the allowlist and the probe — the argv is
// natively safe, so the refusal is about the evidence, not the tail —
// and the provider executable never runs.
func TestClaudeHostLaunchRefusesUnverifiableProvenance(t *testing.T) {
	locked := lockedClaudeDaemonProvenance()
	encoded, err := json.Marshal(locked)
	if err != nil {
		t.Fatal(err)
	}
	inconsistent := locked
	inconsistent.LockSHA256 = strings.Repeat("b", 64)
	inconsistentRaw, err := json.Marshal(inconsistent)
	if err != nil {
		t.Fatal(err)
	}
	provenanceSource := func(raw string) LaunchPlanSource {
		return LaunchPlanSource{Kind: CuratorProvenanceSourceKind, Path: raw}
	}
	// duplicateMember splices a repeated object member into the valid
	// snapshot. The splice anchors on exact marshalled substrings and
	// fails the test when an anchor is missing or ambiguous, so a
	// provenance shape change breaks loudly instead of testing
	// something else. Without the duplicate-member check every
	// spliced snapshot below still validates (Go keeps the last
	// member), which is what makes these rows prove the check.
	duplicateMember := func(member, repeat string) string {
		t.Helper()
		if strings.Count(string(encoded), member) != 1 {
			t.Fatalf("anchor %q occurs %d times, want exactly once", member, strings.Count(string(encoded), member))
		}
		return strings.Replace(string(encoded), member, member+","+repeat, 1)
	}
	conflictingPermissions := strings.Replace(
		string(encoded),
		`"permissions":{"mode":"native","locked":true,"source":"global"}`,
		`"permissions":{"mode":"native","locked":true,"source":"global","locked":false,"source":"profile"}`,
		1,
	)
	if conflictingPermissions == string(encoded) {
		t.Fatal("permissions anchor missing: cannot splice the conflicting duplicate")
	}
	// r5 case-fold aliases: the decoder maps each of these onto the
	// same field as the locked grant and keeps the last, so without
	// the fold-aware scan every spliced snapshot below validates as
	// UNLOCKED and launches. The long-s spelling uses an explicit
	// escape: the attack is invisible in literal form.
	caseAliasPermissions := strings.Replace(
		string(encoded),
		`"permissions":{"mode":"native","locked":true,"source":"global"}`,
		`"permissions":{"mode":"native","locked":true,"source":"global"},"Permissions":{"mode":"native","locked":false,"source":"profile"}`,
		1,
	)
	if caseAliasPermissions == string(encoded) {
		t.Fatal("permissions anchor missing: cannot splice the case-alias duplicate")
	}
	longSAliasPermissions := strings.Replace(
		string(encoded),
		`"permissions":{"mode":"native","locked":true,"source":"global"}`,
		`"permissions":{"mode":"native","locked":true,"source":"global"},"permi`+"\u017f"+`sions":{"mode":"native","locked":false,"source":"profile"}`,
		1,
	)
	if longSAliasPermissions == string(encoded) {
		t.Fatal("permissions anchor missing: cannot splice the long-s alias duplicate")
	}
	innerCaseVariantPermissions := strings.Replace(
		string(encoded),
		`"permissions":{"mode":"native","locked":true,"source":"global"}`,
		`"permissions":{"mode":"native","locked":true,"source":"global","Locked":false}`,
		1,
	)
	if innerCaseVariantPermissions == string(encoded) {
		t.Fatal("permissions anchor missing: cannot splice the inner case variant")
	}
	for _, row := range []struct {
		name    string
		sources []LaunchPlanSource
	}{
		{"missing provenance", []LaunchPlanSource{
			{Kind: "curator-fragment", Path: "/managed/home", Scope: "profile=test"},
			{Kind: "curator-fragment-pins", Path: "lock=" + strings.Repeat("a", 64), Scope: "digest=d"},
		}},
		{"duplicated provenance", []LaunchPlanSource{provenanceSource(string(encoded)), provenanceSource(string(encoded))}},
		{"malformed JSON", []LaunchPlanSource{provenanceSource("{bad")}},
		{"trailing data", []LaunchPlanSource{provenanceSource(string(encoded) + " {}")}},
		{"inconsistent snapshot", []LaunchPlanSource{provenanceSource(string(inconsistentRaw))}},
		// r4 ambiguous provenance: a duplicate member anywhere in the
		// snapshot refuses, even when last-wins would validate. The
		// conflicting permissions duplicate is the r3 repro: without
		// the check it validates as UNLOCKED and admits bypass argv.
		{"conflicting permissions duplicate", []LaunchPlanSource{provenanceSource(conflictingPermissions)}},
		{"repeated fragment member", []LaunchPlanSource{provenanceSource(duplicateMember(`"environment":"claude_code"`, `"environment":"claude_code"`))}},
		{"repeated top-level member", []LaunchPlanSource{provenanceSource(duplicateMember(`"managed_home":"/managed/home"`, `"managed_home":"/managed/home"`))}},
		// r5 case-fold aliases: without the fold-aware scan the
		// decoder folds each fragment-level alias onto the locked
		// grant, the unlocked repeat wins, and the launch
		// proceeds. The inner variant already refuses through
		// the permissions map decoder (unknown member); the scan
		// now refuses it first with the duplicate cause (pinned
		// in TestExtractCuratorProvenanceRefusesFoldedPermissionsMember).
		{"permissions case-alias conflict", []LaunchPlanSource{provenanceSource(caseAliasPermissions)}},
		{"permissions long-s alias conflict", []LaunchPlanSource{provenanceSource(longSAliasPermissions)}},
		{"permissions inner case variant", []LaunchPlanSource{provenanceSource(innerCaseVariantPermissions)}},
	} {
		t.Run(row.name, func(t *testing.T) {
			fixture := newFakeClaudeFixture(t)
			factory := newTestClaudeFactory(t)
			plan := fixture.plan()
			plan.LaunchVariants.ManagedHost.Argv = []string{"--model", "x"}
			plan.Sources = row.sources
			launch := ClaudePreparedLaunch{
				Contract: ClaudeSessionLaunchContract,
				Plan:     plan,
				Rows:     32,
				Cols:     100,
			}
			payload := LaunchSpec{Executable: plan.Executable, Cwd: plan.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &launch}
			host, err := factory.Launch(context.Background(), ProviderLaunchRequest{
				SessionID: "SES-unverifiable-provenance",
				Launch:    payload,
			})
			if host != nil {
				stopClaudeHost(t, host)
			}
			requireClaudeDaemonProvenanceRefusal(t, err)
			if lines := fixture.argvLines(t); len(lines) != 0 {
				t.Fatalf("provider executable ran despite provenance refusal for %s: %v", row.name, lines)
			}
		})
	}
}
