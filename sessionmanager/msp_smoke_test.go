package sessionmanager

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	museSmokeBinaryEnv       = "TASK_BOARD_MUSE_SMOKE_BIN"
	museSmokeExpectedVersion = "Muse Code 1.4.1 (1.4.1-R4503.1)"
	museSmokeExpectedSHA256  = "1b9ada5f943cd44d5aacd2380be17203798a953e993589f8654352cf524a00c7"
	museSmokeBinaryName      = "muse-bin-1.4.1-R4503.1"
	// Second qualified smoke identity (decision B): the 1.4.2-R4684.1
	// ARM64 release qualified by the pinned export qualification. The smoke keeps the
	// 1.4.1 copy and accepts 1.4.2 alongside it; version, digest, and
	// name always match as one bound triple, never cross-release.
	museSmokeExpectedVersion142 = "Muse Code 1.4.2 (1.4.2-R4684.1)"
	museSmokeExpectedSHA256142  = "e9987ef4267a648dc1931c2836a23f6f16f8b4417368b126a810bece2abd308d"
	museSmokeBinaryName142      = "muse-bin-1.4.2-R4684.1"
)

func museSmokeBinaryFromEnvironment(getenv func(string) string) string {
	return strings.TrimSpace(getenv(museSmokeBinaryEnv))
}

func museSmokePathLooksPinned(path string) bool {
	switch filepath.Base(path) {
	case museSmokeBinaryName, museSmokeBinaryName142:
		return true
	default:
		return false
	}
}

func museSmokeIdentityMatches(version, digest string) bool {
	version = strings.TrimSpace(version)
	return version == museSmokeExpectedVersion && digest == museSmokeExpectedSHA256 ||
		version == museSmokeExpectedVersion142 && digest == museSmokeExpectedSHA256142
}

func museSmokeEnvironment(home string) []string {
	return []string{
		"HOME=" + home,
		"XDG_CONFIG_HOME=" + filepath.Join(home, ".config"),
		"XDG_DATA_HOME=" + filepath.Join(home, ".local", "share"),
		"XDG_STATE_HOME=" + filepath.Join(home, ".local", "state"),
		"XDG_CACHE_HOME=" + filepath.Join(home, ".cache"),
		"TMPDIR=" + filepath.Join(home, "tmp"),
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"LANG=C.UTF-8",
		"MUSE_NO_AUTO_UPDATE=1",
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestMuseMSPSmokeRequiresExplicitBinary(t *testing.T) {
	if got := museSmokeBinaryFromEnvironment(func(string) string { return "" }); got != "" {
		t.Fatalf("unset opt-in selected a binary %q", got)
	}
	if got := museSmokeBinaryFromEnvironment(func(string) string { return "  \t" }); got != "" {
		t.Fatalf("blank opt-in selected a binary %q", got)
	}
}

func TestMuseMSPSmokeRejectsAutoUpdatingLauncherPath(t *testing.T) {
	if museSmokePathLooksPinned("/Users/example/.local/bin/muse") {
		t.Fatal("auto-updating launcher passed the pinned-binary path check")
	}
	if !museSmokePathLooksPinned("/tmp/bin/" + museSmokeBinaryName) {
		t.Fatal("versioned binary name did not pass the pinned-binary path check")
	}
}

func TestMuseMSPSmokeRejectsUnpinnedIdentity(t *testing.T) {
	if museSmokeIdentityMatches("Muse Code 1.3.0", museSmokeExpectedSHA256) {
		t.Fatal("wrong version passed smoke identity check")
	}
	if museSmokeIdentityMatches(museSmokeExpectedVersion, "sha256:wrong") {
		t.Fatal("wrong binary digest passed smoke identity check")
	}
	if !museSmokeIdentityMatches(museSmokeExpectedVersion, museSmokeExpectedSHA256) {
		t.Fatal("pinned version and digest did not pass smoke identity check")
	}
}

func TestMuseMSPSmokeAcceptsSecondQualifiedIdentity(t *testing.T) {
	if !museSmokePathLooksPinned("/tmp/bin/" + museSmokeBinaryName142) {
		t.Fatal("qualified 1.4.2 binary name did not pass the pinned-binary path check")
	}
	if !museSmokeIdentityMatches(museSmokeExpectedVersion142, museSmokeExpectedSHA256142) {
		t.Fatal("qualified 1.4.2 version and digest did not pass smoke identity check")
	}
	if museSmokeIdentityMatches(museSmokeExpectedVersion, museSmokeExpectedSHA256142) {
		t.Fatal("cross-release 1.4.1 version with 1.4.2 digest passed smoke identity check")
	}
	if museSmokeIdentityMatches(museSmokeExpectedVersion142, museSmokeExpectedSHA256) {
		t.Fatal("cross-release 1.4.2 version with 1.4.1 digest passed smoke identity check")
	}
}

func TestMuseMSPSmokeEnvironmentIsIsolatedAndScrubbed(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	env := museSmokeEnvironment(home)
	seen := make(map[string]string, len(env))
	for _, entry := range env {
		name, _, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed environment entry %q", entry)
		}
		value := entry[len(name)+1:]
		seen[name] = value
		upper := strings.ToUpper(name)
		if strings.HasPrefix(upper, "TASK_BOARD_") || strings.HasPrefix(upper, "CODEX_") ||
			strings.HasPrefix(upper, "CLAUDE") || strings.HasSuffix(upper, "_TOKEN") ||
			strings.HasSuffix(upper, "_KEY") {
			t.Fatalf("forbidden inherited environment variable %q", name)
		}
	}
	for _, required := range []string{"HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME", "MUSE_NO_AUTO_UPDATE"} {
		if _, ok := seen[required]; !ok {
			t.Fatalf("isolated smoke environment omits %s", required)
		}
	}
	for name, want := range map[string]string{
		"HOME":                home,
		"XDG_CONFIG_HOME":     filepath.Join(home, ".config"),
		"XDG_DATA_HOME":       filepath.Join(home, ".local", "share"),
		"XDG_STATE_HOME":      filepath.Join(home, ".local", "state"),
		"XDG_CACHE_HOME":      filepath.Join(home, ".cache"),
		"MUSE_NO_AUTO_UPDATE": "1",
	} {
		if got := seen[name]; got != want {
			t.Fatalf("smoke environment %s = %q, want %q", name, got, want)
		}
	}
	for _, forbidden := range []string{"TASK_BOARD_RUN_ID", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "API_TOKEN", "SIGNING_KEY"} {
		if _, ok := seen[forbidden]; ok {
			t.Fatalf("smoke environment contains forbidden variable %s", forbidden)
		}
	}
}

// TestMuseMSPSmoke is an opt-in integration test. It starts only the explicit
// versioned binary supplied in TASK_BOARD_MUSE_SMOKE_BIN and gives that process
// a fresh HOME and XDG tree with no inherited task-board, agent, token, or key
// variables.
func TestMuseMSPSmoke(t *testing.T) {
	binary := museSmokeBinaryFromEnvironment(os.Getenv)
	if binary == "" {
		t.Skipf("set %s to a copied qualified Muse binary (1.4.1-R4503.1 or 1.4.2-R4684.1) to run the offline smoke", museSmokeBinaryEnv)
	}
	if !museSmokePathLooksPinned(binary) {
		t.Fatalf("smoke binary path must end in %q or %q; refusing to invoke %q", museSmokeBinaryName, museSmokeBinaryName142, binary)
	}
	digest, err := museSmokeFileSHA256(binary)
	if err != nil {
		t.Fatalf("hash pinned Muse binary: %v", err)
	}
	if digest != museSmokeExpectedSHA256 && digest != museSmokeExpectedSHA256142 {
		t.Fatalf("pinned Muse binary sha256 = %s, want %s or %s; refusing to start it", digest, museSmokeExpectedSHA256, museSmokeExpectedSHA256142)
	}

	root := t.TempDir()
	home := filepath.Join(root, "home")
	workspace := filepath.Join(root, "workspace")
	for _, dir := range []string{
		home,
		filepath.Join(home, ".config"),
		filepath.Join(home, ".local", "share"),
		filepath.Join(home, ".local", "state"),
		filepath.Join(home, ".cache"),
		filepath.Join(home, "tmp"),
		workspace,
	} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("create isolated smoke directory: %v", err)
		}
	}
	env := museSmokeEnvironment(home)
	stderr := &lockedBuffer{}
	versionCmd := exec.Command(binary, "--version")
	versionCmd.Env = env
	versionOutput, err := versionCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("version command %q --version failed: %v; output=%s", binary, err, strings.TrimSpace(string(versionOutput)))
	}
	version := strings.TrimSpace(string(versionOutput))
	if !museSmokeIdentityMatches(version, digest) {
		t.Fatalf("smoke binary identity mismatch: version=%q sha256=%s", version, digest)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "serve")
	command.Env = env
	command.Stderr = stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("create Muse stdin: %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("create Muse stdout: %v", err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start pinned Muse command %q serve --no-session-log: %v", binary, err)
	}
	client := NewMSPClient(stdout, stdin)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := client.Shutdown(shutdownCtx); err != nil && !errors.Is(err, ErrMSPServerExited) {
			t.Errorf("shutdown Muse smoke server: %v; stderr=%s", err, strings.TrimSpace(stderr.String()))
			if command.Process != nil {
				_ = command.Process.Kill()
			}
		}
		waited := make(chan error, 1)
		go func() { waited <- command.Wait() }()
		select {
		case waitErr := <-waited:
			if waitErr != nil && ctx.Err() == nil {
				t.Errorf("Muse smoke server exit: %v; stderr=%s", waitErr, strings.TrimSpace(stderr.String()))
			}
		case <-time.After(5 * time.Second):
			if command.Process != nil {
				_ = command.Process.Kill()
			}
			<-waited
			t.Error("Muse smoke server did not exit after stdin EOF")
		}
	})

	t.Logf("command: %q --version", binary)
	t.Logf("command: %q serve", binary)
	t.Logf("environment: MUSE_NO_AUTO_UPDATE=1; isolated HOME and XDG_CONFIG_HOME/XDG_DATA_HOME/XDG_STATE_HOME/XDG_CACHE_HOME; inherited environment scrubbed")
	t.Logf("binary version: %s", version)
	t.Logf("binary sha256: %s", digest)

	experimental := false
	initialized, err := client.Initialize(ctx, MSPInitializeParams{
		ClientInfo:   MSPClientInfo{Name: "task_board", Version: "msp-smoke"},
		Capabilities: &MSPClientCapabilities{ExperimentalAPI: &experimental},
	})
	if err != nil {
		t.Fatalf("initialize: %v; stderr=%s", err, strings.TrimSpace(stderr.String()))
	}
	t.Logf("initialize: schemaVersion=%d server=%s %s", initialized.Schema.Version, initialized.ServerInfo.Name, initialized.ServerInfo.Version)

	provider := "echo"
	started, err := client.SessionStart(ctx, MSPSessionStartParams{
		CommandID:     museSmokeUUIDv7(t),
		ProviderID:    &provider,
		WorkspaceRoot: workspace,
	})
	if err != nil {
		t.Fatalf("session/start with offline echo provider: %v; stderr=%s", err, strings.TrimSpace(stderr.String()))
	}
	sessionID := started.Session.SessionID
	if sessionID == "" {
		t.Fatal("session/start returned an empty sessionId")
	}
	t.Logf("session/start: sessionId=%s provider=echo", sessionID)

	objective := "Pinned native protocol smoke fixture"
	setResult, err := client.GoalSet(ctx, MSPGoalSetParams{
		CommandID: museSmokeUUIDv7(t), SessionID: sessionID, Objective: objective,
	})
	if err != nil {
		t.Fatalf("goal/set: %v; stderr=%s", err, strings.TrimSpace(stderr.String()))
	}
	t.Logf("goal/set: status=%s", setResult.Status)
	setNotification, err := waitForMSPGoalChange(t, ctx, client.Notifications(), sessionID, false)
	if err != nil {
		t.Fatalf("observe goalChanged after goal/set: %v; stderr=%s", err, strings.TrimSpace(stderr.String()))
	}
	if setNotification.Goal.Value.Objective != objective {
		t.Fatalf("goalChanged objective = %q, want %q", setNotification.Goal.Value.Objective, objective)
	}
	t.Logf("notification: session/goalChanged cursor=%s objective=%q", setNotification.ViewCursor, setNotification.Goal.Value.Objective)

	clearResult, err := client.GoalClear(ctx, MSPGoalClearParams{
		CommandID: museSmokeUUIDv7(t), SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("goal/clear: %v; stderr=%s", err, strings.TrimSpace(stderr.String()))
	}
	t.Logf("goal/clear: status=%s", clearResult.Status)
	clearNotification, err := waitForMSPGoalChange(t, ctx, client.Notifications(), sessionID, true)
	if err != nil {
		t.Fatalf("observe cleared goalChanged: %v; stderr=%s", err, strings.TrimSpace(stderr.String()))
	}
	t.Logf("notification: session/goalChanged cursor=%s explicitNull=%t", clearNotification.ViewCursor, clearNotification.Goal.Present && clearNotification.Goal.Null)
}

func waitForMSPGoalChange(t *testing.T, ctx context.Context, notifications <-chan MSPNotification, sessionID string, cleared bool) (*MSPSessionGoalChangedNotification, error) {
	t.Helper()
	for {
		select {
		case notification, ok := <-notifications:
			if !ok {
				return nil, fmt.Errorf("MSP notification stream closed: %w", ErrMSPServerExited)
			}
			t.Logf("notification observed while waiting for goalChanged: %s", notification.MSPMethod())
			goal, ok := notification.(*MSPSessionGoalChangedNotification)
			if !ok || goal.SessionID != sessionID || !goal.Goal.Present {
				continue
			}
			if cleared && goal.Goal.Null {
				return goal, nil
			}
			if !cleared && !goal.Goal.Null {
				return goal, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func museSmokeUUIDv7(t *testing.T) string {
	t.Helper()
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("generate UUIDv7: %v", err)
	}
	millis := uint64(time.Now().UnixMilli())
	value[0] = byte(millis >> 40)
	value[1] = byte(millis >> 32)
	value[2] = byte(millis >> 24)
	value[3] = byte(millis >> 16)
	value[4] = byte(millis >> 8)
	value[5] = byte(millis)
	value[6] = (value[6] & 0x0f) | 0x70
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(value[0:4]), hex.EncodeToString(value[4:6]),
		hex.EncodeToString(value[6:8]), hex.EncodeToString(value[8:10]),
		hex.EncodeToString(value[10:16]))
}

func museSmokeFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
