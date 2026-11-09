// claude-notice-child is first-slice dogfood glue for one real Claude Code
// run kept by swarma-session-host. It is the runner's one direct child and
// speaks the existing json-fixture envelope, so the unmodified host can keep
// it with `-adapter json-fixture`. For each notice it resolves the reference
// to a prompt file inside its admitted workspace, runs one bounded
// `claude -p --output-format json` one-shot with the prompt on stdin and
// output captured to files (never to the runner's retained stdout), and
// replies `applied` only after claude exits zero, returns a result for the
// exact preallocated session with is_error false and no permission denials,
// and the receipt is written.
//
// It is not a Claude protocol adapter: it does not speak stream-json to a
// long-lived claude process, keep a continuing session across notices,
// grant admission, answer permission prompts, or prove cleanup of claude's
// own descendants. Each notice is a fresh ephemeral session
// (--no-session-persistence). The runner's stop receipt covers this child;
// this child forwards TERM to the process group it created for claude and
// reaps it before exiting.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
)

const (
	maxPromptBytes  = 1 << 20
	maxCaptureBytes = 16 << 20
	termGrace       = time.Second
)

type config struct {
	workspace      string
	claudeBin      string
	model          string
	effort         string
	permissionMode string
	allowedTools   string
	expectedVer    string
	outDir         string
	timeout        time.Duration
}

func loadConfig(getenv func(string) string, cwd string) (config, error) {
	cfg := config{
		workspace:      getenv("CLAUDE_CHILD_WORKSPACE"),
		claudeBin:      getenv("CLAUDE_CHILD_BIN"),
		model:          getenv("CLAUDE_CHILD_MODEL"),
		effort:         getenv("CLAUDE_CHILD_EFFORT"),
		permissionMode: getenv("CLAUDE_CHILD_PERMISSION_MODE"),
		allowedTools:   getenv("CLAUDE_CHILD_ALLOWED_TOOLS"),
		expectedVer:    getenv("CLAUDE_CHILD_EXPECTED_VERSION"),
		outDir:         getenv("CLAUDE_CHILD_OUTDIR"),
		timeout:        600 * time.Second,
	}
	if cfg.workspace == "" {
		cfg.workspace = cwd
	}
	if cfg.permissionMode == "" {
		cfg.permissionMode = "manual"
	}
	if raw := getenv("CLAUDE_CHILD_TIMEOUT_SECONDS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 10 || n > 3600 {
			return config{}, errors.New("invalid_timeout_seconds")
		}
		cfg.timeout = time.Duration(n) * time.Second
	}
	if !filepath.IsAbs(cfg.workspace) || !filepath.IsAbs(cfg.claudeBin) {
		return config{}, errors.New("absolute_workspace_and_claude_bin_required")
	}
	// An explicit model is required: silently inheriting the CLI default
	// would let the effective selection drift from the admitted one.
	if cfg.model == "" {
		return config{}, errors.New("explicit_model_required")
	}
	switch cfg.effort {
	case "", "low", "medium", "high", "xhigh", "max":
	default:
		return config{}, errors.New("unsupported_effort")
	}
	// bypassPermissions is deliberately refused in this slice. Like the
	// Codex work-reference yolo policy, it needs a separately admitted
	// policy choice, never a shim default or an env side door.
	switch cfg.permissionMode {
	case "manual", "dontAsk", "acceptEdits", "plan", "auto":
	default:
		return config{}, errors.New("unsupported_permission_mode")
	}
	if cfg.outDir == "" {
		cfg.outDir = filepath.Join(cfg.workspace, "output")
	}
	if !filepath.IsAbs(cfg.outDir) {
		return config{}, errors.New("absolute_outdir_required")
	}
	return cfg, nil
}

// resolveReference confines a notice reference to a regular file under the
// workspace. References are identifiers for admitted prompt files, never
// arbitrary paths or prompt bodies.
func resolveReference(workspace, reference string) (string, error) {
	if reference == "" || filepath.IsAbs(reference) || strings.ContainsRune(reference, '\x00') {
		return "", errors.New("invalid_reference")
	}
	root, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return "", errors.New("workspace_unavailable")
	}
	joined := filepath.Join(root, filepath.Clean("/"+reference))
	resolved, err := filepath.EvalSymlinks(joined)
	if err != nil {
		return "", errors.New("reference_unavailable")
	}
	if resolved != root && !strings.HasPrefix(resolved, root+string(filepath.Separator)) {
		return "", errors.New("reference_escapes_workspace")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > maxPromptBytes {
		return "", errors.New("reference_not_bounded_regular_file")
	}
	return resolved, nil
}

// claudeArgs builds the one-shot argv. The prompt itself never appears in
// argv: it is fed on stdin, which `claude -p` accepts as the prompt when no
// positional prompt is given. Permission prompts are never answered by the
// host in this slice (--permission-prompts none): anything that would
// prompt is denied and surfaces as permission_denials in the result.
func claudeArgs(cfg config, sessionID string) []string {
	args := []string{"-p", "--output-format", "json",
		"--no-session-persistence",
		"--session-id", sessionID,
		"--model=" + cfg.model,
		"--permission-mode", cfg.permissionMode,
		"--permission-prompts", "none"}
	if cfg.effort != "" {
		args = append(args, "--effort", cfg.effort)
	}
	if cfg.allowedTools != "" {
		args = append(args, "--allowedTools="+cfg.allowedTools)
	}
	return args
}

func newSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// claudeResult is the parsed subset of the single `--output-format json`
// result object. Every other field (including the result text) is payload:
// it stays in the capture file and never enters host state or the receipt.
type claudeResult struct {
	Type              string `json:"type"`
	Subtype           string `json:"subtype"`
	IsError           *bool  `json:"is_error"`
	SessionID         string `json:"session_id"`
	NumTurns          int    `json:"num_turns"`
	TerminalReason    string `json:"terminal_reason"`
	PermissionDenials []struct {
		ToolName string `json:"tool_name"`
	} `json:"permission_denials"`
	Usage struct {
		InputTokens          uint64 `json:"input_tokens"`
		OutputTokens         uint64 `json:"output_tokens"`
		CacheReadInputTokens uint64 `json:"cache_read_input_tokens"`
	} `json:"usage"`
}

func parseResult(data []byte, expectedSession string) (claudeResult, string) {
	var r claudeResult
	// Token validation rejects duplicate keys at every depth, including
	// vendor extensions, before Go's last-key-wins unmarshaling can hide them.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := uniqueJSON(dec); err != nil {
		return r, "result_unparseable"
	}
	if _, err := dec.Token(); err != io.EOF {
		return r, "result_not_single_object"
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return r, "result_unparseable"
	}
	for _, key := range []string{"type", "session_id", "is_error", "permission_denials"} {
		raw, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return r, "result_required_evidence"
		}
	}
	// Decode exact map keys rather than struct-unmarshaling the whole object:
	// encoding/json's case-insensitive field matching must not let a vendor
	// extension such as IS_ERROR override the required is_error evidence.
	for key, target := range map[string]any{
		"type": &r.Type, "session_id": &r.SessionID, "is_error": &r.IsError,
		"permission_denials": &r.PermissionDenials, "subtype": &r.Subtype,
		"num_turns": &r.NumTurns, "terminal_reason": &r.TerminalReason, "usage": &r.Usage,
	} {
		if raw, ok := fields[key]; ok && json.Unmarshal(raw, target) != nil {
			return claudeResult{}, "result_unparseable"
		}
	}
	if r.Type != "result" || r.SessionID != expectedSession {
		return r, "result_session_mismatch"
	}
	// Observed on 2.1.317 without login: subtype is "success" while
	// is_error is true. Only is_error decides; subtype alone proves
	// nothing and is recorded, never trusted.
	if *r.IsError {
		return r, "claude_result_error"
	}
	if len(r.PermissionDenials) > 0 {
		return r, "permission_denied"
	}
	return r, ""
}

// uniqueJSON consumes one JSON value while refusing duplicate object keys.
func uniqueJSON(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return errors.New("duplicate_or_invalid_key")
			}
			seen[name] = true
			if err := uniqueJSON(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := uniqueJSON(dec); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected_delimiter")
	}
	_, err = dec.Token()
	return err
}

// readPrompt binds confinement, regular-file validation and bounded reading
// to one opened object. Resolution permits internal absolute symlinks; the
// subsequent rooted open cannot follow an ancestor replaced with an escape.
func readPrompt(workspace, reference string, afterResolve func()) ([]byte, error) {
	root, err := os.OpenRoot(workspace)
	if err != nil {
		return nil, errors.New("workspace_unavailable")
	}
	defer root.Close()
	resolved, err := resolveReference(workspace, reference)
	if err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return nil, errors.New("workspace_unavailable")
	}
	rel, err := filepath.Rel(canonical, resolved)
	if err != nil || !filepath.IsLocal(rel) {
		return nil, errors.New("reference_escapes_workspace")
	}
	if afterResolve != nil {
		afterResolve()
	}
	f, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("reference_unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > maxPromptBytes {
		return nil, errors.New("reference_not_bounded_regular_file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxPromptBytes+1))
	if err != nil || len(data) == 0 || len(data) > maxPromptBytes {
		return nil, errors.New("reference_unreadable")
	}
	return data, nil
}

// outputRoot confines every output operation to the admitted workspace,
// including configured output directories and their symlink ancestors.
func outputRoot(cfg config) (*os.Root, error) {
	workspace, err := filepath.EvalSymlinks(cfg.workspace)
	if err != nil {
		return nil, err
	}
	// Accept both the configured workspace spelling and its canonical form
	// (for example a caller's symlink alias); Root enforces actual containment.
	rel, err := filepath.Rel(filepath.Clean(cfg.workspace), filepath.Clean(cfg.outDir))
	if err != nil || !filepath.IsLocal(rel) {
		rel, err = filepath.Rel(workspace, filepath.Clean(cfg.outDir))
	}
	if err != nil || !filepath.IsLocal(rel) {
		return nil, errors.New("outdir_escapes_workspace")
	}
	root, err := os.OpenRoot(workspace)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if err := root.MkdirAll(rel, 0700); err != nil {
		return nil, err
	}
	return root.OpenRoot(rel)
}

type receipt struct {
	ID             string `json:"id"`
	Reference      string `json:"reference"`
	Model          string `json:"model"`
	Effort         string `json:"effort,omitempty"`
	PermissionMode string `json:"permission_mode"`
	SessionID      string `json:"session_id,omitempty"`
	Subtype        string `json:"subtype,omitempty"`
	IsError        *bool  `json:"is_error,omitempty"`
	NumTurns       int    `json:"num_turns,omitempty"`
	InputTokens    uint64 `json:"input_tokens,omitempty"`
	OutputTokens   uint64 `json:"output_tokens,omitempty"`
	Denials        int    `json:"permission_denials,omitempty"`
	ClaudeExit     int    `json:"claude_exit"`
	StdoutBytes    int64  `json:"stdout_bytes"`
	StdoutSHA256   string `json:"stdout_sha256"`
	Truncated      bool   `json:"stdout_truncated"`
	Error          string `json:"error,omitempty"`
	StartedUTC     string `json:"started_utc"`
	FinishedUTC    string `json:"finished_utc"`
}

type child struct {
	cfg       config
	parent    context.Context
	stdout    io.Writer
	stderr    io.Writer
	sessionID func() (string, error)
	// syncReceipt is an I/O seam for exercising cancellation during durability.
	syncReceipt func(*os.File) error
	runCmd      func(ctx context.Context, args []string, stdin io.Reader, out, errOut io.Writer) (int, error)
}

// handle runs one notice. It writes the applied envelope only on success;
// any failure writes a receipt with an error class and returns an error so
// the caller can exit without claiming delivery.
func (c *child) handle(env fixtureadapters.Envelope) error {
	if env.Kind != "notice" {
		return errors.New("unsupported_envelope_kind")
	}
	if !validID(env.Notice.ID) {
		return errors.New("invalid_notice_id")
	}
	started := time.Now().UTC()
	rec := receipt{ID: env.Notice.ID, Reference: env.Notice.Reference,
		Model: c.cfg.model, Effort: c.cfg.effort, PermissionMode: c.cfg.permissionMode,
		ClaudeExit: -1, StartedUTC: started.Format(time.RFC3339)}
	fail := func(class string) error {
		rec.Error = class
		rec.FinishedUTC = time.Now().UTC().Format(time.RFC3339)
		_ = c.writeReceipt(rec)
		return errors.New(class)
	}
	prompt, err := readPrompt(c.cfg.workspace, env.Notice.Reference, nil)
	if err != nil {
		return fail(err.Error())
	}

	sessionID, err := c.sessionID()
	if err != nil {
		return fail("session_id_unavailable")
	}
	rec.SessionID = sessionID
	outputs, err := outputRoot(c.cfg)
	if err != nil {
		return fail("outdir_unavailable")
	}
	defer outputs.Close()
	outFile, err := outputs.OpenFile(env.Notice.ID+".claude.json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fail("output_exists_or_unavailable")
	}
	defer outFile.Close()
	errFile, err := outputs.OpenFile(env.Notice.ID+".claude.stderr", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fail("stderr_capture_unavailable")
	}
	defer errFile.Close()
	// The result object is small; keep it in memory for parsing while the
	// bounded writer hashes and stores the full stream.
	var captured bytes.Buffer
	bounded := &boundedWriter{w: io.MultiWriter(outFile, &captured), hash: sha256.New()}
	parent := c.parent
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, c.cfg.timeout)
	defer cancel()
	exit, runErr := c.runCmd(ctx, claudeArgs(c.cfg, sessionID), bytes.NewReader(prompt), bounded, errFile)
	rec.ClaudeExit = exit
	rec.StdoutBytes = bounded.written
	rec.Truncated = bounded.truncated
	rec.StdoutSHA256 = hex.EncodeToString(bounded.hash.Sum(nil))
	if bounded.truncated {
		return fail("result_truncated")
	}
	res, class := parseResult(captured.Bytes(), sessionID)
	rec.Subtype = res.Subtype
	rec.NumTurns = res.NumTurns
	rec.InputTokens = res.Usage.InputTokens
	rec.OutputTokens = res.Usage.OutputTokens
	rec.Denials = len(res.PermissionDenials)
	if res.Type == "result" {
		rec.IsError = res.IsError
	}
	if ctx.Err() != nil {
		return fail("claude_run_interrupted")
	}
	if runErr != nil || exit != 0 {
		if class == "" {
			class = "claude_run_failed"
		}
		return fail(class)
	}
	if class != "" {
		return fail(class)
	}
	rec.FinishedUTC = time.Now().UTC().Format(time.RFC3339)
	// Capture durability precedes receipt publication. Completion is checked
	// again after receipt I/O; cancellation during that I/O cannot acknowledge.
	if outFile.Sync() != nil || errFile.Sync() != nil {
		return fail("capture_sync_failed")
	}
	if err := c.publishReceipt(outputs, rec); err != nil {
		if ctx.Err() != nil {
			return errors.New("claude_run_interrupted")
		}
		return errors.New("receipt_write_failed")
	}
	if ctx.Err() != nil {
		return errors.New("claude_run_interrupted")
	}
	if _, class := parseResult(captured.Bytes(), sessionID); class != "" {
		return errors.New(class)
	}
	if ctx.Err() != nil {
		return errors.New("claude_run_interrupted")
	}
	env.Kind = "applied"
	return json.NewEncoder(c.stdout).Encode(env)
}

func (c *child) writeReceipt(rec receipt) error {
	if !validID(rec.ID) {
		return errors.New("invalid_notice_id")
	}
	root, err := outputRoot(c.cfg)
	if err != nil {
		return err
	}
	defer root.Close()
	return c.publishReceipt(root, rec)
}

func (c *child) publishReceipt(root *os.Root, rec receipt) error {
	if !validID(rec.ID) {
		return errors.New("invalid_notice_id")
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	id, err := newSessionID()
	if err != nil {
		return err
	}
	tmp := ".receipt-" + id
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	defer root.Remove(tmp)
	if _, err = f.Write(append(data, '\n')); err != nil {
		return err
	}
	if c.syncReceipt != nil {
		err = c.syncReceipt(f)
	} else {
		err = f.Sync()
	}
	if err != nil {
		return err
	}
	if c.parent != nil && c.parent.Err() != nil && rec.Error == "" {
		return c.parent.Err()
	}
	// Link atomically publishes a fully written receipt without replacing any
	// existing file, symlink, FIFO or directory. No target is opened/truncated.
	if err := root.Link(tmp, rec.ID+".receipt.json"); err != nil {
		return err
	}
	if err := root.Remove(tmp); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func validID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return !strings.Contains(id, "..")
}

type boundedWriter struct {
	w         io.Writer
	hash      hash.Hash
	written   int64
	truncated bool
}

func (b *boundedWriter) Write(p []byte) (int, error) {
	b.hash.Write(p)
	b.written += int64(len(p))
	remain := int64(maxCaptureBytes) - (b.written - int64(len(p)))
	if remain <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if int64(len(p)) > remain {
		b.truncated = true
		if _, err := b.w.Write(p[:remain]); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	_, err := b.w.Write(p)
	return len(p), err
}

// runClaude starts claude in its own process group with the shim's cwd set
// to the admitted workspace, forwards context cancellation as TERM to that
// group, then KILL after the grace period.
func runClaude(bin, workspace string) func(ctx context.Context, args []string, stdin io.Reader, out, errOut io.Writer) (int, error) {
	return func(ctx context.Context, args []string, stdin io.Reader, out, errOut io.Writer) (int, error) {
		if err := ctx.Err(); err != nil {
			return -1, err
		}
		cmd := exec.Command(bin, args...)
		cmd.Dir = workspace
		cmd.Stdin = stdin
		cmd.Stdout = out
		cmd.Stderr = errOut
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			return -1, err
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			// Wait reaps only the leader. A completed version probe (or notice)
			// can still own descendants whose redirected pipes did not delay Wait.
			if cleanupErr := killGroup(cmd.Process.Pid); cleanupErr != nil {
				return exitCode(cmd, err), cleanupErr
			}
			return exitCode(cmd, err), errFromExit(cmd, err)
		case <-ctx.Done():
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			select {
			case err := <-done:
				_ = killGroup(cmd.Process.Pid)
				return exitCode(cmd, err), errors.New("claude_run_interrupted")
			case <-time.After(termGrace):
				_ = killGroup(cmd.Process.Pid)
				err := <-done
				return exitCode(cmd, err), errors.New("claude_run_interrupted")
			}
		}
	}
}

// Observe group disappearance after KILL before reporting normal completion.
// Wait reaps our direct child; orphaned descendants are reaped by the OS.
// Observation is bounded; failure cannot qualify a successful invocation.
// Detached descendants remain outside scope.
func killGroup(pid int) error {
	if err := syscall.Kill(-pid, syscall.SIGKILL); errors.Is(err, syscall.ESRCH) {
		return nil
	}
	// Darwin can refuse a signal while the last orphan is being reaped.
	// Only observed disappearance qualifies cleanup, regardless of signal error.
	deadline := time.Now().Add(termGrace)
	for {
		err := syscall.Kill(-pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errors.New("claude_group_cleanup_failed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func exitCode(cmd *exec.Cmd, err error) int {
	if err == nil {
		return 0
	}
	if cmd.ProcessState != nil {
		return cmd.ProcessState.ExitCode()
	}
	return -1
}

func errFromExit(cmd *exec.Cmd, err error) error {
	if err == nil {
		return nil
	}
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		return nil // nonzero exit is reported by code, not as a start error
	}
	return err
}

// checkVersion binds the shim to one explicitly admitted claude release,
// mirroring the Codex adapter's NativeRelease discipline: the caller
// declares the expected `--version` output and any mismatch refuses before
// the first notice. The observed string is returned for the run record; it
// is evidence about this binary invocation, not a catalog lookup.
func checkVersion(parent context.Context, bin, workspace, expected string) (string, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	exit, err := runClaude(bin, workspace)(ctx, []string{"--version"}, nil, &out, io.Discard)
	if parent.Err() != nil {
		return "", errors.New("claude_run_interrupted")
	}
	if err != nil || exit != 0 || ctx.Err() != nil {
		return "", errors.New("claude_version_unavailable")
	}
	observed := strings.TrimSpace(out.String())
	if expected != "" && observed != expected {
		return observed, errors.New("claude_version_mismatch")
	}
	return observed, nil
}

func main() {
	// Own shutdown before any native invocation, including the version probe.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cwd_unavailable")
		os.Exit(2)
	}
	cfg, err := loadConfig(os.Getenv, cwd)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	observed, err := checkVersion(ctx, cfg.claudeBin, cfg.workspace, cfg.expectedVer)
	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "claude_run_interrupted")
			os.Exit(3)
		}
		fmt.Fprintln(os.Stderr, err, observed)
		os.Exit(2)
	}
	c := &child{cfg: cfg, parent: ctx, stdout: os.Stdout, stderr: os.Stderr,
		sessionID: newSessionID, runCmd: runClaude(cfg.claudeBin, cfg.workspace)}
	// Select on shutdown independently of stdin: a native pipe/terminal read
	// need not wake when another goroutine closes its file descriptor.
	type inputLine struct {
		data []byte
		err  error
	}
	input := make(chan inputLine)
	go func() {
		defer close(input)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 4096), 65536)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case input <- inputLine{data: line}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case input <- inputLine{err: err}:
			case <-ctx.Done():
			}
		}
	}()
	for {
		var line inputLine
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "claude_run_interrupted")
			os.Exit(3)
		case next, ok := <-input:
			if ctx.Err() != nil {
				fmt.Fprintln(os.Stderr, "claude_run_interrupted")
				os.Exit(3)
			}
			if !ok {
				return
			} // Normal stdin EOF.
			line = next
		}
		if line.err != nil {
			fmt.Fprintln(os.Stderr, "stdin_read_failed")
			os.Exit(4)
		}
		var env fixtureadapters.Envelope
		if err := json.Unmarshal(line.data, &env); err != nil {
			fmt.Fprintln(os.Stderr, "invalid_envelope")
			os.Exit(4)
		}
		if err := c.handle(env); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
	}
}
