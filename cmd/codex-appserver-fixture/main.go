// Command codex-appserver-fixture is a synthetic Codex app-server child for
// rehearsing the session-host Codex lane without credentials or model calls.
// It speaks only the bounded stdio subset the pinned adapter consumes:
// initialize, initialized, thread/start and turn/start, followed by
// turn/started and turn/completed notifications. It is not Codex, performs
// no work, and never sends server-originated requests.
//
// Optional environment (supplied explicitly by the runner plan):
//
//	CODEX_FIXTURE_MARKER     absolute path; each completed turn appends one
//	                         line "turn=<id> sha256=<hex of input text>"
//	CODEX_FIXTURE_DELAY_MS   delay before turn/completed, 0-60000 (default 0)
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const maxFrame = 64 * 1024

type fixture struct {
	thread string
	delay  time.Duration
	marker string
}

type request struct {
	ID     uint64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func main() {
	f, err := configure(os.Getenv("CODEX_FIXTURE_MARKER"), os.Getenv("CODEX_FIXTURE_DELAY_MS"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "codex_fixture_refused")
		os.Exit(2)
	}
	if err := f.run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "codex_fixture_refused")
		os.Exit(2)
	}
}

func configure(marker, delayMS string) (*fixture, error) {
	f := &fixture{thread: "fixture-thread", marker: marker}
	if marker != "" && !filepath.IsAbs(marker) {
		return nil, errors.New("invalid_marker")
	}
	if delayMS != "" {
		ms, err := strconv.Atoi(delayMS)
		if err != nil || ms < 0 || ms > 60000 {
			return nil, errors.New("invalid_delay")
		}
		f.delay = time.Duration(ms) * time.Millisecond
	}
	return f, nil
}

func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil || len(b)+1 > maxFrame {
		return errors.New("frame_refused")
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func (f *fixture) run(in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), maxFrame)
	for scanner.Scan() {
		var r request
		if json.Unmarshal(scanner.Bytes(), &r) != nil {
			return errors.New("invalid_frame")
		}
		switch r.Method {
		case "initialize":
			err := writeFrame(out, map[string]any{"id": r.ID, "result": map[string]any{
				"userAgent": "codex-appserver-fixture", "platformFamily": "unix", "platformOs": "fixture"}})
			if err != nil {
				return err
			}
		case "initialized":
			// Notification; no reply.
		case "thread/start":
			if err := f.threadStart(out, r); err != nil {
				return err
			}
		case "turn/start":
			if err := f.turnStart(out, r); err != nil {
				return err
			}
		default:
			return errors.New("unsupported_method")
		}
	}
	return scanner.Err()
}

func (f *fixture) threadStart(out io.Writer, r request) error {
	var p struct {
		Model          string `json:"model"`
		Provider       string `json:"modelProvider"`
		Cwd            string `json:"cwd"`
		ApprovalPolicy string `json:"approvalPolicy"`
		Sandbox        string `json:"sandbox"`
	}
	if json.Unmarshal(r.Params, &p) != nil || p.Model == "" || p.Provider == "" || p.Cwd == "" {
		return errors.New("invalid_thread_start")
	}
	result := map[string]any{
		"thread":        map[string]any{"id": f.thread},
		"model":         p.Model,
		"modelProvider": p.Provider,
	}
	if p.ApprovalPolicy != "" || p.Sandbox != "" {
		if p.ApprovalPolicy != "never" || p.Sandbox != "danger-full-access" {
			return errors.New("invalid_thread_policy")
		}
		result["approvalPolicy"] = "never"
		result["cwd"] = p.Cwd
		result["sandbox"] = map[string]string{"type": "dangerFullAccess"}
	}
	return writeFrame(out, map[string]any{"id": r.ID, "result": result})
}

func (f *fixture) turnStart(out io.Writer, r request) error {
	var p struct {
		Thread string `json:"threadId"`
		Input  []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"input"`
	}
	if json.Unmarshal(r.Params, &p) != nil || p.Thread != f.thread || len(p.Input) != 1 || p.Input[0].Type != "text" {
		return errors.New("invalid_turn_start")
	}
	turn := fmt.Sprintf("fixture-turn-%d", r.ID)
	turnObj := map[string]any{"id": turn, "status": "inProgress"}
	if err := writeFrame(out, map[string]any{"id": r.ID, "result": map[string]any{"turn": turnObj}}); err != nil {
		return err
	}
	if err := writeFrame(out, map[string]any{"method": "turn/started", "params": map[string]any{
		"threadId": f.thread, "turn": turnObj}}); err != nil {
		return err
	}
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.marker != "" {
		sum := sha256.Sum256([]byte(p.Input[0].Text))
		line := fmt.Sprintf("turn=%s sha256=%s\n", turn, hex.EncodeToString(sum[:]))
		file, err := os.OpenFile(f.marker, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		_, werr := file.WriteString(line)
		cerr := file.Close()
		if werr != nil {
			return werr
		}
		if cerr != nil {
			return cerr
		}
	}
	return writeFrame(out, map[string]any{"method": "turn/completed", "params": map[string]any{
		"threadId": f.thread,
		"turn":     map[string]any{"id": turn, "status": "completed", "error": nil}}})
}
