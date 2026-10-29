// Package codexadapter implements the bounded stdio protocol for Codex 0.164.0.
// It owns no processes, credentials, durable receipts, or transport reconnection.
package codexadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
)

const MaxFrame = 64 * 1024

type State string

const (
	Unknown  State = "unknown"
	Idle     State = "idle"
	Busy     State = "busy"
	Approval State = "approval"
)

// Config passes selection strings unchanged; supported values belong to Codex.
// Workspace is admitted by the caller. No permission or credential overrides are added.
type Config struct{ Model, Provider, Effort, Workspace string }
type Snapshot struct {
	State                       State
	Thread, Turn, CompletedTurn string
	AuthorityRequired           bool
}

// Protocol requires one ordered consumer. The mutex permits concurrent snapshots.
// A transport gap must call Lost rather than silently continuing a partial stream.
type Protocol struct {
	mu        sync.Mutex
	cfg       Config
	view      Snapshot
	phase     string
	request   uint64
	pending   uint64
	operation string
}

func New(cfg Config) (*Protocol, error) {
	for _, v := range []string{cfg.Model, cfg.Provider, cfg.Effort} {
		if !identifier(v) {
			return nil, errors.New("invalid_selection_framing")
		}
	}
	if !filepath.IsAbs(cfg.Workspace) || len(cfg.Workspace) > 4096 || strings.ContainsAny(cfg.Workspace, "\x00\r\n") {
		return nil, errors.New("invalid_workspace")
	}
	return &Protocol{cfg: cfg, phase: "new", view: Snapshot{State: Unknown}}, nil
}

func identifier(v string) bool {
	if len(v) == 0 || len(v) > 128 {
		return false
	}
	for _, r := range v {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

func frame(v any) []byte { b, _ := json.Marshal(v); return append(b, '\n') }
func (p *Protocol) requestFrame(method string, params any) []byte {
	p.request++
	p.pending = p.request
	p.operation = method
	return frame(struct {
		ID     uint64 `json:"id"`
		Method string `json:"method"`
		Params any    `json:"params"`
	}{p.request, method, params})
}

func (p *Protocol) Snapshot() Snapshot { p.mu.Lock(); defer p.mu.Unlock(); return p.view }

func (p *Protocol) fail(code string) ([][]byte, error) {
	p.phase = "lost"
	p.view.State = Unknown
	p.view.CompletedTurn = ""
	return nil, errors.New(code)
}

// Lost permanently closes this parser after an incomplete or missing output frame.
func (p *Protocol) Lost() { p.mu.Lock(); defer p.mu.Unlock(); p.fail("transport_lost") }

func (p *Protocol) Initialize() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.phase != "new" {
		return nil, errors.New("initialize_not_eligible")
	}
	p.phase = "initialize"
	return p.requestFrame("initialize", map[string]any{"clientInfo": map[string]string{"name": "swarma_session_host", "title": "Session Host", "version": "0.1.0"}}), nil
}

// Submit prepares one request. The host must persist unknown intent BEFORE writing it.
// A returned frame or a successful transport write is never a completion receipt.
func (p *Protocol) Submit(text string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.phase != "ready" || p.view.State != Idle {
		return nil, errors.New("input_not_eligible")
	}
	if len(text) == 0 || len(text) > 4096 || strings.ContainsRune(text, 0) {
		return nil, errors.New("invalid_input")
	}
	p.phase = "turn"
	p.view.State = Busy
	p.view.Turn = ""
	p.view.CompletedTurn = ""
	return p.requestFrame("turn/start", map[string]any{"threadId": p.view.Thread, "model": p.cfg.Model, "effort": p.cfg.Effort, "input": []any{map[string]string{"type": "text", "text": text}}}), nil
}

type envelope struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

// Consume accepts one complete JSON frame; callers must bound line accumulation.
// Unrelated event payloads are discarded and never included in diagnostics.
func (p *Protocol) Consume(b []byte) ([][]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.view.AuthorityRequired && p.phase != "lost" {
		return nil, nil
	} // Observe-only closed authority wait; never answer it.
	if p.phase == "lost" {
		return nil, errors.New("protocol_closed")
	}
	if len(b) == 0 || len(b) > MaxFrame || !json.Valid(b) || bytes.TrimSpace(b)[0] != '{' || !uniqueKeys(b) {
		return p.fail("invalid_frame")
	}
	var e envelope
	if json.Unmarshal(b, &e) != nil {
		return p.fail("invalid_frame")
	}
	if e.Method != "" {
		if len(e.Result) != 0 || len(e.Error) != 0 {
			return p.fail("ambiguous_frame")
		}
		if len(e.ID) != 0 { // No server-originated action is authorized by this adapter.
			p.phase = "approval"
			p.view.State = Approval
			p.view.CompletedTurn = ""
			p.view.AuthorityRequired = true
			return nil, nil
		}
		return p.notification(e)
	}
	var id uint64
	if json.Unmarshal(e.ID, &id) != nil || id == 0 || id != p.pending || p.pending == 0 || len(e.Result) == 0 || len(e.Error) != 0 {
		return p.fail("rpc_reply_refused")
	}
	op := p.operation
	p.pending = 0
	p.operation = ""
	switch op {
	case "initialize":
		var r struct {
			UserAgent      string `json:"userAgent"`
			PlatformFamily string `json:"platformFamily"`
			PlatformOS     string `json:"platformOs"`
		}
		if json.Unmarshal(e.Result, &r) != nil || r.UserAgent == "" || r.PlatformFamily == "" || r.PlatformOS == "" {
			return p.fail("initialize_reply_refused")
		}
		p.phase = "thread"
		return [][]byte{frame(map[string]any{"method": "initialized", "params": map[string]any{}}), p.requestFrame("thread/start", map[string]any{"model": p.cfg.Model, "modelProvider": p.cfg.Provider, "cwd": p.cfg.Workspace})}, nil
	case "thread/start":
		var r struct {
			Thread struct {
				ID string `json:"id"`
			} `json:"thread"`
			Model    string `json:"model"`
			Provider string `json:"modelProvider"`
		}
		if json.Unmarshal(e.Result, &r) != nil || !identifier(r.Thread.ID) || r.Model != p.cfg.Model || r.Provider != p.cfg.Provider {
			return p.fail("thread_reply_refused")
		}
		p.view.Thread = r.Thread.ID
		p.view.State = Idle
		p.phase = "ready"
	case "turn/start":
		var r struct {
			Turn turn `json:"turn"`
		}
		if json.Unmarshal(e.Result, &r) != nil || !identifier(r.Turn.ID) || r.Turn.Status != "inProgress" || (p.view.Turn != "" && p.view.Turn != r.Turn.ID) {
			return p.fail("turn_reply_refused")
		}
		p.view.Turn = r.Turn.ID
	default:
		return p.fail("unexpected_reply")
	}
	return nil, nil
}

// Duplicate keys are ambiguous protocol evidence even when encoding/json accepts them.
func uniqueKeys(b []byte) bool {
	d := json.NewDecoder(bytes.NewReader(b))
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 32 {
			return false
		}
		t, err := d.Token()
		if err != nil {
			return false
		}
		switch t {
		case json.Delim('{'):
			seen := make(map[string]bool)
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return false
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return false
				}
				seen[s] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim('}')
		case json.Delim('['):
			for d.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim(']')
		}
		_, delim := t.(json.Delim)
		return !delim
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}

type turn struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Error  json.RawMessage `json:"error"`
}

func (p *Protocol) notification(e envelope) ([][]byte, error) {
	if e.Method != "turn/started" && e.Method != "turn/completed" {
		return nil, nil
	}
	var n struct {
		Thread string `json:"threadId"`
		Turn   turn   `json:"turn"`
	}
	if json.Unmarshal(e.Params, &n) != nil || !identifier(n.Thread) || !identifier(n.Turn.ID) {
		return p.fail("invalid_turn_event")
	}
	if n.Thread != p.view.Thread {
		return nil, nil
	}
	if p.phase != "turn" || p.view.State != Busy {
		return p.fail("unexpected_turn_event")
	}
	if e.Method == "turn/started" {
		if n.Turn.Status != "inProgress" || (p.view.Turn != "" && p.view.Turn != n.Turn.ID) {
			return p.fail("turn_identity_conflict")
		}
		p.view.Turn = n.Turn.ID
		return nil, nil
	}
	if n.Turn.ID != p.view.Turn {
		return nil, nil
	}
	if p.pending != 0 {
		return p.fail("completion_before_acceptance")
	}
	if n.Turn.Status != "completed" || (len(n.Turn.Error) != 0 && string(n.Turn.Error) != "null") {
		return p.fail("turn_not_completed")
	}
	p.view.CompletedTurn = n.Turn.ID
	p.view.State = Idle
	p.phase = "ready"
	return nil, nil
}
