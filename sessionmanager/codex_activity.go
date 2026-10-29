package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var errCodexTurnFenced = errors.New("Codex session is fenced against new turns")

// codexTurnGate is shared by the host's RPC clients and its attachment relay.
// The lock covers each write and the idle-check/fence transition. Accepted but
// unacknowledged requests and queued work cannot be mistaken for an idle thread.
// Activity is volatile: restart starts unknown, never from persisted timestamps.
type codexTurnGate struct {
	mu              sync.Mutex
	fenced          bool
	fenceGeneration uint64
	threadID        string
	state           ProviderActivityState
	last            time.Time
	pending         map[uint64]bool
	next            uint64
	turns           map[string]bool
	completed       map[string]bool
	queued          bool
	uncertain       bool
	malformed       map[*codexGateClient]bool
}

const codexTrackingLimit = 1024

type codexGateClient struct {
	gate    *codexTurnGate
	methods map[string]string
	pending map[string]uint64 // protected by gate.mu, scoped to this connection
}

func (g *codexTurnGate) client() *codexGateClient {
	return &codexGateClient{gate: g, pending: map[string]uint64{}, methods: map[string]string{}}
}
func (g *codexTurnGate) activityLocked() ProviderActivitySnapshot {
	state := g.state
	if state == "" {
		state = ProviderActivityUnknown
	}
	if len(g.turns) > 0 {
		state = ProviderActivityBusy
	}
	if len(g.pending) > 0 || g.queued || g.uncertain || len(g.malformed) > 0 {
		state = ProviderActivityUnknown
	}
	return ProviderActivitySnapshot{State: state, LastEventAt: g.last}
}

// The published 0.162.1 ClientRequest schema identifies these work-starting
// methods. turn/steer can add work to a live turn; queue/add is the manager's
// compatibility fallback and remains gated even when this server rejects it.
var codexTurnStartingMethods = map[string]bool{
	"turn/start": true, "turn/steer": true, "review/start": true,
	"thread/compact/start": true, "thread/shellCommand": true, "thread/queue/add": true,
}

// Known non-turn methods from the same schema may pass a retirement fence.
// A future method is potential work until explicitly classified here.
var codexNonTurnMethods = map[string]bool{
	"initialize":                               true,
	"thread/start":                             true,
	"thread/resume":                            true,
	"thread/fork":                              true,
	"thread/archive":                           true,
	"thread/delete":                            true,
	"thread/unsubscribe":                       true,
	"thread/name/set":                          true,
	"thread/goal/set":                          true,
	"thread/goal/get":                          true,
	"thread/goal/clear":                        true,
	"thread/metadata/update":                   true,
	"thread/attachment/add":                    true,
	"thread/attachment/list":                   true,
	"thread/attachmentOwner/list":              true,
	"thread/attachment/remove":                 true,
	"thread/section/move":                      true,
	"thread/unarchive":                         true,
	"thread/approveGuardianDeniedAction":       true,
	"thread/revert":                            true,
	"thread/list":                              true,
	"threadSection/list":                       true,
	"threadSection/create":                     true,
	"threadSection/update":                     true,
	"threadSection/delete":                     true,
	"thread/loaded/list":                       true,
	"thread/read":                              true,
	"thread/turns/list":                        true,
	"thread/items/list":                        true,
	"thread/inject_items":                      true,
	"skills/list":                              true,
	"skills/extraRoots/set":                    true,
	"hooks/list":                               true,
	"marketplace/add":                          true,
	"marketplace/remove":                       true,
	"marketplace/upgrade":                      true,
	"plugin/list":                              true,
	"plugin/installed":                         true,
	"plugin/reconcile":                         true,
	"plugin/read":                              true,
	"plugin/skill/read":                        true,
	"plugin/share/save":                        true,
	"plugin/share/updateTargets":               true,
	"plugin/share/list":                        true,
	"plugin/share/checkout":                    true,
	"plugin/share/delete":                      true,
	"app/read":                                 true,
	"app/list":                                 true,
	"app/installed":                            true,
	"fs/readFile":                              true,
	"fs/writeFile":                             true,
	"fs/createDirectory":                       true,
	"fs/getMetadata":                           true,
	"fs/readDirectory":                         true,
	"fs/remove":                                true,
	"fs/copy":                                  true,
	"fs/watch":                                 true,
	"fs/unwatch":                               true,
	"skills/config/write":                      true,
	"plugin/install":                           true,
	"plugin/uninstall":                         true,
	"turn/interrupt":                           true,
	"model/list":                               true,
	"account/gatewayOAuth/read":                true,
	"account/gatewayOAuth/login":               true,
	"account/gatewayOAuth/cancel":              true,
	"modelProvider/capabilities/read":          true,
	"experimentalFeature/list":                 true,
	"permissionProfile/list":                   true,
	"experimentalFeature/enablement/set":       true,
	"mcpServer/oauth/login":                    true,
	"config/mcpServer/reload":                  true,
	"mcpServerStatus/list":                     true,
	"mcpServer/resource/read":                  true,
	"mcpServer/tool/call":                      true,
	"windowsSandbox/setupStart":                true,
	"windowsSandbox/readiness":                 true,
	"account/login/start":                      true,
	"account/login/cancel":                     true,
	"account/logout":                           true,
	"account/rateLimits/read":                  true,
	"account/rateLimitResetCredit/consume":     true,
	"account/usage/read":                       true,
	"account/workspaceMessages/read":           true,
	"account/sendAddCreditsNudgeEmail":         true,
	"feedback/upload":                          true,
	"command/exec":                             true,
	"command/exec/write":                       true,
	"command/exec/terminate":                   true,
	"command/exec/resize":                      true,
	"config/read":                              true,
	"externalAgentConfig/detect":               true,
	"externalAgentConfig/import":               true,
	"externalAgentConfig/import/recordHistory": true,
	"externalAgentConfig/import/readHistories": true,
	"config/value/write":                       true,
	"config/batchWrite":                        true,
	"configRequirements/read":                  true,
	"account/read":                             true,
	"fuzzyFileSearch":                          true,
	"initialized":                              true,
}

type codexGateMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

func decodeCodexGateMessage(data []byte) (codexGateMessage, bool) {
	var raw map[string]json.RawMessage
	var f codexGateMessage
	if json.Unmarshal(data, &raw) != nil || raw == nil || json.Unmarshal(data, &f) != nil {
		return f, false
	}
	if v, ok := raw["jsonrpc"]; ok && string(v) != `"2.0"` {
		return f, false
	}
	if len(f.ID) > 0 && string(f.ID) != "null" {
		var id any
		if json.Unmarshal(f.ID, &id) != nil {
			return f, false
		}
		switch id.(type) {
		case string, float64:
		default:
			return f, false
		}
	}
	if f.Method != "" {
		if len(f.Result) > 0 || len(f.Error) > 0 {
			return f, false
		}
		if p := raw["params"]; len(p) > 0 && string(p) != "null" && p[0] != '{' && p[0] != '[' {
			return f, false
		}
		return f, true
	}
	return f, len(f.ID) > 0 && ((len(f.Result) > 0) != (len(f.Error) > 0))
}
func codexGateID(raw json.RawMessage) string { return string(raw) }

// write never forwards a rejected frame. An unreadable frame taints this
// attached connection until detach, including when the frame itself is refused.
func (c *codexGateClient) write(ctx context.Context, data []byte, text bool, forward func() error) error {
	g := c.gate
	if err := lockLifecycle(ctx, &g.mu); err != nil {
		return err
	}
	defer g.mu.Unlock()
	f, valid := decodeCodexGateMessage(data)
	if !text {
		valid = false
	}
	if !valid {
		if g.malformed == nil {
			g.malformed = map[*codexGateClient]bool{}
		}
		g.malformed[c] = true
	}
	potential := !valid || codexTurnStartingMethods[f.Method] || (f.Method != "" && !codexNonTurnMethods[f.Method])
	if potential && g.fenced {
		return errCodexTurnFenced
	}
	var token uint64
	if potential {
		g.next++
		token = g.next
		if g.pending == nil {
			g.pending = map[uint64]bool{}
		}
		if len(g.pending) >= codexTrackingLimit || len(f.ID) > codexTrackingLimit || len(f.Method) > codexTrackingLimit {
			g.uncertain = true
		} else {
			g.pending[token] = true
			if valid && codexUsableID(f.ID) {
				key := codexGateID(f.ID)
				if _, duplicate := c.pending[key]; duplicate {
					// Ambiguous acknowledgements cannot erase either request.
					g.uncertain = true
				} else if len(c.pending) < codexTrackingLimit {
					c.pending[key] = token
					c.methods[key] = f.Method
				} else {
					g.uncertain = true
				}
			}
		}
	}
	if err := forward(); err != nil {
		// A failed write may already have delivered a complete request. Never
		// reinterpret an unknown delivery outcome as permission to stop idle.
		if potential {
			g.uncertain = true
		}
		delete(g.pending, token)
		delete(c.pending, codexGateID(f.ID))
		delete(c.methods, codexGateID(f.ID))
		g.state = ProviderActivityUnknown
		return err
	}
	return nil
}
func (c *codexGateClient) lost() {
	g := c.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.malformed, c)
	// Unacknowledged writes may already have taken effect. Keep those pending:
	// a disconnect cannot prove that an accepted turn was cancelled.
	g.state = ProviderActivityUnknown
}
func (c *codexGateClient) observe(data []byte) {
	g := c.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	f, ok := decodeCodexGateMessage(data)
	if !ok {
		g.state = ProviderActivityUnknown
		return
	}
	if token, ok := c.pending[codexGateID(f.ID)]; ok && f.Method == "" {
		delete(g.pending, token)
		method := c.methods[codexGateID(f.ID)]
		delete(c.pending, codexGateID(f.ID))
		delete(c.methods, codexGateID(f.ID))
		if len(f.Error) == 0 || string(f.Error) == "null" {
			var result struct {
				Turn   codexTurn `json:"turn"`
				TurnID string    `json:"turnId"`
			}
			recognized := false
			if json.Unmarshal(f.Result, &result) == nil {
				id := result.Turn.ID
				if id == "" {
					id = result.TurnID
				}
				recognized = id != "" && (g.completed[id] || result.TurnID != "" || result.Turn.Status == "inProgress" || result.Turn.Status == "completed" || result.Turn.Status == "interrupted" || result.Turn.Status == "failed")
				if id != "" && !g.completed[id] && (result.Turn.Status == "inProgress" || result.TurnID != "") {
					if g.turns == nil {
						g.turns = map[string]bool{}
					}
					g.trackTurnLocked(id)
				}
			}
			// The compatibility queue can launch work later without any connected
			// reader. An acknowledgement is not evidence that its work is exhausted.
			if !codexTurnStartingMethods[method] || !recognized {
				g.uncertain = true
			}
			if method == "thread/queue/add" {
				g.queued = true
			}
		}
	}
	g.observeLocked(f)
}
func (g *codexTurnGate) observeLocked(f codexGateMessage) {
	if f.Method == "thread/status/changed" {
		var p struct {
			ThreadID string            `json:"threadId"`
			Status   codexThreadStatus `json:"status"`
		}
		if json.Unmarshal(f.Params, &p) != nil {
			g.state = ProviderActivityUnknown
			return
		}
		if p.ThreadID != g.threadID {
			return
		}
		g.setStatusLocked(p.Status.Type)
		if p.Status.Type == "idle" || p.Status.Type == "active" {
			g.last = time.Now()
		}
	}
	if f.Method == "turn/started" || f.Method == "turn/completed" {
		var p struct {
			ThreadID string    `json:"threadId"`
			Turn     codexTurn `json:"turn"`
		}
		if json.Unmarshal(f.Params, &p) != nil {
			g.state = ProviderActivityUnknown
			return
		}
		if p.ThreadID != g.threadID {
			return
		}
		if p.Turn.ID == "" {
			g.state = ProviderActivityUnknown
			return
		}
		if (f.Method == "turn/started" && p.Turn.Status != "inProgress") || (f.Method == "turn/completed" && p.Turn.Status != "completed" && p.Turn.Status != "interrupted" && p.Turn.Status != "failed") {
			g.state = ProviderActivityUnknown
			return
		}
		g.last = time.Now()
		if f.Method == "turn/started" {
			if g.turns == nil {
				g.turns = map[string]bool{}
			}
			if !g.completed[p.Turn.ID] {
				g.trackTurnLocked(p.Turn.ID)
			}
			g.state = ProviderActivityBusy
		} else {
			delete(g.turns, p.Turn.ID)
			if g.completed == nil {
				g.completed = map[string]bool{}
			}
			// Bounded correlation for a completion that races its start acknowledgement.
			if len(g.completed) >= codexTrackingLimit || len(p.Turn.ID) > codexTrackingLimit {
				g.uncertain = true
			} else {
				g.completed[p.Turn.ID] = true
			}
			// Completion alone is not proof of idle: another queued turn can start.
			g.state = ProviderActivityUnknown
		}
	}
}
func (g *codexTurnGate) trackTurnLocked(id string) {
	if g.turns == nil {
		g.turns = map[string]bool{}
	}
	if len(g.turns) >= codexTrackingLimit || len(id) > codexTrackingLimit {
		g.uncertain = true
		return
	}
	g.turns[id] = true
}
func (g *codexTurnGate) setStatusLocked(status string) {
	switch status {
	case "idle":
		g.state = ProviderActivityIdle
	case "active":
		g.state = ProviderActivityBusy
	default:
		g.state = ProviderActivityUnknown
	}
}

// refresh is a read-only provider snapshot. It never fabricates an activity
// timestamp; only the notifications folded above advance last_activity_at.
func codexUsableID(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	var id any
	if json.Unmarshal(raw, &id) != nil {
		return false
	}
	switch id.(type) {
	case string, float64:
		return true
	}
	return false
}
