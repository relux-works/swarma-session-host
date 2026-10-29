package sessionrun

import (
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"math"
	"strings"
)

// This transient native event fold is not a replacement snapshot or ledger.
// Durable accounting uses the codec's original exported native field group.
type agentMessage struct {
	ItemID   string `json:"itemId"`
	Revision int64  `json:"revision"`
	Kind     string `json:"kind"`
	TurnID   string `json:"turnId"`
	Text     string `json:"text"`
}
type finals struct {
	order map[string][]string
	items map[string]agentMessage
}

func newFinals() *finals {
	return &finals{order: map[string][]string{}, items: map[string]agentMessage{}}
}
func (f *finals) apply(item agentMessage) error {
	if item.ItemID == "" || item.TurnID == "" || item.Revision < 1 || item.Kind != "agentMessage" {
		return core.ErrMuseObservationInvalid
	}
	prior, exists := f.items[item.ItemID]
	if exists {
		if prior.TurnID != item.TurnID {
			return core.ErrMuseObservationUncertain
		}
		if item.Revision < prior.Revision {
			return nil
		}
		if item.Revision == prior.Revision {
			if prior.Text != item.Text {
				return core.ErrMuseObservationUncertain
			}
			return nil
		}
	} else {
		f.order[item.TurnID] = append(f.order[item.TurnID], item.ItemID)
	}
	f.items[item.ItemID] = item
	return nil
}
func (f *finals) render(turn string) (output, final string) {
	texts := make([]string, 0, len(f.order[turn]))
	for _, id := range f.order[turn] {
		text := f.items[id].Text
		texts = append(texts, text)
		if trimmed := strings.TrimSpace(text); trimmed != "" {
			final = trimmed
		}
	}
	return strings.Join(texts, "\n"), final
}
func negativeUsage(u *core.MSPTokenUsage) bool {
	return u != nil && (u.InputTokens < 0 || u.OutputTokens < 0 || u.CachedTokens < 0 || u.ReasoningTokens < 0 || u.CacheReadTokens != nil && *u.CacheReadTokens < 0 || u.CacheWriteTokens != nil && *u.CacheWriteTokens < 0)
}
func museRunResult(e core.MSPTurnCompletedNotification, output string) *core.ProviderRunResult {
	status := core.ProviderRunComplete
	if e.Error != nil || e.Terminal != "completed" {
		status = core.ProviderRunFailed
	}
	r := &core.ProviderRunResult{Status: status, ProviderStatus: e.Terminal, Detail: e.Reason, Output: strings.TrimSpace(output)}
	if e.Error != nil {
		r.Detail = e.Error.Message
	}
	if negativeUsage(e.Usage) {
		r.Status = core.ProviderRunFailed
		r.ProviderStatus = "usage_malformed"
		r.Detail = "turn reported negative usage"
		r.Recoverable = true
		return r
	}
	if e.Usage != nil {
		u := e.Usage
		r.Usage.InputTokens = u.InputTokens
		r.Usage.CachedInputTokens = u.CachedTokens
		r.Usage.OutputTokens = u.OutputTokens
		r.Usage.ReasoningOutputTokens = u.ReasoningTokens
		if u.InputTokens > math.MaxInt64-u.OutputTokens {
			r.Usage.TotalTokens = math.MaxInt64
		} else {
			r.Usage.TotalTokens = u.InputTokens + u.OutputTokens
		}
	}
	return r
}
func aborted(reason string) *core.ProviderRunResult {
	detail := map[string]string{"turn_unqueued": "the server reclaimed the awaited turn before running it", "session_closed": "the session was unloaded before the awaited turn completed", "session_not_loaded": "the session is not loaded, so the awaited turn cannot complete", "session_deleted": "the session was deleted before the awaited turn completed", "view_unavailable": "the session view stream became unavailable, so no terminal can be attested", "view_gap": "the session view holed the item/usage evidence, so no terminal can be attested", "approval_unresolvable": "the awaited turn is blocked on an approval this host cannot resolve", "user_input_unresolvable": "the awaited turn is blocked on a user input this host cannot resolve", "request_inventory_unavailable": "the pending-request inventory could not be read or resolved, so a possibly blocked turn cannot be resolved"}[reason]
	if detail == "" {
		detail = "the awaited turn will not run"
	}
	recoverable := reason != "request_resolution_failed" && reason != "session_deleted" && reason != "approval_unresolvable" && reason != "user_input_unresolvable"
	return &core.ProviderRunResult{Status: core.ProviderRunFailed, ProviderStatus: reason, Detail: detail, Recoverable: recoverable}
}

var errNoTerminal = errors.New("native run has no attested terminal")
