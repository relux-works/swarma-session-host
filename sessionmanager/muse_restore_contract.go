package sessionmanager

import "encoding/json"

// Contract delta D4b: native folded state is a reconciliation base, never live events.
type MuseRestoreFold struct {
	SessionID  string
	Cursor     string
	Cumulative MuseRestoreUsage
	Items      []json.RawMessage
	ActiveTurn *MuseRestoreTurnRef
	// D8: actual session activeTurnId; empty means native null. No priority here.
	SessionActiveTurnID string
	// D6: actual native stated terms; empty means unstated. Observation only.
	EffectiveApprovalMode string
	EffectiveModelID      string
}
type MuseRestoreUsage struct {
	OutputTokens int64
	PromptTokens int64
	TotalTokens  int64
}
type MuseRestoreTurnRef struct {
	CommandID string
	TurnID    string
}
