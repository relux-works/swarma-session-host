package sessionmanager

import (
	"bytes"
	"encoding/json"
)

// MSPClientInfo identifies the client during the initialize handshake.
type MSPClientInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

// MSPClientCapabilities are the stable capabilities understood by Muse MSP
// 1.4. Unknown capability names remain strings so the wire shape stays
// forward-compatible.
type MSPClientCapabilities struct {
	ExperimentalAPI           *bool    `json:"experimentalApi,omitempty"`
	OptOutNotificationMethods []string `json:"optOutNotificationMethods,omitempty"`
	RequestedCapabilities     []string `json:"requestedCapabilities,omitempty"`
	UserInputDialogs          *bool    `json:"userInputDialogs,omitempty"`
}

// MSPInitializeParams opens an MSP connection. Initialize also sends Muse's
// required, response-free "initialized" notification after the reply arrives.
type MSPInitializeParams struct {
	ClientInfo   MSPClientInfo          `json:"clientInfo"`
	Capabilities *MSPClientCapabilities `json:"capabilities,omitempty"`
}

type MSPSchemaInfo struct {
	Fingerprint string `json:"fingerprint"`
	Version     int    `json:"version"`
}

type MSPServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// MSPInitializeResult is the initialize result from the pinned MSP schema.
type MSPInitializeResult struct {
	ExperimentalAPI     bool          `json:"experimentalApi"`
	GrantedCapabilities []string      `json:"grantedCapabilities"`
	MuseHome            string        `json:"museHome"`
	PlatformFamily      string        `json:"platformFamily"`
	PlatformOS          string        `json:"platformOs"`
	Schema              MSPSchemaInfo `json:"schema"`
	ServerInfo          MSPServerInfo `json:"serverInfo"`
	SessionDurability   *string       `json:"sessionDurability,omitempty"`
	UserAgent           string        `json:"userAgent"`
}

// MSPSessionConfig is kept as raw JSON because MSP 1.4 owns the nested
// configuration contract and may add compatible optional members.
type MSPSessionConfig = json.RawMessage

// MSPSessionStartParams creates a session. CommandID is supplied by the
// caller and is the server's idempotency handle.
type MSPSessionStartParams struct {
	CommandID     string           `json:"commandId"`
	SessionID     string           `json:"sessionId,omitempty"`
	ProviderID    *string          `json:"providerId,omitempty"`
	ModelID       string           `json:"modelId,omitempty"`
	WorkspaceRoot string           `json:"workspaceRoot,omitempty"`
	ApprovalMode  *string          `json:"approvalMode,omitempty"`
	Config        MSPSessionConfig `json:"config,omitempty"`
}

type MSPForkProvenance = json.RawMessage
type MSPEffectiveApprovalModeState = json.RawMessage

// MSPSession is the session projection returned by session/start and
// session/resume. Optional fields are pointers where the schema admits null.
type MSPSession struct {
	ActiveTurnID    *string                       `json:"activeTurnId"`
	ApprovalMode    MSPEffectiveApprovalModeState `json:"approvalMode,omitempty"`
	Attention       []string                      `json:"attention,omitempty"`
	Branch          *string                       `json:"branch,omitempty"`
	CreatedAt       string                        `json:"createdAt"`
	FirstUserPrompt string                        `json:"firstUserPrompt,omitempty"`
	ForkedFrom      MSPForkProvenance             `json:"forkedFrom"`
	LastActivityAt  string                        `json:"lastActivityAt,omitempty"`
	ModelID         *string                       `json:"modelId"`
	Name            string                        `json:"name,omitempty"`
	Path            string                        `json:"path"`
	ProviderID      *string                       `json:"providerId"`
	SessionID       string                        `json:"sessionId"`
	Status          string                        `json:"status"`
	Title           string                        `json:"title,omitempty"`
	TurnCount       int                           `json:"turnCount"`
	UpdatedAt       string                        `json:"updatedAt"`
	WorkspaceRoot   *string                       `json:"workspaceRoot"`
}

type MSPSessionStartResult struct {
	Session    MSPSession `json:"session"`
	ViewCursor string     `json:"viewCursor"`
}

// MSPHistoryPreference selects the server's history presentation mode.
type MSPHistoryPreference string

const (
	MSPHistoryAuto     MSPHistoryPreference = "auto"
	MSPHistoryInline   MSPHistoryPreference = "inline"
	MSPHistorySnapshot MSPHistoryPreference = "snapshot"
	MSPHistoryAnchored MSPHistoryPreference = "anchored"
)

type MSPSessionResumeParams struct {
	CommandID    string               `json:"commandId"`
	SessionID    string               `json:"sessionId"`
	Cursor       *string              `json:"cursor,omitempty"`
	ExcludeItems *bool                `json:"excludeItems,omitempty"`
	History      MSPHistoryPreference `json:"history,omitempty"`
	Config       MSPSessionConfig     `json:"config,omitempty"`
}

// MSPHistory keeps transcript items and snapshots as raw JSON because the
// client work here needs session/resume, while item rendering belongs to a
// separate MSP surface.
type MSPHistory struct {
	Items      json.RawMessage `json:"items"`
	Mode       string          `json:"mode"`
	NoneReason string          `json:"noneReason,omitempty"`
	Snapshot   json.RawMessage `json:"snapshot"`
}

type MSPPendingRequestPointer struct {
	ApprovalID  string `json:"approvalId,omitempty"`
	Kind        string `json:"kind"`
	UserInputID string `json:"userInputId,omitempty"`
	ViewCursor  string `json:"viewCursor"`
}

type MSPSessionResumeResult struct {
	History         MSPHistory                 `json:"history"`
	PendingRequests []MSPPendingRequestPointer `json:"pendingRequests"`
	Session         MSPSession                 `json:"session"`
	ViewCursor      string                     `json:"viewCursor"`
}

type MSPGoalSetParams struct {
	CommandID string `json:"commandId"`
	SessionID string `json:"sessionId"`
	Objective string `json:"objective"`
}

type MSPGoalEditParams = MSPGoalSetParams

type MSPGoalPauseParams struct {
	CommandID string `json:"commandId"`
	SessionID string `json:"sessionId"`
}

type MSPGoalResumeParams = MSPGoalPauseParams
type MSPGoalClearParams = MSPGoalPauseParams

type MSPCommandStatus string

const MSPCommandAccepted MSPCommandStatus = "accepted"

type MSPGoalCommandResult struct {
	CommandID string           `json:"commandId"`
	Status    MSPCommandStatus `json:"status"`
	TurnID    string           `json:"turnId,omitempty"`
}

type MSPIfBusy string

const (
	MSPIfBusyQueue   MSPIfBusy = "queue"
	MSPIfBusySteer   MSPIfBusy = "steer"
	MSPIfBusyReplace MSPIfBusy = "replace"
)

type MSPTurnInputPart struct {
	Type       string `json:"type"`
	Text       string `json:"text,omitempty"`
	Base64Data string `json:"base64Data,omitempty"`
	MediaType  string `json:"mediaType,omitempty"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
	Selector   string `json:"selector,omitempty"`
	Arguments  string `json:"arguments,omitempty"`
}

type MSPTurnStartParams struct {
	CommandID       string             `json:"commandId"`
	SessionID       string             `json:"sessionId"`
	Input           []MSPTurnInputPart `json:"input"`
	DisplayText     string             `json:"displayText,omitempty"`
	IfBusy          MSPIfBusy          `json:"ifBusy,omitempty"`
	ReasoningEffort string             `json:"reasoningEffort,omitempty"`
}

type MSPTurnStartDisposition string

const (
	MSPTurnStarted MSPTurnStartDisposition = "started"
	MSPTurnQueued  MSPTurnStartDisposition = "queued"
	MSPTurnSteered MSPTurnStartDisposition = "steered"
)

type MSPTurnStartResult struct {
	CommandID      string                  `json:"commandId"`
	Disposition    MSPTurnStartDisposition `json:"disposition"`
	StartedNewTurn bool                    `json:"startedNewTurn"`
	Status         MSPCommandStatus        `json:"status"`
	TurnID         string                  `json:"turnId"`
}

type MSPTurnSteerParams struct {
	CommandID       string             `json:"commandId"`
	SessionID       string             `json:"sessionId"`
	ExpectedTurnID  string             `json:"expectedTurnId"`
	Input           []MSPTurnInputPart `json:"input"`
	ReasoningEffort string             `json:"reasoningEffort,omitempty"`
}

type MSPTurnSteerResult struct {
	CommandID string           `json:"commandId"`
	Status    MSPCommandStatus `json:"status"`
	TurnID    string           `json:"turnId"`
}

type MSPTurnInterruptParams struct {
	CommandID string `json:"commandId"`
	SessionID string `json:"sessionId"`
	TurnID    string `json:"turnId,omitempty"`
	Retract   bool   `json:"retract,omitempty"`
}

type MSPTurnInterruptResult struct {
	CommandID string           `json:"commandId"`
	Status    MSPCommandStatus `json:"status"`
	TurnID    string           `json:"turnId"`
}

// MSPOptional distinguishes an omitted property from an explicit JSON null.
// MSP uses both states on session/goalChanged and session/statusChanged.
type MSPOptional[T any] struct {
	Present bool
	Null    bool
	Value   T
}

func (o *MSPOptional[T]) UnmarshalJSON(data []byte) error {
	o.Present = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		o.Null = true
		var zero T
		o.Value = zero
		return nil
	}
	o.Null = false
	return json.Unmarshal(data, &o.Value)
}

// MSPNotification is one typed event from the MSP session host.
type MSPNotification interface {
	MSPMethod() string
}

type MSPRawNotification struct {
	Name   string
	Params json.RawMessage
}

func (n MSPRawNotification) MSPMethod() string { return n.Name }

type MSPGoal struct {
	CurrentWork     string `json:"currentWork,omitempty"`
	NextWork        string `json:"nextWork,omitempty"`
	Objective       string `json:"objective"`
	PercentComplete int    `json:"percentComplete"`
	Status          string `json:"status"`
}

type MSPSessionGoalChangedNotification struct {
	SessionID   string               `json:"sessionId"`
	Goal        MSPOptional[MSPGoal] `json:"goal"`
	SourceRange json.RawMessage      `json:"sourceRange"`
	ViewCursor  string               `json:"viewCursor"`
}

func (MSPSessionGoalChangedNotification) MSPMethod() string { return "session/goalChanged" }

type MSPSessionStatusChangedNotification struct {
	Attention  []string            `json:"attention,omitempty"`
	SessionID  string              `json:"sessionId"`
	Status     string              `json:"status"`
	ViewCursor MSPOptional[string] `json:"viewCursor"`
}

func (MSPSessionStatusChangedNotification) MSPMethod() string { return "session/statusChanged" }

type MSPTokenUsage struct {
	CacheReadTokens  *int64 `json:"cacheReadTokens,omitempty"`
	CacheWriteTokens *int64 `json:"cacheWriteTokens,omitempty"`
	CachedTokens     int64  `json:"cachedTokens"`
	InputTokens      int64  `json:"inputTokens"`
	OutputTokens     int64  `json:"outputTokens"`
	ReasoningTokens  int64  `json:"reasoningTokens"`
}

type MSPCumulativeTokenUsage struct {
	OutputTokens     int64              `json:"outputTokens"`
	PromptTokens     int64              `json:"promptTokens"`
	TotalTokens      int64              `json:"totalTokens"`
	CacheReadTokens  *int64             `json:"cacheReadTokens,omitempty"`
	CacheWriteTokens *int64             `json:"cacheWriteTokens,omitempty"`
	Cost             *MSPCumulativeCost `json:"cost,omitempty"`
}

// MSPCumulativeCost is the optional 1.4.2 cost leg of cumulative usage
// (the pinned export qualification): a USD estimate with a partial flag. It is
// provenance only, never an invariant input: cost can move either way on
// price re-adoption, so the non-negative and non-decreasing token rules
// deliberately ignore it.
type MSPCumulativeCost struct {
	USD     float64 `json:"usd"`
	Partial bool    `json:"partial"`
}

type MSPSessionTokenUsageNotification struct {
	Cumulative   MSPCumulativeTokenUsage `json:"cumulative"`
	DurationMs   *int64                  `json:"durationMs,omitempty"`
	FinishReason string                  `json:"finishReason,omitempty"`
	ModelID      *string                 `json:"modelId,omitempty"`
	PromptTokens int64                   `json:"promptTokens"`
	SessionID    string                  `json:"sessionId"`
	SourceRange  json.RawMessage         `json:"sourceRange"`
	TotalTokens  int64                   `json:"totalTokens"`
	TurnID       string                  `json:"turnId"`
	Usage        MSPTokenUsage           `json:"usage"`
	ViewCursor   string                  `json:"viewCursor"`
}

func (MSPSessionTokenUsageNotification) MSPMethod() string { return "session/tokenUsage" }

type MSPTurnStartedNotification struct {
	CommandID   string          `json:"commandId"`
	SessionID   string          `json:"sessionId"`
	SourceRange json.RawMessage `json:"sourceRange"`
	TurnID      string          `json:"turnId"`
	ViewCursor  string          `json:"viewCursor"`
}

func (MSPTurnStartedNotification) MSPMethod() string { return "turn/started" }

type MSPTurnError struct {
	Kind      string `json:"kind"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type MSPTurnCompletedNotification struct {
	DurationMs         *int64          `json:"durationMs,omitempty"`
	Error              *MSPTurnError   `json:"error,omitempty"`
	Reason             string          `json:"reason,omitempty"`
	SessionID          string          `json:"sessionId"`
	SourceRange        json.RawMessage `json:"sourceRange"`
	Terminal           string          `json:"terminal"`
	TimeToFirstTokenMs *int64          `json:"timeToFirstTokenMs,omitempty"`
	TurnID             string          `json:"turnId"`
	Usage              *MSPTokenUsage  `json:"usage,omitempty"`
	ViewCursor         string          `json:"viewCursor"`
}

func (MSPTurnCompletedNotification) MSPMethod() string { return "turn/completed" }

type MSPTurnRetractedNotification struct {
	CommandID   string          `json:"commandId"`
	SessionID   string          `json:"sessionId"`
	SourceRange json.RawMessage `json:"sourceRange"`
	TurnID      string          `json:"turnId"`
	ViewCursor  string          `json:"viewCursor"`
}

func (MSPTurnRetractedNotification) MSPMethod() string { return "turn/retracted" }

type MSPTurnRetryScheduledNotification struct {
	Attempt      int             `json:"attempt"`
	MaxAttempts  int             `json:"maxAttempts"`
	NextAttempt  int             `json:"nextAttempt"`
	Reason       string          `json:"reason"`
	RetryDelayMs int64           `json:"retryDelayMs"`
	SessionID    string          `json:"sessionId"`
	SourceRange  json.RawMessage `json:"sourceRange"`
	TurnID       string          `json:"turnId"`
	ViewCursor   string          `json:"viewCursor"`
}

func (MSPTurnRetryScheduledNotification) MSPMethod() string { return "turn/retryScheduled" }

type MSPTurnUnqueuedNotification struct {
	CommandID   string          `json:"commandId"`
	SessionID   string          `json:"sessionId"`
	SourceRange json.RawMessage `json:"sourceRange"`
	TurnID      string          `json:"turnId"`
	ViewCursor  string          `json:"viewCursor"`
}

func (MSPTurnUnqueuedNotification) MSPMethod() string { return "turn/unqueued" }

// mspTokenUsageNegative reports whether any counter in a raw usage block is
// negative. The pinned schema carries no minimum on these integers, so only
// the host can refuse them; every consumed usage number must be non-negative
// (r20 rule 3). A nil block is absent evidence, never negative evidence.
func mspTokenUsageNegative(usage *MSPTokenUsage) bool {
	if usage == nil {
		return false
	}
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.CachedTokens < 0 || usage.ReasoningTokens < 0 {
		return true
	}
	if usage.CacheReadTokens != nil && *usage.CacheReadTokens < 0 {
		return true
	}
	if usage.CacheWriteTokens != nil && *usage.CacheWriteTokens < 0 {
		return true
	}
	return false
}

// mspCumulativeUsageNegative reports whether any counter in a cumulative
// usage block is negative. Cumulative totals are adopted by max only, so a
// negative total is unreadable evidence, never a skip. The optional 1.4.2
// cost leg is deliberately excluded: cost can move either way and never
// feeds the token invariants.
func mspCumulativeUsageNegative(cumulative MSPCumulativeTokenUsage) bool {
	return cumulative.TotalTokens < 0 || cumulative.OutputTokens < 0 || cumulative.PromptTokens < 0
}

// mspTokenUsagesEqual reports whether two usage blocks state the same spend.
// A nil block equals only another nil block; presence is evidence.
func mspTokenUsagesEqual(first, second *MSPTokenUsage) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	if first.InputTokens != second.InputTokens || first.OutputTokens != second.OutputTokens ||
		first.CachedTokens != second.CachedTokens || first.ReasoningTokens != second.ReasoningTokens {
		return false
	}
	if (first.CacheReadTokens == nil) != (second.CacheReadTokens == nil) {
		return false
	}
	if first.CacheReadTokens != nil && *first.CacheReadTokens != *second.CacheReadTokens {
		return false
	}
	if (first.CacheWriteTokens == nil) != (second.CacheWriteTokens == nil) {
		return false
	}
	if first.CacheWriteTokens != nil && *first.CacheWriteTokens != *second.CacheWriteTokens {
		return false
	}
	return true
}
