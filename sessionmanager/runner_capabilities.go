package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

var ErrProviderHostNotFound = errors.New("provider host not found")

const (
	ProviderHostRunning ProviderHostState = "running"
	ProviderHostStopped ProviderHostState = "stopped"
)

type ProviderActivityObserver interface {
	Activity(context.Context) (ProviderActivitySnapshot, error)
}

type ProviderActivityState string

const (
	ProviderActivityUnknown ProviderActivityState = "unknown"
	ProviderActivityIdle    ProviderActivityState = "idle"
	ProviderActivityBusy    ProviderActivityState = "busy"
)

type ProviderActivitySnapshot struct {
	State       ProviderActivityState
	LastEventAt time.Time
}

type ProviderAttachmentTarget interface {
	AttachmentTarget(context.Context) (string, error)
}

type ProviderSessionNotifier interface {
	DeliverSessionNotice(context.Context, string) error
}

type ProviderClientHome interface {
	ClientHome() string
}

type ProviderClientToolPolicy interface {
	ClientToolPolicy() ClientToolPolicy
}

type ProviderNativeIdentity interface {
	NativeIdentity() string
}

type ProviderLaunchReporter interface {
	ProviderLaunchMetadata() ProviderLaunchMetadata
}

type ProviderLaunchMetadata struct {
	PID        int
	Executable string
}

type ProviderRunStatus string

const (
	ProviderRunComplete      ProviderRunStatus = "complete"
	ProviderRunBlocked       ProviderRunStatus = "blocked"
	ProviderRunUsageLimited  ProviderRunStatus = "usage_limited"
	ProviderRunBudgetLimited ProviderRunStatus = "budget_limited"
	ProviderRunIdle          ProviderRunStatus = "idle"
	ProviderRunFailed        ProviderRunStatus = "provider_failure"
)

type ProviderRunRequest struct {
	Prompt string `json:"prompt"`
}

type ProviderRunResult struct {
	ExitCode       int               `json:"exit_code"`
	Status         ProviderRunStatus `json:"status"`
	ProviderStatus string            `json:"provider_status,omitempty"`
	AckSource      string            `json:"ack_source,omitempty"`
	Detail         string            `json:"detail,omitempty"`
	Recoverable    bool              `json:"recoverable,omitempty"`
	Output         string            `json:"output,omitempty"`
	Usage          ProviderUsage     `json:"usage,omitempty"`
}

type ProviderUsage struct {
	InputTokens           int64 `json:"input_tokens,omitempty"`
	CachedInputTokens     int64 `json:"cached_input_tokens,omitempty"`
	CacheCreationTokens   int64 `json:"cache_creation_tokens,omitempty"`
	OutputTokens          int64 `json:"output_tokens,omitempty"`
	ReasoningOutputTokens int64 `json:"reasoning_output_tokens,omitempty"`
	TotalTokens           int64 `json:"total_tokens,omitempty"`
}

type ProviderOwnerRunner interface {
	RunOwner(context.Context, ProviderRunRequest) (*ProviderRunResult, error)
}

type PublicError struct {
	Code       string
	Message    string
	StatusCode int
}

type TerminalHost interface {
	AttachTerminal(ctx context.Context) (io.ReadWriteCloser, error)
	ResizeTerminal(rows, cols uint16) error
}

var ErrTerminalUnsupported = errors.New("provider session does not expose a terminal")

func (e *PublicError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("%s (%d): %s", e.Code, e.StatusCode, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// ProviderHost is the neutral runner lifecycle contract.
type ProviderHost = RunnerHost

// Published refusal shared by both Curator-managed provider classifiers.
const ClaudeFragmentPermissionsLocked = "fragment_permissions_locked"
