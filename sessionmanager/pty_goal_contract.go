package sessionmanager

// D14 classifies actual evidence. A host request identity is not a native echo,
// and normalized expected text is never represented as observed native text.
type InputEvidenceClass string

const (
	InputEvidenceExactProviderAck          InputEvidenceClass = "exact-provider-ack"
	InputEvidenceClaudePTYConditioned      InputEvidenceClass = "claude-pty-conditioned"
	InputEvidenceClaudePTYLossyConditioned InputEvidenceClass = "claude-pty-lossy-conditioned"
	InputEvidenceClaudePTYClearObserved    InputEvidenceClass = "claude-pty-clear-observed"
	InputEvidenceRunObservedAfterSnapshot  InputEvidenceClass = "run-observed-after-snapshot"
)

type ClaudeGoalCommand struct {
	CommandID string
	Set       *ClaudeGoalSet
	Clear     *ClaudeGoalClear
}
type ClaudeGoalSet struct {
	Text  string
	Token string
}
type ClaudeGoalClear struct{ Text string }
type ClaudeGoalOperation string

const (
	ClaudeGoalOperationSet   ClaudeGoalOperation = "set"
	ClaudeGoalOperationClear ClaudeGoalOperation = "clear"
)

type ClaudeGoalObservation struct {
	CommandID       string
	Operation       ClaudeGoalOperation
	ObservedText    string
	NormalizedToken string
}

// The host owns the pre-input native run snapshot. RunID is the actually
// observed set difference, not evidence that the run began after Enter.
type PTYRunObservation struct{ RunID string }
