package sessionmanager

import "strings"

// This checks host-owned evidence against its admitted command, independently
// of the fork's canonical promotion. No weaker class can satisfy provider-ack.
func ValidPTYInputObservation(input InputSubmission, observation InputObservation) bool {
	if command := input.Payload.ClaudeGoal; command != nil {
		actual := observation.ClaudeGoal
		if observation.PTYRun != nil || actual == nil || actual.CommandID != command.CommandID || strings.TrimSpace(actual.ObservedText) == "" {
			return false
		}
		if command.Clear != nil {
			_, ok := firstClaudeMarker(actual.ObservedText, []string{"goal cleared", "no goal set"})
			return ok && actual.Operation == ClaudeGoalOperationClear && actual.NormalizedToken == "" && observation.EvidenceClass == InputEvidenceClaudePTYClearObserved
		}
		if actual.Operation != ClaudeGoalOperationSet || command.Set == nil || actual.NormalizedToken != command.Set.Token {
			return false
		}
		var markers []claudeConditionedMarker
		for _, prefix := range []string{"goal set:", "goal updated:", "goal active:", "goal:"} {
			markers = append(markers, claudeConditionedMarker{prefix: prefix, key: command.Set.Token, threshold: claudeGoalTokenCoverageThreshold})
		}
		match, ok := firstClaudeConditionedEvidence(actual.ObservedText, markers)
		return ok && match.evidence == observation.EvidenceClass
	}
	if input.Expected.Kind == "run-observed-after-snapshot" {
		return input.Payload.Text != nil && observation.ClaudeGoal == nil && observation.EvidenceClass == InputEvidenceRunObservedAfterSnapshot && observation.PTYRun != nil && strings.TrimSpace(observation.PTYRun.RunID) != ""
	}
	return observation.ClaudeGoal == nil && observation.PTYRun == nil && (observation.EvidenceClass == "" || input.Expected.Kind == "provider-ack" && observation.EvidenceClass == InputEvidenceExactProviderAck)
}
