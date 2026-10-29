package sessionmanager

import "strings"

// The native requested terms select the existing divergence-over-echo ordering.
// They are never returned in place of an unstated term. Only the fork decides
// whether these actual stated facts permit its canonical owner outcome.
func museStatedNativeTerms(session MSPSession, goal museGoalRead, requestedApproval, requestedModel string) (string, string) {
	mode, _ := museEffectiveApprovalMode(session.ApprovalMode)
	model := ""
	if session.ModelID != nil {
		model = strings.TrimSpace(*session.ModelID)
	}
	return musePickStatedTerm(requestedApproval, mode, goal.EffectiveApprovalMode), musePickStatedTerm(requestedModel, model, goal.EffectiveModelID)
}
