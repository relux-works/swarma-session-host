package sessioncontext

import (
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"io"
	"time"
)

// ProviderPolicy supplies the existing Codex decision hooks with the actual
// structural scanner and presence/recovery policy. It performs no native RPC.
type ProviderPolicy struct{}

var _ sm.CodexContextPolicy = ProviderPolicy{}

func (ProviderPolicy) RequirePayloadPresent(r sm.CodexPresenceReport, ready time.Time, mode sm.ContextResolutionMode) error {
	report := ContextPresenceReport{CtxID: r.CtxID, Provider: "codex", Presence: ContextPayloadPresence(r.Presence), Detail: r.Detail, CheckedAt: r.CheckedAt}
	return contextPayloadVanished(r.CtxID, RequireContextPayloadPresent(presenceProbeRecord(r.CtxID, "codex", ready), report, mode, sm.RetentionPolicy{Now: r.CheckedAt}))
}
func (ProviderPolicy) ResumeUnusable(a sm.CodexResumeAttempt) error {
	if a.Cause == nil {
		return fmt.Errorf("actual failed resume cause is required")
	}
	return contextResumeUnusable(ContextResumeAttempt{CtxID: a.CtxID, Provider: sm.ContextProvider(a.Provider), RecordedVersion: a.RecordedVersion, ActiveVersion: a.ActiveVersion, Operation: a.Operation, Cause: a.Cause})
}
func (ProviderPolicy) InspectTranscript(r io.Reader) (string, string, error) {
	i, e := InspectTranscript("codex", r)
	if e != nil {
		return "", "", e
	}
	marker := ""
	if i.Compaction != nil {
		marker = i.Compaction.Marker
	}
	return i.SHA256, marker, nil
}

// Claude failures carry actual provider observations. Unknown probe failures
// refuse without claiming the payload vanished or invalidating the registry.
func ClaudePolicyError(e error) error {
	switch x := e.(type) {
	case *sm.ClaudePayloadPresenceError:
		if x == nil {
			return fmt.Errorf("Claude payload presence facts unavailable")
		}
		report := ContextPresenceReport{CtxID: x.ContextID, Provider: "claude", Presence: ContextPayloadPresence(x.Presence), Detail: x.Detail, CheckedAt: x.CheckedAt}
		return contextPayloadVanished(x.ContextID, RequireContextPayloadPresent(presenceProbeRecord(x.ContextID, "claude", x.ReadyAt), report, x.Mode, sm.RetentionPolicy{Now: x.CheckedAt}))
	case *sm.ClaudeResumeAttempt:
		if x == nil || x.Cause == nil {
			return fmt.Errorf("actual failed Claude resume cause required")
		}
		return contextResumeUnusable(ContextResumeAttempt{CtxID: x.CtxID, Provider: "claude", RecordedVersion: x.RecordedVersion, ActiveVersion: x.ActiveVersion, Operation: x.Operation, Cause: x.Cause})
	default:
		return e
	}
}
