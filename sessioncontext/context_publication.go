package sessioncontext

import (
	"context"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"strings"
)

// Publication uses the actual completed private snapshot, not a control-plane
// path. The injected authority still must scan/classify the exact local file and
// validate the builder receipt; a path or digest alone is no publication grant.
func (m *Service) publishCompletedContextLocked(ctx context.Context, r *materializationRecord, c *sm.ContextRecord, h sm.ProviderHostRecord) (*sm.ContextRecord, error) {
	path, err := materializationTranscriptPath(r.Provider, h)
	if err != nil {
		return nil, err
	}
	inspection, err := inspectTranscriptPath(r.Provider, path)
	if err != nil {
		return nil, err
	}
	if inspection.Compaction != nil {
		return nil, &ContextError{Code: "context_integrity_unverified", Message: "compacted transcript cannot be published"}
	}
	var enforcement *sm.ContextBuilderEnforcement
	if m.builders != nil {
		enforcement, err = m.builders.Enforcement(ctx, r.CtxID)
		if err != nil {
			return nil, err
		}
	}
	if c.RequiresConfinement && enforcement == nil {
		return nil, fmt.Errorf("builder enforcement receipt unavailable")
	}
	policy, err := m.localPolicySnapshot(ctx)
	if err != nil {
		return nil, err
	}
	out, err := m.materializationAuthority.PublishContext(ctx, sm.PublicationRequest{CtxID: r.CtxID, ExpectedRevision: c.Revision, TranscriptPath: path, BuilderAllowlist: policy.BuilderAllowlist, BuilderEnforcement: enforcement, Now: m.now()})
	if err != nil {
		return nil, err
	}
	if out == nil || out.ID != r.CtxID || out.Provider != r.Provider || out.State != "ready" || out.Payload.SHA256 != inspection.SHA256 {
		return nil, fmt.Errorf("publication returned no matching ready receipt")
	}
	return out, nil
}
func (m *Service) abandonPublishedCompletionLocked(ctx context.Context, r *materializationRecord, cause error) error {
	err := m.releaseFailedMaterializationLocked(ctx, r, contextPublicationFailureReason(cause))
	if err != nil {
		return fmt.Errorf("%w (the writable child could not be released after the failed publication: %v)", cause, err)
	}
	return cause
}
func contextPublicationFailureReason(e error) sm.StateReason {
	if isContextErrorCode(e, "context_classification_failed") || isContextErrorCode(e, "context_integrity_unverified") {
		return "classification_failed"
	}
	return "building_failed"
}
func inspectTranscriptPath(p sm.ContextProvider, path string) (TranscriptInspection, error) {
	if err := rejectSymlinkAncestors(path); err != nil {
		return TranscriptInspection{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return TranscriptInspection{}, err
	}
	defer f.Close()
	i, err := f.Stat()
	if err != nil {
		return TranscriptInspection{}, err
	}
	if !i.Mode().IsRegular() {
		return TranscriptInspection{}, fmt.Errorf("provider transcript is not a regular file")
	}
	return InspectTranscript(p, f)
}
func materializationTranscriptPath(p sm.ContextProvider, h sm.ProviderHostRecord) (string, error) {
	unavailable := func(detail string) (string, error) {
		return "", &ContextError{Code: "context_run_transcript_unavailable", Message: fmt.Sprintf("the completed %s run left no transcript this machine can verify (%s), so its writable child cannot be published as a base", p, detail)}
	}
	if h.Provider != string(p) {
		return unavailable("provider-host mismatch")
	}
	switch p {
	case "codex":
		s := h.Snapshot.Codex
		if s == nil {
			return unavailable("its provider-host snapshot could not be read")
		}
		found, e := locateCodexRollouts(s.CodexHome, s.ThreadID)
		if e != nil {
			return unavailable("its rollout store could not be scanned")
		}
		if len(found.Live) != 1 {
			return unavailable("exactly one live rollout is required; archived rollouts cannot be served")
		}
		return found.Live[0], nil
	case "claude":
		s := h.Snapshot.Claude
		if s == nil || s.SessionUUID == "" || filepath.Base(s.SessionUUID) != s.SessionUUID || strings.ContainsAny(s.SessionUUID, `/\`) {
			return unavailable("its provider-host snapshot could not be read")
		}
		// Use the source materializer's observed project slug. No guessed cwd or
		// fabricated provider identity can be substituted when it is unavailable.
		if s.ContextProjectSlug == "" || filepath.Base(s.ContextProjectSlug) != s.ContextProjectSlug || strings.ContainsAny(s.ContextProjectSlug, `/\`) {
			return unavailable("its observed provider project slug is unavailable")
		}
		home := os.Getenv("CLAUDE_CONFIG_DIR")
		if home == "" {
			v, e := os.UserHomeDir()
			if e != nil {
				return unavailable("this machine has no readable Claude home")
			}
			home = filepath.Join(v, ".claude")
		}
		if !filepath.IsAbs(home) {
			return unavailable("provider home must be absolute")
		}
		return filepath.Join(home, "projects", s.ContextProjectSlug, s.SessionUUID+".jsonl"), nil
	default:
		return unavailable("that provider has no forkable transcript")
	}
}
