package sessioncontext

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// A registry entry is metadata. It outlives its payload by design, and the two
// providers make that happen in opposite ways: Claude deletes session
// transcripts on a timer nobody tells the board about, while no Codex pruning is
// documented or has been observed, so an absent Codex rollout is a loss the
// board has no cleanup schedule to explain. Neither case is knowable from the
// record, and "the provider has never been seen deleting these" is not the same
// as "these cannot be gone" — so the only layer that can answer "is the payload
// still here" is this one. The Session Manager is the machine-local half that
// owns provider homes and handles, so every serve path goes through a probe here
// before it touches a transcript or opens an RPC.

const (
	// codexSessionsDirName is the live rollout store inside a Codex home.
	codexSessionsDirName = "sessions"
	// codexArchivedSessionsDirName is where `codex` moves a rollout when it is
	// archived. Archiving moves rather than deletes, so a payload that is absent
	// from the live store may still be occupying disk here with everything it
	// captured — which matters both for an honest presence message and for
	// reclamation, which has to sweep both.
	codexArchivedSessionsDirName = "archived_sessions"
)

// codexRolloutLocations is every place one Codex thread's rollout was found.
//
// Live and Archived are kept apart rather than merged because they answer
// different questions: only a live rollout can be served, while an archived one
// still has to be reclaimed.
type codexRolloutLocations struct {
	Live     []string
	Archived []string
}

// locateCodexRollouts finds every rollout file belonging to one thread under a
// Codex home.
//
// A missing directory is not an error: an empty or never-created store is a
// perfectly ordinary "not here", and turning it into a failure would make the
// probe unable to report the one verdict it exists to report.
func locateCodexRollouts(codexHome, threadID string) (codexRolloutLocations, error) {
	var found codexRolloutLocations
	home := strings.TrimSpace(codexHome)
	thread := strings.TrimSpace(threadID)
	if home == "" || thread == "" {
		return found, fmt.Errorf("a Codex home and thread id are required to locate a rollout")
	}
	suffix := "-" + strings.ToLower(thread) + ".jsonl"
	scan := func(root string) ([]string, error) {
		var matches []string
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), suffix) {
				return nil
			}
			matches = append(matches, path)
			return nil
		})
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("scanning Codex rollout store %s: %w", root, err)
		}
		return matches, nil
	}
	live, err := scan(filepath.Join(home, codexSessionsDirName))
	if err != nil {
		return found, err
	}
	archived, err := scan(filepath.Join(home, codexArchivedSessionsDirName))
	if err != nil {
		return found, err
	}
	found.Live = live
	found.Archived = archived
	return found, nil
}

// probeCodexPayloadPresence answers whether a Codex context still has a rollout
// that can be served on this machine.
//
// Only the live store counts as present. An archived rollout is on disk but the
// app-server does not serve it, so reporting it as present would produce a
// context that resolves and then fails at RPC time — the exact surprise this
// probe removes. It is named in the detail instead, because "the bytes are in
// the archive" is a different operator action from "the bytes are gone".
func probeCodexPayloadPresence(ctxID, codexHome, threadID string, at time.Time) ContextPresenceReport {
	report := ContextPresenceReport{
		CtxID:     NormalizeContextID(ctxID),
		Provider:  ContextProviderCodex,
		CheckedAt: at.UTC(),
	}
	found, err := locateCodexRollouts(codexHome, threadID)
	if err != nil {
		report.Presence = ContextPayloadUnverifiable
		report.Detail = err.Error()
		return report
	}
	switch {
	case len(found.Live) > 0:
		report.Presence = ContextPayloadPresent
	case len(found.Archived) > 0:
		report.Presence = ContextPayloadVanished
		report.Detail = fmt.Sprintf(
			"the live Codex rollout store holds no thread %s, but %d archived rollout(s) for it remain "+
				"on disk; unarchive it to serve this context again, or let board retention reclaim it",
			threadID, len(found.Archived),
		)
	default:
		report.Presence = ContextPayloadVanished
		report.Detail = fmt.Sprintf(
			"neither the live nor the archived Codex rollout store holds thread %s", threadID,
		)
	}
	return report
}

// probeClaudePayloadPresence answers whether a Claude base transcript is still
// on this machine.
//
// A stat is the whole probe and that is deliberate: the question is existence,
// not readability. Content problems are a different failure with a different
// remedy, and folding them together is what previously made a provider-deleted
// base look like a corrupt one.
func probeClaudePayloadPresence(ctxID, transcriptPath string, at time.Time) ContextPresenceReport {
	report := ContextPresenceReport{
		CtxID:     NormalizeContextID(ctxID),
		Provider:  ContextProviderClaude,
		CheckedAt: at.UTC(),
	}
	if strings.TrimSpace(transcriptPath) == "" {
		report.Presence = ContextPayloadUnverifiable
		report.Detail = "the base transcript path could not be derived on this machine"
		return report
	}
	switch info, err := os.Stat(transcriptPath); {
	case err == nil && info.IsDir():
		report.Presence = ContextPayloadUnverifiable
		report.Detail = "the base transcript path resolves to a directory"
	case err == nil:
		report.Presence = ContextPayloadPresent
	case errors.Is(err, fs.ErrNotExist):
		report.Presence = ContextPayloadVanished
		report.Detail = "the provider session directory holds no transcript for this base session id"
	default:
		report.Presence = ContextPayloadUnverifiable
		report.Detail = err.Error()
	}
	return report
}

// presenceProbeRecord is the minimum record a presence refusal needs.
//
// The provider adapters hold a private fork plan, not a board record, and
// deliberately so — a provider path that could read the registry would be a
// second authority over it. The plan carries the two facts the refusal message
// is derived from: which context this is, and when it was published, which is
// what decides whether "gone" is attributable to the provider's own timer.
func presenceProbeRecord(
	ctxID string,
	provider ContextProvider,
	readyAt time.Time,
) *ContextRecord {
	return &ContextRecord{
		ID:       NormalizeContextID(ctxID),
		Provider: provider,
		State:    ContextStateReady,
		ReadyAt:  readyAt.UTC(),
	}
}

// contextPayloadUnusableError is a provider-path failure the registry can
// recover from rather than only report.
//
// It exists because the two halves live in different layers. The adapter is the
// only layer that can tell a vanished payload from a broken one, and the
// manager is the only layer that may write to the registry — so the adapter
// names the context and the reason, and the manager turns that into the stale
// transition and the rebuild plan.
type contextPayloadUnusableError struct {
	CtxID  string
	Reason ContextStateReason
	Err    error
}

func (e *contextPayloadUnusableError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return fmt.Sprintf("context %s payload is unusable (%s)", e.CtxID, e.Reason)
	}
	return e.Err.Error()
}

func (e *contextPayloadUnusableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// contextPayloadVanished wraps a presence refusal so the manager can route it.
func contextPayloadVanished(ctxID string, err error) error {
	if err == nil {
		return nil
	}
	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != ContextErrorPayloadVanished {
		// Unverified is a refusal, not a recovery: the board learned nothing about
		// the payload, so marking the record stale would record a finding the probe
		// never made.
		return err
	}
	return &contextPayloadUnusableError{
		CtxID:  NormalizeContextID(ctxID),
		Reason: ContextReasonPayloadVanished,
		Err:    err,
	}
}

// contextResumeUnusable wraps a failed cross-build resume so the manager can
// route it the same way.
//
// The ordering the constructor enforces upstream is the whole policy: the
// provider attempt has already happened and failed by the time this is called.
// The accepted research measured contexts surviving large CLI version gaps, so
// refusing ahead of the attempt would reject work that demonstrably succeeds.
func contextResumeUnusable(attempt ContextResumeAttempt) error {
	return &contextPayloadUnusableError{
		CtxID:  NormalizeContextID(attempt.CtxID),
		Reason: ContextReasonProviderIncompatible,
		Err:    ContextResumeFallback(attempt),
	}
}
