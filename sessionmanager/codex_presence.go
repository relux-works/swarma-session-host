package sessionmanager

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"
)

const (
	codexSessionsDirName         = "sessions"
	codexArchivedSessionsDirName = "archived_sessions"
)

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
func probeCodexPayloadPresence(ctxID, codexHome, threadID string, at time.Time) CodexPresenceReport {
	report := CodexPresenceReport{
		CtxID:     ctxID,
		CheckedAt: at.UTC(),
	}
	found, err := locateCodexRollouts(codexHome, threadID)
	if err != nil {
		report.Presence = "unverifiable"
		report.Detail = err.Error()
		return report
	}
	switch {
	case len(found.Live) > 0:
		report.Presence = "present"
	case len(found.Archived) > 0:
		report.Presence = "vanished"
		report.Detail = fmt.Sprintf(
			"the live Codex rollout store holds no thread %s, but %d archived rollout(s) for it remain "+
				"on disk; unarchive it to serve this context again, or let board retention reclaim it",
			threadID, len(found.Archived),
		)
	default:
		report.Presence = "vanished"
		report.Detail = fmt.Sprintf(
			"neither the live nor the archived Codex rollout store holds thread %s", threadID,
		)
	}
	return report
}
