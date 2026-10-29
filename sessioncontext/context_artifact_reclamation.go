package sessioncontext

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// This is the consumer of task-board-claude-reclamation-v1.
//
// The producer exists because a destructive path-based cleanup in the Claude
// materializer survived three rework cycles and was removed rather than
// hardened. That materializer now deletes nothing and records what it left
// behind, and this file is the only place allowed to remove it. So it is written
// against that history rather than around it:
//
//   - only paths the manifest named, never a path derived from another one;
//   - never a directory and never a recursive walk, because the defect that
//     caused the removal was a derived path plus a recursive delete;
//   - never through a symlink, at the file or at any parent, because a link is
//     how a delete confined to a private directory reaches out of one;
//   - only a file whose name is one this board minted, so ownership is proved by
//     the manifest rather than assumed from location;
//   - quarantine, not deletion, whenever ownership cannot be established.
//
// Everything it removes is also joined against the live catalog first: an
// artifact set whose base or child is still materialized, leased, or read by a
// session is not debris, whatever the event says.

const claudeReclaimedLedgerDirName = ".reclaimed"

// claudeStagingArtifactPrefix is the name a board-written staging transcript
// carries. It is the ownership evidence for the one artifact role that has no
// session UUID of its own.
const claudeStagingArtifactPrefix = ".task-board-materializing-"

// ContextArtifactReclamationStatus is what the sweep did with one artifact set.
type ContextArtifactReclamationStatus string

const (
	ContextArtifactReclaimed  ContextArtifactReclamationStatus = "reclaimed"
	ContextArtifactPlanned    ContextArtifactReclamationStatus = "planned"
	ContextArtifactRetained   ContextArtifactReclamationStatus = "retained"
	ContextArtifactQuarantine ContextArtifactReclamationStatus = "quarantined"
	ContextArtifactFailed     ContextArtifactReclamationStatus = "failed"
)

// ContextArtifactReclamationOutcome is one artifact set's result. It carries
// counts and a reason, never a provider path or a session UUID: this value
// crosses the control plane.
type ContextArtifactReclamationOutcome struct {
	ArtifactSetID string                           `json:"artifact_set_id"`
	Kind          string                           `json:"kind,omitempty"`
	Status        ContextArtifactReclamationStatus `json:"status"`
	Files         int                              `json:"files,omitempty"`
	// Quarantined is how many named artifacts were left in place because
	// ownership could not be proved. A set with any quarantine is reported as
	// quarantined even when other files went, because a partial reclamation that
	// reads as complete is how the leftover bytes stop being looked for.
	Quarantined int    `json:"quarantined,omitempty"`
	Detail      string `json:"detail"`
}

// claudeReclamationLedgerEntry is the private deletion audit.
//
// It exists because the event catalog is append-only and its state enum is the
// producer's, not this consumer's. Without a terminal record every pass would
// re-plan every set it has ever reclaimed, and "already gone" would be
// indistinguishable from "still owed" — the same convergence problem the payload
// sweep solves with a confirmation on the board.
type claudeReclamationLedgerEntry struct {
	ArtifactSetID string    `json:"artifact_set_id"`
	Files         int       `json:"files"`
	Quarantined   int       `json:"quarantined"`
	Detail        string    `json:"detail,omitempty"`
	ReclaimedAt   time.Time `json:"reclaimed_at"`
}

// claudeReclamationNamesContext reports whether the private catalog holds a
// record able to name one context's own artifacts.
//
// It is the release precondition for a Claude handle. The catalog is keyed on
// the child context an artifact set was produced for, and that set's
// `writable_fork` entry is the transcript the materialization record points at,
// so a match means the bytes survive the loss of the catalog entry with a name
// still attached to them.
//
// The deletion ledger is deliberately not consulted. A set already reclaimed
// left nothing to strand, and a set still owed is named by the event this walk
// just matched, so both answers are the same one and reading the ledger would
// only look like a condition without being one.
func claudeReclamationNamesContext(layout sm.Layout, ctxID string) bool {
	normalized := normalizeContextID(ctxID)
	if normalized == "" {
		return false
	}
	root := filepath.Join(layout.StateDir, claudeReclamationDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(root, entry.Name()))
		if readErr != nil {
			continue
		}
		var event claudeReclamationEvent
		if json.Unmarshal(data, &event) != nil ||
			event.Contract != claudeReclamationContract ||
			normalizeContextID(event.ChildCtxID) != normalized {
			continue
		}
		return true
	}
	return false
}

// reclaimContextArtifactsLocked reclaims the provider-local debris the Claude
// adapters deliberately left behind.
//
// `releasing` is the set of contexts whose private handles this same pass is
// dropping. It is passed in rather than re-read because the two halves have to
// agree inside one pass: the materialization sweep is what makes an artifact set
// debris, and a dry run that reported every set retained because the handle it
// was about to release still existed would describe a pass nobody will ever run.
func (m *Service) reclaimContextArtifactsLocked(
	settings sm.ContextRetentionSettings,
	now time.Time,
	dryRun bool,
	releasing map[string]struct{},
) []ContextArtifactReclamationOutcome {
	sets, corrupt := scanClaudeReclamationCatalog(m.layout)
	outcomes := make([]ContextArtifactReclamationOutcome, 0, len(sets)+len(corrupt))
	for _, name := range corrupt {
		// A record this consumer cannot read is not permission to delete anything.
		// It is reported and left, because the alternative — guessing at what a
		// damaged manifest meant — is exactly the derived-path deletion this whole
		// path exists to avoid.
		outcomes = append(outcomes, ContextArtifactReclamationOutcome{
			ArtifactSetID: name,
			Status:        ContextArtifactQuarantine,
			Detail: "the private reclamation record could not be read, so nothing it may have " +
				"named is removed",
		})
	}

	ids := make([]string, 0, len(sets))
	for id := range sets {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		event := sets[id]
		outcome := ContextArtifactReclamationOutcome{
			ArtifactSetID: id,
			Kind:          string(event.Kind),
		}
		if reason, ok := m.artifactSetRetainedLocked(event, settings, now, releasing); !ok {
			outcome.Status = ContextArtifactRetained
			outcome.Detail = reason
			outcomes = append(outcomes, outcome)
			continue
		}
		targets, refusals := m.ownedClaudeArtifacts(event)
		outcome.Quarantined = len(refusals)
		if dryRun {
			outcome.Status = ContextArtifactPlanned
			outcome.Files = len(targets)
			outcome.Detail = fmt.Sprintf(
				"would remove %d artifact files this board minted; %d named artifacts could not be "+
					"proved board-owned and would be left in place",
				len(targets), len(refusals),
			)
			if len(refusals) > 0 {
				outcome.Detail += ": " + strings.Join(refusals, "; ")
			}
			outcomes = append(outcomes, outcome)
			continue
		}
		removed, err := removeClaudeArtifacts(targets)
		outcome.Files = removed
		if err != nil {
			outcome.Status = ContextArtifactFailed
			outcome.Detail = fmt.Sprintf(
				"removal did not complete and was not recorded, so the next sweep retries it: %v", err,
			)
			m.logOperatorError("context_artifact_reclamation_failed", err, map[string]any{
				"artifact_set_id": id,
			})
			outcomes = append(outcomes, outcome)
			continue
		}
		outcome.Status = ContextArtifactReclaimed
		outcome.Detail = fmt.Sprintf("removed %d board-minted artifact files", removed)
		if len(refusals) > 0 {
			outcome.Status = ContextArtifactQuarantine
			outcome.Detail += fmt.Sprintf(
				"; %d named artifacts were left in place because ownership could not be proved: %s",
				len(refusals), strings.Join(refusals, "; "),
			)
		}
		if err := recordClaudeReclamationLedger(m.layout, claudeReclamationLedgerEntry{
			ArtifactSetID: id,
			Files:         removed,
			Quarantined:   len(refusals),
			Detail:        outcome.Detail,
			ReclaimedAt:   now,
		}); err != nil {
			// The files are gone and the ledger does not know it. Saying so is the
			// honest report: the next pass re-plans the set, finds nothing left to
			// prove ownership of, and converges on a zero-file reclamation.
			outcome.Status = ContextArtifactFailed
			outcome.Detail += fmt.Sprintf(
				"; the deletion audit could not be written, so the next sweep re-plans this set: %v", err,
			)
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

// artifactSetRetainedLocked joins one event against the live catalog.
//
// A pending_gc event is the adapter saying it is finished with the artifacts,
// never that nobody else is. The base it forked from may be a live fork source
// and the child it produced may be a published context this machine still holds,
// so both are checked before anything is removed.
//
// An active event is joined rather than refused, which is what the producer's
// contract asks for: "retention must join active records with the private
// session/materialization catalogs and leases before reclamation". Waiting for a
// terminal event instead would reclaim nothing at all from a successful fork —
// the adapter emits pending_gc only when an assay exits or a launch fails, so a
// fork that worked stays active forever. What makes an active set debris is the
// disappearance of the handle it belongs to: once this machine no longer holds a
// materialization for the child context, nothing will ever resume, fork, or read
// those files again.
func (m *Service) artifactSetRetainedLocked(
	event claudeReclamationEvent,
	settings sm.ContextRetentionSettings,
	now time.Time,
	releasing map[string]struct{},
) (string, bool) {
	if event.State != claudeReclamationPendingGC {
		child := normalizeContextID(event.ChildCtxID)
		if child == "" {
			return "the adapter still reports these artifacts as active and the record names no " +
				"child context to join them against", false
		}
		if _, released := releasing[child]; !released {
			if record := m.materializations[child]; record != nil {
				return fmt.Sprintf(
					"context %s is still materialized on this machine, so the adapter's active record "+
						"is accurate", child,
				), false
			}
		}
	}
	if !elapsedSince(event.RecordedAt, now, settings.RetiredTTL) {
		// The grace window applies to both event states, so the sentence names the
		// transition that actually happened. An active set reaches this line once
		// the handle it belonged to is gone, and telling an operator it "entered
		// pending_gc" would describe a transition the adapter never recorded.
		return fmt.Sprintf(
			"the artifact set was recorded %s less than the %s grace period ago",
			event.State, FormatContextRetentionDuration(settings.RetiredTTL),
		), false
	}
	for _, ctxID := range []string{event.BaseCtxID, event.ChildCtxID} {
		normalized := normalizeContextID(ctxID)
		if normalized == "" {
			continue
		}
		if m.materializationReferencedLocked(normalized) {
			return fmt.Sprintf("context %s has live or unknown host references", normalized), false
		}
		if record := m.materializations[normalized]; record != nil {
			if record.State == MaterializationLeased || record.ActiveRunID != "" || record.UnresolvedRunID != "" ||
				record.State == MaterializationMaterializing ||
				m.materializationReferencedLocked(normalized) {
				return fmt.Sprintf(
					"context %s is still live on this machine, so its artifacts are not debris",
					normalized,
				), false
			}
		}
	}
	return "", true
}

// ownedClaudeArtifacts turns one event's named artifacts into the exact files
// this sweep may remove, and explains every one it refuses.
func (m *Service) ownedClaudeArtifacts(event claudeReclamationEvent) ([]string, []string) {
	targets := make([]string, 0, len(event.Artifacts))
	refusals := make([]string, 0, len(event.Artifacts))
	roots, err := m.claudeArtifactRoots()
	if err != nil {
		for _, artifact := range event.Artifacts {
			refusals = append(refusals, fmt.Sprintf(
				"%s: the private Claude home could not be resolved, so no path can be proved board-owned",
				artifact.Role,
			))
		}
		return targets, refusals
	}
	for _, artifact := range event.Artifacts {
		err := claudeArtifactOwned(artifact, event, roots)
		if errors.Is(err, errNoSuchArtifact) {
			// Already gone is the goal, not a refusal. Counting it as one would keep
			// the set quarantined forever and stop the audit from ever being written.
			continue
		}
		if err != nil {
			refusals = append(refusals, fmt.Sprintf("%s: %v", artifact.Role, err))
			continue
		}
		targets = append(targets, filepath.Clean(artifact.Path))
	}
	sort.Strings(targets)
	return targets, refusals
}

// claudeArtifactRoots are the two trees a board-minted Claude artifact can live
// under: the provider project store and this manager's own private state.
func (m *Service) claudeArtifactRoots() ([]string, error) {
	home, err := claudeProviderHome()
	if err != nil {
		return nil, err
	}
	roots := []string{filepath.Join(filepath.Clean(home), "projects")}
	if state := strings.TrimSpace(m.layout.StateDir); state != "" {
		roots = append(roots, filepath.Clean(state))
	}
	return roots, nil
}

// claudeArtifactOwned proves that one named file is a file this board created.
//
// Every check below refuses rather than repairs, and the order matters: the
// symlink check runs before anything reads or removes, because a link is how a
// delete that looked confined reaches the operator's own conversations.
func claudeArtifactOwned(
	artifact claudeReclamationArtifact,
	event claudeReclamationEvent,
	roots []string,
) error {
	path := strings.TrimSpace(artifact.Path)
	if path == "" {
		return errors.New("the record names no path")
	}
	if !filepath.IsAbs(path) {
		return errors.New("the recorded path is not absolute")
	}
	cleaned := filepath.Clean(path)
	if cleaned != path {
		// A path that does not survive cleaning contains traversal or redundant
		// separators. Normalizing it here would mean deleting something other than
		// what the record named, which is the derived-path failure again.
		return errors.New("the recorded path is not in canonical form")
	}
	dir := filepath.Dir(cleaned)
	recorded := recordedClaudeArtifactDirs(event)
	if !containsPath(recorded, dir) {
		return errors.New(
			"the file does not sit in a directory this artifact set recorded, so it was not " +
				"named by this record",
		)
	}
	if !pathInsideAnyRoot(roots, dir) {
		return errors.New(
			"the recorded directory is outside both the private Claude project store and this " +
				"manager's own state",
		)
	}
	if err := requireRealPathInsideRoot(dir, roots); err != nil {
		return err
	}
	info, err := os.Lstat(cleaned)
	if errors.Is(err, fs.ErrNotExist) {
		return errNoSuchArtifact
	}
	if err != nil {
		return fmt.Errorf("the file could not be inspected: %w", err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		// Removing the link would report a reclamation the board did not perform,
		// because the bytes it points at would still be there; following it would
		// delete a file this board never created.
		return errors.New("the named artifact is a symlink and is not board-owned")
	}
	if !info.Mode().IsRegular() {
		// A directory is never removed here. The recorded working and project
		// directories are context, not artifacts, and removing one would mean a
		// recursive delete of a tree this record did not enumerate.
		return errors.New("the named artifact is not a regular file")
	}
	return requireBoardMintedName(artifact, filepath.Base(cleaned))
}

// errNoSuchArtifact is the benign case: the file this record named is already
// gone, which is the reclamation's own goal.
var errNoSuchArtifact = errors.New("the file is already absent")

// requireBoardMintedName proves ownership from the manifest rather than from
// where the file happens to sit.
//
// A Claude project directory is shared with the operator's own sessions, so
// location alone proves nothing. What does prove it is the name: a transcript is
// named after the session UUID this board minted and recorded, and a staging file
// carries the board's own prefix.
func requireBoardMintedName(artifact claudeReclamationArtifact, base string) error {
	if uuid := strings.TrimSpace(artifact.SessionUUID); uuid != "" {
		if base != uuid+".jsonl" {
			return fmt.Errorf(
				"the file name does not match the session identity this board minted for it",
			)
		}
		return nil
	}
	if strings.HasPrefix(base, claudeStagingArtifactPrefix) {
		return nil
	}
	return errors.New(
		"the record carries no session identity and the file name is not one this board minted, " +
			"so ownership cannot be established",
	)
}

func recordedClaudeArtifactDirs(event claudeReclamationEvent) []string {
	dirs := make([]string, 0, 2)
	for _, dir := range []string{event.ProjectDir, event.WorkingDir} {
		trimmed := strings.TrimSpace(dir)
		if trimmed == "" || !filepath.IsAbs(trimmed) {
			continue
		}
		dirs = append(dirs, filepath.Clean(trimmed))
	}
	return dirs
}

func containsPath(candidates []string, path string) bool {
	for _, candidate := range candidates {
		if candidate == path {
			return true
		}
	}
	return false
}

func pathInsideAnyRoot(roots []string, path string) bool {
	for _, root := range roots {
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == ".." ||
			strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
			continue
		}
		return true
	}
	return false
}

// requireRealPathInsideRoot refuses a file reached through a link.
//
// Two checks, and both are needed. The file itself is inspected with Lstat by
// the caller, so a symlinked artifact is refused before anything reads it. The
// parent is resolved here and the resolved directory must still sit inside a
// resolved permitted root, which is what stops a planted link in the path from
// making a delete that looked confined reach the operator's own conversations.
//
// Resolving the roots too is not laxity: a platform temp root is itself commonly
// a link, and comparing an unresolved root against a resolved path would refuse
// every artifact on such a machine while proving nothing.
func requireRealPathInsideRoot(dir string, roots []string) error {
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return errNoSuchArtifact
	}
	if err != nil {
		return fmt.Errorf("the directory could not be resolved: %w", err)
	}
	resolvedRoots := make([]string, 0, len(roots))
	for _, root := range roots {
		if real, rootErr := filepath.EvalSymlinks(root); rootErr == nil {
			resolvedRoots = append(resolvedRoots, real)
			continue
		}
		resolvedRoots = append(resolvedRoots, root)
	}
	if !pathInsideAnyRoot(resolvedRoots, resolvedDir) {
		return errors.New(
			"the file is reached through a symlink that leaves the private Claude project store " +
				"and this manager's own state, so it is not board-owned",
		)
	}
	return nil
}

// removeClaudeArtifacts removes the proved-owned files.
//
// It keeps going after a failure. A partial pass is not recorded either way, so
// the useful thing to do with the remaining files is remove them; stopping early
// would leave more bytes on disk for the same unrecorded result.
func removeClaudeArtifacts(targets []string) (int, error) {
	var failures []error
	removed := 0
	for _, path := range targets {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			failures = append(failures, fmt.Errorf("removing %s: %w", filepath.Base(path), err))
			continue
		}
		removed++
	}
	return removed, errors.Join(failures...)
}

// scanClaudeReclamationCatalog reads the private catalog tolerantly.
//
// It never fails the sweep on one damaged file. A catalog with a torn record
// still describes every other artifact set correctly, and refusing to run at all
// would let one bad write stop every reclamation on the machine — while the
// damaged record itself is reported so it is not silently skipped.
//
// The latest event per artifact set wins, ordered by recorded time then event
// id, which is the supersession rule the producer's contract states.
func scanClaudeReclamationCatalog(
	layout sm.Layout,
) (map[string]claudeReclamationEvent, []string) {
	root := filepath.Join(layout.StateDir, claudeReclamationDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		return map[string]claudeReclamationEvent{}, nil
	}
	latest := make(map[string]claudeReclamationEvent)
	reclaimed := loadClaudeReclamationLedger(layout)
	var corrupt []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(root, entry.Name()))
		if readErr != nil {
			corrupt = append(corrupt, entry.Name())
			continue
		}
		var event claudeReclamationEvent
		if json.Unmarshal(data, &event) != nil ||
			event.Contract != claudeReclamationContract ||
			strings.TrimSpace(event.ArtifactSetID) == "" ||
			event.Provider != "claude" {
			corrupt = append(corrupt, entry.Name())
			continue
		}
		if _, done := reclaimed[event.ArtifactSetID]; done {
			continue
		}
		current, seen := latest[event.ArtifactSetID]
		if !seen || claudeReclamationEventNewer(event, current) {
			latest[event.ArtifactSetID] = event
		}
	}
	sort.Strings(corrupt)
	return latest, corrupt
}

func claudeReclamationEventNewer(candidate, current claudeReclamationEvent) bool {
	if candidate.RecordedAt.Equal(current.RecordedAt) {
		return candidate.EventID > current.EventID
	}
	return candidate.RecordedAt.After(current.RecordedAt)
}

func recordClaudeReclamationLedger(layout sm.Layout, entry claudeReclamationLedgerEntry) error {
	root := filepath.Join(layout.StateDir, claudeReclamationDirName, claudeReclaimedLedgerDirName)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("creating Claude reclamation audit: %w", err)
	}
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding Claude reclamation audit: %w", err)
	}
	path := filepath.Join(root, entry.ArtifactSetID+".json")
	staging := path + ".staging"
	if err := os.WriteFile(staging, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("writing Claude reclamation audit: %w", err)
	}
	if err := os.Rename(staging, path); err != nil {
		return fmt.Errorf("committing Claude reclamation audit: %w", err)
	}
	return syncDirectory(root)
}

func loadClaudeReclamationLedger(layout sm.Layout) map[string]claudeReclamationLedgerEntry {
	root := filepath.Join(layout.StateDir, claudeReclamationDirName, claudeReclaimedLedgerDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		return map[string]claudeReclamationLedgerEntry{}
	}
	ledger := make(map[string]claudeReclamationLedgerEntry, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(root, entry.Name()))
		if readErr != nil {
			continue
		}
		var record claudeReclamationLedgerEntry
		if json.Unmarshal(data, &record) != nil ||
			strings.TrimSpace(record.ArtifactSetID) == "" {
			continue
		}
		ledger[record.ArtifactSetID] = record
	}
	return ledger
}
