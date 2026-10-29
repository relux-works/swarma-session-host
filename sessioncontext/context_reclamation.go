package sessioncontext

import (
	"context"
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

// Codex retention is the board's problem by elimination. No provider pruning is
// documented or has been observed — a historical rollout census found
// rollouts 158 days old — and its archive operation moves a rollout rather than
// deleting it, so a context the registry stopped needing keeps its bytes, and
// whatever repository content and secrets they captured, on disk under another
// name with nothing scheduled to remove them.
//
// That is an observation and an absent guarantee, not a promise of permanence,
// which is why nothing here treats an absent rollout as impossible: presence is
// probed before a context is served, and a payload that is gone is a handled
// path on Codex exactly as it is on Claude.
//
// The registry can name what is owed but cannot perform it: provider homes are
// machine-local and the board record deliberately holds no path. This file is
// the other half — the Session Manager owns the private CTX-to-provider-handle
// catalog, so it is the only layer that can turn "CTX-x is surplus" into "these
// exact files are gone" and then tell the registry so the next sweep converges.
//
// Everything here is written against one hazard. A reclaimer that deletes
// slightly too much destroys conversations nobody can recover, so the rules are:
// only a context the registry itself planned, only through a handle this
// machine owns, only files whose name matches that handle's thread, only inside
// that handle's own private home, never through a symlink, and never for an
// adopted thread that belongs to the operator's real Codex home.

// ContextReclamationRequest parameterizes one board-owned payload sweep.
type ContextReclamationRequest struct {
	// WarnWindowSeconds overrides how far ahead a provider deletion horizon is
	// surfaced. Zero selects the registry default.
	WarnWindowSeconds int `json:"warn_window_seconds,omitempty"`
	// IncludePinned lets the sweep consider pinned records. It is off by default:
	// a pin is an operator statement that this payload is still wanted.
	IncludePinned bool `json:"include_pinned,omitempty"`
	// DryRun plans and reports without deleting or confirming anything.
	DryRun bool `json:"dry_run,omitempty"`
}

// ContextReclamationStatus is what the sweep did to one planned payload.
type ContextReclamationStatus string

const (
	// ContextReclamationDone means the payload files were deleted and the
	// registry recorded it.
	ContextReclamationDone ContextReclamationStatus = "reclaimed"
	// ContextReclamationPlanned is the dry-run outcome: this payload would be
	// deleted.
	ContextReclamationPlanned ContextReclamationStatus = "planned"
	// ContextReclamationSkipped means the sweep declined to act, with a reason. A
	// skip is the normal outcome for a payload another machine holds.
	ContextReclamationSkipped ContextReclamationStatus = "skipped"
	// ContextReclamationFailed means deletion was attempted and did not fully
	// succeed. Nothing is confirmed, so the next sweep tries again.
	ContextReclamationFailed ContextReclamationStatus = "failed"
)

// ContextReclamationOutcome is one payload's result. It carries counts and a
// reason, never a path: this value crosses the control plane.
type ContextReclamationOutcome struct {
	CtxID    string                   `json:"ctx_id"`
	Provider sm.ContextProvider       `json:"provider"`
	Status   ContextReclamationStatus `json:"status"`
	// Files is how many live rollout files were removed.
	Files int `json:"files,omitempty"`
	// ArchivedFiles is how many archived rollout files were removed. It is
	// reported separately because a sweep that missed the archive is the failure
	// this whole path exists to prevent.
	ArchivedFiles int    `json:"archived_files,omitempty"`
	Detail        string `json:"detail,omitempty"`
}

// ContextReclamationReport is one full pass over both halves of the divergent
// retention problem.
type ContextReclamationReport struct {
	// Expiring is every context the provider is about to delete on its own timer.
	// It is carried here so a single operation answers both providers: the ones
	// the board must delete, and the ones the board must rebuild before they are
	// deleted for it.
	Expiring []sm.ContextRetentionOutlook `json:"expiring"`
	// Retired is every registry record this sweep moved to retired so its payload
	// could be reclaimed.
	Retired  []string                    `json:"retired"`
	Outcomes []ContextReclamationOutcome `json:"outcomes"`
	// Skipped carries registry-level skips: a record that moved underneath the
	// plan and was left for the next pass.
	Skipped     []sm.ContextRetentionSkip `json:"skipped,omitempty"`
	DryRun      bool                      `json:"dry_run,omitempty"`
	EvaluatedAt time.Time                 `json:"evaluated_at"`
}

// ReclaimContextPayloads deletes the provider payloads the registry no longer
// needs and records that it did.
//
// The confirmation is the load-bearing part. Without durable evidence that the
// deletion happened, every pass would re-plan every Codex context ever retired
// and "already reclaimed" would be indistinguishable from "still owed" — so the
// registry is told only after the files are actually gone, and a partial
// deletion is deliberately left unconfirmed so the next sweep finishes it.
func (m *Service) ReclaimContextPayloads(
	ctx context.Context,
	request ContextReclamationRequest,
) (ContextReclamationReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	report := ContextReclamationReport{
		Expiring:    []sm.ContextRetentionOutlook{},
		Retired:     []string{},
		Outcomes:    []ContextReclamationOutcome{},
		DryRun:      request.DryRun,
		EvaluatedAt: m.now().UTC(),
	}
	if m.materializationAuthority == nil {
		return report, fmt.Errorf("no layer-1 authority is configured for context retention")
	}
	policy := sm.RetentionPolicy{
		WarnWindow:    time.Duration(request.WarnWindowSeconds) * time.Second,
		Now:           report.EvaluatedAt,
		IncludePinned: request.IncludePinned,
	}

	// A dry run must not retire anything either: retirement is the irreversible
	// half of the plan, so a "show me what would happen" that performs it is not
	// a dry run. It reads instead, and reports the same shape.
	var (
		planned []sm.ContextRetentionCollection
		err     error
	)
	if request.DryRun {
		planned, report.Expiring, err = m.planContextRetentionLocked(ctx, policy)
	} else {
		planned, report.Expiring, report.Retired, report.Skipped, err =
			m.collectContextRetentionLocked(ctx, policy)
	}
	if err != nil {
		return report, err
	}

	for _, collection := range planned {
		report.Outcomes = append(
			report.Outcomes,
			m.reclaimOnePayloadLocked(ctx, collection, request.DryRun, report.EvaluatedAt),
		)
	}
	sort.SliceStable(report.Outcomes, func(i, j int) bool {
		return report.Outcomes[i].CtxID < report.Outcomes[j].CtxID
	})
	return report, nil
}

func (m *Service) planContextRetentionLocked(
	ctx context.Context,
	policy sm.RetentionPolicy,
) ([]sm.ContextRetentionCollection, []sm.ContextRetentionOutlook, error) {
	plan, err := m.materializationAuthority.ReportContextRetention(ctx, policy)
	if err != nil {
		return nil, nil, err
	}
	if plan == nil {
		return nil, nil, fmt.Errorf("retention plan unavailable")
	}
	return plan.Collect, plan.Expiring, nil
}

func (m *Service) collectContextRetentionLocked(
	ctx context.Context,
	policy sm.RetentionPolicy,
) (
	[]sm.ContextRetentionCollection,
	[]sm.ContextRetentionOutlook,
	[]string,
	[]sm.ContextRetentionSkip,
	error,
) {
	result, err := m.materializationAuthority.CollectContextRetention(ctx, policy)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if result == nil {
		return nil, nil, nil, nil, fmt.Errorf("retention collection unavailable")
	}
	retired := make([]string, 0, len(result.Retired))
	for _, record := range result.Retired {
		if record == nil {
			continue
		}
		retired = append(retired, normalizeContextID(record.ID))
	}
	return result.Delete, result.Expiring, retired, result.Skipped, nil
}

func (m *Service) reclaimOnePayloadLocked(
	ctx context.Context,
	collection sm.ContextRetentionCollection,
	dryRun bool,
	at time.Time,
) ContextReclamationOutcome {
	outcome := ContextReclamationOutcome{
		CtxID:    normalizeContextID(collection.CtxID),
		Provider: collection.Provider,
	}
	skip := func(detail string) ContextReclamationOutcome {
		outcome.Status = ContextReclamationSkipped
		outcome.Detail = detail
		return outcome
	}

	retention := ContextRetentionForProvider(collection.Provider)
	if !retention.BoardReclaims {
		// Unreachable through the planner, which only collects for providers with
		// no deletion horizon — and stated anyway, because this is the one function
		// in the system that deletes provider payloads, and it should refuse on its
		// own terms rather than on a caller's promise.
		return skip(fmt.Sprintf(
			"provider %s deletes its own payloads; the board never reclaims them",
			collection.Provider,
		))
	}
	if collection.Provider != ContextProviderCodex {
		return skip(fmt.Sprintf(
			"no payload reclaimer is implemented for provider %s on this machine",
			collection.Provider,
		))
	}

	current, readErr := m.materializationAuthority.Context(ctx, outcome.CtxID)
	if readErr != nil || current == nil || current.ID != outcome.CtxID || current.Provider != collection.Provider || (!dryRun && current.State != "retired") {
		return skip("the authoritative context is not a matching retired record")
	}
	record := m.materializations[outcome.CtxID]
	if record == nil {
		// The expected outcome on every machine but the one that built the context.
		// A registry entry is board-wide; the payload behind it exists on exactly
		// one host, and that host is the only one that can reclaim it.
		return skip("no materialization on this machine holds a provider handle for this context")
	}
	if record.State == MaterializationLeased || record.ActiveRunID != "" || record.UnresolvedRunID != "" ||
		m.materializationReferencedLocked(outcome.CtxID) {
		return skip("the materialization is live; its payload is still being read")
	}
	source, err := decodeCodexHostSnapshot(record.ProviderHost)
	if err != nil {
		return skip(fmt.Sprintf("the private provider handle could not be decoded: %v", err))
	}
	if source.Adopted {
		// An adopted thread is the operator's own conversation, exposed into the
		// manager's private home by symlink. The board never created these bytes and
		// must never delete them.
		return skip("the thread was adopted from the operator's own Codex home and is not board-owned")
	}
	if !collection.IncludeArchived {
		// Codex archives by moving. A plan that did not ask for the archive would
		// leave the payload on disk under another name while the registry recorded
		// it as gone.
		return skip("the plan does not cover the archive location, which Codex moves rollouts into")
	}

	targets, err := codexReclamationTargets(source.CodexHome, source.ThreadID)
	if err != nil {
		return skip(fmt.Sprintf("the owned rollout files could not be enumerated: %v", err))
	}
	if dryRun {
		outcome.Status = ContextReclamationPlanned
		outcome.Files = len(targets.Live)
		outcome.ArchivedFiles = len(targets.Archived)
		outcome.Detail = "would delete the live and archived rollouts this handle owns"
		return outcome
	}

	live, archived, deleteErr := deleteCodexRollouts(targets)
	outcome.Files = live
	outcome.ArchivedFiles = archived
	if deleteErr != nil {
		outcome.Status = ContextReclamationFailed
		outcome.Detail = fmt.Sprintf(
			"deletion did not complete and was not confirmed, so the next sweep retries it: %v", deleteErr,
		)
		m.logOperatorError("context_reclamation_failed", deleteErr, map[string]any{
			"ctx_id": outcome.CtxID,
		})
		return outcome
	}

	if _, err := m.materializationAuthority.ConfirmContextReclamation(ctx, outcome.CtxID, sm.ProviderReclamation{
		ReclaimedAt:      at,
		ArchivedIncluded: true,
		Files:            live + archived,
	}); err != nil {
		// The bytes are gone and the registry does not know it. Saying so is the
		// only honest report: the next sweep will re-plan this context, find no
		// files, and confirm a zero-file reclamation, which converges.
		outcome.Status = ContextReclamationFailed
		outcome.Detail = fmt.Sprintf(
			"the payload was deleted but the registry could not record it, so the next sweep will "+
				"re-plan it: %v", err,
		)
		return outcome
	}

	// The private handle is dropped with the payload. Keeping a catalog entry that
	// points at deleted files would reproduce, one layer down, exactly the problem
	// this sweep exists to fix.
	if err := m.dropMaterializationLocked(outcome.CtxID); err != nil {
		m.logOperatorError("context_reclamation_catalog_stale", err, map[string]any{
			"ctx_id": outcome.CtxID,
		})
	}
	outcome.Status = ContextReclamationDone
	outcome.Detail = "the live and archived rollouts this handle owned were deleted"
	m.logOperatorInfo("context_reclaimed", map[string]any{
		"ctx_id":         outcome.CtxID,
		"files":          live,
		"archived_files": archived,
	})
	return outcome
}

// planMaterializationByteReleaseLocked answers what releasing one handle would
// destroy, without destroying it.
//
// It exists so a dry run describes the destructive half of the pass rather than
// only its bookkeeping. A dry run that reported a handle being dropped and said
// nothing about the provider transcripts going with it would let an operator
// approve a deletion they were never shown — the same understatement, one layer
// down, that the payload reclaimer already avoids by counting its targets under
// a planned status.
//
// It returns the file count the applying pass would remove, the explanation, and
// the refusal that would keep the handle instead.
func (m *Service) planMaterializationByteReleaseLocked(
	record *materializationRecord,
) (int, string, error) {
	if record == nil {
		return 0, "", nil
	}
	ctxID := normalizeContextID(record.CtxID)
	if hostEmpty(record.ProviderHost) {
		return 0, "the record captured no provider handle, so it names no bytes to reclaim", nil
	}
	if !hostPresent(record.ProviderHost) {
		return 0, "", fmt.Errorf("captured provider handle is malformed; byte ownership is unknown")
	}
	switch record.Provider {
	case ContextProviderCodex:
		source, targets, err := m.codexReleaseTargets(record)
		if err != nil {
			return 0, "", err
		}
		if source.Adopted {
			return 0, adoptedCodexReleaseDetail, nil
		}
		return len(targets.Live) + len(targets.Archived), fmt.Sprintf(
			"the %d live and %d archived rollout files this handle owns would be deleted before the "+
				"handle was released", len(targets.Live), len(targets.Archived),
		), nil
	case ContextProviderClaude:
		if err := m.claudeReleasePrecondition(ctxID); err != nil {
			return 0, "", err
		}
		return 0, claudeReleaseDetail, nil
	default:
		return 0, "", unsupportedReclaimerError(record.Provider)
	}
}

// releaseMaterializationBytesLocked deletes the board-owned provider bytes one
// handle names, so that handle can be released without stranding them.
//
// The private catalog is the only thing on this machine that can turn a CTX id
// into a provider payload. Dropping an entry therefore does not merely forget a
// handle, it destroys the last name the bytes had: nothing afterwards can
// enumerate them, size them, or delete them, and a transcript holding whatever
// its run read stays on disk forever. So a release is ordered, always: prove the
// bytes are gone, or refuse to release the name.
//
// It returns the number of files removed and, when the release must not happen,
// the reason.
func (m *Service) releaseMaterializationBytesLocked(
	ctx context.Context,
	record *materializationRecord,
) (int, string, error) {
	if record == nil {
		return 0, "", nil
	}
	ctxID := normalizeContextID(record.CtxID)
	if hostEmpty(record.ProviderHost) {
		// A record that never captured a handle names no provider bytes at all, so
		// there is nothing to strand. This is the ordinary shape of a preflight that
		// failed before the provider produced anything.
		return 0, "the record captured no provider handle, so it named no bytes to reclaim", nil
	}
	if !hostPresent(record.ProviderHost) {
		return 0, "", fmt.Errorf("captured provider handle is malformed; byte ownership is unknown")
	}
	switch record.Provider {
	case ContextProviderCodex:
		source, targets, err := m.codexReleaseTargets(record)
		if err != nil {
			return 0, "", err
		}
		if source.Adopted {
			// The operator's own conversation, exposed into the private home by
			// symlink. The board created none of these bytes and must never delete
			// them, so there is nothing to strand and the handle is safe to release.
			return 0, adoptedCodexReleaseDetail, nil
		}
		live, archived, deleteErr := deleteCodexRollouts(targets)
		if deleteErr != nil {
			return live + archived, "", deleteErr
		}
		m.confirmSweptReclamationLocked(ctx, ctxID, live+archived)
		return live + archived, fmt.Sprintf(
			"the %d live and %d archived rollout files this handle owned were deleted before the "+
				"handle was released", live, archived,
		), nil
	case ContextProviderClaude:
		// Claude bytes are not deleted from here, and deliberately not: the only
		// safe deleter for them is the manifest-driven artifact sweep, which proves
		// ownership from the record the materializer wrote rather than from a path
		// this layer reconstructed. What matters for the release is that such a
		// record exists — an artifact set keyed on this context can still name
		// every file after the handle is gone, so the bytes stay reachable.
		if err := m.claudeReleasePrecondition(ctxID); err != nil {
			return 0, "", err
		}
		return 0, claudeReleaseDetail, nil
	default:
		return 0, "", unsupportedReclaimerError(record.Provider)
	}
}

// codexReleaseTargets decodes one handle and enumerates exactly the rollout
// files it owns.
//
// The planning and applying passes share it so a dry run cannot answer a
// different question from the pass it claims to describe: the same decode, the
// same adoption verdict, the same confined enumeration.
func (m *Service) codexReleaseTargets(
	record *materializationRecord,
) (*sm.CodexRunnerSnapshot, codexRolloutLocations, error) {
	source, err := decodeCodexHostSnapshot(record.ProviderHost)
	if err != nil {
		// An undecodable handle is the one case where the bytes exist and cannot be
		// named. Releasing anyway would be the strand this path exists to prevent,
		// so the record is kept and the next pass tries again.
		return source, codexRolloutLocations{},
			fmt.Errorf("the private provider handle could not be decoded: %w", err)
	}
	if source.Adopted {
		return source, codexRolloutLocations{}, nil
	}
	targets, err := codexReclamationTargets(source.CodexHome, source.ThreadID)
	if err != nil {
		return source, codexRolloutLocations{},
			fmt.Errorf("the owned rollout files could not be enumerated: %w", err)
	}
	return source, targets, nil
}

func (m *Service) claudeReleasePrecondition(ctxID string) error {
	if claudeReclamationNamesContext(m.layout, ctxID) {
		return nil
	}
	return fmt.Errorf(
		"no private Claude reclamation record names this context's artifacts, so releasing " +
			"the handle would leave its transcript with nothing able to name it",
	)
}

func unsupportedReclaimerError(provider sm.ContextProvider) error {
	return fmt.Errorf(
		"no payload reclaimer is implemented for provider %s on this machine", provider,
	)
}

const (
	adoptedCodexReleaseDetail = "the thread was adopted from the operator's own Codex home, so no " +
		"board-owned bytes are left behind"
	claudeReleaseDetail = "its artifacts stay named by the private Claude reclamation catalog, " +
		"which the artifact sweep reclaims them through"
)

// confirmSweptReclamationLocked tells the registry the bytes went, when the
// registry is in a state that can hear it.
//
// Only a retired record can carry a reclamation, so a still-ready context — an
// idle local copy evicted while the board keeps advertising the snapshot — gets
// no confirmation and needs none: nothing was reclaimed on the board's behalf.
// For a retired one the confirmation is what stops the payload sweep re-planning
// a context whose files this pass already removed.
func (m *Service) confirmSweptReclamationLocked(ctx context.Context, ctxID string, files int) {
	if m.materializationAuthority == nil {
		return
	}
	registry, err := m.materializationAuthority.Context(ctx, ctxID)
	if err != nil || registry == nil || registry.State != "retired" {
		return
	}
	if _, err := m.materializationAuthority.ConfirmContextReclamation(
		ctx, ctxID, sm.ProviderReclamation{
			ReclaimedAt:      m.now().UTC(),
			ArchivedIncluded: true,
			Files:            files,
		},
	); err != nil {
		// The bytes are gone and the registry does not know it. The next payload
		// sweep re-plans this context, finds no files, and confirms a zero-file
		// reclamation, which converges — so this is logged rather than failed.
		m.logOperatorError("context_reclamation_catalog_stale", err, map[string]any{
			"ctx_id": ctxID,
		})
	}
}

func (m *Service) dropMaterializationLocked(ctxID string) error {
	record := m.materializations[ctxID]
	if record == nil {
		return nil
	}
	if err := m.materializationRecords.remove(ctxID); err != nil {
		return err
	}
	if record.State == MaterializationQuarantined && m.quarantinedMaterializations > 0 {
		m.quarantinedMaterializations--
	}
	delete(m.materializations, ctxID)
	return nil
}

// codexReclamationTargets enumerates exactly the rollout files one handle owns.
//
// The confinement is the point. Every returned path came from a walk rooted at
// this handle's own private Codex home, matched that handle's thread id, and is
// re-proved to sit inside the home afterwards — because the alternative to
// proving it is deleting somebody's conversation on the strength of a string
// match.
func codexReclamationTargets(codexHome, threadID string) (codexRolloutLocations, error) {
	home := strings.TrimSpace(codexHome)
	if home == "" {
		return codexRolloutLocations{}, fmt.Errorf("a Codex home is required")
	}
	absoluteHome, err := filepath.Abs(home)
	if err != nil {
		return codexRolloutLocations{}, err
	}
	absoluteHome = filepath.Clean(absoluteHome)
	for _, root := range []string{absoluteHome, filepath.Join(absoluteHome, codexSessionsDirName), filepath.Join(absoluteHome, codexArchivedSessionsDirName)} {
		if err := rejectSymlinkAncestors(root); err != nil {
			return codexRolloutLocations{}, err
		}
	}
	found, err := locateCodexRollouts(absoluteHome, threadID)
	if err != nil {
		return codexRolloutLocations{}, err
	}
	verify := func(paths []string) ([]string, error) {
		checked := make([]string, 0, len(paths))
		for _, path := range paths {
			relative, err := filepath.Rel(absoluteHome, path)
			if err != nil || relative == "." || relative == ".." ||
				strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
				return nil, fmt.Errorf("rollout %s is outside the private Codex home", filepath.Base(path))
			}
			info, err := os.Lstat(path)
			if err != nil {
				return nil, err
			}
			if info.Mode()&fs.ModeSymlink != 0 {
				// The adoption store links a real user rollout into the private home.
				// Following it would delete the operator's conversation; even removing
				// the link would report a reclamation the board did not perform, since
				// the bytes would still be there.
				return nil, fmt.Errorf(
					"rollout %s is a symlink into another Codex home and is not board-owned",
					filepath.Base(path),
				)
			}
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("rollout %s is not a regular file", filepath.Base(path))
			}
			checked = append(checked, path)
		}
		return checked, nil
	}
	live, err := verify(found.Live)
	if err != nil {
		return codexRolloutLocations{}, err
	}
	archived, err := verify(found.Archived)
	if err != nil {
		return codexRolloutLocations{}, err
	}
	return codexRolloutLocations{Live: live, Archived: archived}, nil
}

// deleteCodexRollouts removes the enumerated files and reports how many of each
// kind went.
//
// It keeps going after a failure rather than stopping at the first one. A
// partial sweep is not confirmed either way, so the useful thing to do with the
// remaining files is delete them; stopping early would leave more bytes on disk
// for the same unconfirmed result.
func deleteCodexRollouts(targets codexRolloutLocations) (int, int, error) {
	var failures []error
	remove := func(paths []string) int {
		removed := 0
		for _, path := range paths {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				failures = append(failures, fmt.Errorf("removing %s: %w", filepath.Base(path), err))
				continue
			}
			removed++
		}
		return removed
	}
	live := remove(targets.Live)
	archived := remove(targets.Archived)
	return live, archived, errors.Join(failures...)
}
