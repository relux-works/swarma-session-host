package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// claudeForkMaterialization is the realized on-disk fork source for one launch.
type claudeForkMaterialization struct {
	// ChildCtxID is the materialization that was written.
	ChildCtxID string
	// PrimaryCtxID is the board identity of the base this fork came from.
	PrimaryCtxID string
	// ResumeSessionUUID is the freshly minted identity of the materialized copy.
	// It is what the provider is asked to resume and it is never the identity the
	// run writes into: a resume source that a turn appends to is no longer a fork
	// source.
	ResumeSessionUUID string
	// ForkSessionUUID is the writable child session the launch will own. The
	// manager chooses it rather than discovering it, because the provider honours
	// --session-id alongside --resume --fork-session, so the identity that ends up
	// in the catalog is the one the run actually writes into.
	ForkSessionUUID string
	// TranscriptPath and ProjectSlug are where the copy was written.
	TranscriptPath string
	ProjectSlug    string
	// ForkTranscriptPath is where the provider must write the writable child. Its
	// appearance at exactly this path is the launch's positive proof that the
	// provider agreed with this adapter's project-slug rule.
	ForkTranscriptPath string
	// InheritedRecordUUIDs are the record identities the materialized copy holds,
	// in order. A fork copies them through unchanged, so their presence in the
	// child transcript is provable lineage rather than an inference from silence.
	InheritedRecordUUIDs []string
	// BaseSessionUUID/BaseSHA256 identify the base and prove it was untouched.
	// BaseTranscriptPath is retained so that proof can be repeated after the
	// provider process has run: the pre-launch digest only rules out damage done by
	// the copy itself, and the launch is what actually opens the base.
	BaseSessionUUID    string
	BaseSHA256         string
	BaseTranscriptPath string
	// MaterializedSHA256 is the digest of the copy the provider will read.
	MaterializedSHA256 string
	// InjectedCtxIDs are the supplemental lineage inputs whose distilled handoff
	// was appended, in order.
	InjectedCtxIDs []string
	// SplicedCtxIDs are the raw-history inputs, which are empty on every default
	// path.
	SplicedCtxIDs []string
	// lease is the one-writer registration held for the life of the launch.
	lease *claudeContextLease
	// ReclamationArtifactSetID links the append-only private events that tell
	// the reclamation catalog which provider paths this fork owns. It never enters the
	// board tree or public session projections.
	ReclamationArtifactSetID string
	RetainedStagingPath      string
	// RecordedProviderVersion and ActiveProviderVersion are the two builds in the
	// resume: the one that wrote the base and the one about to be launched
	// against it. They are carried past materialization because the launch is
	// where the resume is actually attempted, and a failure there can only name
	// the gap if the gap travelled with the materialization.
	RecordedProviderVersion string
	ActiveProviderVersion   string
}

func (m *claudeForkMaterialization) recordReclamation(
	layout Layout,
	state claudeReclamationState,
	reason string,
	now func() time.Time,
) error {
	if m == nil {
		return nil
	}
	artifacts := []claudeReclamationArtifact{
		{Role: "materialized_resume_source", Path: m.TranscriptPath, SessionUUID: m.ResumeSessionUUID},
		{Role: "writable_fork", Path: m.ForkTranscriptPath, SessionUUID: m.ForkSessionUUID},
	}
	if m.RetainedStagingPath != "" {
		artifacts = append(artifacts, claudeReclamationArtifact{
			Role: "retained_staging_file",
			Path: m.RetainedStagingPath,
		})
	}
	return recordClaudeReclamationEvent(layout, claudeReclamationEvent{
		ArtifactSetID: m.ReclamationArtifactSetID,
		Kind:          claudeReclamationFork,
		State:         state,
		Reason:        reason,
		BaseCtxID:     m.PrimaryCtxID,
		ChildCtxID:    m.ChildCtxID,
		ProjectDir:    filepath.Dir(m.TranscriptPath),
		Artifacts:     artifacts,
	}, now)
}

// claudeResumeAttemptFailed reports a failure that happened after the active
// Claude binary was launched against a materialized base.
//
// This is the half the board could not reach before. The materializer's own
// failures already fell back, but they are the board's parser refusing a file;
// the resume the acceptance criterion talks about is `--resume <copy>
// --fork-session` executed by the installed CLI, and everything it can fail at —
// the session never coming up, the fork never being committed — lives past the
// launch. Returned bare, those failures left the base recorded ready, so the next
// consumer selected the same unresumable payload and rediscovered the same
// refusal.
//
// Two kinds of failure are deliberately not converted:
//
//   - a mutated base or resume source, which is a finding about the payload's
//     contents rather than a verdict on this build's ability to resume it, and
//     which already has its own refusal and its own preserved evidence;
//   - a cancelled or expired caller context, which says nothing about the
//     payload at all and would otherwise mark a base stale because an operator
//     pressed Ctrl-C.
func claudeResumeAttemptFailed(
	forked *claudeForkMaterialization,
	operation string,
	err error,
) error {
	if forked == nil || err == nil {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var public *PublicError
	if errors.As(err, &public) && public.Code == claudeContextSourceMutated {
		return err
	}
	return claudeResumeUnusable(ClaudeResumeAttempt{
		CtxID:           forked.PrimaryCtxID,
		Provider:        "claude",
		RecordedVersion: forked.RecordedProviderVersion,
		ActiveVersion:   forked.ActiveProviderVersion,
		Operation:       operation,
		Cause:           err,
	})
}

// claudeMaterializedHook runs immediately after a materialized transcript is
// published and before it is verified.
//
// It exists so the post-write failure branches — a base that moved under the
// copy, a copy that is no longer what was written — can be exercised against the
// real code path instead of only against the predicates they call. Production
// leaves it nil.
var claudeMaterializedHook func(materializedPath, basePath string)

// claudeForkArgv is the only place a Claude session selector is composed.
//
// --resume without --fork-session appends into the base file — measured at
// 178,596 -> 188,428 bytes on a base that was supposed to stay immutable — so
// the two flags are emitted together by construction and there is no code path
// that can produce one without the other.
//
// --session-id names the writable child. It is honoured alongside the pair —
// probe-verified on claude 2.1.220 in both --print and interactive PTY mode: the
// requested id came back as the session id and the provider wrote exactly
// <requested>.jsonl. That matters beyond tidiness: it is why the manager can
// persist the identity the run actually writes into instead of persisting the
// resume source and hoping a later restart guesses right.
//
// An empty forkSessionUUID emits the pair alone, which is what the disposable
// assay path used before it started naming its fork too.
func claudeForkArgv(resumeSessionUUID string, forkSessionUUID ...string) []string {
	argv := []string{"--resume", resumeSessionUUID, "--fork-session"}
	for _, fork := range forkSessionUUID {
		if fork != "" {
			argv = append(argv, "--session-id", fork)
		}
	}
	return argv
}

// materializeClaudeContextFork copies an immutable base into the target working
// directory's project slug under a fresh identity and returns the fork source.
//
// The whole function exists because a Claude context is not portable: resuming a
// base session id from another directory answers "No conversation found", which
// collides with every worktree-based spawn. The copy is the proven mitigation,
// and everything around it is there because the failure modes are quiet:
//
//   - a wrong slug writes into a directory the provider never reads, so the
//     context is absent rather than broken; the slug rule is calibrated against
//     provider ground truth before anything is written;
//   - a base that gets appended to instead of forked loses its immutability, so
//     its digest is verified both before and after;
//   - a partially written transcript would be discovered as a truncated
//     conversation, so the copy is staged and renamed;
//   - a second writer against one materialization branches the transcript and
//     one branch is silently dropped, so a lease is held for the launch.
//
// activeProviderVersion is the build attempting the resume. It is the version
// the launch already probed, threaded through rather than re-derived so the
// failure report names the binary that actually ran.
func materializeClaudeContextFork(
	plan *claudeContextForkPlan,
	layout Layout,
	targetProjectDir string,
	activeProviderVersion string,
	now func() time.Time,
) (result *claudeForkMaterialization, err error) {
	if plan == nil {
		return nil, fmt.Errorf("Claude fork plan is required")
	}
	if now == nil {
		now = time.Now
	}
	// Both sides are compared in resolved form: the launch directory and the
	// planned one may be spelled differently through a symlink and still be the
	// same directory, and it is the resolved form the provider slugifies.
	requested, err := claudeResolveProjectDir(targetProjectDir)
	if err != nil {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"the working directory this Claude fork would run in cannot be resolved, so the project "+
					"directory the provider reads cannot be derived: %v",
				err,
			),
		}
	}
	if requested != claudeResolveProjectDirIfPresent(plan.TargetProjectDir) {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"this Claude fork was planned for working directory %q but the launch runs in %q; a Claude "+
					"context is discoverable only from the directory it was materialized into, so the fork is "+
					"refused rather than written where the provider would not find it",
				plan.TargetProjectDir, requested,
			),
		}
	}
	home, err := claudeProviderHome()
	if err != nil {
		return nil, err
	}
	basePath, err := claudeSessionTranscriptPath(home, plan.BaseProjectDir, plan.BaseSessionUUID)
	if err != nil {
		return nil, err
	}
	// Presence first, and separately from readability. A base the provider's
	// 30-day timer already collected is the expected end of a Claude base's life
	// and its answer is "rebuild"; a base that is present but unreadable is a
	// different fault with a different answer. Folding them together is what made
	// the normal case look like corruption.
	if err := requireClaudePrimaryPayloadPresent(plan, basePath, now()); err != nil {
		return nil, err
	}
	baseRaw, facts, err := inspectClaudeTranscriptFile(basePath)
	if err != nil {
		// The file is there and the adapter cannot read it. The provider build that
		// wrote it is the first thing worth naming, so this goes through the
		// version-aware fallback — after the read was attempted, never instead of
		// it, because transcripts were measured surviving large version gaps.
		return nil, claudeResumeUnusable(ClaudeResumeAttempt{
			CtxID:           plan.PrimaryCtxID,
			Provider:        "claude",
			RecordedVersion: plan.BaseFormatVersion,
			ActiveVersion:   activeProviderVersion,
			Operation:       "base transcript read",
			Cause:           err,
		})
	}
	// The slug rule is re-calibrated here, not just at plan time: the provider
	// binary can be upgraded between the two, and a changed rule turns every
	// derived target directory into one the provider ignores.
	if err := verifyClaudeSlugRule(basePath, facts.RecordedCWD); err != nil {
		return nil, err
	}
	if err := requireForkableClaudeBase(plan.PrimaryCtxID, facts); err != nil {
		return nil, err
	}
	if facts.SHA256 != plan.BaseSHA256 {
		return nil, &PublicError{
			Code: claudeContextBaseNotForkable,
			Message: fmt.Sprintf(
				"Claude context %s changed after its fork was planned; an immutable base that moved is no "+
					"longer the context the registry described, so the fork is refused",
				plan.PrimaryCtxID,
			),
		}
	}

	// The lease is taken on the child being written, not on the base being read.
	// Concurrent forks from one base are the design's only concurrency mechanism
	// and each of them writes a different materialization.
	lease, err := acquireClaudeContextWriterLease(layout, plan.ChildCtxID, now)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			lease.release()
		}
	}()

	targetSlug, err := ClaudeProjectSlug(requested)
	if err != nil {
		return nil, err
	}
	sessionUUID, err := newClaudeSessionUUID()
	if err != nil {
		return nil, err
	}
	// Two identities, minted together and never interchangeable: the first names
	// the immutable copy the provider resumes, the second names the writable child
	// the run appends to. Conflating them is what made a restart able to bare-resume
	// a fork source.
	forkSessionUUID, err := newClaudeSessionUUID()
	if err != nil {
		return nil, err
	}
	targetPath := filepath.Join(
		claudeProjectsRoot(home), targetSlug, sessionUUID+claudeTranscriptSuffix)
	forkPath := filepath.Join(
		claudeProjectsRoot(home), targetSlug, forkSessionUUID+claudeTranscriptSuffix)
	realized := &claudeForkMaterialization{
		ChildCtxID:               plan.ChildCtxID,
		PrimaryCtxID:             plan.PrimaryCtxID,
		ResumeSessionUUID:        sessionUUID,
		ForkSessionUUID:          forkSessionUUID,
		TranscriptPath:           targetPath,
		ProjectSlug:              targetSlug,
		ForkTranscriptPath:       forkPath,
		BaseSessionUUID:          plan.BaseSessionUUID,
		BaseSHA256:               facts.SHA256,
		BaseTranscriptPath:       basePath,
		ReclamationArtifactSetID: forkSessionUUID,
		RecordedProviderVersion:  plan.BaseFormatVersion,
		ActiveProviderVersion:    activeProviderVersion,
		lease:                    lease,
	}

	inputs, injected, spliced, err := resolveClaudeSupplemental(home, plan)
	if err != nil {
		return nil, err
	}
	appended := 0
	for _, input := range inputs {
		if input.Raw {
			appended += len(input.RawRecords)
			continue
		}
		appended++
	}
	uuids := make([]string, 0, appended)
	for index := 0; index < appended; index++ {
		generated, err := newClaudeSessionUUID()
		if err != nil {
			return nil, err
		}
		uuids = append(uuids, generated)
	}
	materialized, err := rewriteClaudeTranscriptSession(
		baseRaw, sessionUUID, requested, facts, inputs, uuids, now(),
	)
	if err != nil {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"Claude context %s could not be rewritten for working directory %q: %v",
				plan.PrimaryCtxID, requested, err,
			),
		}
	}
	if recordErr := realized.recordReclamation(
		layout, claudeReclamationActive, "materialization_started", now,
	); recordErr != nil {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"Claude context %s could not record its private reclamation ownership before "+
					"materialization: %v",
				plan.PrimaryCtxID, recordErr,
			),
		}
	}
	defer func() {
		if err == nil {
			return
		}
		if recordErr := realized.recordReclamation(
			layout, claudeReclamationPendingGC, "materialization_failed", now,
		); recordErr != nil {
			err = errors.Join(err, fmt.Errorf(
				"recording retained Claude materialization artifacts for reclamation: %w", recordErr,
			))
		}
	}()
	stagedPath, writeErr := writeClaudeTranscript(targetPath, materialized)
	realized.RetainedStagingPath = stagedPath
	if writeErr != nil {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"Claude context %s could not be materialized into %q: %v",
				plan.PrimaryCtxID, targetSlug, writeErr,
			),
		}
	}
	if claudeMaterializedHook != nil {
		claudeMaterializedHook(targetPath, basePath)
	}

	// Read the copy back rather than trusting the buffer: what the provider will
	// read is the file, and the attested digest has to be the file's.
	writtenRaw, writtenFacts, err := inspectClaudeTranscriptFile(targetPath)
	if err != nil {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"the materialized Claude transcript for %s could not be verified: %v",
				plan.PrimaryCtxID, err,
			),
		}
	}
	if len(writtenFacts.SessionUUIDs) != 1 || writtenFacts.SessionUUIDs[0] != sessionUUID {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"the materialized Claude transcript for %s still carries %d session identities; a copy that "+
					"keeps the base identity would resume the base instead of the fork",
				plan.PrimaryCtxID, len(writtenFacts.SessionUUIDs),
			),
		}
	}
	if writtenFacts.Records != facts.Records+appended {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"the materialized Claude transcript for %s holds %d records where the base's %d plus %d "+
					"composed record(s) were expected; a copy that lost or duplicated history is not the "+
					"context the registry described",
				plan.PrimaryCtxID, writtenFacts.Records, facts.Records, appended,
			),
		}
	}
	// With nothing appended the copy must still end where the base did. With a
	// composed tail it deliberately ends on the inherited handoff instead: that
	// trailing record is history the fork reads, and the completed-turn
	// requirement was already established against the base above.
	if appended == 0 && !writtenFacts.CompletedTurn {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"the materialized Claude transcript for %s does not end on a completed turn", plan.PrimaryCtxID,
			),
		}
	}
	// Base immutability, verified rather than assumed: the digest is recomputed
	// from disk after the copy, so an accidental append or in-place rewrite is a
	// failed fork instead of a corrupted shared base.
	afterDigest, err := claudeFileDigest(basePath)
	if err != nil {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"the Claude base transcript for %s could not be re-read to prove it was untouched: %v",
				plan.PrimaryCtxID, err,
			),
		}
	}
	if afterDigest != facts.SHA256 {
		return nil, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"the Claude base transcript for %s changed while its fork was materialized; the base is a "+
					"shared fork source and must stay byte-identical",
				plan.PrimaryCtxID,
			),
		}
	}

	realized.InheritedRecordUUIDs = writtenFacts.RecordUUIDs
	realized.MaterializedSHA256 = claudeBytesDigest(writtenRaw)
	realized.InjectedCtxIDs = injected
	realized.SplicedCtxIDs = spliced
	return realized, nil
}

// resolveClaudeSupplemental expands the plan's lineage inputs into the records
// that will be appended, and reports which ones were distilled and which were
// spliced.
//
// The raw path is re-guarded here even though the plan was already guarded: the
// provider version behind the source session can change between planning and
// launch, and a splice against a layout this adapter has not verified is exactly
// the silently-wrong-context case the guard exists for.
func resolveClaudeSupplemental(
	home string,
	plan *claudeContextForkPlan,
) ([]claudeCompositionInput, []string, []string, error) {
	resolved := make([]claudeCompositionInput, 0, len(plan.Supplemental)+1)
	injected := make([]string, 0, len(plan.Supplemental))
	spliced := make([]string, 0)
	// The boundary banner goes first, ahead of every inherited handoff.
	//
	// Order is load-bearing here. The banner's job is to stop the consumer
	// reading the base's silence as absence, and a model reads a tail in order:
	// a coverage statement arriving after three inherited handoffs is a footnote
	// to them rather than a frame around the whole inheritance.
	if banner := strings.TrimSpace(plan.BoundaryBanner); banner != "" {
		resolved = append(resolved, claudeCompositionInput{
			CtxID:    plan.PrimaryCtxID,
			Handoff:  banner,
			Boundary: true,
		})
	}
	for _, input := range plan.Supplemental {
		if !input.Raw {
			resolved = append(resolved, claudeCompositionInput{
				CtxID:   input.CtxID,
				Handoff: input.Handoff,
			})
			injected = append(injected, input.CtxID)
			continue
		}
		path, err := claudeSessionTranscriptPath(home, input.RawProjectDir, input.RawSessionUUID)
		if err != nil {
			return nil, nil, nil, err
		}
		raw, facts, err := inspectClaudeTranscriptFile(path)
		if err != nil {
			return nil, nil, nil, &PublicError{
				Code: claudeContextTranscriptDrift,
				Message: fmt.Sprintf(
					"raw-history supplemental %s has no readable transcript: %v", input.CtxID, err,
				),
			}
		}
		if err := requireSupportedClaudeSpliceFormat(input.CtxID, facts.FormatVersion); err != nil {
			return nil, nil, nil, err
		}
		if err := requireForkableClaudeBase(input.CtxID, facts); err != nil {
			return nil, nil, nil, err
		}
		records, err := claudeSpliceableRecords(raw)
		if err != nil {
			return nil, nil, nil, &PublicError{
				Code: claudeContextTranscriptDrift,
				Message: fmt.Sprintf(
					"raw-history supplemental %s cannot be spliced: %v", input.CtxID, err,
				),
			}
		}
		resolved = append(resolved, claudeCompositionInput{
			CtxID:      input.CtxID,
			Handoff:    input.Handoff,
			Raw:        true,
			RawRecords: records,
		})
		spliced = append(spliced, input.CtxID)
	}
	return resolved, injected, spliced, nil
}
