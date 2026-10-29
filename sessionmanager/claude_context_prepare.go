package sessionmanager

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// PrepareContextFork exposes the original provider-owned base inspection and
// supplemental normalization for an already-admitted host operation. The host
// owns authority, child lineage and current validation. This method reads only
// the actual provider transcript; it never launches, copies or publishes it.
// Caller-supplied Base* facts are replaced by the observed source facts.
func (f *ClaudeHostFactory) PrepareContextFork(ctx context.Context, source ProviderHostRecord, input ClaudeContextForkPlan) (ProviderHostRecord, error) {
	fail := func(e error) (ProviderHostRecord, error) { return ProviderHostRecord{}, e }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if input.Contract != claudeContextForkPlanContract {
		return fail(fmt.Errorf("unsupported Claude fork plan contract %q", input.Contract))
	}
	input.ChildCtxID = normalizeClaudeContextKey(input.ChildCtxID)
	input.PrimaryCtxID = normalizeClaudeContextKey(input.PrimaryCtxID)
	if err := validateClaudeContextKey(input.ChildCtxID); err != nil {
		return fail(err)
	}
	if err := validateClaudeContextKey(input.PrimaryCtxID); err != nil {
		return fail(err)
	}
	target := strings.TrimSpace(input.TargetProjectDir)
	if !filepath.IsAbs(target) {
		return fail(&PublicError{Code: claudeContextMaterializeError, Message: "a Claude fork requires the absolute target working directory its session will be resumed from"})
	}
	uuid, dir, err := claudeBaseTranscriptRef(source)
	if err != nil {
		return fail(err)
	}
	facts, err := claudeInspectBase(input.PrimaryCtxID, uuid, dir)
	if err != nil {
		return fail(err)
	}
	supplemental := make([]ClaudeSupplementalContext, 0, len(input.Supplemental))
	for _, item := range input.Supplemental {
		normalized, e := normalizeClaudeSupplemental(item, facts.FormatVersion)
		if e != nil {
			return fail(e)
		}
		supplemental = append(supplemental, normalized)
	}
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	input.BaseSessionUUID = uuid
	input.BaseProjectDir = dir
	input.BaseSHA256 = facts.SHA256
	input.BaseRecords = facts.Records
	input.BaseFormatVersion = facts.FormatVersion
	input.TargetProjectDir = filepath.Clean(target)
	input.Supplemental = supplemental
	if input.PreparedAt.IsZero() {
		input.PreparedAt = time.Now().UTC()
	}
	return ProviderHostRecord{Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude-fork", ClaudeFork: &input}}, nil
}
