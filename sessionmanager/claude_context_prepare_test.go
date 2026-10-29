package sessionmanager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The exported provider mechanism uses the original real inspector/normalizer;
// no caller-supplied digest/identity/format can replace actual transcript facts.
// This is preparation only: no provider process or transcript copy is launched.
func TestClaudeContextPreparationUsesActualOriginalSource(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	factory := NewClaudeHostFactory(fixture.layout)
	source := fixture.assaySource(t)
	input := *fixture.plan(fixture.dir("target"))
	input.BaseSessionUUID = "forged"
	input.BaseProjectDir = "/forged"
	input.BaseSHA256 = "forged"
	input.BaseRecords = 999
	input.BaseFormatVersion = "99.9"
	input.Supplemental = []ClaudeSupplementalContext{{CtxID: "CTX-260730-extra1", Handoff: " actual boundary "}}
	host, err := factory.PrepareContextFork(context.Background(), source, input)
	if err != nil {
		t.Fatal(err)
	}
	plan := host.Snapshot.ClaudeFork
	if plan == nil || host.Provider != "claude" || plan.BaseSessionUUID != fixture.baseUUID || plan.BaseProjectDir != fixture.baseDir || plan.BaseSHA256 != fixture.baseSHA256 || plan.BaseRecords != fixture.baseRecords || plan.BaseFormatVersion == "99.9" || plan.Supplemental[0].Handoff != "actual boundary" || plan.Supplemental[0].Raw {
		t.Fatal("caller facts replaced actual inspected source")
	}
	fixture.assertBaseUnchanged(t)
	fixture.assertNoTranscriptFor(t, input.TargetProjectDir)
}

// Original missing/incomplete/compacted and malformed input refusals occur
// before transcript copying or any native/provider side effect.
func TestClaudeContextPreparationRefusalsPreserveSourceAndTarget(t *testing.T) {
	for _, variant := range []string{"wrong-provider", "relative-target", "bad-child", "missing", "incomplete", "compacted", "empty-handoff", "cancelled"} {
		t.Run(variant, func(t *testing.T) {
			fixture := newClaudeForkFixture(t)
			factory := NewClaudeHostFactory(fixture.layout)
			source := fixture.assaySource(t)
			input := *fixture.plan(fixture.dir("target"))
			ctx := context.Background()
			switch variant {
			case "wrong-provider":
				source.Provider = "codex"
			case "relative-target":
				input.TargetProjectDir = "relative"
			case "bad-child":
				input.ChildCtxID = "../foreign"
			case "missing":
				if e := os.Remove(fixture.basePath); e != nil {
					t.Fatal(e)
				}
			case "incomplete":
				fixture.rewriteBase(t, []string{claudeUserRecord("u1", "", fixture.baseUUID, fixture.baseDir, "interrupted")})
			case "compacted":
				fixture.rewriteBase(t, append(claudeCompletedTranscriptRecords(fixture.baseUUID, fixture.baseDir), `{"type":"system","sessionId":"`+fixture.baseUUID+`","compactMetadata":{"trigger":"auto"}}`))
			case "empty-handoff":
				input.Supplemental = []ClaudeSupplementalContext{{CtxID: "CTX-260730-extra1"}}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before, e := os.ReadFile(fixture.basePath)
			if e != nil && !errors.Is(e, os.ErrNotExist) {
				t.Fatal(e)
			}
			_, e = factory.PrepareContextFork(ctx, source, input)
			if e == nil {
				t.Fatal("unproved source yielded a provider plan")
			}
			switch variant {
			case "wrong-provider":
				if !errors.Is(e, ErrProviderHostNotFound) {
					t.Fatalf("wrong source refusal: %v", e)
				}
			case "relative-target":
				assertPublicErrorCode(t, e, claudeContextMaterializeError)
			case "missing", "incomplete":
				assertPublicErrorCode(t, e, claudeContextBaseNotForkable)
			case "compacted":
				assertContextErrorCode(t, e, "context_compacted")
			case "cancelled":
				if !errors.Is(e, context.Canceled) {
					t.Fatal("cancelled preparation lost cause", e)
				}
			}
			after, e := os.ReadFile(fixture.basePath)
			if string(after) != string(before) || e != nil && !errors.Is(e, os.ErrNotExist) {
				t.Fatal("refused source preparation changed original bytes")
			}
			entries, e := os.ReadDir(filepath.Dir(fixture.basePath))
			if e != nil {
				t.Fatal(e)
			}
			_ = entries
			fixture.assertNoTranscriptFor(t, fixture.dir("target"))
		})
	}
}
