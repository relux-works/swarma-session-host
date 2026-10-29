//go:build unix

package sessionmanager_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	defaults "github.com/relux-works/swarma-session-host/hostdefaults"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type mappingPhysicalSource struct {
	base  *legacyPhysicalAttachSource
	calls int
}

// Exact isolated request/namespace/environment verification uses the recorded
// mapping, not a new native liveness read. This is a controlled source supplier,
// not production caller/registrar qualification or a replacement HTTP handler.
func (s *mappingPhysicalSource) WithVerifiedAttach(ctx context.Context, q defaults.LegacyAttachRequest, target defaults.LegacyAttachTarget, fn func(context.Context, []string) error) error {
	s.calls++
	return s.base.WithVerifiedAttach(ctx, q, target, fn)
}

// Real handler -> current lease pin -> actual Attach reports a cross-provider
// conflict even when native State is unreadable. The same-provider path still
// refuses unknown liveness before verification/effects; its live neighbor really
// reattaches. These in-process regressions do not qualify an external provider.
func TestLegacyOwnerMappingPreservesConflictAndSameProviderAdmission(t *testing.T) {
	for _, row := range []struct {
		name, provider string
		unknown        bool
		code           string
		calls          int
	}{
		{"live same provider", "codex", false, "", 1},
		{"live cross provider", "claude", false, core.ErrorSessionProviderConflict, 1},
		{"unknown cross provider", "claude", true, core.ErrorSessionProviderConflict, 1},
		{"unknown same provider", "codex", true, "attach_failed", 0},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := legacyPhysicalNewTakeoverFixture(t, "owner-mapping")
			native := core.LegacyPhysicalTestHost(f.codex, f.incumbent.Session.SessionID)
			if native == nil {
				t.Fatal("actual admitted incumbent missing")
			}
			fault := errors.New("fixture native state unavailable")
			if row.unknown {
				core.LegacyPhysicalTestFailState(native, fault)
			}
			path := filepath.Join(f.layout.SessionsDir, f.incumbent.Session.SessionID, "record.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			source := &mappingPhysicalSource{base: f.client.source}
			handler, err := defaults.NewLegacyAttachHandler(f.manager, source)
			if err != nil {
				t.Fatal(err)
			}
			q := defaults.LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: row.provider, ProjectDir: source.base.project, BoardFingerprint: source.base.namespace, Launch: source.base.launches[row.provider]}
			raw, err := json.Marshal(q)
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/sessions/attach", bytes.NewReader(raw)))
			if source.calls != row.calls {
				t.Fatalf("verified source calls=%d, want%d", source.calls, row.calls)
			}
			if row.code == "" {
				var attached defaults.LegacyAttachment
				if err = json.Unmarshal(response.Body.Bytes(), &attached); err != nil || response.Code != http.StatusOK || attached.AttachmentID == "" || attached.Session.SessionID != f.incumbent.Session.SessionID || attached.Session.AttachedClients != 2 {
					t.Fatalf("live same-provider attach failed: status%d err%v", response.Code, err)
				}
			} else {
				var failure struct{ Error struct{ Code string } }
				if err = json.Unmarshal(response.Body.Bytes(), &failure); err != nil || response.Code != http.StatusConflict || failure.Error.Code != row.code {
					t.Fatalf("refusal=%d/%s want409/%s err%v", response.Code, failure.Error.Code, row.code, err)
				}
				after, err := os.ReadFile(path)
				view, _, mapped, mapErr := f.manager.LookupOwnerMapping(context.Background(), "parent/primary")
				if err != nil || mapErr != nil || !mapped || !bytes.Equal(before, after) || view.SessionID != f.incumbent.Session.SessionID || view.AttachedClients != 1 || view.Eviction != nil {
					t.Fatalf("refused attach changed recorded incumbent: read%v mapping%v view%+v", err, mapErr, view)
				}
			}
			state, stateErr := native.State(context.Background())
			if (row.unknown && !errors.Is(stateErr, fault)) || (!row.unknown && stateErr != nil) || state != core.ProviderHostRunning || core.LegacyPhysicalTestLaunchCount(f.codex) != 1 || core.LegacyPhysicalTestLaunchCount(f.claude) != 0 || f.ownerSlotHolder() != f.incumbent.Session.SessionID {
				t.Fatal("mapping observation launched/stopped/replaced the incumbent")
			}
		})
	}
}
