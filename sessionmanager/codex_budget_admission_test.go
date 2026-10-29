package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var errCodexAdmissionLaunchReached = errors.New("isolated Codex launch boundary reached")

type codexAdmissionFactory struct{ calls int }

func (f *codexAdmissionFactory) Provider() string { return "codex" }
func (f *codexAdmissionFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	f.calls++
	return nil, errCodexAdmissionLaunchReached
}
func (f *codexAdmissionFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return nil, ErrProviderHostNotFound
}

func codexAdmissionHost(t *testing.T, budget string) (*SessionHost, *codexAdmissionFactory, string) {
	t.Helper()
	dir := t.TempDir()
	id := "SES-cccccccccccccccccccccccccccccccc"
	journal, e := newControllerEpochJournal(dir, "admission")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = journal.reserveContext(context.Background(), id); e != nil {
		t.Fatal(e)
	}
	field := ""
	if budget != "" {
		field = fmt.Sprintf(",%q:%s", "goal_token_budget", budget)
	}
	raw := []byte(fmt.Sprintf(`{"contract":"task-board-session-record-v3","session_id":%q,"owner":{"kind":"parent","id":"primary"},"provider":"codex","state":"stopped","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","provider_host":{"provider":"codex","opaque":{"contract":"task-board.codex-provider-host-v1","pid":42,"url":"ws://127.0.0.1:1","thread_id":"fixture-thread","project_dir":"/workspace","codex_home":"/fixture-home","stopped":true%s}}}`, id, field))
	path := filepath.Join(dir, "sessions", id, "record.json")
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	// Real record layout/path is resolved by the actual codec storage helper.
	codec := NewLegacyRecordCodec()
	records, e := newCodecRecords(filepath.Join(dir, "sessions"), codec)
	if e != nil {
		t.Fatal(e)
	}
	path, e = records.path(id)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	f := &codexAdmissionFactory{}
	h, e := NewHost(HostOptions{Layout: Layout{Namespace: "admission", StateDir: dir, SessionsDir: filepath.Join(dir, "sessions"), LockFile: filepath.Join(dir, "host.lock")}, Codec: codec, Config: hostCommitProbeConfig{}, Notices: hostCommitProbeNotices{}, Factories: []ProviderHostFactory{f}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(h.Close)
	return h, f, id
}

// Actual loaded stopped records carrying an explicit prior budget cannot reach
// epoch reservation or provider launch through either attach path. Nil-budget
// control reaches the isolated launch boundary, proving this is not reject-all.
// This qualifies preflight/real storage ordering, not a live Codex consumer.
func TestHostCodexArchivedBudgetRefusedBeforeEffects(t *testing.T) {
	for _, staged := range []bool{false, true} {
		for _, budget := range []string{"", "0", "7654321"} {
			name := map[bool]string{false: "ordinary", true: "staged"}[staged] + "/" + map[bool]string{false: budget, true: "none"}[budget == ""]
			t.Run(name, func(t *testing.T) {
				h, f, id := codexAdmissionHost(t, budget)
				path, _ := h.records.path(id)
				before, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				epochPath := filepath.Join(h.options.Layout.StateDir, controllerEpochFile)
				oldEpoch, epochErr := os.ReadFile(epochPath)
				if epochErr != nil && !errors.Is(epochErr, os.ErrNotExist) {
					t.Fatal(epochErr)
				}
				q := AttachRequest{Owner: "parent/primary", Provider: "codex", ResumeSessionID: id}
				if staged {
					_, e = h.PrepareAttach(context.Background(), q)
				} else {
					_, e = h.Attach(context.Background(), q)
				}
				if budget == "" {
					if !errors.Is(e, errCodexAdmissionLaunchReached) || f.calls != 1 {
						t.Fatalf("goal-less control did not reach launch: %v calls%d", e, f.calls)
					}
					return
				}
				after, re := os.ReadFile(path)
				newEpoch, ee := os.ReadFile(epochPath)
				sameEpoch := (errors.Is(epochErr, os.ErrNotExist) && errors.Is(ee, os.ErrNotExist)) || (epochErr == nil && ee == nil && bytes.Equal(oldEpoch, newEpoch))
				if !errors.Is(e, ErrHostCapabilityUnavailable) || f.calls != 0 || re != nil || !bytes.Equal(before, after) || !sameEpoch || len(h.preparations) != 0 || len(h.inputs) != 0 || len(h.attachments) != 0 {
					t.Fatalf("archived budget crossed preflight: %v calls%d sameEpoch%t", e, f.calls, sameEpoch)
				}
			})
		}
	}
}
