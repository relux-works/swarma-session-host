package sessionmanager

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Only the native boundary is controlled. Launch records a real owned file so
// a late codec rejection cannot masquerade as refusal before factory effects.
type codexBudgetRelaunchHostFactory struct {
	root  string
	calls int
}

func (*codexBudgetRelaunchHostFactory) Provider() string { return "codex" }
func (f *codexBudgetRelaunchHostFactory) Launch(_ context.Context, q ProviderLaunchRequest) (RunnerHost, error) {
	f.calls++
	if err := os.WriteFile(filepath.Join(f.root, fmt.Sprintf("launch-%d", f.calls)), []byte(q.SessionID), 0600); err != nil {
		return nil, err
	}
	s := CodexRunnerSnapshot{Contract: codexHostSnapshotContract, PID: os.Getpid(), URL: "ws://127.0.0.1:1", ThreadID: "fixture-thread", ProjectDir: f.root, CodexHome: filepath.Join(f.root, "native-home")}
	if q.Prior != nil {
		s = *q.Prior.Snapshot.Codex
	}
	// The real fresh constructor selects nil. The common host must refuse an
	// unsupported prior before calling any provider, even this controlled one.
	prepared, err := prepareCodexLaunchSnapshot(&CodexPreparedLaunch{Snapshot: s})
	if err != nil {
		return nil, err
	}
	prepared.Stopped = false
	return &codexBudgetRelaunchHostRunner{snapshot: prepared}, nil
}
func (*codexBudgetRelaunchHostFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return nil, errors.New("stopped archive must not restore a native runner")
}

type codexBudgetRelaunchHostRunner struct {
	snapshot CodexRunnerSnapshot
	stopped  bool
}

func (r *codexBudgetRelaunchHostRunner) Snapshot() (RunnerSnapshot, error) {
	return copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &r.snapshot}), nil
}
func (r *codexBudgetRelaunchHostRunner) State(context.Context) (ProviderHostState, error) {
	if r.stopped {
		return ProviderHostStopped, nil
	}
	return ProviderHostRunning, nil
}
func (r *codexBudgetRelaunchHostRunner) Stop(context.Context) error { r.stopped = true; return nil }

// Staging installs the actual input binding, but this fixture grants no input
// or native-goal capability and never fabricates an acknowledgement.
func (*codexBudgetRelaunchHostRunner) SupportsNativeGoals() bool         { return false }
func (*codexBudgetRelaunchHostRunner) AcceptsInput(InputSubmission) bool { return false }
func (*codexBudgetRelaunchHostRunner) ObserveNativeGoal(context.Context) (NativeGoalObservation, error) {
	return NativeGoalObservation{}, ErrHostCapabilityUnavailable
}
func (*codexBudgetRelaunchHostRunner) ExecuteInput(context.Context, InputSubmission, InputReceipt) (func(context.Context) (InputObservation, error), error) {
	return nil, ErrHostCapabilityUnavailable
}

func codexBudgetRelaunchHostFixture(t *testing.T, budget *int64) (*SessionHost, *codexBudgetRelaunchHostFactory, string, string) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, "host")
	layout := Layout{Namespace: "budget-relaunch", StateDir: state, SessionsDir: filepath.Join(state, "sessions"), LockFile: filepath.Join(state, "manager.lock"), NoticesDir: filepath.Join(root, "notices")}
	notices, err := NewSessionNoticeStore(layout)
	if err != nil {
		t.Fatal(err)
	}
	factory := &codexBudgetRelaunchHostFactory{root: root}
	options := HostOptions{Layout: layout, Config: hostCommitProbeConfig{}, Codec: NewLegacyRecordCodec(), Notices: notices, Factories: []ProviderHostFactory{factory}}
	h, err := NewHost(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	if err := h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	a, err := h.Attach(context.Background(), AttachRequest{Owner: "parent/primary", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	id := a.Session.SessionID
	if _, err := h.Stop(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	// Seed a legitimate archive edit through the actual codec/store. No live
	// write, epoch reset, fake codec or source authorization is substituted.
	h.mu.Lock()
	record := copyRunnerRecord(h.sessions[id])
	record.ProviderHost.Snapshot.Codex.GoalTokenBudget = copyCodexGoalTokenBudget(budget)
	err = h.updateRecord(context.Background(), record)
	if err == nil {
		h.sessions[id] = record
	}
	h.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	h, err = NewHost(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	if err := h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	return h, factory, id, root
}

func codexBudgetRelaunchFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	image := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		image[path] = fmt.Sprintf("%x", sha256.Sum256(raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return image
}

// Both actual common entrypoints refuse the committed archived selection before
// factory/epoch/file effects. The real stopped goal-less neighbor relaunches and
// commits the same session through each path. No provider process is started.
func TestCodexBudgetRelaunchCommonRefusesBeforeEffects(t *testing.T) {
	for _, staged := range []bool{false, true} {
		for _, budget := range []*int64{nil, codexBudgetConstructorPointer(0), codexBudgetConstructorPointer(7654321)} {
			name := fmt.Sprintf("staged=%t/absent", staged)
			if budget != nil {
				name = fmt.Sprintf("staged=%t/%d", staged, *budget)
			}
			t.Run(name, func(t *testing.T) {
				h, factory, id, root := codexBudgetRelaunchHostFixture(t, budget)
				before := codexBudgetRelaunchFiles(t, root)
				calls := factory.calls
				q := AttachRequest{Owner: "parent/primary", Provider: "codex", Resume: true, ResumeSessionID: id}
				var attachment *Attachment
				var err error
				if staged {
					var p AttachmentPreparation
					p, err = h.PrepareAttach(context.Background(), q)
					if budget == nil && err == nil {
						attachment, err = h.CommitAttach(context.Background(), AttachmentCommit{Token: p.Token, Lease: p.Lease, BaseSHA256: p.BaseSHA256})
					} else if budget != nil && !reflect.DeepEqual(p, AttachmentPreparation{}) {
						t.Fatal("unsupported prior exposed a staged token/writer")
					}
				} else {
					attachment, err = h.Attach(context.Background(), q)
				}
				if budget == nil {
					if err != nil || attachment == nil || attachment.Session.SessionID != id || attachment.Session.State != SessionRunning || factory.calls != calls+1 {
						t.Fatalf("goal-less relaunch control failed: %#v / %v / launches=%d", attachment, err, factory.calls)
					}
					return
				}
				if !errors.Is(err, ErrHostCapabilityUnavailable) || attachment != nil || factory.calls != calls || !reflect.DeepEqual(before, codexBudgetRelaunchFiles(t, root)) {
					t.Fatalf("unsupported archive reached factory/epoch/files: err=%v launches=%d before=%d attachment=%#v", err, factory.calls, calls, attachment)
				}
			})
		}
	}
}
