package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// This codec fixture stores no private provider state. Its exact fixed bytes
// and explicit typed record isolate the host's initial transaction state machine.
type initialRetryCodec struct {
	record  RunnerRecord
	raw     []byte
	initial int
}

func (c *initialRetryCodec) DecodeLegacy(_ context.Context, input LegacyRecordInput) (RunnerRecord, error) {
	if input.SessionID != c.record.SessionID || !bytes.Equal(input.Bytes, []byte("initial-record-image")) {
		return RunnerRecord{}, ErrLegacyCodecMismatch
	}
	return cloneRunnerRecord(c.record)
}
func (c *initialRetryCodec) EncodeInitial(_ context.Context, record RunnerRecord) ([]byte, error) {
	c.initial++
	if !reflect.DeepEqual(record, c.record) {
		return nil, ErrLegacyCodecMismatch
	}
	return append([]byte(nil), c.raw...), nil
}
func (c *initialRetryCodec) EncodeMerge(_ context.Context, input LegacyRecordInput, record RunnerRecord) ([]byte, error) {
	if !reflect.DeepEqual(record, c.record) {
		return nil, ErrLegacyCodecMismatch
	}
	return append([]byte(nil), input.Bytes...), nil
}
func initialRetryStore(t *testing.T) (*codecRecords, *initialRetryCodec, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "sessions")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	record := RunnerRecord{Contract: sessionRecordContract, SessionID: "SES-" + strings.Repeat("a", 32), Owner: "owner", Provider: "muse", State: SessionRunning, Liveness: SessionLivenessLive, CreatedAt: now, UpdatedAt: now, ProviderHost: ProviderHostRecord{Provider: "muse", Snapshot: RunnerSnapshot{Kind: "muse-pty", MusePTY: &MusePTYSnapshot{Contract: "task-board.muse-pty-host-v1", SessionID: "native", PID: 123, LogPath: filepath.Join(root, "log")}}}}
	codec := &initialRetryCodec{record: record, raw: []byte("initial-record-image")}
	store, err := newCodecRecords(root, codec)
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.path(record.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	return store, codec, path
}

// The live host retries only its exact validated initial attempt. Both an
// untouched path and an empty directory left before publishing recover using
// the actual writer; a rename-then-error admits exact visible bytes once.
// These are process/filesystem-call boundaries, not power-loss simulation.
func TestHostCodecInitialRetryCommitBoundaries(t *testing.T) {
	for _, stage := range []string{"before-directory", "empty-directory", "after-rename"} {
		t.Run(stage, func(t *testing.T) {
			store, codec, path := initialRetryStore(t)
			injected := errors.New("initial write boundary")
			commits := 0
			store.commit = func(path string, raw []byte) error {
				commits++
				switch stage {
				case "empty-directory":
					if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
						return err
					}
				case "after-rename":
					if err := writePrivateFile(path, raw); err != nil {
						return err
					}
				}
				return injected
			}
			if err := store.create(context.Background(), codec.record); !errors.Is(err, ErrLegacyCommitUncertain) || !errors.Is(err, injected) {
				t.Fatalf("initial failure: %v", err)
			}
			if !store.uncertain[codec.record.SessionID] || len(store.pendingInitial) != 1 || len(store.bases) != 0 {
				t.Fatal("failed initial write lost pending identity or promoted bytes")
			}
			store.commit = func(path string, raw []byte) error { commits++; return writePrivateFile(path, raw) }
			actual, err := store.recoverInitial(context.Background(), codec.record)
			if err != nil {
				t.Fatal(err)
			}
			expectedCommits := 2
			if stage == "after-rename" {
				expectedCommits = 1
			}
			raw, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(raw, codec.raw) || !reflect.DeepEqual(actual, codec.record) || commits != expectedCommits || len(store.pendingInitial) != 0 || store.uncertain[codec.record.SessionID] || store.bases[codec.record.SessionID].SHA256 != legacyRecordImage(codec.record.SessionID, raw).SHA256 {
				t.Fatalf("initial resolution: commits=%d error=%v", commits, err)
			}
		})
	}
}

// Recovery never overwrites foreign bytes, follows a foreign directory link,
// accepts a changed codec image, or clears the pending attempt on refusal.
// The successful neighboring boundary cases are the positive controls.
func TestHostCodecInitialRetryRefusesForeignState(t *testing.T) {
	for _, change := range []string{"record", "symlink", "symlink-record", "nonempty", "codec"} {
		t.Run(change, func(t *testing.T) {
			store, codec, path := initialRetryStore(t)
			store.commit = func(string, []byte) error { return errors.New("no publication") }
			if err := store.create(context.Background(), codec.record); !errors.Is(err, ErrLegacyCommitUncertain) {
				t.Fatal(err)
			}
			foreign := []byte("foreign record bytes")
			switch change {
			case "record":
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, foreign, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(t.TempDir(), filepath.Dir(path)); err != nil {
					t.Fatal(err)
				}
			case "symlink-record":
				target := t.TempDir()
				if err := os.WriteFile(filepath.Join(target, "record.json"), codec.raw, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Dir(path)); err != nil {
					t.Fatal(err)
				}
			case "nonempty":
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(filepath.Dir(path), "foreign"), foreign, 0600); err != nil {
					t.Fatal(err)
				}
			case "codec":
				codec.raw = []byte("different candidate")
			}
			writes := 0
			store.commit = func(string, []byte) error { writes++; return nil }
			if _, err := store.recoverInitial(context.Background(), codec.record); !errors.Is(err, ErrLegacyViewStale) {
				t.Fatalf("foreign initial state admitted: %v", err)
			}
			if writes != 0 || len(store.pendingInitial) != 1 || !store.uncertain[codec.record.SessionID] || len(store.bases) != 0 {
				t.Fatal("refused initial recovery wrote or lost pending attempt")
			}
			if change == "record" {
				raw, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(raw, foreign) {
					t.Fatal("foreign initial bytes overwritten")
				}
			}
		})
	}
}
