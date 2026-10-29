package historyguard

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// A process exit at witness-temp sync cannot replace the durable replica. The
// initial write has no admitted history and must refuse recovery as unknown;
// later prepares/finalizations retain their existing checked recovery images.
func TestBoundWitnessProcessExitBeforeReplicaRename(t *testing.T) {
	if path := os.Getenv("SH1_FIX4_REPLICA_CRASH_PATH"); path != "" {
		id := StoreIdentity("test", "namespace")
		w := boundWitness{StoreID: id, Pending: sealedFixture(t, 1, "initial")}
		if os.Getenv("SH1_FIX4_REPLICA_CRASH_STAGE") != "initial" {
			var err error
			w, err = readBound(path, id)
			if err != nil {
				t.Fatal(err)
			}
			if os.Getenv("SH1_FIX4_REPLICA_CRASH_STAGE") == "prepare" {
				w.Pending = sealedFixture(t, 2, "retired", "attempted")
			} else {
				w.Committed = w.Pending
				w.Pending = nil
			}
		}
		err := writeBoundWithSync(path, w, func(f *os.File) error {
			if err := f.Sync(); err != nil {
				return err
			}
			os.Exit(73)
			return nil
		})
		t.Fatalf("crash boundary not reached: %v", err)
	}
	for _, stage := range []string{"initial", "prepare", "finalize"} {
		for _, suffix := range []string{"", ".intent"} {
			t.Run(stage+suffix, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "state.lock")
				id := StoreIdentity("test", "namespace")
				first := sealedFixture(t, 1, "retired")
				second := sealedFixture(t, 2, "retired", "attempted")
				if stage != "initial" {
					bindFixture(t, path, first)
				}
				if stage == "finalize" {
					if err := PrepareBound(path, second, id, false); err != nil {
						t.Fatal(err)
					}
				}
				before, _ := os.ReadFile(path + suffix)
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBoundWitnessProcessExitBeforeReplicaRename$")
				child.Env = append(os.Environ(), "SH1_FIX4_REPLICA_CRASH_PATH="+path+suffix, "SH1_FIX4_REPLICA_CRASH_STAGE="+stage)
				if err := child.Start(); err != nil {
					t.Fatal(err)
				}
				t.Logf("child PID=%d", child.Process.Pid)
				err := child.Wait()
				if err == nil || child.ProcessState.ExitCode() != 73 {
					t.Fatalf("expected injected exit73: %v", err)
				}
				after, err := os.ReadFile(path + suffix)
				if stage == "initial" {
					if !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("initial unrenamed replica became durable: %v", err)
					}
					if _, err := RecoveryImages(path, nil, id); err == nil {
						t.Fatal("unknown history recovered as empty")
					}
					return
				}
				if err != nil || !bytes.Equal(before, after) {
					t.Fatal("unrenamed replica changed")
				}
				images, err := RecoveryImages(path, first, id)
				if err != nil {
					t.Fatal(err)
				}
				want := first
				if stage == "finalize" {
					want = second
				}
				found := false
				for _, image := range images {
					found = found || bytes.Equal(image, want)
				}
				if !found {
					t.Fatal("committed or attempted image lost")
				}
			})
		}
	}
}
