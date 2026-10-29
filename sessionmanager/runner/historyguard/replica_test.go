package historyguard

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// A JSON-write attempt has two durable pending replicas. Losing, corrupting or
// rolling back either one closes ordinary admission and keeps the attempted
// image available for explicit recovery. The intact and repaired stores admit.
func TestBoundWitnessPendingSurvivesEitherReplicaFault(t *testing.T) {
	for _, suffix := range []string{"", ".intent"} {
		for _, fault := range []string{"intact", "missing", "empty", "partial", "stale"} {
			t.Run(suffix+"/"+fault, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "state.lock")
				id := StoreIdentity("test", "namespace")
				first := sealedFixture(t, 1, "retired")
				second := sealedFixture(t, 2, "retired", "attempted")
				bindFixture(t, path, first)
				previous, err := os.ReadFile(path + suffix)
				if err != nil {
					t.Fatal(err)
				}
				if err := CheckBound(path, first, id, false); err != nil {
					t.Fatal(err)
				}
				if err := PrepareBound(path, second, id, false); err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "missing":
					err = os.Remove(path + suffix)
				case "empty":
					err = os.WriteFile(path+suffix, nil, 0600)
				case "partial":
					err = os.WriteFile(path+suffix, []byte("{"), 0600)
				case "stale":
					err = os.WriteFile(path+suffix, previous, 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := CheckBound(path, first, id, false); err == nil {
					t.Fatal("pending intent erased by one-file fault")
				}
				images, err := RecoveryImages(path, first, id)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, image := range images {
					found = found || bytes.Equal(image, second)
				}
				if !found {
					t.Fatal("attempted image lost after one-file fault")
				}
				third := sealedFixture(t, 3, "retired", "attempted", "reconciled")
				if err := PrepareBound(path, third, id, true); err != nil {
					t.Fatal(err)
				}
				if err := FinishBound(path, third, id); err != nil {
					t.Fatal(err)
				}
				if err := CheckBound(path, third, id, false); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// A crash between replica prepares or finalizations leaves disagreement, not a
// clean old image. Recovery retains the newest pending/committed candidate.
func TestBoundWitnessInterruptedReplicaStages(t *testing.T) {
	for _, stage := range []string{"first-prepare", "first-finalize"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.lock")
			id := StoreIdentity("test", "namespace")
			first := sealedFixture(t, 1, "retired")
			second := sealedFixture(t, 2, "retired", "attempted")
			bindFixture(t, path, first)
			w, err := readBound(path, id)
			if err != nil {
				t.Fatal(err)
			}
			w.Pending = second
			if stage == "first-finalize" {
				if err := PrepareBound(path, second, id, false); err != nil {
					t.Fatal(err)
				}
				w.Committed = second
				w.Pending = nil
			}
			if err := writeBound(path, w); err != nil {
				t.Fatal(err)
			}
			if err := CheckBound(path, first, id, false); err == nil {
				t.Fatal("interrupted replicas admitted old image")
			}
			if err := CheckBound(path, second, id, false); err == nil {
				t.Fatal("interrupted replicas admitted new image")
			}
			images, err := RecoveryImages(path, first, id)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, image := range images {
				found = found || bytes.Equal(image, second)
			}
			if !found {
				t.Fatal("interrupted candidate lost")
			}
		})
	}
}
