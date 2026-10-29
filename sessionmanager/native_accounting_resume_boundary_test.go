package sessionmanager

import (
	"context"
	"errors"
	"os"
	"testing"
)

// A valid admitted consumer resumes and hydrates once; its matching refusal
// returns before native launch, epoch persistence or stopped-image mutation.
// This is core ordering with real storage/codec, not budget/runtime acceptance;
// the separate real-consumer integration tests remain the production neighbor.
func TestNativeAccountingStoppedResumeBoundaryValidAndRefused(t *testing.T) {
	for _, staged := range []bool{false, true} {
		for _, refuse := range []bool{false, true} {
			name := map[bool]string{false: "ordinary", true: "staged"}[staged] + "/" + map[bool]string{false: "valid", true: "refused"}[refuse]
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				factory := &rev5CountingFactory{}
				h := rev5Host(t, factory)
				a, e := h.Attach(ctx, AttachRequest{Owner: "parent/primary", Provider: "muse"})
				if e != nil {
					t.Fatal(e)
				}
				id := a.Session.SessionID
				if _, e = h.Stop(ctx, id); e != nil {
					t.Fatal(e)
				}
				path, _ := h.records.path(id)
				before, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				epochPath := h.options.Layout.StateDir + "/" + controllerEpochFile
				epoch, e := os.ReadFile(epochPath)
				if e != nil {
					t.Fatal(e)
				}
				launches := factory.launches
				validated, hydrated := 0, 0
				fault := errors.New("resume source refused")
				h.options.OwnerRuns = &nativeRestoreBoundaryProbe{validate: func(context.Context, RunnerRecord, LegacyRecordInput) error {
					if validated == 0 && factory.launches != launches {
						t.Error("first validation followed native launch")
					}
					validated++
					if refuse {
						return fault
					}
					return nil
				}, hydrate: func(context.Context, CapturedOwnerRun) error { hydrated++; return nil }}
				q := AttachRequest{Owner: "parent/primary", Provider: "muse", ResumeSessionID: id}
				var out *Attachment
				if staged {
					var p AttachmentPreparation
					p, e = h.PrepareAttach(ctx, q)
					if e == nil {
						out, e = h.CommitAttach(ctx, attachmentCommit(p))
					}
				} else {
					out, e = h.Attach(ctx, q)
				}
				if refuse {
					after, readErr := os.ReadFile(path)
					actualEpoch, epochErr := os.ReadFile(epochPath)
					if !errors.Is(e, fault) || out != nil || factory.launches != launches || validated != 1 || hydrated != 0 || readErr != nil || epochErr != nil || string(after) != string(before) || string(actualEpoch) != string(epoch) {
						t.Fatalf("invalid resume crossed preflight: %v launches%d validated%d hydrated%d", e, factory.launches, validated, hydrated)
					}
					return
				}
				if e != nil || out == nil || out.Session.State != SessionRunning || factory.launches != launches+1 || validated != 2 || hydrated != 1 {
					t.Fatalf("valid resume/hydration failed: %+v %v launch%d validate%d hydrate%d", out, e, factory.launches, validated, hydrated)
				}
			})
		}
	}
}
