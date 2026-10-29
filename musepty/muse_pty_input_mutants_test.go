//go:build mutants

package musepty

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Controls execute the real native-fixture assertions against production-only
// overlays. Compilation, setup failure and an unnamed failure are never kills.
func TestMusePTYWriterNarrowingMutants(t *testing.T) {
	type plant struct{ name, file, old, replacement, test, assertion string }
	plants := []plant{
		{"run_evidence_relabelled_exact", "muse_pty_input.go", `observation.EvidenceClass = sm.InputEvidenceRunObservedAfterSnapshot`, `observation.EvidenceClass = sm.InputEvidenceExactProviderAck`, "TestMusePTYEvidenceClass", "actual PTY run evidence missing"},
		{"expected_correlation_used_as_run_id", "muse_pty_input.go", `sm.PTYRunObservation{RunID: runID}`, `sm.PTYRunObservation{RunID: func()string{_ = runID;return expected.Correlation}()}`, "TestMusePTYEvidenceClass", "run evidence was not present in owned native fold"},
		{"foreign_native_session", "muse_pty_input.go", `in.Expected.Correlation == h.NativeIdentity()`, `true`, "TestMusePTYWriterAdmissionRefusals/foreign-correlation", "invalid input admitted or wrong refusal"},
		{"foreign_receipt", "muse_pty_input.go", `if receipt.ID != in.ID || receipt.SessionID != in.Lease.SessionID || receipt.Epoch != in.Lease.Epoch {`, `if false {`, "TestMusePTYWriterAdmissionRefusals/foreign-receipt", "invalid input admitted or wrong refusal"},
		{"mixed_payload", "muse_pty_input.go", `p.MuseGoal != nil ||`, `false ||`, "TestMusePTYWriterAdmissionRefusals/mixed", "invalid input admitted or wrong refusal"},
		{"cancelled_lock_wait", "muse_pty_host.go", `if err := historyguard.LockMutex(ctx, &h.writeMu); err != nil {`, `_ = historyguard.LockMutex; h.writeMu.Lock(); if err := error(nil); err != nil {`, "TestMusePTYWriterCancelledLockWait", "cancelled lock acquisition remained blocked"},
		{"cancelled_waiter", "muse_pty_input.go", `return func(wait context.Context) (sm.InputObservation, error) {
		if err := wait.Err(); err != nil {
			return sm.InputObservation{}, err
		}
		select {
		case <-wait.Done():
			return sm.InputObservation{}, wait.Err()
		case <-done:
			if err := wait.Err(); err != nil {
				return sm.InputObservation{}, err
			}
			return observation, resultErr
		}
	}, nil`, `return func(wait context.Context) (sm.InputObservation, error) { <-done; return observation, resultErr }, nil`, "TestMusePTYWriterObservedSessionAck", "cancelled waiter fabricated ack"},
		{"pre_submit_run", "muse_pty_host.go", `for _, id := range finalFold.Started {`, `for _, id := range []string(nil) {`, "TestMusePTYWriterNoAckCancellation/completed-before-submit", "pre-submit unrelated run fabricated provider ack"},
		{"written_as_provider_ack", "muse_pty_input.go", `return func(wait context.Context) (sm.InputObservation, error) {`, `return func(wait context.Context) (sm.InputObservation, error) { return sm.InputObservation{Receipt:receipt, Kind:sm.InputObservationKind(expected.Kind), Correlation:expected.Correlation}, nil;`, "TestMusePTYWriterNoAckCancellation/no-ack", "unobserved run-start fabricated provider ack"},
		{"waiter_writes_enter", "muse_pty_input.go", `return func(wait context.Context) (sm.InputObservation, error) {`, `return func(wait context.Context) (sm.InputObservation, error) { if _, err := h.ptmx.Write([]byte{'\r'}); err != nil {return sm.InputObservation{}, err};`, "TestMusePTYWriterObservedSessionAck", "acknowledgement waiter wrote native input"},
		{"busy_publishes_held", "muse_pty_input.go", `text, expected := *in.Payload.Text, in.Expected`, `h.held = *in.Payload.Text; text, expected := *in.Payload.Text, in.Expected`, "TestMusePTYWriterBusyNoWrite", "refused input retained state"},
		{"trust_native_gate", "muse_pty_screen.go", `if dialog {`, `if strings.Contains(screen, "do you trust") { return true }; if dialog {`, "TestMusePTYWriterIndependentRefusalFixture/trust", "known unsafe native fixture admitted input"},
		{"queue_native_gate", "muse_pty_log.go", `len(f.Queued) == 0`, `len(f.Queued) <= 1`, "TestMusePTYWriterIndependentRefusalFixture/queued", "known unsafe native fixture admitted input"},
	}
	run := func(overlay, selection string) ([]byte, int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		args := []string{"test", "-p", "1"}
		if overlay != "" {
			args = append(args, "-overlay", overlay)
		}
		args = append(args, ".", "-run", selection, "-count=1", "-timeout=45s", "-v")
		cmd := exec.CommandContext(ctx, "go", args...)
		out, _ := cmd.CombinedOutput()
		if cmd.ProcessState == nil {
			return out, -1
		}
		return out, cmd.ProcessState.ExitCode()
	}
	if out, code := run("", "^Test(MusePTYWriter(ObservedSessionAck|AdmissionRefusals|BusyNoWrite|IndependentRefusalFixture|CancelledLockWait|NoAckCancellation)|MusePTYEvidenceClass)$"); code != 0 {
		t.Fatalf("baseline exit=%d\n%s", code, out)
	}
	for _, m := range plants {
		t.Run(m.name, func(t *testing.T) {
			path, err := filepath.Abs(m.file)
			if err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			count := strings.Count(string(original), m.old)
			if count != 1 {
				t.Fatalf("plant anchor count=%d", count)
			}
			dir := t.TempDir()
			mutant := filepath.Join(dir, m.file)
			if err := os.WriteFile(mutant, []byte(strings.ReplaceAll(string(original), m.old, m.replacement)), 0600); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(map[string]any{"Replace": map[string]string{path: mutant}})
			if err != nil {
				t.Fatal(err)
			}
			overlay := filepath.Join(dir, "overlay.json")
			if err := os.WriteFile(overlay, data, 0600); err != nil {
				t.Fatal(err)
			}
			out, code := run(overlay, "^"+m.test+"$")
			t.Logf("mutant=%s exit=%d named_test=%s\n%s", m.name, code, m.test, out)
			root := strings.Split(m.test, "/")[0]
			if code != 1 || !strings.Contains(string(out), "--- FAIL: "+root) || !strings.Contains(string(out), m.assertion) || strings.Contains(string(out), "[build failed]") {
				t.Fatalf("SURVIVOR: no named behavioral kill for %s", m.name)
			}
		})
	}
}
