//go:build darwin || linux

package musebuilds

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAttemptEscapingRefusesBeforePublication reaches the public transient
// recorder with an input valid before JSON escaping but oversized afterward.
func TestAttemptEscapingRefusesBeforePublication(t *testing.T) {
	f := newFreeze(t)
	id, d := probeBuild(t, f, "1.4.4-R5419.1")
	obs := timeoutObservation("escaped-attempt", d.SnapshotID, CleanupConfirmed)
	obs.Detail = strings.Repeat("<", 4096)
	_, err := RecordProbeOutcome(context.Background(), f.config(), id, obs)
	requireProbeCode(t, err, CodeFailureRecordUnavailable, StageProbe)
	_, err = ReadAttemptReceipt(context.Background(), f.config(), id, obs.AttemptID)
	requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
	requireCopied(t, f.config(), id)
	obs.Detail = strings.Repeat("<>&\"\\\n", 128)
	record, err := RecordProbeOutcome(context.Background(), f.config(), id, obs)
	if err != nil || record.Receipt == nil {
		t.Fatalf("bounded escaped receipt: %+v, %v", record, err)
	}
	requireReceiptRoundTrip(t, f.config(), id, *record.Receipt)
}

// TestBADEscapingRefusesBeforePublication reaches BAD publication after its
// independently bounded attempt receipt succeeded. A large but valid copied
// transaction leaves insufficient serialized space for the escaped verdict.
func TestBADEscapingRefusesBeforePublication(t *testing.T) {
	f := newFreeze(t)
	id, d := probeBuild(t, f, "1.4.4-R5419.1")
	state := requireCopied(t, f.config(), id)
	state.Transaction = strings.Repeat("x", 60*1024)
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	path := statePath(f.root, freezeKey(id.Build, id.SHA256))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	requireCopied(t, f.config(), id) // fixture must be readable before the attack
	obs := ProbeObservation{AttemptID: "escaped-bad", SelectionID: d.SnapshotID, Outcome: ProbeExitOKUnparseable, Cleanup: CleanupConfirmed, Detail: strings.Repeat("<", 1000)}
	record, err := RecordProbeOutcome(context.Background(), f.config(), id, obs)
	requireProbeCode(t, err, CodeProbePublicationFailed, StageProbe)
	if record.Receipt == nil {
		t.Fatal("BAD publication stage was never reached: attempt receipt absent")
	}
	requireReceiptRoundTrip(t, f.config(), id, *record.Receipt)
	retained := requireCopied(t, f.config(), id)
	if retained.Transaction != state.Transaction {
		t.Fatal("rejected BAD replaced the prior state")
	}
	obs.AttemptID, obs.Detail = "bounded-bad", strings.Repeat("<>&", 16)
	record, err = RecordProbeOutcome(context.Background(), f.config(), id, obs)
	if err != nil || record.State.State != StateBAD {
		t.Fatalf("bounded BAD publication: %+v %v", record, err)
	}
	got, err := ReadBuildState(context.Background(), f.config(), id)
	if err != nil || got.Verdict == nil || got.Verdict.Detail != obs.Detail {
		t.Fatalf("BAD readback: %+v %v", got, err)
	}
}

func TestLastGoodEscapingRoundTripAndOverflow(t *testing.T) {
	f := newFreeze(t)
	id := probeOKBuild(t, f, "1.4.4-R5419.1")
	success := genuineSuccess(id, "escaping-start")
	success.By = strings.Repeat("<>&\"\\\n", 128)
	seeded, err := WriteLastGood(context.Background(), f.config(), success)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadLastGood(context.Background(), f.config())
	if err != nil || got != seeded {
		t.Fatalf("escaped startup readback: %+v %v", got, err)
	}
	path := filepath.Join(f.root, lastGoodFileName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, by := range []string{strings.Repeat("<", 12*1024), "invalid-\xff"} {
		success.By = by
		_, err := WriteLastGood(context.Background(), f.config(), success)
		requireLastGoodCode(t, err, CodeLastGoodInvalid, StageLastGood)
		after, err := os.ReadFile(path)
		if err != nil || string(before) != string(after) {
			t.Fatalf("invalid serialized receipt replaced last-good: %v", err)
		}
	}
	seeded.Sequence = ^uint64(0)
	raw, err := json.Marshal(seeded)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadLastGood(context.Background(), f.config()); err != nil {
		t.Fatal(err)
	}
	success.By = "valid"
	_, err = WriteLastGood(context.Background(), f.config(), success)
	requireLastGoodCode(t, err, CodeLastGoodInvalid, StageLastGood)
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(raw) {
		t.Fatalf("overflow published an unreadable sequence: %v", err)
	}
}
