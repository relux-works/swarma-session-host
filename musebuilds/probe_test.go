//go:build darwin || linux

package musebuilds

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// requireProbeCode fails unless err chains to a probe refusal with the
// exact code and stage. Refusal identity is the code, never message prose.
func requireProbeCode(t *testing.T, err error, code, stage string) *ProbeError {
	t.Helper()
	if err == nil {
		t.Fatalf("want refusal %s, got success", code)
	}
	refusal, ok := AsProbeError(err)
	if !ok {
		t.Fatalf("want refusal %s, got untyped %v", code, err)
	}
	if refusal.Code != code {
		t.Fatalf("want refusal %s, got %s (%v)", code, refusal.Code, err)
	}
	if refusal.Stage != stage {
		t.Fatalf("want stage %s for %s, got %s (%v)", stage, code, refusal.Stage, err)
	}
	return refusal
}

// probeBuild publishes one frozen build through the public freeze entry
// and returns its frozen identity plus the discovery selection it binds
// to. Every probe test starts from a genuine COPIED publication.
func probeBuild(t *testing.T, f *freezeFixture, build string) (FrozenIdentity, Discovery) {
	t.Helper()
	discovery := f.discoveryFor(build)
	published := f.publishForOther(t, discovery)
	if published.Frozen.Build != build {
		t.Fatalf("published %s, want %s", published.Frozen.Build, build)
	}
	return published.Frozen, discovery
}

// timeoutObservation bodies a first-probe timeout the consumer observed:
// an attempted start with unconfirmed teardown and bounded evidence.
func timeoutObservation(attempt, selection string, cleanup CleanupResult) ProbeObservation {
	return ProbeObservation{
		AttemptID: attempt, SelectionID: selection, Outcome: ProbeTimeout,
		Detail: "first probe timed out after 30s", Cleanup: cleanup,
		OutputDigest: strings.Repeat("ab", 32), OutputBytes: 120,
	}
}

// successObservation bodies a clean matching first probe.
func successObservation(attempt, selection string) ProbeObservation {
	return ProbeObservation{
		AttemptID: attempt, SelectionID: selection, Outcome: ProbeExitOKMatch,
		Cleanup:      CleanupConfirmed,
		OutputDigest: strings.Repeat("cd", 32), OutputBytes: 48,
	}
}

// requireCopied fails unless the build's authoritative state is COPIED
// with no verdict.
func requireCopied(t *testing.T, cfg HostConfig, id FrozenIdentity) BuildState {
	t.Helper()
	state, err := ReadBuildState(context.Background(), cfg, id)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != StateCopied {
		t.Fatalf("state = %s, want %s", state.State, StateCopied)
	}
	if state.Verdict != nil {
		t.Fatalf("COPIED state carries verdict %+v", state.Verdict)
	}
	return state
}

// requireReceiptRoundTrip reads one attempt receipt back through the
// public entry and checks its identity and evidence fields.
func requireReceiptRoundTrip(t *testing.T, cfg HostConfig, id FrozenIdentity, want AttemptReceipt) AttemptReceipt {
	t.Helper()
	got, err := ReadAttemptReceipt(context.Background(), cfg, id, want.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.equivalent(want) {
		t.Fatalf("receipt round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
	if got.RecordedAt == "" {
		t.Fatal("receipt carries no admission timestamp")
	}
	return got
}

// TestLoadedHostTimeoutFallsBackWithoutBAD proves a first timeout
// records the failed-attempt receipt the section-5 controller falls
// back from, keeps COPIED, and never mints a byte verdict. The
// "fallback" half is the receipt's existence and content; routing the
// fallback itself belongs to the L2 recovery controller.
func TestLoadedHostTimeoutFallsBackWithoutBAD(t *testing.T) {
	for _, cleanup := range []CleanupResult{CleanupIncomplete, CleanupConfirmed} {
		t.Run(string(cleanup), func(t *testing.T) {
			f := newFreeze(t)
			cfg := f.config()
			frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
			obs := timeoutObservation("attempt-timeout-1", discovery.SnapshotID, cleanup)
			record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
			if err != nil {
				t.Fatal(err)
			}
			if record.State.State != StateCopied {
				t.Fatalf("state = %s, want %s", record.State.State, StateCopied)
			}
			if record.Receipt == nil {
				t.Fatal("timeout recorded no attempt receipt")
			}
			if record.Receipt.Outcome != ProbeTimeout || record.Receipt.Cleanup != cleanup {
				t.Fatalf("receipt = %+v, want timeout with cleanup %s", record.Receipt, cleanup)
			}
			if record.Receipt.AttemptID != obs.AttemptID || record.Receipt.SelectionID != discovery.SnapshotID {
				t.Fatalf("receipt = %+v, want the recorded attempt and selection", record.Receipt)
			}
			requireCopied(t, cfg, frozen)
			requireReceiptRoundTrip(t, cfg, frozen, *record.Receipt)
			if verdict, bad, err := BadRecord(context.Background(), cfg, frozen); err != nil {
				t.Fatal(err)
			} else if bad {
				t.Fatalf("timeout minted verdict %+v", verdict)
			}
		})
	}
}

// TestConcurrentTimeoutsDoNotMintBinaryVerdict proves repeated and
// concurrent timeouts never mint a byte verdict: every recording
// succeeds, every receipt persists, and the state stays COPIED.
func TestConcurrentTimeoutsDoNotMintBinaryVerdict(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
	for _, attempt := range []string{"attempt-timeout-1", "attempt-timeout-2", "attempt-timeout-3"} {
		obs := timeoutObservation(attempt, discovery.SnapshotID, CleanupIncomplete)
		if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs); err != nil {
			t.Fatal(err)
		}
		requireCopied(t, cfg, frozen)
	}
	const racers = 8
	start := make(chan struct{})
	var group sync.WaitGroup
	errs := make(chan error, racers)
	for i := 0; i < racers; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			<-start
			obs := timeoutObservation(strings.Repeat("r", 8)+string(rune('a'+i)), discovery.SnapshotID, CleanupConfirmed)
			_, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
			errs <- err
		}(i)
	}
	close(start)
	done := make(chan struct{})
	go func() { group.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent timeout recordings never returned")
	}
	for i := 0; i < racers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	requireCopied(t, cfg, frozen)
	if verdict, bad, err := BadRecord(context.Background(), cfg, frozen); err != nil {
		t.Fatal(err)
	} else if bad {
		t.Fatalf("timeouts minted verdict %+v", verdict)
	}
	for _, attempt := range []string{"attempt-timeout-1", "attempt-timeout-2", "attempt-timeout-3"} {
		if _, err := ReadAttemptReceipt(context.Background(), cfg, frozen, attempt); err != nil {
			t.Fatal(err)
		}
	}
}

// TestProbeBinaryVerdictMarksBAD proves both deterministic first-probe
// contradictions mint a persistent BAD verdict scoped to the exact
// bytes and probe contract: a clean exit-0 with unparseable output,
// and a clean exit-0 attesting a different well-formed build.
func TestProbeBinaryVerdictMarksBAD(t *testing.T) {
	t.Run("unparseable identity", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		obs := ProbeObservation{
			AttemptID: "attempt-unparseable-1", SelectionID: discovery.SnapshotID,
			Outcome: ProbeExitOKUnparseable, Detail: "clean exit with output outside the identity grammar",
			Cleanup: CleanupConfirmed, OutputDigest: strings.Repeat("01", 32), OutputBytes: 64,
		}
		record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		if err != nil {
			t.Fatal(err)
		}
		if record.State.State != StateBAD {
			t.Fatalf("state = %s, want %s", record.State.State, StateBAD)
		}
		if record.State.Verdict == nil || record.State.Verdict.Code != VerdictProbeIdentityUnparseable {
			t.Fatalf("verdict = %+v, want %s", record.State.Verdict, VerdictProbeIdentityUnparseable)
		}
		if record.Receipt == nil || record.Receipt.Outcome != ProbeExitOKUnparseable {
			t.Fatalf("receipt = %+v, want the recorded contradiction", record.Receipt)
		}
		requireReceiptRoundTrip(t, cfg, frozen, *record.Receipt)
		verdict, bad, err := BadRecord(context.Background(), cfg, frozen)
		if err != nil {
			t.Fatal(err)
		}
		if !bad || verdict.Code != VerdictProbeIdentityUnparseable {
			t.Fatalf("BadRecord = %+v bad=%v, want the unparseable verdict", verdict, bad)
		}
	})

	t.Run("mismatched identity", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		obs := ProbeObservation{
			AttemptID: "attempt-mismatch-1", SelectionID: discovery.SnapshotID,
			Outcome: ProbeExitOKMismatch, ObservedBuild: "1.4.3-R5000",
			Detail: "clean exit attesting another build", Cleanup: CleanupConfirmed,
			OutputDigest: strings.Repeat("23", 32), OutputBytes: 48,
		}
		record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		if err != nil {
			t.Fatal(err)
		}
		if record.State.State != StateBAD {
			t.Fatalf("state = %s, want %s", record.State.State, StateBAD)
		}
		if record.State.Verdict == nil || record.State.Verdict.Code != VerdictProbeIdentityMismatch {
			t.Fatalf("verdict = %+v, want %s", record.State.Verdict, VerdictProbeIdentityMismatch)
		}
		if record.Receipt == nil || record.Receipt.ObservedBuild != "1.4.3-R5000" {
			t.Fatalf("receipt = %+v, want the attested build as evidence", record.Receipt)
		}
		requireReceiptRoundTrip(t, cfg, frozen, *record.Receipt)
	})
}

// TestTransientProbeFailuresNeverMintBAD proves every non-deterministic
// probe failure class records a transient receipt and leaves COPIED
// alone: exec failure, nonzero exit, signal, output cap and drain
// failure. Neither volume nor exit status proves a byte failure.
func TestTransientProbeFailuresNeverMintBAD(t *testing.T) {
	outcomes := []ProbeOutcome{
		ProbeExecFailed, ProbeExitNonzero, ProbeSignaled, ProbeOutputLimit, ProbeDrainFailed,
	}
	for _, outcome := range outcomes {
		t.Run(string(outcome), func(t *testing.T) {
			f := newFreeze(t)
			cfg := f.config()
			frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
			obs := ProbeObservation{
				AttemptID: "attempt-" + string(outcome) + "-1", SelectionID: discovery.SnapshotID,
				Outcome: outcome, Detail: "observed " + string(outcome), Cleanup: CleanupIncomplete,
				OutputDigest: strings.Repeat("45", 32), OutputBytes: 96,
			}
			record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
			if err != nil {
				t.Fatal(err)
			}
			if record.State.State != StateCopied {
				t.Fatalf("state = %s, want %s", record.State.State, StateCopied)
			}
			if record.Receipt == nil || record.Receipt.Outcome != outcome {
				t.Fatalf("receipt = %+v, want the recorded %s", record.Receipt, outcome)
			}
			if record.Receipt.Cleanup != CleanupIncomplete {
				t.Fatalf("receipt cleanup = %s, want the observed teardown result", record.Receipt.Cleanup)
			}
			requireCopied(t, cfg, frozen)
			requireReceiptRoundTrip(t, cfg, frozen, *record.Receipt)
			if verdict, bad, err := BadRecord(context.Background(), cfg, frozen); err != nil {
				t.Fatal(err)
			} else if bad {
				t.Fatalf("%s minted verdict %+v", outcome, verdict)
			}
		})
	}
}

// TestProbePublicationLockDeadline proves post-probe publication
// refuses bounded when the metadata lock is contended: no verdict,
// no receipt and no lock wait past the cap.
func TestProbePublicationLockDeadline(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
	holder, err := os.OpenFile(filepath.Join(f.root, "state.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Flock(int(holder.Fd()), syscall.LOCK_UN) }()
	type result struct {
		record ProbeRecord
		err    error
	}
	done := make(chan result, 1)
	go func() {
		obs := timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)
		record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		done <- result{record: record, err: err}
	}()
	select {
	case r := <-done:
		requireProbeCode(t, r.err, CodeProbeLockFailed, StageProbe)
	case <-time.After(60 * time.Second):
		t.Fatal("contended probe publication never refused")
	}
	requireCopied(t, cfg, frozen)
	if _, err := ReadAttemptReceipt(context.Background(), cfg, frozen, "attempt-timeout-1"); err == nil {
		t.Fatal("contended publication recorded a receipt")
	} else {
		requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
	}
}

// TestProbeSuccessMarksProbeOK proves a clean matching probe
// transitions COPIED to PROBE_OK, that re-recording success is
// idempotent, and that success writes no attempt receipt and no
// last-good: the PROBE_OK state is the evidence.
func TestProbeSuccessMarksProbeOK(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
	record, err := RecordProbeOutcome(context.Background(), cfg, frozen, successObservation("attempt-success-1", discovery.SnapshotID))
	if err != nil {
		t.Fatal(err)
	}
	if record.State.State != StateProbeOK {
		t.Fatalf("state = %s, want %s", record.State.State, StateProbeOK)
	}
	if record.State.Verdict != nil {
		t.Fatalf("PROBE_OK state carries verdict %+v", record.State.Verdict)
	}
	if record.Receipt != nil {
		t.Fatalf("success recorded receipt %+v", record.Receipt)
	}
	echo := successObservation("attempt-success-2", discovery.SnapshotID)
	echo.ObservedBuild = frozen.Build
	again, err := RecordProbeOutcome(context.Background(), cfg, frozen, echo)
	if err != nil {
		t.Fatal(err)
	}
	if again.State.State != StateProbeOK || again.Receipt != nil {
		t.Fatalf("re-recorded success = %+v, want idempotent PROBE_OK without receipt", again)
	}
	state, err := ReadBuildState(context.Background(), cfg, frozen)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != StateProbeOK || state.Build != frozen.Build || state.SHA256 != frozen.SHA256 {
		t.Fatalf("state = %+v, want the PROBE_OK tuple", state)
	}
	if _, bad, err := BadRecord(context.Background(), cfg, frozen); err != nil {
		t.Fatal(err)
	} else if bad {
		t.Fatal("success reads back as BAD")
	}
	if _, err := ReadAttemptReceipt(context.Background(), cfg, frozen, "attempt-success-1"); err == nil {
		t.Fatal("success left an attempt receipt behind")
	} else {
		requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
	}
}

// TestProbeEvidenceConflictRefuses proves contradictory terminal
// evidence refuses without conferring truth: a deterministic BAD over
// PROBE_OK persists its receipt and keeps PROBE_OK, and success over
// a recorded BAD keeps the BAD verdict.
func TestProbeEvidenceConflictRefuses(t *testing.T) {
	t.Run("bad over probe-ok", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, successObservation("attempt-success-1", discovery.SnapshotID)); err != nil {
			t.Fatal(err)
		}
		obs := ProbeObservation{
			AttemptID: "attempt-mismatch-1", SelectionID: discovery.SnapshotID,
			Outcome: ProbeExitOKMismatch, ObservedBuild: "1.4.3-R5000",
			Detail: "contradicting observation", Cleanup: CleanupConfirmed,
		}
		record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		requireProbeCode(t, err, CodeProbeEvidenceConflict, StageProbe)
		if record.Receipt == nil || record.Receipt.AttemptID != obs.AttemptID {
			t.Fatalf("conflict kept no receipt of the contradicting observation: %+v", record)
		}
		state, err := ReadBuildState(context.Background(), cfg, frozen)
		if err != nil {
			t.Fatal(err)
		}
		if state.State != StateProbeOK {
			t.Fatalf("state = %s, want the standing PROBE_OK", state.State)
		}
		requireReceiptRoundTrip(t, cfg, frozen, *record.Receipt)
	})

	t.Run("success over bad", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		condemn := ProbeObservation{
			AttemptID: "attempt-unparseable-1", SelectionID: discovery.SnapshotID,
			Outcome: ProbeExitOKUnparseable, Detail: "recorded contradiction", Cleanup: CleanupConfirmed,
		}
		if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, condemn); err != nil {
			t.Fatal(err)
		}
		record, err := RecordProbeOutcome(context.Background(), cfg, frozen, successObservation("attempt-success-1", discovery.SnapshotID))
		requireProbeCode(t, err, CodeProbeEvidenceConflict, StageProbe)
		if record.State.State != StateBAD || record.State.Verdict == nil {
			t.Fatalf("conflict record = %+v, want the standing BAD", record.State)
		}
		verdict, bad, err := BadRecord(context.Background(), cfg, frozen)
		if err != nil {
			t.Fatal(err)
		}
		if !bad || verdict.Code != VerdictProbeIdentityUnparseable {
			t.Fatalf("BadRecord = %+v bad=%v, want the standing verdict", verdict, bad)
		}
	})
}

// probeInvalidCells pairs a name with a frozen identity and observation
// that must refuse as invalid. The frozen reference is literal: every
// cell fails before any store work, so no publication is needed.
func probeInvalidCells(root string) []struct {
	name string
	id   FrozenIdentity
	obs  ProbeObservation
} {
	digest := strings.Repeat("ab", 32)
	build := "1.4.4-R5419.1"
	id := FrozenIdentity{Build: build, SHA256: digest, Path: frozenBinaryPath(root, freezeKey(build, digest), build)}
	obs := ProbeObservation{
		AttemptID: "attempt-1", SelectionID: strings.Repeat("c", 64),
		Outcome: ProbeTimeout, Detail: "timed out", Cleanup: CleanupIncomplete,
	}
	with := func(mut func(*FrozenIdentity, *ProbeObservation)) (FrozenIdentity, ProbeObservation) {
		nextID, nextObs := id, obs
		mut(&nextID, &nextObs)
		return nextID, nextObs
	}
	long := strings.Repeat("a", 129)
	big := strings.Repeat("d", probeDetailMaxBytes+1)
	cells := []struct {
		name string
		id   FrozenIdentity
		obs  ProbeObservation
	}{}
	add := func(name string, mut func(*FrozenIdentity, *ProbeObservation)) {
		nextID, nextObs := with(mut)
		cells = append(cells, struct {
			name string
			id   FrozenIdentity
			obs  ProbeObservation
		}{name: name, id: nextID, obs: nextObs})
	}
	add("empty attempt", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.AttemptID = "" })
	add("parent attempt", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.AttemptID = ".." })
	add("slashed attempt", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.AttemptID = "a/b" })
	add("long attempt", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.AttemptID = long })
	add("empty selection", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.SelectionID = "" })
	add("slashed selection", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.SelectionID = "a/b" })
	add("unknown outcome", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.Outcome = "exploded" })
	add("empty cleanup", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.Cleanup = "" })
	add("unknown cleanup", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.Cleanup = "maybe" })
	add("detail past bound", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.Detail = big })
	add("malformed output digest", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.OutputDigest = "xyz" })
	add("negative output size", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.OutputBytes = -1 })
	add("failed row carries a build", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.ObservedBuild = build })
	add("failed row carries another build", func(_ *FrozenIdentity, obs *ProbeObservation) { obs.ObservedBuild = "1.4.3-R5000" })
	add("unparseable row carries a build", func(_ *FrozenIdentity, obs *ProbeObservation) {
		obs.Outcome = ProbeExitOKUnparseable
		obs.ObservedBuild = build
	})
	add("mismatch row names nothing", func(_ *FrozenIdentity, obs *ProbeObservation) {
		obs.Outcome = ProbeExitOKMismatch
		obs.ObservedBuild = ""
	})
	add("mismatch row echoes selected", func(_ *FrozenIdentity, obs *ProbeObservation) {
		obs.Outcome = ProbeExitOKMismatch
		obs.ObservedBuild = build
	})
	add("mismatch row unparseable", func(_ *FrozenIdentity, obs *ProbeObservation) {
		obs.Outcome = ProbeExitOKMismatch
		obs.ObservedBuild = "tomorrow"
	})
	add("match row names another", func(_ *FrozenIdentity, obs *ProbeObservation) {
		obs.Outcome = ProbeExitOKMatch
		obs.ObservedBuild = "1.4.3-R5000"
	})
	add("unparseable frozen build", func(id *FrozenIdentity, _ *ProbeObservation) { id.Build = "tomorrow" })
	add("malformed frozen digest", func(id *FrozenIdentity, _ *ProbeObservation) { id.SHA256 = "xyz" })
	add("relative frozen path", func(id *FrozenIdentity, _ *ProbeObservation) { id.Path = "muse-bin-1.4.4-R5419.1" })
	add("disagreeing frozen path", func(id *FrozenIdentity, _ *ProbeObservation) { id.Path = filepath.Join(root, "elsewhere") })
	return cells
}

// TestProbeInvalidObservationsRefuse proves every malformed probe input
// refuses before any store work: bad attempt and selection tokens,
// unknown outcome and cleanup, unbounded detail, malformed digests,
// identity evidence on rows that must not carry any, and frozen
// references that do not bind to the store copy.
func TestProbeInvalidObservationsRefuse(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	for _, cell := range probeInvalidCells(f.root) {
		t.Run(cell.name, func(t *testing.T) {
			_, err := RecordProbeOutcome(context.Background(), cfg, cell.id, cell.obs)
			requireProbeCode(t, err, CodeProbeInvalid, StageProbe)
		})
	}
	if _, err := ReadBuildState(context.Background(), cfg, FrozenIdentity{
		Build: "1.4.4-R5419.1", SHA256: strings.Repeat("ab", 32),
		Path: frozenBinaryPath(f.root, freezeKey("1.4.4-R5419.1", strings.Repeat("ab", 32)), "1.4.4-R5419.1"),
	}); err == nil {
		t.Fatal("invalid observations left state behind")
	} else {
		requireProbeCode(t, err, CodeProbeStateAbsent, StageProbe)
	}
}

// TestProbeDuplicateAttemptRefuses proves an attempt identity is
// single-use for its observation: re-recording the identical
// observation is idempotent and returns the recorded receipt, while
// reusing the identity for a conflicting observation refuses.
func TestProbeDuplicateAttemptRefuses(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
	first := timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)
	record, err := RecordProbeOutcome(context.Background(), cfg, frozen, first)
	if err != nil {
		t.Fatal(err)
	}
	again, err := RecordProbeOutcome(context.Background(), cfg, frozen, first)
	if err != nil {
		t.Fatal(err)
	}
	if again.Receipt == nil || !again.Receipt.equivalent(*record.Receipt) || again.Receipt.RecordedAt != record.Receipt.RecordedAt {
		t.Fatalf("identical retry = %+v, want the recorded receipt back", again.Receipt)
	}
	requireCopied(t, cfg, frozen)
	conflicting := first
	conflicting.Detail = "a different account of the same attempt"
	if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, conflicting); err == nil {
		t.Fatal("conflicting detail reused the attempt identity")
	} else {
		requireProbeCode(t, err, CodeFailureRecordUnavailable, StageProbe)
		if !errors.Is(err, errDuplicateAttempt) {
			t.Fatalf("duplicate cause = %v, want the conflicting-observation refusal", err)
		}
	}
	otherOutcome := first
	otherOutcome.Outcome = ProbeExitNonzero
	if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, otherOutcome); err == nil {
		t.Fatal("conflicting outcome reused the attempt identity")
	} else {
		requireProbeCode(t, err, CodeFailureRecordUnavailable, StageProbe)
		if !errors.Is(err, errDuplicateAttempt) {
			t.Fatalf("duplicate cause = %v, want the conflicting-observation refusal", err)
		}
	}
	requireCopied(t, cfg, frozen)
	requireReceiptRoundTrip(t, cfg, frozen, *record.Receipt)
}

// TestProbeUnpreparedRefuses proves observations against missing or
// unusable probe state refuse with distinct codes: no state means
// freeze first, while malformed, disagreeing or foreign-contract
// state is inconsistency, never absence.
func TestProbeUnpreparedRefuses(t *testing.T) {
	t.Run("absent state", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.4-R5419.1")
		install.writeSidecar("1.4.4-R5419.1\n")
		discovery, err := ResolveNewest(context.Background(), install.env())
		if err != nil {
			t.Fatal(err)
		}
		digest := discovery.Identity.SHA256
		id := FrozenIdentity{
			Build: "1.4.4-R5419.1", SHA256: digest,
			Path: frozenBinaryPath(f.root, freezeKey("1.4.4-R5419.1", digest), "1.4.4-R5419.1"),
		}
		obs := timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)
		_, err = RecordProbeOutcome(context.Background(), cfg, id, obs)
		requireProbeCode(t, err, CodeProbeUnprepared, StageProbe)
	})

	t.Run("malformed state", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		key := freezeKey(frozen.Build, frozen.SHA256)
		if err := os.MkdirAll(filepath.Join(f.root, "meta", key), 0o700); err != nil {
			t.Fatal(err)
		}
		f.writeFileSync(filepath.Join(f.root, "meta", key, "STATE.json"), []byte("{malformed"), 0o600)
		obs := timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)
		_, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		requireProbeCode(t, err, CodeProbeStateInconsistent, StageProbe)
	})

	t.Run("foreign contract", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		key := freezeKey(frozen.Build, frozen.SHA256)
		foreign := BuildState{
			Schema: buildStateSchema, Build: frozen.Build, SHA256: frozen.SHA256,
			Size: 32, Transaction: "txn-foreign", ProbeContract: "muse-first-probe-v0", State: StateCopied,
		}
		raw, err := json.Marshal(foreign)
		if err != nil {
			t.Fatal(err)
		}
		f.writeFileSync(filepath.Join(f.root, "meta", key, "STATE.json"), raw, 0o600)
		obs := timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)
		_, err = RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		requireProbeCode(t, err, CodeProbeContractMismatch, StageProbe)
	})

	t.Run("disagreeing state", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		key := freezeKey(frozen.Build, frozen.SHA256)
		other := BuildState{
			Schema: buildStateSchema, Build: "1.4.3-R5000", SHA256: strings.Repeat("0", 64),
			Size: 32, Transaction: "txn-other", ProbeContract: ProbeContractID, State: StateCopied,
		}
		raw, err := json.Marshal(other)
		if err != nil {
			t.Fatal(err)
		}
		f.writeFileSync(filepath.Join(f.root, "meta", key, "STATE.json"), raw, 0o600)
		obs := timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)
		_, err = RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		requireProbeCode(t, err, CodeProbeStateInconsistent, StageProbe)
	})
}

// TestRecordProbeOutcomeHonorsCancel proves a canceled context refuses
// before any store work: no directories, no state, no receipt.
func TestRecordProbeOutcomeHonorsCancel(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	digest := strings.Repeat("ab", 32)
	build := "1.4.4-R5419.1"
	id := FrozenIdentity{Build: build, SHA256: digest, Path: frozenBinaryPath(f.root, freezeKey(build, digest), build)}
	obs := timeoutObservation("attempt-timeout-1", strings.Repeat("c", 64), CleanupIncomplete)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := RecordProbeOutcome(ctx, cfg, id, obs)
	requireProbeCode(t, err, CodeProbeCanceled, StageProbe)
	if _, err := os.Stat(filepath.Join(f.root, "builds")); !os.IsNotExist(err) {
		t.Fatalf("canceled recording created store layout: %v", err)
	}
}

// TestAttemptReceiptReadClasses proves attempt-receipt reads stay
// typed: missing attempts read absent, while malformed or disagreeing
// receipts are inconsistency, never absence.
func TestAttemptReceiptReadClasses(t *testing.T) {
	t.Run("absent build", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		digest := strings.Repeat("ab", 32)
		build := "1.4.4-R5419.1"
		id := FrozenIdentity{Build: build, SHA256: digest, Path: frozenBinaryPath(f.root, freezeKey(build, digest), build)}
		_, err := ReadAttemptReceipt(context.Background(), cfg, id, "attempt-1")
		requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
	})

	t.Run("no attempts recorded", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, _ := probeBuild(t, f, "1.4.4-R5419.1")
		_, err := ReadAttemptReceipt(context.Background(), cfg, frozen, "attempt-1")
		requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
	})

	t.Run("unknown attempt", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)); err != nil {
			t.Fatal(err)
		}
		_, err := ReadAttemptReceipt(context.Background(), cfg, frozen, "attempt-2")
		requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
	})

	t.Run("malformed receipt", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)); err != nil {
			t.Fatal(err)
		}
		key := freezeKey(frozen.Build, frozen.SHA256)
		f.writeFileSync(filepath.Join(f.root, "meta", key, "attempts", "attempt-bad.json"), []byte("{malformed"), 0o600)
		_, err := ReadAttemptReceipt(context.Background(), cfg, frozen, "attempt-bad")
		requireProbeCode(t, err, CodeProbeStateInconsistent, StageProbe)
	})

	t.Run("disagreeing receipt", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, timeoutObservation("attempt-timeout-1", discovery.SnapshotID, CleanupIncomplete)); err != nil {
			t.Fatal(err)
		}
		key := freezeKey(frozen.Build, frozen.SHA256)
		foreign := AttemptReceipt{
			Schema: attemptReceiptSchema, AttemptID: "attempt-foreign", SelectionID: discovery.SnapshotID,
			Build: "1.4.3-R5000", SHA256: strings.Repeat("0", 64), Outcome: ProbeTimeout,
			Cleanup: CleanupIncomplete, RecordedAt: "2026-10-10T09:00:00Z",
		}
		raw, err := json.Marshal(foreign)
		if err != nil {
			t.Fatal(err)
		}
		f.writeFileSync(filepath.Join(f.root, "meta", key, "attempts", "attempt-foreign.json"), raw, 0o600)
		_, err = ReadAttemptReceipt(context.Background(), cfg, frozen, "attempt-foreign")
		requireProbeCode(t, err, CodeProbeStateInconsistent, StageProbe)
	})
}

// TestProbeVerdictVerifiesFrozenBytes proves a terminal probe verdict
// verifies the frozen bytes through the held FD before publishing:
// changed, missing and linked executables refuse with inconsistency,
// the COPIED state stands, and no BAD verdict is recorded.
func TestProbeVerdictVerifiesFrozenBytes(t *testing.T) {
	tamperBytes := func(t *testing.T, path string) {
		t.Helper()
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho tampered\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o555); err != nil {
			t.Fatal(err)
		}
	}
	removeBytes := func(t *testing.T, frozen FrozenIdentity) {
		t.Helper()
		leaf := filepath.Dir(frozen.Path)
		if err := os.Chmod(leaf, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(frozen.Path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(leaf, 0o555); err != nil {
			t.Fatal(err)
		}
	}
	linkBytes := func(t *testing.T, f *freezeFixture, frozen FrozenIdentity) {
		t.Helper()
		leaf := filepath.Dir(frozen.Path)
		if err := os.Chmod(leaf, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(frozen.Path); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(f.root, "link-target.sh")
		f.writeFileSync(target, []byte("#!/bin/sh\necho linked\n"), 0o755)
		if err := os.Symlink(target, frozen.Path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(leaf, 0o555); err != nil {
			t.Fatal(err)
		}
	}
	badObs := func(selection string) ProbeObservation {
		return ProbeObservation{
			AttemptID: "attempt-unparseable-1", SelectionID: selection,
			Outcome: ProbeExitOKUnparseable, Detail: "clean contradiction over bad bytes",
			Cleanup: CleanupConfirmed, OutputDigest: strings.Repeat("01", 32), OutputBytes: 64,
		}
	}
	requireUnpublished := func(t *testing.T, cfg HostConfig, frozen FrozenIdentity, attempt string) {
		t.Helper()
		requireCopied(t, cfg, frozen)
		if verdict, bad, err := BadRecord(context.Background(), cfg, frozen); err != nil {
			t.Fatal(err)
		} else if bad {
			t.Fatalf("unverified bytes minted verdict %+v", verdict)
		}
		if _, err := ReadAttemptReceipt(context.Background(), cfg, frozen, attempt); err == nil {
			t.Fatal("refused verdict left an attempt receipt behind")
		} else {
			requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
		}
	}

	t.Run("changed bytes refuse BAD", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		tamperBytes(t, frozen.Path)
		_, err := RecordProbeOutcome(context.Background(), cfg, frozen, badObs(discovery.SnapshotID))
		requireProbeCode(t, err, CodeProbeStateInconsistent, StageProbe)
		requireUnpublished(t, cfg, frozen, "attempt-unparseable-1")
	})

	t.Run("missing bytes refuse BAD", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		removeBytes(t, frozen)
		_, err := RecordProbeOutcome(context.Background(), cfg, frozen, badObs(discovery.SnapshotID))
		requireProbeCode(t, err, CodeProbeStateInconsistent, StageProbe)
		requireUnpublished(t, cfg, frozen, "attempt-unparseable-1")
	})

	t.Run("linked bytes refuse BAD", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		linkBytes(t, f, frozen)
		_, err := RecordProbeOutcome(context.Background(), cfg, frozen, badObs(discovery.SnapshotID))
		requireProbeCode(t, err, CodeProbeStateInconsistent, StageProbe)
		requireUnpublished(t, cfg, frozen, "attempt-unparseable-1")
	})

	t.Run("changed bytes refuse success", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		tamperBytes(t, frozen.Path)
		_, err := RecordProbeOutcome(context.Background(), cfg, frozen, successObservation("attempt-success-1", discovery.SnapshotID))
		requireProbeCode(t, err, CodeProbeStateInconsistent, StageProbe)
		requireCopied(t, cfg, frozen)
		if _, err := ReadAttemptReceipt(context.Background(), cfg, frozen, "attempt-success-1"); err == nil {
			t.Fatal("refused success left an attempt receipt behind")
		} else {
			requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
		}
	})
}

// TestProbeOutputCapNeverParsedForIdentity proves the M65 recording
// half: an over-cap observation is a transient, never identity
// evidence. A build carried on an over-cap row refuses as invalid;
// a bare over-cap row records its receipt and leaves COPIED alone.
// The executing drain bound itself belongs to the ProbeFrozen runner
// the L2 consumer owns; this test pins what 2b records for that gate.
func TestProbeOutputCapNeverParsedForIdentity(t *testing.T) {
	t.Run("over-cap with build refuses", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		obs := ProbeObservation{
			AttemptID: "attempt-overcap-1", SelectionID: discovery.SnapshotID,
			Outcome: ProbeOutputLimit, ObservedBuild: "1.4.3-R5000",
			Detail: "over-cap output naming a build", Cleanup: CleanupIncomplete,
			OutputDigest: strings.Repeat("67", 32), OutputBytes: 2 * 1024 * 1024,
		}
		_, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		requireProbeCode(t, err, CodeProbeInvalid, StageProbe)
		requireCopied(t, cfg, frozen)
	})

	t.Run("bare over-cap records transient", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		obs := ProbeObservation{
			AttemptID: "attempt-overcap-1", SelectionID: discovery.SnapshotID,
			Outcome: ProbeOutputLimit, Detail: "output past the during-read cap",
			Cleanup:      CleanupIncomplete,
			OutputDigest: strings.Repeat("67", 32), OutputBytes: 2 * 1024 * 1024,
		}
		record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
		if err != nil {
			t.Fatal(err)
		}
		if record.State.State != StateCopied {
			t.Fatalf("state = %s, want %s", record.State.State, StateCopied)
		}
		if record.Receipt == nil || record.Receipt.Outcome != ProbeOutputLimit {
			t.Fatalf("receipt = %+v, want the recorded over-cap", record.Receipt)
		}
		requireReceiptRoundTrip(t, cfg, frozen, *record.Receipt)
	})
}

// TestProbeCleanupIncompletePreserved proves the M67 recording half:
// the observed teardown result is preserved verbatim in the attempt
// receipt, so the L2 second-child gate reads honest input. Timeout
// with incomplete teardown records incomplete; timeout with confirmed
// teardown records confirmed. Refusing the second child itself belongs
// to the executing controller, not to this recording.
func TestProbeCleanupIncompletePreserved(t *testing.T) {
	for _, cleanup := range []CleanupResult{CleanupIncomplete, CleanupConfirmed} {
		t.Run(string(cleanup), func(t *testing.T) {
			f := newFreeze(t)
			cfg := f.config()
			frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
			obs := timeoutObservation("attempt-timeout-1", discovery.SnapshotID, cleanup)
			record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
			if err != nil {
				t.Fatal(err)
			}
			if record.Receipt == nil || record.Receipt.Cleanup != cleanup {
				t.Fatalf("receipt = %+v, want cleanup %s", record.Receipt, cleanup)
			}
			got := requireReceiptRoundTrip(t, cfg, frozen, *record.Receipt)
			if got.Cleanup != cleanup {
				t.Fatalf("round-trip cleanup = %s, want %s", got.Cleanup, cleanup)
			}
		})
	}
}
