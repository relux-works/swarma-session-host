//go:build darwin || linux

package musebuilds

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestDedupeRequiresProbeOK proves a hash match alone never returns
// a runnable build: bytes without state and without a transaction
// refuse as unknown, a COPIED state resumes probe-pending, and only
// a PROBE_OK state reuses runnable — all without touching admission.
func TestDedupeRequiresProbeOK(t *testing.T) {
	t.Run("bytes without state or transaction refuse", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		f.plantFrozenBytes(t, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build, raw)
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageDedupe)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times on the unknown path", acquires)
		}
	})

	t.Run("copied state resumes probe-pending", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
		f.plantState(key, BuildState{
			Schema: buildStateSchema, Build: discovery.Identity.Build, SHA256: discovery.Identity.SHA256,
			Size: int64(len(raw)), Transaction: "txn-test", ProbeContract: ProbeContractID, State: StateCopied,
		})
		leases := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeLeaseAcquired {
				leases++
			}
		})
		result, err := PublishFrozen(context.Background(), f.config(), discovery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomeResumed || result.Runnable() {
			t.Fatalf("outcome = %s runnable=%v, want resumed and not runnable", result.Outcome, result.Runnable())
		}
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times on the resume path", acquires)
		}
		if leases != 1 {
			t.Fatalf("leases acquired %d times on the resume path, want the shared verification lease", leases)
		}
	})

	t.Run("probe-ok state reuses runnable under the shared lease", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
		f.plantState(key, BuildState{
			Schema: buildStateSchema, Build: discovery.Identity.Build, SHA256: discovery.Identity.SHA256,
			Size: int64(len(raw)), Transaction: "txn-test", ProbeContract: ProbeContractID, State: StateProbeOK,
		})
		leases := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeLeaseAcquired {
				leases++
			}
		})
		result, err := PublishFrozen(context.Background(), f.config(), discovery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomeReused || !result.Runnable() {
			t.Fatalf("outcome = %s runnable=%v, want reused and runnable", result.Outcome, result.Runnable())
		}
		if result.Frozen.Path != frozenBinaryPath(f.root, key, discovery.Identity.Build) {
			t.Fatalf("reused path = %s", result.Frozen.Path)
		}
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times on the reuse path", acquires)
		}
		if leases != 1 {
			t.Fatalf("leases acquired %d times on the reuse path, want the shared verification lease", leases)
		}
	})

	t.Run("contended lease refuses reuse", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
		f.plantState(key, BuildState{
			Schema: buildStateSchema, Build: discovery.Identity.Build, SHA256: discovery.Identity.SHA256,
			Size: int64(len(raw)), Transaction: "txn-test", ProbeContract: ProbeContractID, State: StateProbeOK,
		})
		holder, err := os.OpenFile(filepath.Join(f.root, "maintenance.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Close() }()
		if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = syscall.Flock(int(holder.Fd()), syscall.LOCK_UN) }()
		type result struct {
			copy FrozenCopy
			err  error
		}
		done := make(chan result, 1)
		go func() {
			got, err := PublishFrozen(context.Background(), f.config(), discovery)
			done <- result{copy: got, err: err}
		}()
		select {
		case r := <-done:
			requireFreezeCode(t, r.err, CodeFreezeLeaseFailed, StagePublish)
		case <-time.After(60 * time.Second):
			t.Fatal("contended reuse lease never refused")
		}
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times past the lease refusal", acquires)
		}
	})
}

// TestDedupeRejectsBAD proves a recorded deterministic verdict
// returns on every path — bytes present or not — instead of a
// runnable copy, a recopy or a silent pass-through.
func TestDedupeRejectsBAD(t *testing.T) {
	verdict := &BadVerdict{Code: "probe_identity_mismatch", Detail: "clean attestation of another build", RecordedAt: "2026-10-10T08:00:00Z"}
	cells := map[string]bool{"bytes present": true, "bytes absent": false}
	for name, withBytes := range cells {
		t.Run(name, func(t *testing.T) {
			f := newFreeze(t)
			discovery := f.discoveryFor("1.4.4-R5419.1")
			key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
			size := int64(0)
			if withBytes {
				raw, err := os.ReadFile(discovery.Identity.Path)
				if err != nil {
					t.Fatal(err)
				}
				f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
				size = int64(len(raw))
			}
			f.plantState(key, BuildState{
				Schema: buildStateSchema, Build: discovery.Identity.Build, SHA256: discovery.Identity.SHA256,
				Size: size, Transaction: "txn-test", ProbeContract: ProbeContractID, State: StateBAD, Verdict: verdict,
			})
			_, err := PublishFrozen(context.Background(), f.config(), discovery)
			refusal := requireFreezeCode(t, err, CodeRecordedBad, StageDedupe)
			if refusal.Bad == nil || refusal.Bad.Code != verdict.Code {
				t.Fatalf("verdict = %+v, want the recorded %s", refusal.Bad, verdict.Code)
			}
			if acquires, _, _ := f.lock.counts(); acquires != 0 {
				t.Fatalf("lock acquired %d times on the recorded path", acquires)
			}
			if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 0 {
				t.Fatalf("ledger = %v err=%v, want no reservation", entries, err)
			}
		})
	}
}

// TestMalformedStateNeverBecomesCrashAbsence proves every malformed
// STATE shape refuses as inconsistent — never as absent, and never
// as an adoption — before any admission runs.
func TestMalformedStateNeverBecomesCrashAbsence(t *testing.T) {
	build := "1.4.4-R5419.1"
	sha := strings.Repeat("a", 64)
	valid := `{"schema":1,"build":"` + build + `","sha256":"` + sha + `","size":10,"transaction":"txn-x","probe_contract":"` + ProbeContractID + `","state":"COPIED","verdict":null}`
	cells := map[string]string{
		"bad json":              `{"schema":1,`,
		"duplicate keys":        `{"schema":1,"schema":1,"build":"` + build + `","sha256":"` + sha + `","size":10,"transaction":"txn-x","probe_contract":"` + ProbeContractID + `","state":"COPIED","verdict":null}`,
		"unknown field":         `{"schema":1,"build":"` + build + `","sha256":"` + sha + `","size":10,"transaction":"txn-x","probe_contract":"` + ProbeContractID + `","state":"COPIED","verdict":null,"extra":1}`,
		"wrong schema":          strings.Replace(valid, `"schema":1`, `"schema":9`, 1),
		"unparseable build":     strings.Replace(valid, build, "tomorrow", 1),
		"short digest":          strings.Replace(valid, sha, "abc", 1),
		"negative size":         strings.Replace(valid, `"size":10`, `"size":-1`, 1),
		"empty transaction":     strings.Replace(valid, `"transaction":"txn-x"`, `"transaction":""`, 1),
		"unknown state":         strings.Replace(valid, `"state":"COPIED"`, `"state":"MAYBE"`, 1),
		"bad without verdict":   strings.Replace(valid, `"state":"COPIED","verdict":null`, `"state":"BAD","verdict":null`, 1),
		"verdict on copied":     strings.Replace(valid, `"state":"COPIED","verdict":null`, `"state":"COPIED","verdict":{"code":"x","detail":"y","recorded_at":"z"}`, 1),
		"trailing garbage":      valid + "x",
		"oversized padding":     valid + strings.Repeat(" ", stateMaxBytes),
		"identity disagreement": strings.Replace(valid, sha, strings.Repeat("b", 64), 1),
	}
	for name, document := range cells {
		t.Run(name, func(t *testing.T) {
			f := newFreeze(t)
			discovery := f.discoveryFor(build)
			key := freezeKey(build, discovery.Identity.SHA256)
			stateDir := filepath.Join(metaDir(f.root), key)
			if err := os.MkdirAll(stateDir, 0o700); err != nil {
				t.Fatal(err)
			}
			// The document names a fixed digest while the
			// discovery carries the live one; malformed cells
			// refuse before that comparison matters.
			f.writeFileSync(filepath.Join(stateDir, "STATE.json"), []byte(document), 0o600)
			_, err := PublishFrozen(context.Background(), f.config(), discovery)
			requireFreezeCode(t, err, CodeStateInconsistent, StageDedupe)
			if acquires, _, _ := f.lock.counts(); acquires != 0 {
				t.Fatalf("lock acquired %d times on the malformed path", acquires)
			}
		})
	}

	t.Run("unreadable state refuses", func(t *testing.T) {
		skipIfRoot(t)
		f := newFreeze(t)
		discovery := f.discoveryFor(build)
		key := freezeKey(build, discovery.Identity.SHA256)
		stateDir := filepath.Join(metaDir(f.root), key)
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(stateDir, "STATE.json")
		f.writeFileSync(path, []byte(valid), 0o600)
		if err := os.Chmod(path, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeStateInconsistent, StageDedupe)
	})

	t.Run("malformed state with bytes refuses through adoption", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor(build)
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(build, discovery.Identity.SHA256)
		f.plantFrozenBytes(t, key, build, raw)
		stateDir := filepath.Join(metaDir(f.root), key)
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			t.Fatal(err)
		}
		f.writeFileSync(filepath.Join(stateDir, "STATE.json"), []byte(`{"schema":1,`), 0o600)
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeStateInconsistent, StageDedupe)
	})
}

// TestFullDigestCollisionRefusesWithoutOverwrite proves identity
// comparison uses the full digest: a single flipped hex digit
// refuses, mismatching bytes at a key refuse, and nothing is
// overwritten either way.
func TestFullDigestCollisionRefusesWithoutOverwrite(t *testing.T) {
	t.Run("flipped digest digit refuses", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		binary := f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
		beforeInfo, err := os.Stat(binary)
		if err != nil {
			t.Fatal(err)
		}
		flipped := discovery.Identity.SHA256[:63] + flipHex(discovery.Identity.SHA256[63:])
		f.plantState(key, BuildState{
			Schema: buildStateSchema, Build: discovery.Identity.Build, SHA256: flipped,
			Size: int64(len(raw)), Transaction: "txn-test", ProbeContract: ProbeContractID, State: StateCopied,
		})
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeStateInconsistent, StageDedupe)
		afterInfo, err := os.Stat(binary)
		if err != nil {
			t.Fatal(err)
		}
		if digestOfFile(t, binary) != discovery.Identity.SHA256 || !afterInfo.ModTime().Equal(beforeInfo.ModTime()) {
			t.Fatal("collision touched the frozen bytes")
		}
	})

	t.Run("mismatching bytes at the key refuse", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		binary := f.plantFrozenBytes(t, key, discovery.Identity.Build, []byte("foreign bytes"))
		before, err := os.ReadFile(binary)
		if err != nil {
			t.Fatal(err)
		}
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFrozenCollision, StageDedupe)
		after, err := os.ReadFile(binary)
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != string(before) {
			t.Fatal("collision overwrote the key content")
		}
	})

	t.Run("exact agreement resumes", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
		f.plantState(key, BuildState{
			Schema: buildStateSchema, Build: discovery.Identity.Build, SHA256: discovery.Identity.SHA256,
			Size: int64(len(raw)), Transaction: "txn-test", ProbeContract: ProbeContractID, State: StateCopied,
		})
		result, err := PublishFrozen(context.Background(), f.config(), discovery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomeResumed {
			t.Fatalf("outcome = %s, want resumed", result.Outcome)
		}
	})
}

func flipHex(digit string) string {
	if digit == "a" {
		return "b"
	}
	return "a"
}

// TestCrashBetweenPublishAndProbeResumes proves a completed copy left
// by a crash is adopted as COPIED after verification — without
// copying or billing again — while partial bytes and missing
// transactions refuse without recopying.
func TestCrashBetweenPublishAndProbeResumes(t *testing.T) {
	plantCrash := func(t *testing.T, f *freezeFixture, day string) (Discovery, string) {
		t.Helper()
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		binary := f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
		// The crash struck before the protection step.
		if err := os.Chmod(binary, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(filepath.Dir(binary), 0o700); err != nil {
			t.Fatal(err)
		}
		f.plantReservation(day, discovery.Identity.Build, discovery.Identity.SHA256, outcomePending)
		return discovery, key
	}

	t.Run("same-day crash adopts without copying", func(t *testing.T) {
		f := newFreeze(t)
		discovery, key := plantCrash(t, f, "2026-10-10")
		copies := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeBeforeCopy {
				copies++
			}
		})
		result, err := PublishFrozen(context.Background(), f.config(), discovery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomeAdopted || result.Runnable() {
			t.Fatalf("outcome = %s runnable=%v, want adopted probe-pending", result.Outcome, result.Runnable())
		}
		if copies != 0 {
			t.Fatalf("adoption copied %d times", copies)
		}
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times on the adoption path", acquires)
		}
		binary, err := os.Stat(result.Frozen.Path)
		if err != nil {
			t.Fatal(err)
		}
		if binary.Mode().Perm() != 0o555 {
			t.Fatalf("adopted mode = %s, want r-xr-xr-x", binary.Mode())
		}
		state, present, err := readBuildState(context.Background(), statePath(f.root, key))
		if err != nil || !present || state.State != StateCopied {
			t.Fatalf("adopted state present=%v err=%v %+v", present, err, state)
		}
		entries, err := os.ReadDir(f.ledger)
		if err != nil || len(entries) != 1 {
			t.Fatalf("ledger = %v err=%v, want the single adopted reservation", entries, err)
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Outcome != outcomePublished {
			t.Fatalf("adopted outcome = %s, want published without a second billing", reservation.Outcome)
		}
	})

	t.Run("previous-day crash adopts", func(t *testing.T) {
		f := newFreeze(t)
		discovery, _ := plantCrash(t, f, "2026-10-09")
		result, err := PublishFrozen(context.Background(), f.config(), discovery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomeAdopted {
			t.Fatalf("outcome = %s, want adopted", result.Outcome)
		}
	})

	t.Run("partial bytes refuse without recopying", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		f.plantFrozenBytes(t, key, discovery.Identity.Build, []byte("partial"))
		f.plantReservation("2026-10-10", discovery.Identity.Build, discovery.Identity.SHA256, outcomePending)
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFrozenCollision, StageDedupe)
		if entries, err := os.ReadDir(stagingDir(f.root)); err != nil || len(entries) != 0 {
			t.Fatalf("staging = %v err=%v, want no recopy", entries, err)
		}
	})

	t.Run("failed outcome with complete bytes adopts", func(t *testing.T) {
		f := newFreeze(t)
		discovery, _ := plantCrash(t, f, "2026-10-10")
		entries, err := os.ReadDir(f.ledger)
		if err != nil || len(entries) != 1 {
			t.Fatal("planted reservation missing")
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := recordReservationOutcome(f.config(), reservation, outcomeFailed, "state write failed"); err != nil {
			t.Fatal(err)
		}
		result, err := PublishFrozen(context.Background(), f.config(), discovery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomeAdopted {
			t.Fatalf("outcome = %s, want adopted", result.Outcome)
		}
	})

	t.Run("foreign host refuses adoption", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		f.plantFrozenBytes(t, key, discovery.Identity.Build, raw)
		f.plantReservationOnHost("2026-10-10", discovery.Identity.Build, discovery.Identity.SHA256, outcomePending, "foreign-host", "test-user")
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageDedupe)
	})

	t.Run("contended lease refuses adoption", func(t *testing.T) {
		f := newFreeze(t)
		discovery, _ := plantCrash(t, f, "2026-10-10")
		holder, err := os.OpenFile(filepath.Join(f.root, "maintenance.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Close() }()
		if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = syscall.Flock(int(holder.Fd()), syscall.LOCK_UN) }()
		type result struct {
			copy FrozenCopy
			err  error
		}
		done := make(chan result, 1)
		go func() {
			got, err := PublishFrozen(context.Background(), f.config(), discovery)
			done <- result{copy: got, err: err}
		}()
		select {
		case r := <-done:
			requireFreezeCode(t, r.err, CodeFreezeLeaseFailed, StagePublish)
		case <-time.After(60 * time.Second):
			t.Fatal("contended adoption lease never refused")
		}
	})
}

// TestSourceSwapDuringFreezeRefusesWithoutFallback proves the freeze
// binds its copy to the discovery digest: swapped bytes refuse at
// the stream gate with no older build, no deferral and no second
// attempt, while swapped gates refuse before admission runs.
func TestSourceSwapDuringFreezeRefusesWithoutFallback(t *testing.T) {
	swapBytes := func(t *testing.T, path string, sameSize bool) {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if sameSize {
			raw[len(raw)-1] ^= 0xff
		} else {
			raw = append(raw, '\n')
		}
		tmp, err := os.CreateTemp(filepath.Dir(path), ".swap-*")
		if err != nil {
			t.Fatal(err)
		}
		tmpName := tmp.Name()
		if _, err := tmp.Write(raw); err != nil {
			_ = tmp.Close()
			t.Fatal(err)
		}
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			t.Fatal(err)
		}
		if err := tmp.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(tmpName, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmpName, path); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("same-size swap refuses at the stream gate", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		swapBytes(t, discovery.Identity.Path, true)
		streams := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeAfterCopyStream {
				streams++
			}
		})
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeSourceUnusable, StageCopy)
		if streams != 0 {
			t.Fatalf("swapped stream passed the digest gate %d times", streams)
		}
		entries, err := os.ReadDir(f.ledger)
		if err != nil || len(entries) != 1 {
			t.Fatalf("ledger = %v err=%v, want the failed reservation", entries, err)
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Outcome != outcomeFailed {
			t.Fatalf("outcome = %s, want failed", reservation.Outcome)
		}
		if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build)); !os.IsNotExist(err) {
			t.Fatalf("swapped copy published: %v", err)
		}
	})

	t.Run("resized swap refuses", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		swapBytes(t, discovery.Identity.Path, false)
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeSourceUnusable, StageCopy)
	})

	t.Run("swapped symlink refuses before admission", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		other := discovery.Identity.Path + ".real"
		if err := os.Rename(discovery.Identity.Path, other); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(other, discovery.Identity.Path); err != nil {
			t.Fatal(err)
		}
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeSourceUnusable, StageCopy)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times past the gate swap", acquires)
		}
	})

	t.Run("de-executed source refuses before admission", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		if err := os.Chmod(discovery.Identity.Path, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeSourceUnusable, StageCopy)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times past the gate swap", acquires)
		}
	})
}

// TestFreezeQuarantinedSourceRefuses proves a quarantined source
// refuses before any reservation or copy: nothing is billed and the
// global lock is never taken.
func TestFreezeQuarantinedSourceRefuses(t *testing.T) {
	t.Run("quarantined refuses", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		cfg := f.config()
		cfg.Quarantine = func(path string) (bool, error) { return true, nil }
		_, err := PublishFrozen(context.Background(), cfg, discovery)
		requireFreezeCode(t, err, CodeSourceQuarantined, StageCopy)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times past quarantine", acquires)
		}
		if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 0 {
			t.Fatalf("ledger = %v err=%v, want no reservation", entries, err)
		}
	})

	t.Run("clear proceeds", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		cfg := f.config()
		cfg.Quarantine = func(path string) (bool, error) { return false, nil }
		result, err := PublishFrozen(context.Background(), cfg, discovery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomePublished {
			t.Fatalf("outcome = %s, want published", result.Outcome)
		}
	})

	t.Run("quarantine lookup failure refuses closed", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		cfg := f.config()
		cfg.Quarantine = func(path string) (bool, error) { return false, errors.New("xattr unreadable") }
		_, err := PublishFrozen(context.Background(), cfg, discovery)
		requireFreezeCode(t, err, CodeSourceUnusable, StageCopy)
	})
}
