package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// A deliberately isolated authority/provider model. It receives only the call
// bytes and its separately supplied Binding, never an engine/store/goal handle.
// Its synthetic counters/proof registry do not establish actual native start.
type managerTimerProviderFixture struct {
	t                                                                         *testing.T
	owner                                                                     managerTimerOwnerObservation
	callRecord, claim, native                                                 map[string]any
	issued                                                                    map[string][]byte
	issuedUseful                                                              map[string][]byte
	counts                                                                    map[string]int
	snapshots, claims, items, turns, revision                                 int
	disposition, commitDisposition, drop, goalAfterSnapshot, goalAfterReserve string
	missingMapping, invalidated                                               bool
	seen                                                                      map[string]bool
	resultReason                                                              string
	invalidatedIDs                                                            map[string]bool
	pendingInvalidation                                                       bool
	resultMutation                                                            func(map[string]any)
}

func timerFixtureClone(o map[string]any) map[string]any {
	b, _ := json.Marshal(o)
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var copy map[string]any
	_ = d.Decode(&copy)
	return copy
}
func timerFixtureBytes(o map[string]any) []byte { b, _ := json.Marshal(o); return append(b, '\n') }
func timerFixtureHash(b []byte) string          { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }

func managerTimerEngineFixture(t *testing.T) (*managerTimerEngine, *managerTimerProviderFixture) {
	t.Helper()
	s, _, _ := managerTimerStoreFixture(t, nil)
	g, ledger, owner := managerTimerGateFixture(t)
	ledger["opportunity_high_water"] = json.Number("0")
	ledger["active"] = nil
	ledger["terminal"] = []any{}
	owner.clock["monotonic_ns"] = ledger["next_due_ns"]
	owner.clock["observed_at"] = "2026-11-17T17:30:00Z"
	if err := saveManagerTimerFixture(s, ledger); err != nil {
		t.Fatal(err)
	}
	f := &managerTimerProviderFixture{t: t, owner: owner, native: managerTimerEmptyNative(), issued: map[string][]byte{}, issuedUseful: map[string][]byte{}, counts: map[string]int{}, seen: map[string]bool{}, commitDisposition: "completed_ok"}
	// The harness observes declared durable state independently of the provider;
	// the provider below still receives no store or engine handle.
	storedPhase := func(want string) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(s.witness.directory, "ledger.json"))
		if err != nil {
			t.Fatal(err)
		}
		var journal map[string]any
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		if err := d.Decode(&journal); err != nil {
			t.Fatal(err)
		}
		if timerObject(journal, "active")["phase"] != want || timerNumber(journal, "opportunity_high_water") == 0 {
			t.Fatalf("effect before durable %s: %v", want, journal["active"])
		}
	}
	f.invalidatedIDs = map[string]bool{}
	ports := managerTimerPorts{
		observe: func(_ context.Context, attempt string) (managerTimerOwnerObservation, *managerTimerClaimObservation, error) {
			o := f.owner
			if f.pendingInvalidation && attempt != "" {
				f.invalidatedIDs[attempt] = true
				f.pendingInvalidation = false
			}
			if f.invalidated && attempt == timerString(f.callRecord, "attempt_id") && attempt != "" {
				f.invalidatedIDs[attempt] = true
			}
			if f.invalidatedIDs[attempt] {
				o.invalidatedAttempt = attempt
			}
			o.binding = timerFixtureClone(o.binding)
			o.clock = timerFixtureClone(o.clock)
			p := *o.primary
			o.primary = &p
			var current *managerTimerClaimObservation
			if f.claim != nil {
				c := f.callRecord
				reg := timerObject(c, "registration")
				current = &managerTimerClaimObservation{attemptID: timerString(c, "attempt_id"), bindingDigest: timerString(c, "binding_digest"), registrationID: timerString(reg, "registration_id"), requestDigest: timerString(c, "request_digest"), submissionDigest: timerDigest("Submission", c["submission"]), revision: timerNumber(reg, "revision"), claim: timerFixtureClone(f.claim), permanentlyInvalidated: f.invalidated}
			}
			return o, current, nil
		},
		snapshot: func(context.Context, string) (string, error) {
			storedPhase("preparing")
			f.snapshots++
			if f.goalAfterSnapshot != "" {
				f.owner.primary.state = f.goalAfterSnapshot
				f.pendingInvalidation = true
				if f.goalAfterSnapshot == "clear-restored" || f.goalAfterSnapshot == "loss-restored" {
					f.owner.primary.state = "active"
				}
			}
			return strings.Repeat("6", 64), nil
		},
		provider: func(ctx context.Context, frame []byte, binding map[string]any) ([]byte, error) {
			var call map[string]any
			if err := json.Unmarshal(frame, &call); err != nil {
				t.Fatal(err)
			}
			switch call["op"] {
			case "reserve":
				storedPhase("reserve_unknown")
			case "commit":
				storedPhase("commit_unknown")
			case "release":
				storedPhase("release_unknown")
			}
			return f.provider(ctx, frame, binding)
		},
		verifyProvider: func(result map[string]any) bool {
			proof := timerObject(result, "proof")
			return proof != nil && bytes.Equal(f.issued[timerString(proof, "evidence_digest")], timerFixtureBytes(result))
		},
		verifyUseful: func(useful map[string]any) bool {
			return bytes.Equal(f.issuedUseful[timerDigest("Useful", useful)], timerFixtureBytes(useful))
		},
	}
	e, err := newManagerTimerEngine(s, g, ports)
	if err != nil {
		t.Fatal(err)
	}
	return e, f
}

func (f *managerTimerProviderFixture) inputReason() string {
	if !f.owner.securityOpen || !f.owner.admissionOpen || !f.owner.fenceOpen {
		return "security_refused"
	}
	if !f.owner.budgetOpen {
		return "budget_refused"
	}
	if f.owner.primary == nil || !f.owner.primary.current || f.owner.primary.state == "unknown" {
		return "capability_missing"
	}
	if f.owner.primary.state != "active" || f.owner.input != InputIdle || f.owner.hostTurn || f.owner.nativeTurn || f.owner.autonomousTurn || f.owner.unknownProducer || f.invalidated {
		return "input_ineligible"
	}
	return "none"
}

func (f *managerTimerProviderFixture) provider(_ context.Context, frame []byte, binding map[string]any) ([]byte, error) {
	f.t.Helper()
	d := json.NewDecoder(bytes.NewReader(frame))
	d.UseNumber()
	var call map[string]any
	if err := d.Decode(&call); err != nil {
		f.t.Fatal(err)
	}
	if len(call) != 11 || !bytes.Equal(timerFixtureBytes(timerObject(timerObject(call, "registration"), "binding")), timerFixtureBytes(binding)) || !bytes.Equal(timerFixtureBytes(binding), timerFixtureBytes(managerTimerFixtureRecord(f.t, "Binding-valid"))) {
		f.t.Fatal("isolated provider received wrong declared call")
	}
	if call["request_digest"] != "caf236cd74fde456e5e1107337ce48e427221dbb7190dcc05b44c2e06c40b724" || call["binding_digest"] != "7c723d49981dbecb22bd9656b449453d97ba7bed445b861682ce049d520abee2" {
		f.t.Fatal("provider intent differs from independently frozen digest")
	}
	tickID := timerString(timerObject(call, "tick"), "tick_id")
	if call["attempt_id"] != timerFixtureHash([]byte("host-manager-timer/1:attempt\n"+tickID)) {
		f.t.Fatal("wrong independent attempt digest")
	}
	opBytes, _ := json.Marshal(map[string]any{"attempt_id": call["attempt_id"], "op": call["op"]})
	if call["request_id"] != timerFixtureHash(append([]byte("host-manager-timer/1:ProviderOperation\n"), opBytes...)) {
		f.t.Fatal("wrong independent operation digest")
	}
	for _, v := range timerObject(call, "native") {
		if v != nil {
			f.t.Fatal("call supplied a native handle")
		}
	}
	op := timerString(call, "op")
	f.counts[op]++
	if op == "reserve" || op == "commit" {
		// Independent frozen invocation, not one regenerated by the Host renderer.
		want := managerTimerFixtureRecord(f.t, "NativeQueueInvocation-valid")
		if !bytes.Equal(timerFixtureBytes(timerObject(timerObject(call, "submission"), "frame")), timerFixtureBytes(want)) {
			f.t.Fatal("wrong exact invocation")
		}
	} else if call["submission"] != nil {
		f.t.Fatal("cleanup received a payload")
	}
	reason := "none"
	switch op {
	case "reserve":
		id := timerString(call, "attempt_id")
		if f.seen[id] {
			f.t.Fatal("duplicate Reserve reached isolated provider")
		}
		f.seen[id] = true
		f.native = managerTimerEmptyNative()
		f.claim = nil
		f.resultReason = ""
		f.invalidated = f.invalidatedIDs[id]
		f.callRecord = timerFixtureClone(call)
		if reason = f.inputReason(); reason != "none" {
			f.disposition = "absent"
			break
		}
		f.claims++
		claimID := "fixture-claim"
		if f.claims > 1 {
			claimID += "-" + strconv.Itoa(f.claims)
		}
		f.native = map[string]any{"claim_id": claimID, "item_id": nil, "turn_id": nil}
		created := timerFixtureClone(f.owner.clock)
		f.claim = map[string]any{"claim_id": claimID, "created_clock": created, "expires_at": timerTime(created, "observed_at").Add(30 * time.Second).Format("2006-01-02T15:04:05Z"), "expires_monotonic_ns": managerTimerNumber(timerNumber(created, "monotonic_ns") + int64(30*time.Second))}
		f.disposition = "reserved"
		if f.goalAfterReserve != "" {
			f.owner.primary.state = f.goalAfterReserve
			f.invalidated = true
		}
	case "commit":
		if f.callRecord == nil || !timerEqual(call["submission"], f.callRecord["submission"]) || call["request_digest"] != f.callRecord["request_digest"] {
			f.t.Fatal("Commit changed immutable intent")
		}
		if f.claim == nil {
			f.t.Fatal("Commit without claim")
		}
		if reason = f.inputReason(); reason != "none" {
			f.disposition = "suppressed"
			break
		}
		if !timerTime(f.owner.clock, "observed_at").Before(timerTime(f.claim, "expires_at")) || timerNumber(f.owner.clock, "monotonic_ns") >= timerNumber(f.claim, "expires_monotonic_ns") {
			reason = "expired"
			f.disposition = "suppressed"
			break
		}
		f.items++
		f.native["item_id"] = "fixture-item"
		f.disposition = f.commitDisposition
		if f.disposition != "queued" {
			f.turns++
			f.native["turn_id"] = "fixture-turn"
		}
	case "query":
		if f.missingMapping || f.callRecord == nil {
			return nil, errors.New("fixture_mapping_unknown")
		}
		if f.resultReason != "" {
			reason = f.resultReason
		}
	case "release":
		if f.native["turn_id"] == nil {
			f.invalidated = true
			if f.native["item_id"] == nil {
				f.disposition = "absent"
			} else {
				f.disposition = "suppressed"
			}
		}
	default:
		f.t.Fatal("unknown provider operation")
	}
	f.revision++
	var enqueue, start any
	if f.native["item_id"] != nil {
		enqueue = true
	}
	if f.native["turn_id"] != nil {
		start = true
	}
	evidence := []byte("fixture provider issuance " + strconv.Itoa(f.revision) + " " + op + " " + timerString(call, "attempt_id") + " " + f.disposition)
	digest := timerFixtureHash(evidence)
	proof := map[string]any{"issuer_binding_digest": call["binding_digest"], "source_revision": managerTimerNumber(int64(f.revision)), "evidence_ref": "/fixture/evidence/provider-" + strconv.Itoa(f.revision) + ".json", "evidence_digest": digest}
	result := map[string]any{"protocol": managerTimerProtocol, "op": op, "request_id": call["request_id"], "attempt_id": call["attempt_id"], "binding_digest": call["binding_digest"], "request_digest": call["request_digest"], "disposition": f.disposition, "native": timerFixtureClone(f.native), "claim": f.claim, "observed_clock": timerFixtureClone(f.owner.clock), "reason": reason, "eligible_at_enqueue": enqueue, "eligible_at_start": start, "proof": proof}
	if f.claim != nil {
		result["claim"] = timerFixtureClone(f.claim)
	}
	if f.resultMutation != nil {
		f.resultMutation(result)
	}
	out := timerFixtureBytes(result)
	f.issued[digest] = append([]byte{}, out...)
	if f.drop == op {
		f.drop = ""
		return nil, errors.New("fixture_lost_reply")
	}
	return out, nil
}

// An admitted source can contradict its own proof. Those well-shaped reports
// retain the last proven attempt and durably fence admission, including reopen
// and later valid Query. Invalid/unverified reports are separate tests below;
// the injected provider here still receives only declared calls and Binding.
func TestManagerTimerAuthoritativeContradictionFence(t *testing.T) {
	for _, which := range []string{"same_revision", "regressing_revision", "native_replacement", "changed_claim", "terminal_regression", "same_revision_at_capacity"} {
		t.Run(which, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			ctx := context.Background()
			if err := e.poll(ctx); err != nil {
				t.Fatal(err)
			}
			if err := e.recover(ctx); err != nil {
				t.Fatalf("valid newer observation=%v", err)
			}
			if which == "same_revision_at_capacity" {
				next, err := cloneManagerTimerObject(e.ledger)
				if err != nil {
					t.Fatal(err)
				}
				next["phase_revision"] = json.Number("9007199254740991")
				if err := e.persist(next); err != nil {
					t.Fatal(err)
				}
			}
			before := timerFixtureClone(timerObject(e.ledger, "active"))
			phase := timerNumber(e.ledger, "phase_revision")
			previousRevision := int64(f.revision) // The issuer's own counter, not Host state.
			f.resultMutation = func(result map[string]any) {
				switch which {
				case "same_revision", "same_revision_at_capacity":
					timerObject(result, "proof")["source_revision"] = managerTimerNumber(previousRevision)
				case "regressing_revision":
					timerObject(result, "proof")["source_revision"] = managerTimerNumber(previousRevision - 1)
				case "native_replacement":
					timerObject(result, "native")["item_id"] = "fixture-contradictory-item"
				case "changed_claim":
					claim := timerObject(result, "claim")
					claim["expires_monotonic_ns"] = managerTimerNumber(timerNumber(claim, "expires_monotonic_ns") - 1)
				case "terminal_regression":
					result["disposition"] = "completed_failed"
				}
				if err := validateManagerTimerRecord("ProviderResult", result); err != nil {
					t.Fatalf("contradiction plant failed shape precondition: %v", err)
				}
			}
			want := managerTimerQueueUnknown
			if which == "same_revision" || which == "same_revision_at_capacity" {
				want = managerTimerRequestConflict
			}
			if err := e.recover(ctx); err != want {
				t.Fatalf("contradictory authority=%v want %v", err, want)
			}
			if e.ledger["registration_state"] != "fenced" || !timerEqual(before, e.ledger["active"]) {
				t.Fatal("contradiction changed proven intent or lost the fence")
			}
			wantPhase := phase + 1
			if which == "same_revision_at_capacity" {
				wantPhase = phase
			}
			if timerNumber(e.ledger, "phase_revision") != wantPhase {
				t.Fatal("fence did not retain its bounded phase revision")
			}
			e = reopenManagerTimerFixture(t, e)
			if e.ledger["registration_state"] != "fenced" || !timerEqual(before, e.ledger["active"]) {
				t.Fatal("reopen lost the durable fence or retained proof")
			}
			f.resultMutation = nil
			if err := e.recover(ctx); err != nil {
				t.Fatalf("valid reconciliation=%v", err)
			}
			if e.ledger["registration_state"] != "fenced" || f.snapshots != 1 || f.claims != 1 || f.items != 1 || f.turns != 1 || f.counts["reserve"] != 1 || f.counts["commit"] != 1 {
				t.Fatal("reconciliation cleared the fence or repeated an effect")
			}
		})
	}
}

// Exact repeated Query evidence is permitted at an unchanged source revision;
// newer noncontradictory evidence also progresses. Neither creates an effect.
func TestManagerTimerExactObservationalProofReplay(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	ctx := context.Background()
	if err := e.poll(ctx); err != nil {
		t.Fatal(err)
	}
	original := e.ports.provider
	var retained []byte
	e.ports.provider = func(ctx context.Context, frame []byte, binding map[string]any) ([]byte, error) {
		if retained != nil {
			return append([]byte{}, retained...), nil
		}
		result, err := original(ctx, frame, binding)
		if err == nil {
			retained = append([]byte{}, result...)
		}
		return result, err
	}
	for i := 0; i < 2; i++ {
		if err := e.recover(ctx); err != nil {
			t.Fatalf("exact observational replay %d=%v", i, err)
		}
	}
	e.ports.provider = original
	if err := e.recover(ctx); err != nil {
		t.Fatalf("newer observation=%v", err)
	}
	if e.ledger["registration_state"] == "fenced" || f.snapshots != 1 || f.turns != 1 || f.counts["reserve"] != 1 || f.counts["commit"] != 1 {
		t.Fatal("valid repeated/newer observations fenced or duplicated an effect")
	}
}

func (f *managerTimerProviderFixture) useful() map[string]any {
	c := f.callRecord
	check := map[string]any{"attempt_id": c["attempt_id"], "binding_digest": c["binding_digest"], "goal_digest": strings.Repeat("6", 64), "turn_id": f.native["turn_id"], "checked_at": f.owner.clock["observed_at"], "goal_digest_verified": true, "own_clocks_checked": true, "outcome": "no_due", "result_ref": "/fixture/result/checked.json", "result_digest": timerFixtureHash([]byte("fixture checked no due"))}
	b, _ := canonicalManagerTimerValue(check)
	digest := timerFixtureHash(append(b, '\n'))
	u := map[string]any{"attempt_id": c["attempt_id"], "binding_digest": c["binding_digest"], "native": timerFixtureClone(f.native), "goal_digest": strings.Repeat("6", 64), "result_kind": "checked_no_action", "check": check, "proof": map[string]any{"issuer_binding_digest": c["binding_digest"], "source_revision": managerTimerNumber(int64(f.revision + 1)), "evidence_ref": "/fixture/result/check-evidence.json", "evidence_digest": digest}}
	f.issuedUseful[timerDigest("Useful", u)] = timerFixtureBytes(u)
	return u
}

// This models a provider-owned conditional consumption point. It exercises an
// independent fictional arbiter only; it is not an actual app-server guarantee.
func (f *managerTimerProviderFixture) startQueued() {
	if f.disposition != "queued" || f.native["turn_id"] != nil {
		return
	}
	reason := f.inputReason()
	if reason == "none" && (f.claim == nil || !timerTime(f.owner.clock, "observed_at").Before(timerTime(f.claim, "expires_at")) || timerNumber(f.owner.clock, "monotonic_ns") >= timerNumber(f.claim, "expires_monotonic_ns")) {
		reason = "expired"
	}
	if reason != "none" {
		f.invalidated = true
		f.disposition = "suppressed"
		f.resultReason = reason
		return
	}
	f.turns++
	f.native["turn_id"] = "fixture-turn"
	f.disposition = "completed_ok"
}

func managerTimerAckPayload(e *managerTimerEngine) map[string]any {
	a := timerObject(e.ledger, "active")
	return map[string]any{"attempt_id": a["attempt_id"], "useful_digest": timerDigest("Useful", a["useful"]), "completion_digest": timerDigest("ProviderResult", a["provider_evidence"])}
}

// Release must disable future admission before cleanup, including a lost reply.
// Running/success can still receive their exact Useful/completion/ACK; failed
// effects retain their slot. These are private model ports, not a native arbiter.
func TestManagerTimerReleaseRetainsDraining(t *testing.T) {
	for _, disposition := range []string{"running", "completed_ok", "completed_failed", "completed_interrupted"} {
		for _, lost := range []bool{false, true} {
			t.Run(disposition+"/lost="+strconv.FormatBool(lost), func(t *testing.T) {
				e, f := managerTimerEngineFixture(t)
				ctx := context.Background()
				f.commitDisposition = disposition
				if err := e.poll(ctx); err != nil {
					t.Fatal(err)
				}
				provider := e.ports.provider
				e.ports.provider = func(ctx context.Context, frame []byte, binding map[string]any) ([]byte, error) {
					call, err := decodeManagerTimerObject(frame)
					if err != nil {
						t.Fatal(err)
					}
					if call["op"] == "release" {
						e.store.mu.Lock()
						stored, err := e.store.loadLocked()
						e.store.mu.Unlock()
						if err != nil {
							t.Fatal(err)
						}
						if stored["registration_state"] != "draining" {
							t.Error("Release reached provider without durable draining")
						}
					}
					return provider(ctx, frame, binding)
				}
				if lost {
					f.drop = "release"
				}
				want := error(nil)
				if lost {
					want = managerTimerQueueUnknown
				}
				if err := e.release(ctx); err != want {
					t.Fatalf("Release=%v, want %v", err, want)
				}
				e = reopenManagerTimerFixture(t, e)
				if e.ledger["registration_state"] != "draining" {
					t.Error("Release draining did not survive reopen")
				}
				if err := e.recover(ctx); err != nil {
					t.Fatal(err)
				}
				if disposition == "running" {
					if err := e.recordUseful(f.useful()); err != nil {
						t.Fatal(err)
					}
					f.disposition = "completed_ok"
					if err := e.recover(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if disposition == "running" || disposition == "completed_ok" {
					if disposition == "completed_ok" {
						if err := e.recordUseful(f.useful()); err != nil {
							t.Fatal(err)
						}
					}
					if err := e.ack(ctx, managerTimerAckPayload(e)); err != nil {
						t.Fatal(err)
					}
					if e.ledger["active"] != nil {
						t.Fatal("proven successful cleanup failed to free its slot")
					}
				} else if err := e.recordUseful(f.useful()); err != managerTimerAckUnproven || e.ledger["active"] == nil {
					t.Fatalf("failed cleanup lost retained slot: %v", err)
				}
				e = reopenManagerTimerFixture(t, e)
				if e.ledger["registration_state"] != "draining" {
					t.Error("reconciliation or ACK reenabled registration")
				}
				f.owner.clock["monotonic_ns"] = e.ledger["next_due_ns"]
				want = managerTimerRegistrationStale
				if e.ledger["active"] != nil {
					want = managerTimerCoalesced
				}
				if err := e.poll(ctx); err != want || f.snapshots != 1 || f.counts["reserve"] != 1 || f.turns != 1 {
					t.Fatalf("cleanup admitted another tick: err=%v snapshots=%d calls=%v turns=%d", err, f.snapshots, f.counts, f.turns)
				}
			})
		}
	}
}

// A normal success without Release keeps the registration usable at its next
// genuine due time; the Release safety rule must not become a universal refusal.
func TestManagerTimerOrdinaryAckAllowsNextDue(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	ctx := context.Background()
	if err := e.poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.recordUseful(f.useful()); err != nil {
		t.Fatal(err)
	}
	if err := e.ack(ctx, managerTimerAckPayload(e)); err != nil {
		t.Fatal(err)
	}
	f.owner.clock["monotonic_ns"] = e.ledger["next_due_ns"]
	if err := e.poll(ctx); err != nil || f.snapshots != 2 || f.turns != 2 {
		t.Fatalf("valid next opportunity refused: %v, snapshots=%d turns=%d", err, f.snapshots, f.turns)
	}
}

// Both handles cache idle before the first effect. A channel pauses its reply
// after claim creation; the stale second handle's failing snapshot must never
// replace that unresolved attempt. Channel joins, not a timeout, prove the result.
func TestManagerTimerSharedStoreHandles(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	ports := e.ports
	secondSnapshots := 0
	ports.snapshot = func(context.Context, string) (string, error) {
		secondSnapshots++
		return "", errors.New("fixture_second_snapshot_failure")
	}
	second, err := newManagerTimerEngine(e.store, e.gate, ports)
	if err != nil {
		t.Fatal(err)
	}
	provider := e.ports.provider
	paused, releaseReply, secondStarted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	e.ports.provider = func(ctx context.Context, frame []byte, binding map[string]any) ([]byte, error) {
		_, err := provider(ctx, frame, binding)
		if err != nil {
			return nil, err
		}
		close(paused)
		<-releaseReply
		return nil, errors.New("fixture_lost_reserve_reply")
	}
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- e.poll(context.Background()) }()
	<-paused
	go func() {
		close(secondStarted)
		secondDone <- second.poll(context.Background())
	}()
	<-secondStarted
	close(releaseReply)
	if err := <-firstDone; err != managerTimerQueueUnknown {
		t.Fatalf("first Poll=%v", err)
	}
	if err := <-secondDone; err != managerTimerCoalesced {
		t.Errorf("stale second Poll=%v, want coalesced", err)
	}
	e = reopenManagerTimerFixture(t, e)
	if timerObject(e.ledger, "active")["phase"] != "reserve_unknown" || len(e.ledger["terminal"].([]any)) != 0 || secondSnapshots != 0 || f.snapshots != 1 || f.claims != 1 || f.counts["reserve"] != 1 {
		t.Fatalf("shared store lost uncertainty: phase=%v terminal=%v secondSnapshots=%d calls=%v", timerObject(e.ledger, "active")["phase"], e.ledger["terminal"], secondSnapshots, f.counts)
	}
}

// Equal high-waters do not establish freshness. A stale handle must not close
// a newer release_unknown attempt; an exact-current write remains permitted.
func TestManagerTimerFullLedgerCompareAndSwap(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	ctx := context.Background()
	f.drop = "reserve"
	if err := e.poll(ctx); err != managerTimerQueueUnknown {
		t.Fatal(err)
	}
	// Hold every counter constant during the later phase-only transition.
	next := timerFixtureClone(e.ledger)
	next["registration_state"] = "draining"
	if err := e.persist(next); err != nil {
		t.Fatal(err)
	}
	stale, err := newManagerTimerEngine(e.store, e.gate, e.ports)
	if err != nil {
		t.Fatal(err)
	}
	f.drop = "release"
	if err := e.recover(ctx); err != managerTimerQueueUnknown {
		t.Fatal(err)
	}
	before := timerFixtureClone(e.ledger)
	for _, counter := range []string{"revision_high_water", "opportunity_high_water", "phase_revision"} {
		if before[counter] != stale.ledger[counter] {
			t.Fatalf("CAS setup changed %s", counter)
		}
	}
	if err := e.persist(timerFixtureClone(e.ledger)); err != nil {
		t.Fatalf("current CAS refused: %v", err)
	}
	if err := stale.closeAttempt("no_effect", nil); err != managerTimerStateInvalid {
		t.Errorf("stale same-highwater close=%v", err)
	}
	e = reopenManagerTimerFixture(t, e)
	if !timerEqual(e.ledger, before) || timerObject(e.ledger, "active")["phase"] != "release_unknown" {
		t.Fatal("stale CAS erased the durable unresolved release")
	}
}

// Verified shape/proof alone cannot turn an impossible no-reason refusal into
// success. Reserve/Commit and an unrelated Query retain their uncertain slot.
func TestManagerTimerProviderReasonContext(t *testing.T) {
	for _, op := range []string{"reserve", "commit", "query"} {
		for _, disposition := range []string{"absent", "suppressed"} {
			if op == "reserve" && disposition == "suppressed" {
				continue // Reserve never allows suppressed, independently of reason.
			}
			t.Run(op+"/"+disposition, func(t *testing.T) {
				e, f := managerTimerEngineFixture(t)
				ctx := context.Background()
				if op == "query" {
					f.commitDisposition = "queued"
					if err := e.poll(ctx); err != nil {
						t.Fatal(err)
					}
				}
				f.resultMutation = func(result map[string]any) {
					if result["op"] != op {
						return
					}
					result["disposition"], result["reason"] = disposition, "none"
					timerObject(result, "native")["turn_id"] = nil
					result["eligible_at_start"] = nil
				}
				var err error
				if op == "query" {
					err = e.recover(ctx)
				} else {
					err = e.poll(ctx)
				}
				if err != managerTimerQueueUnknown {
					t.Errorf("invalid %s %s/none=%v", op, disposition, err)
				}
				e = reopenManagerTimerFixture(t, e)
				want := map[string]string{"reserve": "reserve_unknown", "commit": "commit_unknown", "query": "queued"}[op]
				if timerObject(e.ledger, "active")["phase"] != want || len(e.ledger["terminal"].([]any)) != 0 {
					t.Fatal("invalid provider reason freed the durable slot")
				}
			})
		}
	}
}

// The zero-effect reason table is grounded in the wire contract. Valid exact
// refusals and explicit cleanup remain accepted; none cannot conceal a refused
// Reserve/Commit, and provider_unavailable cannot carry a no-effect proof.
func TestManagerTimerProviderReasonCompatibility(t *testing.T) {
	for _, op := range []string{"reserve", "commit", "query", "release"} {
		for _, disposition := range []string{"absent", "suppressed"} {
			for _, reason := range []string{"none", "input_ineligible", "security_refused", "budget_refused", "expired", "binding_mismatch", "role_refused", "capability_missing", "cancelled", "provider_unavailable"} {
				t.Run(op+"/"+disposition+"/"+reason, func(t *testing.T) {
					result := managerTimerFixtureRecord(t, "ProviderResult-valid")
					result["op"], result["disposition"], result["reason"] = op, disposition, reason
					result["request_id"] = timerDigest("ProviderOperation", map[string]any{"attempt_id": result["attempt_id"], "op": op})
					result["eligible_at_enqueue"] = nil
					if op == "reserve" {
						timerObject(result, "native")["item_id"] = nil
					}
					allowed := !(op == "reserve" && disposition == "suppressed") && reason != "provider_unavailable" && (reason != "none" || op == "release" || op == "query")
					err := validateManagerTimerRecord("ProviderResult", result)
					if allowed && err != nil || !allowed && err != managerTimerInvalid {
						t.Fatalf("compatibility allowed=%v, got=%v", allowed, err)
					}
				})
			}
		}
	}
}

// A proven explicit Release or owner-cancel cleanup may report no-reason
// absence/suppression. A lost Release reply is reconciled by Query after reopen.
func TestManagerTimerProviderCleanupReasonPositive(t *testing.T) {
	for _, disposition := range []string{"absent", "suppressed"} {
		for _, cancelled := range []bool{false, true} {
			t.Run(disposition+"/cancelled="+strconv.FormatBool(cancelled), func(t *testing.T) {
				e, f := managerTimerEngineFixture(t)
				ctx := context.Background()
				f.commitDisposition = "queued"
				if err := e.poll(ctx); err != nil {
					t.Fatal(err)
				}
				if cancelled {
					next := timerFixtureClone(e.ledger)
					next["registration_state"] = "cancelled"
					if err := e.persist(next); err != nil {
						t.Fatal(err)
					}
				}
				f.resultMutation = func(result map[string]any) { result["disposition"] = disposition }
				f.drop = "release"
				if err := e.release(ctx); err != managerTimerQueueUnknown {
					t.Fatalf("lost cleanup=%v", err)
				}
				e = reopenManagerTimerFixture(t, e)
				if err := e.recover(ctx); err != nil || e.ledger["active"] != nil || f.turns != 0 || f.counts["release"] != 1 || f.counts["reserve"] != 1 {
					t.Fatalf("valid cleanup refused or replayed: %v calls=%v", err, f.counts)
				}
			})
		}
	}
}

// The positive starts with zero claims, observes durable-before-effect through
// real fixture storage, and ACKs only both proofs. No actual provider is tested.
func TestManagerTimerEnginePositiveAndReplay(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	ctx := context.Background()
	if err := e.poll(ctx); err != nil {
		t.Fatal(err)
	}
	if f.snapshots != 1 || f.claims != 1 || f.items != 1 || f.turns != 1 || timerObject(e.ledger, "active")["phase"] != "completed_unacked" {
		t.Fatalf("trace snapshots=%d claims=%d items=%d turns=%d phase=%v", f.snapshots, f.claims, f.items, f.turns, timerObject(e.ledger, "active")["phase"])
	}
	if err := e.ack(ctx, map[string]any{"attempt_id": timerObject(e.ledger, "active")["attempt_id"], "useful_digest": strings.Repeat("1", 64), "completion_digest": strings.Repeat("2", 64)}); err != managerTimerAckUnproven {
		t.Fatalf("unproven ACK=%v", err)
	}
	tick := timerFixtureClone(timerObject(timerObject(e.ledger, "active"), "tick"))
	if err := e.recordUseful(f.useful()); err != nil {
		t.Fatal(err)
	}
	payload := managerTimerAckPayload(e)
	if err := e.ack(ctx, payload); err != nil {
		t.Fatal(err)
	}
	if err := e.ack(ctx, payload); err != nil {
		t.Fatalf("repeated ACK=%v", err)
	}
	if e.ledger["active"] != nil || timerNumber(e.ledger, "opportunity_high_water") != 1 {
		t.Fatal("ACK lost high-water or retained slot")
	}
	if err := e.continueTick(tick); err != managerTimerTickStale {
		t.Fatalf("old continuation=%v", err)
	}
	if err := e.poll(ctx); err != managerTimerNotDue {
		t.Fatalf("immediate replay=%v", err)
	}
	if f.snapshots != 1 || f.turns != 1 {
		t.Fatal("duplicate effect")
	}
}

// Absent/cleared/unknown primary and independent busy paths consume the due
// ordinal but have zero snapshot/claim/item/turn; no later idle flush exists.
func TestManagerTimerEngineSkippedGoalAndBusy(t *testing.T) {
	for _, which := range []string{"absent", "cleared", "expired", "unavailable", "busy"} {
		t.Run(which, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			want := error(managerTimerInputIneligible)
			if which == "busy" {
				f.owner.input = InputBusy
			} else if which == "unavailable" {
				f.owner.primary.current = false
				want = managerTimerCapabilityMissing
			} else {
				f.owner.primary.state = which
			}
			if err := e.poll(context.Background()); err != want {
				t.Fatalf("skip=%v", err)
			}
			if f.snapshots != 0 || f.claims != 0 || f.items != 0 || f.turns != 0 || e.ledger["active"] != nil || timerNumber(e.ledger, "opportunity_high_water") != 1 {
				t.Fatal("skipped opportunity had effects or lost HWM")
			}
			f.owner.primary.current = true
			f.owner.primary.state = "active"
			f.owner.input = InputIdle
			if err := e.poll(context.Background()); err != managerTimerNotDue {
				t.Fatalf("idle flush=%v", err)
			}
		})
	}
}

// A lost effect reply occupies one durable slot. Recovery Query never repeats
// Reserve/Commit; recovered reservations are released, completions can ACK.
func TestManagerTimerEngineLostReplies(t *testing.T) {
	for _, op := range []string{"reserve", "commit"} {
		t.Run(op, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			f.drop = op
			if err := e.poll(context.Background()); err != managerTimerQueueUnknown {
				t.Fatalf("lost reply=%v", err)
			}
			if e.ledger["active"] == nil {
				t.Fatal("unknown freed slot")
			}
			if err := e.poll(context.Background()); err != managerTimerCoalesced {
				t.Fatalf("pending=%v", err)
			}
			f.missingMapping = true
			if err := e.recover(context.Background()); err != managerTimerQueueUnknown {
				t.Fatalf("missing mapping=%v", err)
			}
			f.missingMapping = false
			if err := e.recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.counts["reserve"] != 1 || f.counts["commit"] > 1 || f.snapshots != 1 {
				t.Fatalf("effect retried: %v snapshots=%d", f.counts, f.snapshots)
			}
			if op == "reserve" {
				if e.ledger["active"] != nil || f.counts["release"] != 1 || f.turns != 0 {
					t.Fatal("reservation recovery committed or orphaned claim")
				}
			} else {
				if err := e.recordUseful(f.useful()); err != nil {
					t.Fatal(err)
				}
				if err := e.ack(context.Background(), managerTimerAckPayload(e)); err != nil {
					t.Fatal(err)
				}
				if f.turns != 1 {
					t.Fatal("duplicate completion turn")
				}
			}
		})
	}
}

// Clear after the snapshot causes no Reserve; clear after Reserve causes only
// proven cleanup. Failed/interrupted native effects stay retained despite Useful.
func TestManagerTimerEngineClearAndFailedCompletion(t *testing.T) {
	for _, where := range []string{"snapshot", "reserve", "clear-restored", "loss-restored"} {
		t.Run("clear-"+where, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			if where == "snapshot" {
				f.goalAfterSnapshot = "cleared"
			} else if where == "clear-restored" || where == "loss-restored" {
				f.goalAfterSnapshot = where
			} else {
				f.goalAfterReserve = "cleared"
			}
			if err := e.poll(context.Background()); err != managerTimerInputIneligible {
				t.Fatalf("clear=%v", err)
			}
			if f.items != 0 || f.turns != 0 || e.ledger["active"] != nil {
				t.Fatal("clear queued work or lost cleanup")
			}
			if where != "reserve" && f.counts["reserve"] != 0 || where == "reserve" && f.counts["release"] != 1 {
				t.Fatalf("wrong boundary calls=%v", f.counts)
			}
		})
	}
	for _, outcome := range []string{"completed_failed", "completed_interrupted"} {
		t.Run(outcome, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			f.commitDisposition = outcome
			if err := e.poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := e.recordUseful(f.useful()); err != managerTimerAckUnproven {
				t.Fatalf("failure accepted Useful=%v", err)
			}
			if e.ledger["active"] == nil || f.turns != 1 {
				t.Fatal("effect failure freed or duplicated")
			}
			if err := e.poll(context.Background()); err != managerTimerCoalesced {
				t.Fatalf("failure replay=%v", err)
			}
		})
	}
}

// Timer serialization coalesces concurrent Polls and fsync refusal prevents the
// first snapshot/provider call. The ordinary/human arbiter is not this mutex.
func TestManagerTimerEngineConcurrentAndDurability(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- e.poll(context.Background()) }()
	}
	wg.Wait()
	close(results)
	good, coalesced := 0, 0
	for err := range results {
		if err == nil {
			good++
		} else if err == managerTimerCoalesced {
			coalesced++
		} else {
			t.Fatal(err)
		}
	}
	if good != 1 || coalesced != 1 || f.snapshots != 1 || f.turns != 1 {
		t.Fatal("parallel Poll did not coalesce")
	}
	e, f = managerTimerEngineFixture(t)
	e.store.durability = func(stage string) error {
		if stage == "replace_file" {
			return errors.New("fixture_sync_failure")
		}
		return nil
	}
	if err := e.poll(context.Background()); err != managerTimerIOUnavailable {
		t.Fatalf("failed consumption=%v", err)
	}
	if f.snapshots != 0 || f.counts["reserve"] != 0 {
		t.Fatal("effect preceded durable consumption")
	}
}

// Every admission/effect boundary has a durability witness. A failed write
// cannot cross its next snapshot/Reserve/Commit boundary, including after an
// earlier claim or turn already happened. Only isolated fixture syncs fail.
func TestManagerTimerEngineWriteAheadFailures(t *testing.T) {
	for failAt := 1; failAt <= 5; failAt++ {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			writes := 0
			e.store.durability = func(stage string) error {
				if stage == "replace_file" {
					writes++
					if writes == failAt {
						return errors.New("fixture_sync_failure")
					}
				}
				return nil
			}
			if err := e.poll(context.Background()); err != managerTimerIOUnavailable {
				t.Fatalf("sync boundary=%v", err)
			}
			wantSnapshots, wantClaims, wantTurns := 1, 0, 0
			if failAt == 1 {
				wantSnapshots = 0
			}
			if failAt >= 3 {
				wantClaims = 1
			}
			if failAt == 5 {
				wantTurns = 1
			}
			if f.snapshots != wantSnapshots || f.claims != wantClaims || f.turns != wantTurns {
				t.Fatalf("boundary %d snapshots=%d claims=%d turns=%d", failAt, f.snapshots, f.claims, f.turns)
			}
			if err := e.poll(context.Background()); err != managerTimerStateInvalid {
				t.Fatalf("failed handle reused=%v", err)
			}
			if err := e.recover(context.Background()); err != managerTimerStateInvalid || f.counts["query"] != 0 {
				t.Fatalf("uncertain storage crossed query=%v", err)
			}
		})
	}
	for _, boundary := range []string{"release", "ack"} {
		t.Run(boundary, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			if boundary == "release" {
				f.goalAfterReserve = "cleared"
			} else {
				if err := e.poll(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := e.recordUseful(f.useful()); err != nil {
					t.Fatal(err)
				}
			}
			writes := 0
			e.store.durability = func(stage string) error {
				if stage == "replace_file" {
					writes++
					if boundary == "ack" || writes == 4 {
						return errors.New("fixture_sync_failure")
					}
				}
				return nil
			}
			var err error
			if boundary == "release" {
				err = e.poll(context.Background())
			} else {
				err = e.ack(context.Background(), managerTimerAckPayload(e))
			}
			if err != managerTimerIOUnavailable || e.ledger["active"] == nil {
				t.Fatalf("%s freed on failed persistence: %v", boundary, err)
			}
			if boundary == "release" && (f.counts["release"] != 0 || f.turns != 0) {
				t.Fatal("Release preceded its write-ahead record")
			}
		})
	}
}

// Malformed/foreign/unverifiable replies after a possible effect retain the
// original pending slot. Even a permissive verifier cannot waive closed shape,
// correlation, original claim expiry or immutable native identity.
func TestManagerTimerEngineContradictoryReplies(t *testing.T) {
	for _, which := range []string{"malformed", "unverified", "issuer", "attempt", "claim_deadline", "native_id"} {
		t.Run(which, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			original := e.ports.provider
			e.ports.provider = func(ctx context.Context, frame []byte, binding map[string]any) ([]byte, error) {
				out, err := original(ctx, frame, binding)
				if err != nil {
					return out, err
				}
				var result map[string]any
				d := json.NewDecoder(bytes.NewReader(out))
				d.UseNumber()
				if err := d.Decode(&result); err != nil {
					t.Fatal(err)
				}
				if result["op"] != "commit" {
					return out, nil
				}
				switch which {
				case "malformed":
					return []byte("{}\n"), nil
				case "unverified":
					timerObject(result, "proof")["evidence_digest"] = strings.Repeat("a", 64)
				case "issuer":
					timerObject(result, "proof")["issuer_binding_digest"] = strings.Repeat("a", 64)
				case "attempt":
					result["attempt_id"] = strings.Repeat("a", 64)
				case "claim_deadline":
					// Still within both 30s bounds, but not the original deadline.
					claim := timerObject(result, "claim")
					claim["expires_monotonic_ns"] = managerTimerNumber(timerNumber(claim, "expires_monotonic_ns") - 1)
				case "native_id":
					timerObject(result, "native")["claim_id"] = "foreign-claim"
					timerObject(result, "claim")["claim_id"] = "foreign-claim"
				}
				return timerFixtureBytes(result), nil
			}
			if which != "unverified" {
				e.ports.verifyProvider = func(map[string]any) bool { return true }
			}
			if err := e.poll(context.Background()); err != managerTimerQueueUnknown {
				t.Fatalf("contradiction=%v", err)
			}
			if a := timerObject(e.ledger, "active"); a == nil || a["phase"] != "commit_unknown" || timerObject(a, "native")["claim_id"] != "fixture-claim" {
				t.Fatal("reply replaced prior facts or freed slot")
			}
			if f.turns != 1 || f.counts["commit"] != 1 {
				t.Fatal("effect witness missing")
			}
			if err := e.poll(context.Background()); err != managerTimerCoalesced {
				t.Fatalf("contradictory reply replayed=%v", err)
			}
		})
	}
}

// Old boot/revision continuations and unissued ordinals cannot trigger a read
// or effect, even while a different legitimate attempt is pending.
func TestManagerTimerEngineContinuationFences(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	if err := e.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	base := timerObject(timerObject(e.ledger, "active"), "tick")
	for _, which := range []string{"boot", "revision", "future"} {
		tick := timerFixtureClone(base)
		want := error(managerTimerTickStale)
		switch which {
		case "boot":
			tick["boot_id"] = "other-boot"
		case "revision":
			tick["revision"] = json.Number("2")
			tick["tick_id"] = timerString(tick, "registration_id") + ":2:1"
		case "future":
			tick["opportunity"] = json.Number("2")
			tick["tick_id"] = timerString(tick, "registration_id") + ":1:2"
			want = managerTimerTickUnissued
		}
		if err := e.continueTick(tick); err != want {
			t.Fatalf("%s=%v", which, err)
		}
	}
	if f.snapshots != 1 || f.turns != 1 {
		t.Fatal("continuation repeated input")
	}
}

// A current queued positive starts once. Clear/loss/expiry before its first
// modeled native consumption permanently suppresses that opportunity: returning
// to active/idle cannot flush it. The real native arbiter remains unqualified.
func TestManagerTimerEngineQueuedConditionalStart(t *testing.T) {
	for _, which := range []string{"current", "cleared", "unavailable", "expired"} {
		t.Run(which, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			f.commitDisposition = "queued"
			if err := e.poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.items != 1 || f.turns != 0 {
				t.Fatal("queue positive did not stop before start")
			}
			want := error(nil)
			switch which {
			case "cleared":
				f.owner.primary.state = "cleared"
				want = managerTimerInputIneligible
			case "unavailable":
				f.owner.primary.current = false
				want = managerTimerCapabilityMissing
			case "expired":
				f.owner.clock["monotonic_ns"] = f.claim["expires_monotonic_ns"]
				f.owner.clock["observed_at"] = f.claim["expires_at"]
				want = managerTimerExpired
			}
			f.startQueued()
			f.owner.primary.state = "active"
			f.owner.primary.current = true
			f.startQueued()
			if err := e.recover(context.Background()); err != want {
				t.Fatalf("conditional start=%v want %v", err, want)
			}
			if which == "current" {
				if f.turns != 1 {
					t.Fatal("valid start refused or duplicated")
				}
				if err := e.recordUseful(f.useful()); err != nil {
					t.Fatal(err)
				}
				if err := e.ack(context.Background(), managerTimerAckPayload(e)); err != nil {
					t.Fatal(err)
				}
			} else if f.turns != 0 || e.ledger["active"] != nil {
				t.Fatal("stale first turn or lost suppression")
			}
			if f.counts["reserve"] != 1 || f.counts["commit"] != 1 {
				t.Fatal("cleanup resent an effect")
			}
		})
	}
}

// Terminal summary rotation and process reopen preserve the execution-wide
// high-water. A genuinely later full interval may run unchanged goal content;
// old ACK/tick/reboot and capacity cannot create a new opportunity or read.
func TestManagerTimerEngineRotationRestartAndCapacity(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	var firstTick, firstAck map[string]any
	for i := 0; i < 35; i++ {
		if i > 0 {
			f.owner.clock["monotonic_ns"] = e.ledger["next_due_ns"]
			f.owner.clock["observed_at"] = timerTime(f.owner.clock, "observed_at").Add(900 * time.Second).Format("2006-01-02T15:04:05Z")
		}
		if err := e.poll(context.Background()); err != nil {
			t.Fatalf("interval%d=%v", i, err)
		}
		if err := e.recordUseful(f.useful()); err != nil {
			t.Fatal(err)
		}
		ack := managerTimerAckPayload(e)
		if i == 0 {
			firstTick = timerFixtureClone(timerObject(timerObject(e.ledger, "active"), "tick"))
			firstAck = timerFixtureClone(ack)
		}
		if err := e.ack(context.Background(), ack); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.ledger["terminal"].([]any)) != 32 || timerNumber(e.ledger, "opportunity_high_water") != 35 || f.snapshots != 35 || f.turns != 35 {
		t.Fatal("rotation lost high-water or interval count")
	}
	witness := e.store.witness
	if err := e.store.Close(); err != nil {
		t.Fatal(err)
	}
	s, _, err := openManagerTimerStore(witness, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e, err = newManagerTimerEngine(s, e.gate, e.ports)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.continueTick(firstTick); err != managerTimerTickStale {
		t.Fatalf("rotated tick=%v", err)
	}
	if err := e.ack(context.Background(), firstAck); err != managerTimerTickStale {
		t.Fatalf("rotated ACK=%v", err)
	}
	f.owner.clock["boot_id"] = "new-boot"
	if err := e.poll(context.Background()); err != managerTimerClockUnknown {
		t.Fatalf("reboot without trusted rebaseline=%v", err)
	}
	f.owner.clock["boot_id"] = "fixture-boot"
	e.ledger["opportunity_high_water"] = managerTimerNumber(managerTimerMaxU)
	if err := saveManagerTimerFixture(e.store, e.ledger); err != nil {
		t.Fatal(err)
	}
	f.owner.clock["monotonic_ns"] = e.ledger["next_due_ns"]
	if err := e.poll(context.Background()); err != managerTimerCapacity {
		t.Fatalf("exhaustion=%v", err)
	}
	if f.snapshots != 35 || f.turns != 35 || timerNumber(e.ledger, "opportunity_high_water") != managerTimerMaxU {
		t.Fatal("replay/reboot/capacity had new effects")
	}
}
