package sessionhost

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"
)

const (
	managerTimerNotDue            managerTimerCode = "timer_not_due"
	managerTimerCoalesced         managerTimerCode = "timer_coalesced"
	managerTimerCapacity          managerTimerCode = "timer_capacity"
	managerTimerQueueUnknown      managerTimerCode = "timer_queue_unknown"
	managerTimerSourceUnavailable managerTimerCode = "timer_source_unavailable"
	managerTimerAckUnproven       managerTimerCode = "timer_ack_unproven"
	managerTimerAckConflict       managerTimerCode = "timer_ack_conflict"
	managerTimerTickStale         managerTimerCode = "timer_tick_stale"
	managerTimerTickUnissued      managerTimerCode = "timer_tick_unissued"
)

// These inert ports have no default implementation and are not a registration,
// native transport or goal-state adapter. The bound ordinary owner must provide
// current serialized observations; a real provider must additionally enforce
// E0/E1 at its own conditional effect points. A successful fake trace cannot
// qualify that actual start ordering or authenticate a real result route.
type managerTimerPorts struct {
	observe        func(context.Context, string) (managerTimerOwnerObservation, *managerTimerClaimObservation, error)
	snapshot       func(context.Context, string) (string, error)
	provider       func(context.Context, []byte, map[string]any) ([]byte, error)
	verifyProvider func(map[string]any) bool
	verifyUseful   func(map[string]any) bool
}

type managerTimerEngine struct {
	mu     *sync.Mutex // Shared store transaction; never the ordinary/human input lock.
	store  *managerTimerStore
	ledger map[string]any
	gate   managerTimerGate
	ports  managerTimerPorts
	failed bool
}

func cloneManagerTimerObject(o map[string]any) (map[string]any, error) {
	b, err := canonicalManagerTimerValue(o)
	if err != nil {
		return nil, managerTimerStateInvalid
	}
	return decodeManagerTimerObjectLimit(append(b, '\n'), managerTimerJournalLimit)
}

func newManagerTimerEngine(store *managerTimerStore, gate managerTimerGate, ports managerTimerPorts) (*managerTimerEngine, error) {
	if store == nil || ports.observe == nil || ports.snapshot == nil || ports.provider == nil || ports.verifyProvider == nil || ports.verifyUseful == nil {
		return nil, managerTimerCapabilityMissing
	}
	store.transaction.Lock()
	defer store.transaction.Unlock()
	store.mu.Lock()
	ledger, err := store.loadLocked()
	store.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &managerTimerEngine{mu: &store.transaction, store: store, ledger: ledger, gate: gate, ports: ports}, nil
}

// Called under the shared transaction lock before any timer operation. Another
// handle may have changed the durable phase without changing any high-water.
func (e *managerTimerEngine) refresh() error {
	if e.failed {
		return managerTimerStateInvalid
	}
	e.store.mu.Lock()
	ledger, err := e.store.loadLocked()
	e.store.mu.Unlock()
	if err != nil {
		e.failed = true
		return err
	}
	e.ledger = ledger
	return nil
}

func (e *managerTimerEngine) persist(next map[string]any) error {
	if e.failed {
		return managerTimerStateInvalid
	}
	if err := e.store.save(e.ledger, next); err != nil {
		e.failed = true
		return err
	}
	copy, err := cloneManagerTimerObject(next)
	if err != nil {
		e.failed = true
		return managerTimerStateInvalid
	}
	e.ledger = copy
	return nil
}

func managerTimerNumber(n int64) json.Number { return json.Number(strconv.FormatInt(n, 10)) }
func managerTimerEmptyNative() map[string]any {
	return map[string]any{"claim_id": nil, "item_id": nil, "turn_id": nil}
}

// Poll has no caller-selected clock, opportunity, message, binding or ID. Due
// consumption and preparing are durable before the single snapshot or call.
func (e *managerTimerEngine) poll(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.refresh(); err != nil {
		return err
	}
	if e.ledger["active"] != nil {
		return managerTimerCoalesced
	}
	reg := timerObject(e.ledger, "registration")
	if reg == nil || !managerTimerMember(e.ledger["registration_state"], "active_unqualified|armed") {
		return managerTimerRegistrationStale
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	owner, _, err := e.ports.observe(ctx, "")
	if err != nil {
		return managerTimerCapabilityMissing
	}
	if err = managerTimerCurrentLifetime(e.ledger, owner.clock); err != nil {
		return err
	}
	if e.ledger["next_due_boot_id"] != owner.clock["boot_id"] || e.ledger["next_due_ns"] == nil {
		return managerTimerClockUnknown
	}
	now := timerNumber(owner.clock, "monotonic_ns")
	if now < timerNumber(e.ledger, "next_due_ns") {
		return managerTimerNotDue
	}
	opportunity := timerNumber(e.ledger, "opportunity_high_water")
	if opportunity >= managerTimerMaxU {
		return managerTimerCapacity
	}
	nextDue, err := managerTimerDeadline(now, timerNumber(reg, "cadence_seconds"))
	if err != nil {
		return err
	}
	eligibility := e.gate.preClaim(e.ledger, owner)
	next, err := cloneManagerTimerObject(e.ledger)
	if err != nil {
		return err
	}
	next["opportunity_high_water"] = managerTimerNumber(opportunity + 1)
	next["next_due_ns"] = managerTimerNumber(nextDue)
	if eligibility != nil {
		if err = e.persist(next); err != nil {
			return err
		}
		return eligibility
	}
	tickID := timerString(reg, "registration_id") + ":" + strconv.FormatInt(timerNumber(reg, "revision"), 10) + ":" + strconv.FormatInt(opportunity+1, 10)
	tick := map[string]any{"registration_id": reg["registration_id"], "revision": reg["revision"], "opportunity": managerTimerNumber(opportunity + 1), "boot_id": owner.clock["boot_id"], "tick_id": tickID}
	a := map[string]any{"attempt_id": timerAttemptID(tick), "tick": tick, "binding_digest": timerDigest("Binding", reg["binding"]), "locator": nil, "goal_digest": nil, "request_digest": nil, "submission": nil, "phase": "preparing", "resume_phase": nil, "native": managerTimerEmptyNative(), "provider_evidence": nil, "useful": nil, "ack_digest": nil}
	next["active"] = a
	if err = e.persist(next); err != nil {
		return err
	}
	binding := timerObject(reg, "binding")
	digest, err := e.ports.snapshot(ctx, timerString(binding, "goal_locator"))
	if err != nil || managerTimerField("H", digest) != nil {
		if closeErr := e.closeAttempt("no_effect", nil); closeErr != nil {
			return closeErr
		}
		if code, ok := err.(managerTimerCode); ok && managerTimerMember(string(code), "timer_source_unavailable|timer_source_invalid|timer_source_too_large|timer_source_changed|timer_frame_too_large") {
			return code
		}
		return managerTimerSourceUnavailable
	}
	owner, _, err = e.ports.observe(ctx, timerString(a, "attempt_id"))
	if err != nil {
		err = managerTimerCapabilityMissing
	} else {
		err = e.gate.preClaim(e.ledger, owner)
	}
	if err != nil {
		if closeErr := e.closeAttempt("no_effect", nil); closeErr != nil {
			return closeErr
		}
		return err
	}
	quoted, _ := json.Marshal(binding["goal_locator"])
	frame := map[string]any{"protocol": "host-codex-queue-invocation/1", "thread_id": binding["thread_id"], "codex_home": binding["codex_home"], "message": "Advance your goals: " + string(quoted) + " sha256 " + digest}
	sub := map[string]any{"goal_digest": digest, "frame": frame}
	next, err = cloneManagerTimerObject(e.ledger)
	if err != nil {
		return err
	}
	a = timerObject(next, "active")
	a["locator"] = binding["goal_locator"]
	a["goal_digest"] = digest
	a["submission"] = sub
	a["request_digest"] = managerTimerInvocationDigest(sub)
	a["phase"] = "reserve_unknown"
	// Validate the entire declared call while the retained state still proves
	// preparing/no provider effect. A frame/build refusal is not queue uncertainty.
	if _, err = managerTimerProviderCall(next, "reserve"); err != nil {
		if closeErr := e.closeAttempt("no_effect", nil); closeErr != nil {
			return closeErr
		}
		return err
	}
	if err = e.persist(next); err != nil {
		return err
	}
	result, err := e.call(ctx, "reserve")
	if err != nil {
		return err
	}
	if err = e.applyResult(result); err != nil {
		return err
	}
	if e.ledger["active"] == nil {
		return nil
	}
	if timerObject(e.ledger, "active")["phase"] != "reserved" {
		return managerTimerQueueUnknown
	}
	owner, claim, err := e.ports.observe(ctx, timerString(timerObject(e.ledger, "active"), "attempt_id"))
	if err != nil {
		err = managerTimerCapabilityMissing
	} else {
		err = e.gate.postClaim(e.ledger, owner, claim)
	}
	if err != nil {
		if cleanupErr := e.releaseLocked(ctx); cleanupErr != nil {
			return cleanupErr
		}
		return err
	}
	next, err = cloneManagerTimerObject(e.ledger)
	if err != nil {
		return err
	}
	timerObject(next, "active")["phase"] = "commit_unknown"
	if err = e.persist(next); err != nil {
		return err
	}
	result, err = e.call(ctx, "commit")
	if err != nil {
		return err
	}
	return e.applyResult(result)
}

func managerTimerProviderCall(ledger map[string]any, op string) (map[string]any, error) {
	a := timerObject(ledger, "active")
	if a == nil {
		return nil, managerTimerTickStale
	}
	var submission any
	if op == "reserve" || op == "commit" {
		submission = a["submission"]
	}
	timeout := int64(10000)
	if op == "query" {
		timeout = 5000
	}
	call := map[string]any{"protocol": managerTimerProtocol, "op": op, "request_id": timerDigest("ProviderOperation", map[string]any{"attempt_id": a["attempt_id"], "op": op}), "registration": ledger["registration"], "attempt_id": a["attempt_id"], "tick": a["tick"], "binding_digest": a["binding_digest"], "request_digest": a["request_digest"], "submission": submission, "native": managerTimerEmptyNative(), "timeout_ms": managerTimerNumber(timeout)}
	if err := validateManagerTimerRecord("ProviderCall", call); err != nil {
		return nil, err
	}
	return call, nil
}

func (e *managerTimerEngine) call(ctx context.Context, op string) (map[string]any, error) {
	call, err := managerTimerProviderCall(e.ledger, op)
	if err != nil {
		return nil, err
	}
	frame, err := canonicalManagerTimerValue(call)
	if err != nil {
		return nil, managerTimerInvalid
	}
	frame = append(frame, '\n')
	if len(frame) > managerTimerFrameLimit {
		return nil, managerTimerFrameTooLarge
	}
	if err := ctx.Err(); err != nil {
		return nil, managerTimerQueueUnknown
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timerNumber(call, "timeout_ms"))*time.Millisecond)
	defer cancel()
	binding, err := cloneManagerTimerObject(timerObject(timerObject(e.ledger, "registration"), "binding"))
	if err != nil {
		return nil, err
	}
	output, err := e.ports.provider(ctx, frame, binding)
	if err != nil {
		return nil, managerTimerQueueUnknown
	}
	result, err := decodeManagerTimerObject(output)
	if err != nil || validateManagerTimerRecord("ProviderResult", result) != nil || result["disposition"] == "unknown" || !e.ports.verifyProvider(result) {
		return nil, managerTimerQueueUnknown
	}
	for _, key := range []string{"op", "request_id", "attempt_id", "binding_digest", "request_digest"} {
		if result[key] != call[key] {
			return nil, managerTimerQueueUnknown
		}
	}
	// Query may observe a no-reason cleanup only when the durable owner state
	// proves an explicit Release/cancel. An unrelated observation cannot turn a
	// provider's silent refusal into a successful no-effect transition.
	if managerTimerMember(result["disposition"], "absent|suppressed") && result["reason"] == "none" && op != "release" {
		a := timerObject(e.ledger, "active")
		if op != "query" || a["phase"] != "release_unknown" && e.ledger["registration_state"] != "cancelled" {
			return nil, managerTimerQueueUnknown
		}
	}
	if claim := timerObject(result, "claim"); claim != nil {
		reg := timerObject(e.ledger, "registration")
		created := timerObject(claim, "created_clock")
		if created["boot_id"] != timerObject(reg, "created_clock")["boot_id"] || timerTime(claim, "expires_at").After(timerTime(reg, "expires_at")) || timerNumber(claim, "expires_monotonic_ns") > timerNumber(e.ledger, "expires_monotonic_ns") {
			return nil, managerTimerQueueUnknown
		}
	}
	return result, nil
}

func (e *managerTimerEngine) applyResult(result map[string]any) error {
	a := timerObject(e.ledger, "active")
	if a == nil {
		return managerTimerTickStale
	}
	for _, key := range []string{"attempt_id", "binding_digest", "request_digest"} {
		if result[key] != a[key] {
			return managerTimerQueueUnknown
		}
	}
	prior := timerObject(a, "provider_evidence")
	if prior != nil {
		before, after := timerNumber(timerObject(prior, "proof"), "source_revision"), timerNumber(timerObject(result, "proof"), "source_revision")
		if after < before {
			return e.fenceProviderContradiction(managerTimerQueueUnknown)
		}
		if after == before && !timerEqual(prior, result) {
			return e.fenceProviderContradiction(managerTimerRequestConflict)
		}
		if managerTimerMember(prior["disposition"], "completed_ok|completed_failed|completed_interrupted") && prior["disposition"] != result["disposition"] {
			return e.fenceProviderContradiction(managerTimerQueueUnknown)
		}
	}
	for _, key := range []string{"claim_id", "item_id", "turn_id"} {
		old := timerObject(a, "native")[key]
		if old != nil && timerObject(result, "native")[key] != old {
			return e.fenceProviderContradiction(managerTimerQueueUnknown)
		}
	}
	if result["claim"] != nil && prior != nil && prior["claim"] != nil && !timerEqual(prior["claim"], result["claim"]) {
		return e.fenceProviderContradiction(managerTimerQueueUnknown)
	}
	disposition := timerString(result, "disposition")
	if disposition == "absent" || disposition == "suppressed" {
		outcome := "no_effect"
		if disposition == "suppressed" {
			outcome = "suppressed"
		}
		if err := e.closeAttempt(outcome, nil); err != nil {
			return err
		}
		reason := timerString(result, "reason")
		if reason == "none" {
			return nil
		}
		if reason == "cancelled" {
			return managerTimerRegistrationStale
		}
		return managerTimerCode("timer_" + reason)
	}
	phases := map[string]string{"reserved": "reserved", "queued": "queued", "running": "running", "completed_ok": "completed_unacked", "completed_failed": "failed_retained", "completed_interrupted": "interrupted_retained"}
	phase, ok := phases[disposition]
	if !ok {
		return managerTimerQueueUnknown
	}
	next, err := cloneManagerTimerObject(e.ledger)
	if err != nil {
		return err
	}
	a = timerObject(next, "active")
	if result["op"] == "release" || a["phase"] == "release_unknown" {
		// Even a Release from a claim-only phase may discover an already
		// started effect. Query after a lost Release retains that same fence;
		// a proven claim-only cleanup still permits a genuinely new interval.
		if managerTimerMember(disposition, "queued|running|completed_ok|completed_failed|completed_interrupted") {
			managerTimerDrainRegistration(next)
		}
	}
	a["phase"] = phase
	a["resume_phase"] = nil
	a["native"] = result["native"]
	a["provider_evidence"] = result
	return e.persist(next)
}

// Only call's admitted, fully correlated proof can reach this transition.
// Preserve the last proven attempt verbatim and fence future admission across
// reopen; a later Query can reconcile the attempt but never clears the fence.
func (e *managerTimerEngine) fenceProviderContradiction(code managerTimerCode) error {
	if e.ledger["registration_state"] == "fenced" {
		return code
	}
	next, err := cloneManagerTimerObject(e.ledger)
	if err != nil {
		return err
	}
	next["registration_state"] = "fenced"
	phase := timerNumber(next, "phase_revision")
	// Exhaustion cannot prevent retaining this safety fence or wrap its counter.
	// No lifecycle operation may reactivate a fenced registration at either value.
	if phase < managerTimerMaxU {
		next["phase_revision"] = managerTimerNumber(phase + 1)
	}
	if err = e.persist(next); err != nil {
		return err
	}
	return code
}

func (e *managerTimerEngine) closeAttempt(outcome string, ack any) error {
	next, err := cloneManagerTimerObject(e.ledger)
	if err != nil {
		return err
	}
	a := timerObject(next, "active")
	if a == nil {
		return managerTimerTickStale
	}
	terminal := map[string]any{"attempt_id": a["attempt_id"], "tick": a["tick"], "outcome": outcome, "ack_digest": ack}
	list := append(next["terminal"].([]any), terminal)
	if len(list) > 32 {
		list = list[len(list)-32:]
	}
	next["terminal"] = list
	next["active"] = nil
	return e.persist(next)
}

func managerTimerDrainRegistration(ledger map[string]any) {
	if managerTimerMember(ledger["registration_state"], "active_unqualified|armed") {
		ledger["registration_state"] = "draining"
		if revision := timerNumber(ledger, "phase_revision"); revision < managerTimerMaxU {
			ledger["phase_revision"] = managerTimerNumber(revision + 1)
		}
	}
}

func (e *managerTimerEngine) release(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.refresh(); err != nil {
		return err
	}
	return e.releaseLocked(ctx)
}

// Poll/recovery already own the shared transaction; cleanup never reacquires it.
func (e *managerTimerEngine) releaseLocked(ctx context.Context) error {
	a := timerObject(e.ledger, "active")
	if a == nil {
		return managerTimerTickStale
	}
	if a["phase"] != "release_unknown" {
		next, err := cloneManagerTimerObject(e.ledger)
		if err != nil {
			return err
		}
		a = timerObject(next, "active")
		// Reserve alone cannot start a turn. Once Commit may have happened,
		// disable admission before cleanup, including a lost Release reply and
		// a later ACK. A proven claim-only cleanup keeps the next interval usable.
		if !managerTimerMember(a["phase"], "preparing|reserve_unknown|reserved") {
			managerTimerDrainRegistration(next)
		}
		a["resume_phase"] = a["phase"]
		a["phase"] = "release_unknown"
		if err = e.persist(next); err != nil {
			return err
		}
	}
	result, err := e.call(ctx, "release")
	if err != nil {
		return err
	}
	return e.applyResult(result)
}

// Recover only observes the exact uncertain attempt. It never repeats Reserve
// or Commit, and a recovered reserved claim is released rather than committed.
func (e *managerTimerEngine) recover(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.refresh(); err != nil {
		return err
	}
	a := timerObject(e.ledger, "active")
	if a == nil {
		return managerTimerTickStale
	}
	if a["phase"] == "preparing" {
		return e.closeAttempt("no_effect", nil)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := e.call(ctx, "query")
	if err != nil {
		return err
	}
	if err = e.applyResult(result); err != nil {
		return err
	}
	if active := timerObject(e.ledger, "active"); active != nil && active["phase"] == "reserved" {
		return e.releaseLocked(ctx)
	}
	return nil
}

func (e *managerTimerEngine) recordUseful(useful map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.refresh(); err != nil {
		return err
	}
	a := timerObject(e.ledger, "active")
	if a == nil || !managerTimerMember(a["phase"], "running|completed_unacked") || validateManagerTimerRecord("Useful", useful) != nil || !e.ports.verifyUseful(useful) {
		return managerTimerAckUnproven
	}
	for _, key := range []string{"attempt_id", "binding_digest", "goal_digest"} {
		if useful[key] != a[key] {
			return managerTimerAckUnproven
		}
	}
	if !timerEqual(useful["native"], a["native"]) {
		return managerTimerAckUnproven
	}
	if a["useful"] != nil {
		if timerEqual(a["useful"], useful) {
			return nil
		}
		return managerTimerAckConflict
	}
	next, err := cloneManagerTimerObject(e.ledger)
	if err != nil {
		return err
	}
	timerObject(next, "active")["useful"] = useful
	return e.persist(next)
}

func (e *managerTimerEngine) ack(ctx context.Context, payload map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.refresh(); err != nil {
		return err
	}
	if validateManagerTimerRecord("Ack", payload) != nil {
		return managerTimerAckUnproven
	}
	a := timerObject(e.ledger, "active")
	if a == nil {
		for _, item := range e.ledger["terminal"].([]any) {
			t := item.(map[string]any)
			if t["attempt_id"] == payload["attempt_id"] && t["outcome"] == "acked" && t["ack_digest"] == timerDigest("Ack", payload) {
				return nil
			}
		}
		return managerTimerTickStale
	}
	completion := timerObject(a, "provider_evidence")
	if a["phase"] != "completed_unacked" || a["useful"] == nil || completion == nil || completion["disposition"] != "completed_ok" {
		return managerTimerAckUnproven
	}
	if payload["attempt_id"] != a["attempt_id"] || payload["useful_digest"] != timerDigest("Useful", a["useful"]) || payload["completion_digest"] != timerDigest("ProviderResult", completion) {
		return managerTimerAckConflict
	}
	owner, _, err := e.ports.observe(ctx, timerString(a, "attempt_id"))
	if err != nil || owner.clock == nil || validateManagerTimerRecord("Clock", owner.clock) != nil || owner.clock["trusted"] != true || owner.clock["boot_id"] != e.ledger["next_due_boot_id"] {
		return managerTimerClockUnknown
	}
	due, err := managerTimerDeadline(timerNumber(owner.clock, "monotonic_ns"), timerNumber(timerObject(e.ledger, "registration"), "cadence_seconds"))
	if err != nil {
		return err
	}
	next, err := cloneManagerTimerObject(e.ledger)
	if err != nil {
		return err
	}
	if due > timerNumber(next, "next_due_ns") {
		next["next_due_ns"] = managerTimerNumber(due)
	}
	terminal := map[string]any{"attempt_id": a["attempt_id"], "tick": a["tick"], "outcome": "acked", "ack_digest": timerDigest("Ack", payload)}
	list := append(next["terminal"].([]any), terminal)
	if len(list) > 32 {
		list = list[len(list)-32:]
	}
	next["terminal"] = list
	next["active"] = nil
	return e.persist(next)
}

// An external continuation cannot mint an opportunity or resume a snapshot or
// effect. Only Poll owns the once-through preparing continuation in this part.
func (e *managerTimerEngine) continueTick(tick map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.refresh(); err != nil {
		return err
	}
	if validateManagerTimerRecord("Tick", tick) != nil {
		return managerTimerInvalid
	}
	reg := timerObject(e.ledger, "registration")
	if reg == nil || tick["registration_id"] != reg["registration_id"] || tick["revision"] != reg["revision"] || tick["boot_id"] != e.ledger["next_due_boot_id"] {
		return managerTimerTickStale
	}
	if timerNumber(tick, "opportunity") > timerNumber(e.ledger, "opportunity_high_water") {
		return managerTimerTickUnissued
	}
	a := timerObject(e.ledger, "active")
	if a == nil || timerObject(a, "tick")["opportunity"] != tick["opportunity"] {
		return managerTimerTickStale
	}
	if !timerEqual(a["tick"], tick) {
		return managerTimerRequestConflict
	}
	return nil
}
