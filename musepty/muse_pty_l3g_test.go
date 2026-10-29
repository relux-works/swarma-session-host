package musepty

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// L3G structural PTY grammar tests for supported startup frames.
// One named test per M-row (M54-M57) plus the historical/newest fixture
// smoke. Production entries: ValidateMuseStartupPrefix (called first by
// readMuseLog), StartMusePTYHost, DeliverSessionNotice, and the host
// State/identity reads. The suite is R198-clean: scripted peers with
// pipe handshakes and the injected test clock, no sleeps or fresh waits.

// l3gLogLines marshals records into log bytes with a complete tail.
func l3gLogLines(t *testing.T, records ...any) []byte {
	t.Helper()
	var b []byte
	for _, r := range records {
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		b = append(b, raw...)
		b = append(b, '\n')
	}
	return b
}

// l3gPrefixCode runs the public witness and returns its refusal code,
// failing on admission or on an untyped refusal.
func l3gPrefixCode(t *testing.T, log []byte) string {
	t.Helper()
	err := ValidateMuseStartupPrefix(log)
	if err == nil {
		t.Fatal("startup prefix admitted, want typed refusal")
	}
	var prefix *MuseStartupPrefixError
	if !errors.As(err, &prefix) {
		t.Fatalf("untyped startup refusal: %v", err)
	}
	return prefix.Code
}

// l3gPlantSibling writes a sibling session log under the peer's sessions
// root. The layout mirrors the owned log's depth so discovery treats it
// as a candidate from another session.
func l3gPlantSibling(t *testing.T, home, name string, content []byte) {
	t.Helper()
	dir := filepath.Join(home, "data", "muse", "sessions", "2026", "10", "02", name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "session.jsonl"), content, 0600); err != nil {
		t.Fatal(err)
	}
}

// l3gStartCleanWithSibling starts the planned idle peer beside a planted
// sibling log and settles it to idle. Its watchdog can only fail startup.
func l3gStartCleanWithSibling(t *testing.T, plan *MusePTYLaunchPlan, home string) *musePTYHost {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), museScriptedWatchdog)
	defer cancel()
	h, err := StartMusePTYHost(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			h.mu.Lock()
			t.Logf("screen=%q", strings.Join(h.vt.lines(), "\n"))
			h.mu.Unlock()
		}
		_ = h.Stop(context.Background())
	})
	museInjectTestSeams(t, h)
	musePeerHandshakeRead(home, musePeerFrameFIFO)
	museWaitScripted(t, h, "peer startup idle", func() bool { return museScriptedReady(h, "") })
	museAdvanceTestClockPastQuiet(h)
	if museTestClockFrozen() {
		if h.idle("") {
			t.Fatal("frozen clock must hold idle false (injection armed)")
		}
		return h
	}
	if !h.idle("") {
		t.Fatalf("peer startup: ready but not idle after clock advance (quiet=%v)", museQuietForReport(h))
	}
	return h
}

// l3gAppendAlignedRun appends an owned started/message/terminal triple
// for one run, applies tweak to each record before append, and returns
// the committed message text the fold must carry.
func l3gAppendAlignedRun(t *testing.T, h *musePTYHost, runID string, tweak func(kind string, record map[string]any)) string {
	t.Helper()
	var committed string
	for _, kind := range []string{"started", "assistant_message_committed", "terminal"} {
		r := museOwnedRecord(h, museFixtureRecord(kind))
		payload := r["payload"].(map[string]any)
		payload["run_id"] = runID
		if event, ok := payload["event"].(map[string]any); ok {
			event["run_id"] = runID
			if kind == "assistant_message_committed" {
				committed, _ = event["text"].(string)
			}
			if kind == "terminal" {
				event["terminal"] = "completed"
			}
		}
		if tweak != nil {
			tweak(kind, r)
		}
		appendMuseRecord(t, h, r)
	}
	if committed == "" {
		t.Fatal("fixture message carries no committed text")
	}
	return committed
}

// TestPTYGrammarFailureDoesNotPoisonMSP (M54): a PTY grammar failure is a
// typed PTY-surface refusal and marks no shared state. The witness
// reports the unknown kind with its code, and a sibling session whose
// log carries that kind still starts, settles, and delivers beside it.
// MSP shares no store with PTY in this tree; the sibling session is the
// in-tree second surface (findMuseLog already names side surfaces), and
// MSP-entry composition lands with the recovery leaves.
func TestPTYGrammarFailureDoesNotPoisonMSP(t *testing.T) {
	t.Run("typed_prefix_refusal", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		route := museFixtureRecord("runtime.session.route_facts")
		future := map[string]any{"payload_type": "runtime.session", "payload": map[string]any{"kind": "future_kind"}}
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route, future)); code != MuseStartupPrefixUnknownRecord {
			t.Fatalf("unknown kind code = %q, want %q", code, MuseStartupPrefixUnknownRecord)
		}
	})
	t.Run("sibling_discovery_isolated", func(t *testing.T) {
		plan, home := musePeerPlan(t, "idle", "")
		future := map[string]any{"payload_type": "runtime.future", "payload": map[string]any{"kind": "future"}}
		l3gPlantSibling(t, home, "l3g-sibling-session", l3gLogLines(t, museFixtureRecord("session.opened.observed"), future))
		h := l3gStartCleanWithSibling(t, plan, home)
		museExpectDelivery(t, h, home, "clean session beside broken grammar")
	})
}

// TestPTYStructureFailureDoesNotPoisonMSP (M54): a PTY structure failure
// is a typed PTY-surface refusal and marks no shared state. Missing and
// malformed required members refuse with their own codes, and a sibling
// session with a malformed prefix does not stop a clean start.
func TestPTYStructureFailureDoesNotPoisonMSP(t *testing.T) {
	t.Run("missing_required_member", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		opened["stream"].(map[string]any)["id"] = "11111111-1111-4111-8111-111111111111"
		delete(opened["payload"].(map[string]any)["record"].(map[string]any), "session_id")
		route := museFixtureRecord("runtime.session.route_facts")
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route)); code != MuseStartupPrefixMissingRequired {
			t.Fatalf("missing member code = %q, want %q", code, MuseStartupPrefixMissingRequired)
		}
	})
	t.Run("malformed_required_value", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		route := museFixtureRecord("runtime.session.route_facts")
		route["stream"].(map[string]any)["id"] = "22222222-2222-4222-8222-222222222222"
		route["payload"].(map[string]any)["record"].(map[string]any)["pid"] = float64(0)
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route)); code != MuseStartupPrefixMalformedRequired {
			t.Fatalf("malformed member code = %q, want %q", code, MuseStartupPrefixMalformedRequired)
		}
	})
	t.Run("sibling_malformed_isolated", func(t *testing.T) {
		plan, home := musePeerPlan(t, "idle", "")
		opened := museFixtureRecord("session.opened.observed")
		delete(opened["payload"].(map[string]any)["record"].(map[string]any), "session_id")
		l3gPlantSibling(t, home, "l3g-sibling-session", l3gLogLines(t, opened, museFixtureRecord("runtime.session.route_facts")))
		h := l3gStartCleanWithSibling(t, plan, home)
		museExpectDelivery(t, h, home, "clean session beside malformed structure")
	})
}

// TestUnknownStructureRefusesTyped (M55): unknown structure refuses, and
// the refusal is typed where the witness owns it. Unknown record kinds
// carry the unknown-record code through the public witness; unknown
// screen shapes still refuse delivery; and a prefix member failure
// surfaces typed through the production fold.
func TestUnknownStructureRefusesTyped(t *testing.T) {
	t.Run("unknown_record_kind", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		route := museFixtureRecord("runtime.session.route_facts")
		future := map[string]any{"payload_type": "runtime.future", "payload": map[string]any{"kind": "future"}}
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route, future)); code != MuseStartupPrefixUnknownRecord {
			t.Fatalf("unknown kind code = %q, want %q", code, MuseStartupPrefixUnknownRecord)
		}
	})
	t.Run("unknown_screen_shape", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "◈ Future action required")
		assertMuseNoInput(t, h, home)
	})
	t.Run("prefix_member_failure_is_typed", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		data, err := os.ReadFile(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		var first map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
			t.Fatal(err)
		}
		delete(first["payload"].(map[string]any)["record"].(map[string]any), "session_id")
		patched, err := json.Marshal(first)
		if err != nil {
			t.Fatal(err)
		}
		lines[0] = string(patched)
		if err := os.WriteFile(h.snapshot.LogPath, []byte(strings.Join(lines, "\n")+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err = h.fold()
		if err == nil {
			t.Fatal("rewritten prefix admitted")
		}
		var prefix *MuseStartupPrefixError
		if !errors.As(err, &prefix) || prefix.Code != MuseStartupPrefixMissingRequired {
			t.Fatalf("fold error = %v, want typed missing-required", err)
		}
		assertMuseNoInput(t, h, home)
	})
}

// TestNovelReleaseWithCoveredStructureParses (M56): the chrome banner is
// structural, not a release list. A novel well-formed release parses
// like the captured one; the historical banner keeps parsing; and a
// malformed banner still refuses the frame.
func TestNovelReleaseWithCoveredStructureParses(t *testing.T) {
	t.Run("novel_banner_parses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "  Muse Code 9.9.9")
		museExpectDelivery(t, h, home, "novel banner control")
	})
	t.Run("historical_banner_parses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, museHeaderRow)
		museExpectDelivery(t, h, home, "historical banner control")
	})
	t.Run("malformed_banner_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "  Muse Code 9.9.9 extra")
		assertMuseNoInput(t, h, home)
	})
}

// TestAdditiveOptionalFieldTolerated (M56): undeclared members are
// additive and tolerated. A run whose records carry a probe member
// folds, delivers, and keeps the committed message authoritative; and
// the public witness accepts an additive prefix.
func TestAdditiveOptionalFieldTolerated(t *testing.T) {
	t.Run("additive_member_delivers", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		runID := museTestUUID("l3g-additive-run")
		committed := l3gAppendAlignedRun(t, h, runID, func(kind string, record map[string]any) {
			record["payload"].(map[string]any)["l3g_additive_probe"] = "probe"
			if kind == "assistant_message_committed" {
				record["payload"].(map[string]any)["event"].(map[string]any)["l3g_additive_probe"] = "probe"
			}
		})
		f, err := h.fold()
		if err != nil {
			t.Fatalf("additive fold refused: %v", err)
		}
		if got := f.Terminals[runID].Text; got != committed {
			t.Fatalf("terminal text = %q, want committed %q", got, committed)
		}
		museExpectDelivery(t, h, home, "additive control")
	})
	t.Run("additive_prefix_witnessed", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		opened["payload"].(map[string]any)["l3g_additive_probe"] = "probe"
		route := museFixtureRecord("runtime.session.route_facts")
		route["payload"].(map[string]any)["record"].(map[string]any)["l3g_additive_probe"] = "probe"
		third := museFixtureRecord("command.invoked")
		third["payload"].(map[string]any)["l3g_additive_probe"] = "probe"
		if err := ValidateMuseStartupPrefix(l3gLogLines(t, opened, route, third)); err != nil {
			t.Fatalf("additive prefix refused: %v", err)
		}
	})
}

// TestPostStartupDriftIsRuntimeNotFallback (M57): structure that arrives
// after startup is a runtime verdict, not a startup verdict and not a
// new leg. Delivery refuses with exactly the runtime refusal, nothing is
// pasted or submitted, and the host keeps running as the same session.
func TestPostStartupDriftIsRuntimeNotFallback(t *testing.T) {
	h, home := musePeer(t, "idle")
	id, pid := h.NativeIdentity(), h.snapshot.PID
	appendMuseRecord(t, h, map[string]any{"payload_type": "runtime.future", "payload": map[string]any{"kind": "future"}})
	if err := h.DeliverSessionNotice(context.Background(), "drift must stay runtime"); err != ErrMusePTYDeferred {
		t.Fatalf("drift verdict = %v, want exactly the runtime refusal", err)
	}
	for _, name := range []string{"peer-paste", "peer-submit"} {
		if b, e := os.ReadFile(filepath.Join(home, name)); !os.IsNotExist(e) {
			t.Fatalf("drift input %s=%q: %v", name, b, e)
		}
	}
	if state, _ := h.State(context.Background()); state != sm.ProviderHostRunning {
		t.Fatalf("drift stopped the host: %v", state)
	}
	if h.NativeIdentity() != id || h.snapshot.PID != pid {
		t.Fatal("drift restarted the host leg")
	}
}

// TestL3GStartupPrefixFixtures smokes the historical and newest prefix
// shapes through the public witness: the captured 1.4.1 opening, a
// synthetic 1.4.2 opening (no 1.4.2 capture exists; the vector pins the
// structural relation a newer build keeps), and a synthetic newest
// opening with additive members and a third covered record.
func TestL3GStartupPrefixFixtures(t *testing.T) {
	additive := func(record map[string]any) map[string]any {
		record["payload"].(map[string]any)["l3g_additive_probe"] = "probe"
		return record
	}
	t.Run("historical_1_4_1_captured", func(t *testing.T) {
		log := l3gLogLines(t, museFixtureRecord("session.opened.observed"), museFixtureRecord("runtime.session.route_facts"), museFixtureRecord("command.invoked"))
		if err := ValidateMuseStartupPrefix(log); err != nil {
			t.Fatalf("captured 1.4.1 prefix refused: %v", err)
		}
	})
	t.Run("historical_1_4_2_synthetic", func(t *testing.T) {
		log := l3gLogLines(t, additive(museFixtureRecord("session.opened.observed")), museFixtureRecord("runtime.session.route_facts"), museFixtureRecord("command.invoked"))
		if err := ValidateMuseStartupPrefix(log); err != nil {
			t.Fatalf("synthetic 1.4.2 prefix refused: %v", err)
		}
	})
	t.Run("newest_synthetic", func(t *testing.T) {
		log := l3gLogLines(t, additive(museFixtureRecord("session.opened.observed")), additive(museFixtureRecord("runtime.session.route_facts")), additive(museFixtureRecord("started")))
		if err := ValidateMuseStartupPrefix(log); err != nil {
			t.Fatalf("synthetic newest prefix refused: %v", err)
		}
	})
}

// TestL3GStartupRequiresThreeRecords (L3G-001): startup admits only after
// three complete, structurally validated records. A valid two-record
// prefix is incomplete through the public witness, and a host whose peer
// holds back the third record is cancelled only after the host observes it; an unknown
// third record refuses instead of admitting. Production entries:
// ValidateMuseStartupPrefix and StartMusePTYHost.
func TestL3GStartupRequiresThreeRecords(t *testing.T) {
	t.Run("two_valid_incomplete", func(t *testing.T) {
		log := l3gLogLines(t, museFixtureRecord("session.opened.observed"), museFixtureRecord("runtime.session.route_facts"))
		if code := l3gPrefixCode(t, log); code != MuseStartupPrefixIncomplete {
			t.Fatalf("two-record code = %q, want %q", code, MuseStartupPrefixIncomplete)
		}
	})
	t.Run("hold_back_third_no_admission", func(t *testing.T) {
		plan, _ := musePeerPlan(t, "hold-third", "")
		l3gExpectStartupRefusal(t, plan, MuseStartupPrefixIncomplete)
	})
	t.Run("unknown_third_refuses", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		route := museFixtureRecord("runtime.session.route_facts")
		future := map[string]any{"payload_type": "runtime.future", "payload": map[string]any{"kind": "future"}}
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route, future)); code != MuseStartupPrefixUnknownRecord {
			t.Fatalf("unknown third code = %q, want %q", code, MuseStartupPrefixUnknownRecord)
		}
		plan, _ := musePeerPlan(t, "unknown-third", "")
		l3gExpectStartupRefusal(t, plan, MuseStartupPrefixUnknownRecord)
	})
}

// The watchdog is failure-only. Cancellation is triggered synchronously by
// the production host observing the owned prefix, never by elapsed time.
// Invalid prefixes must return their typed verdict even with cancellation
// now pending; incomplete prefixes preserve both that type and cancellation.
func l3gExpectStartupRefusal(t *testing.T, plan *MusePTYLaunchPlan, code string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), museScriptedWatchdog)
	defer cancel()
	observed := false
	plan.startupObserved = func(h *musePTYHost, f *museLogFold, err error) {
		var prefix *MuseStartupPrefixError
		if !errors.As(err, &prefix) || prefix.Code != code {
			return
		}
		if code == MuseStartupPrefixIncomplete && (f == nil || !f.Opened || f.StartupPrefixWitnessed || f.PID != h.snapshot.PID) {
			return
		}
		observed = true
		cancel()
	}
	h, err := StartMusePTYHost(ctx, plan)
	if h != nil {
		defer h.Stop(context.Background())
	}
	if !observed || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup refusal did not precede the watchdog: observed=%v context=%v err=%v", observed, ctx.Err(), err)
	}
	var prefix *MuseStartupPrefixError
	if !errors.As(err, &prefix) || prefix.Code != code {
		t.Fatalf("startup admitted or lost typed refusal: h=%v err=%v, want %s", h != nil, err, code)
	}
	if code == MuseStartupPrefixIncomplete {
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "startup unobserved") {
			t.Fatalf("held prefix did not retain event cancellation: %v", err)
		}
	} else if errors.Is(err, context.Canceled) {
		t.Fatalf("typed refusal was postponed until cancellation: %v", err)
	}
	if h == nil || h.NativeIdentity() != "" {
		t.Fatal("startup refusal exposed an admitted native identity")
	}
}

func l3gSetOpenedPayload(t *testing.T, home string, tweak func(map[string]any)) {
	t.Helper()
	payload := museFixturePayload("session.opened.observed")
	tweak(payload)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "peer-opened-payload"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

// TestL3GStartupRefusesBeforeDeadline addresses L3G-005 through the public
// start entry. The host must observe and return each typed refusal; a stalled
// child, a timeout, and an untyped cancellation all fail this assay.
func TestL3GStartupRefusesBeforeDeadline(t *testing.T) {
	t.Run("unknown_third", func(t *testing.T) {
		plan, _ := musePeerPlan(t, "unknown-third", "")
		l3gExpectStartupRefusal(t, plan, MuseStartupPrefixUnknownRecord)
	})
	t.Run("missing_required", func(t *testing.T) {
		plan, home := musePeerPlan(t, "idle", "")
		l3gSetOpenedPayload(t, home, func(p map[string]any) {
			delete(p["record"].(map[string]any), "security_mode")
		})
		l3gExpectStartupRefusal(t, plan, MuseStartupPrefixMissingRequired)
	})
	t.Run("malformed_required", func(t *testing.T) {
		plan, home := musePeerPlan(t, "idle", "")
		l3gSetOpenedPayload(t, home, func(p map[string]any) {
			p["record"].(map[string]any)["security_mode"] = map[string]any{"wrong": true}
		})
		l3gExpectStartupRefusal(t, plan, MuseStartupPrefixMalformedRequired)
	})
}

// TestL3GRequiredMembersTyped (L3G-002): every required member in the
// startup window refuses typed when missing or malformed, through the
// public witness and through the production fold. Covers members beyond
// session_id and pid, including the third record. Production entries:
// ValidateMuseStartupPrefix via readMuseLog, and fold.
func TestL3GRequiredMembersTyped(t *testing.T) {
	t.Run("route_cwd_missing", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		route := museFixtureRecord("runtime.session.route_facts")
		delete(route["payload"].(map[string]any)["record"].(map[string]any), "cwd")
		third := museFixtureRecord("command.invoked")
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route, third)); code != MuseStartupPrefixMissingRequired {
			t.Fatalf("route cwd code = %q, want %q", code, MuseStartupPrefixMissingRequired)
		}
	})
	t.Run("opened_security_mode_missing", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		delete(opened["payload"].(map[string]any)["record"].(map[string]any), "security_mode")
		route := museFixtureRecord("runtime.session.route_facts")
		third := museFixtureRecord("command.invoked")
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route, third)); code != MuseStartupPrefixMissingRequired {
			t.Fatalf("opened security_mode code = %q, want %q", code, MuseStartupPrefixMissingRequired)
		}
	})
	t.Run("third_record_required_missing", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		route := museFixtureRecord("runtime.session.route_facts")
		third := museFixtureRecord("started")
		delete(third["payload"].(map[string]any)["event"].(map[string]any), "prompt")
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route, third)); code != MuseStartupPrefixMissingRequired {
			t.Fatalf("third record code = %q, want %q", code, MuseStartupPrefixMissingRequired)
		}
	})
	t.Run("route_cwd_malformed", func(t *testing.T) {
		opened := museFixtureRecord("session.opened.observed")
		route := museFixtureRecord("runtime.session.route_facts")
		route["payload"].(map[string]any)["record"].(map[string]any)["cwd"] = float64(7)
		third := museFixtureRecord("command.invoked")
		if code := l3gPrefixCode(t, l3gLogLines(t, opened, route, third)); code != MuseStartupPrefixMalformedRequired {
			t.Fatalf("route cwd malformed code = %q, want %q", code, MuseStartupPrefixMalformedRequired)
		}
	})
	t.Run("route_cwd_missing_through_fold", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		data, err := os.ReadFile(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		patched := false
		for i, line := range lines {
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatal(err)
			}
			if rec["payload_type"] != "runtime.session.route_facts" {
				continue
			}
			delete(rec["payload"].(map[string]any)["record"].(map[string]any), "cwd")
			raw, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			lines[i] = string(raw)
			patched = true
			break
		}
		if !patched {
			t.Fatal("peer log carries no route record")
		}
		if err := os.WriteFile(h.snapshot.LogPath, []byte(strings.Join(lines, "\n")+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err = h.fold()
		if err == nil {
			t.Fatal("cwd-stripped prefix admitted through fold")
		}
		var prefix *MuseStartupPrefixError
		if !errors.As(err, &prefix) || prefix.Code != MuseStartupPrefixMissingRequired {
			t.Fatalf("fold error = %v, want typed missing-required", err)
		}
		assertMuseNoInput(t, h, home)
	})
}

// TestL3GKnownVariantExactness (L3G-003): a present known field must match
// a variant that declares it. A task_stream_linked display set to an
// array, or with required display members removed, refuses through the
// public delivery entry; a link without display still delivers through
// its subset variant. Production entry: DeliverSessionNotice.
func TestL3GKnownVariantExactness(t *testing.T) {
	t.Run("display_array_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		l := museCorpusRecord(t, "runtime.session", "run", "task_stream_linked")
		run := l["payload"].(map[string]any)["run_id"].(string)
		task := l["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, l)
		r["payload"].(map[string]any)["event"].(map[string]any)["display"] = []any{"wrong_type"}
		appendMuseRecord(t, h, r)
		museEmitTaskPlanned(t, h, run, task)
		end := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
		end["payload"].(map[string]any)["run_id"] = run
		end["payload"].(map[string]any)["task_id"] = task
		end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("display_member_missing_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		l := museCorpusRecord(t, "runtime.session", "run", "task_stream_linked")
		run := l["payload"].(map[string]any)["run_id"].(string)
		task := l["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, l)
		event := r["payload"].(map[string]any)["event"].(map[string]any)
		display, ok := event["display"].(map[string]any)
		if !ok {
			display = map[string]any{"label": "link", "model": "echo", "role": "worker"}
			event["display"] = display
		}
		delete(display, "label")
		appendMuseRecord(t, h, r)
		museEmitTaskPlanned(t, h, run, task)
		end := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
		end["payload"].(map[string]any)["run_id"] = run
		end["payload"].(map[string]any)["task_id"] = task
		end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("subset_without_display_delivers", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		l := museCorpusRecord(t, "runtime.session", "run", "task_stream_linked")
		run := l["payload"].(map[string]any)["run_id"].(string)
		task := l["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, l)
		delete(r["payload"].(map[string]any)["event"].(map[string]any), "display")
		appendMuseRecord(t, h, r)
		museEmitTaskPlanned(t, h, run, task)
		end := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
		end["payload"].(map[string]any)["run_id"] = run
		end["payload"].(map[string]any)["task_id"] = task
		end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "subset link control")
	})
}

// TestL3GAdditiveDroppedBeforeDecode (L3G-004): admitted additive members
// are dropped before downstream decoding, so they can neither fail a
// decode nor influence identity or state. A terminal with additive text
// set to a non-string value folds and delivers with the committed
// message authoritative; an additive identity-named member is ignored.
// Production entries: fold and DeliverSessionNotice.
func TestL3GAdditiveDroppedBeforeDecode(t *testing.T) {
	t.Run("terminal_envelope_frame_marker_ignored", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		runID := museTestUUID("l3g-envelope-frame")
		committed := l3gAppendAlignedRun(t, h, runID, func(kind string, r map[string]any) {
			if kind == "terminal" {
				r["retained_frame"] = "session_permission_transaction"
			}
		})
		f, err := h.fold()
		if err != nil || f.Terminals[runID].Text != committed {
			t.Fatalf("additive envelope marker affected terminal: %v", err)
		}
		museExpectDelivery(t, h, home, "additive envelope marker control")
	})
	t.Run("opened_payload_event_ignored", func(t *testing.T) {
		plan, home := musePeerPlan(t, "idle", "")
		l3gSetOpenedPayload(t, home, func(p map[string]any) {
			p["event"] = map[string]any{"kind": "l3g_future", "run_id": false}
		})
		h := l3gStartCleanWithSibling(t, plan, home)
		museExpectDelivery(t, h, home, "additive selector control")
	})
	t.Run("opened_payload_pid_object_ignored", func(t *testing.T) {
		plan, home := musePeerPlan(t, "idle", "")
		l3gSetOpenedPayload(t, home, func(p map[string]any) {
			p["pid"] = map[string]any{"l3g_pid_probe": true}
		})
		h := l3gStartCleanWithSibling(t, plan, home)
		if h.NativeIdentity() != museTestUUID("peer-session") {
			t.Fatal("additive pid changed the native identity")
		}
		museExpectDelivery(t, h, home, "additive payload pid control")
	})
	t.Run("opened_payload_session_id_ignored", func(t *testing.T) {
		plan, home := musePeerPlan(t, "idle", "")
		l3gSetOpenedPayload(t, home, func(p map[string]any) {
			p["session_id"] = "33333333-3333-4333-8333-333333333333"
		})
		h := l3gStartCleanWithSibling(t, plan, home)
		if h.NativeIdentity() != museTestUUID("peer-session") {
			t.Fatal("additive session_id changed the native identity")
		}
		museExpectDelivery(t, h, home, "additive payload session control")
	})
	t.Run("nested_route_session_id_ignored", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
		nested := r["payload"].(map[string]any)["record"].(map[string]any)
		nested["pid"] = h.snapshot.PID
		nested["session_id"] = "44444444-4444-4444-8444-444444444444"
		appendMuseRecord(t, h, r)
		if _, err := h.fold(); err != nil {
			t.Fatalf("additive nested identity reached binding: %v", err)
		}
		museExpectDelivery(t, h, home, "additive nested identity control")
	})
	t.Run("terminal_text_object_ignored", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		runID := museTestUUID("l3g-additive-text-object")
		committed := l3gAppendAlignedRun(t, h, runID, func(kind string, record map[string]any) {
			if kind != "terminal" {
				return
			}
			record["payload"].(map[string]any)["event"].(map[string]any)["text"] = map[string]any{"nested": "object"}
		})
		f, err := h.fold()
		if err != nil {
			t.Fatalf("additive object text refused: %v", err)
		}
		if got := f.Terminals[runID].Text; got != committed {
			t.Fatalf("terminal text = %q, want committed %q", got, committed)
		}
		museExpectDelivery(t, h, home, "additive object text control")
	})
	t.Run("terminal_run_id_ignored", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		runID := museTestUUID("l3g-additive-run-id")
		other := museTestUUID("l3g-additive-run-id-other")
		committed := l3gAppendAlignedRun(t, h, runID, func(kind string, record map[string]any) {
			if kind != "terminal" {
				return
			}
			record["payload"].(map[string]any)["event"].(map[string]any)["run_id"] = other
		})
		f, err := h.fold()
		if err != nil {
			t.Fatalf("additive run_id refused: %v", err)
		}
		term, ok := f.Terminals[runID]
		if !ok {
			t.Fatalf("terminal for %s missing", runID)
		}
		if term.Text != committed {
			t.Fatalf("terminal text = %q, want committed %q", term.Text, committed)
		}
		if _, ok := f.Terminals[other]; ok {
			t.Fatalf("additive run_id %s took effect", other)
		}
		museExpectDelivery(t, h, home, "additive run_id control")
	})
}

// TestL3GRequiredIdentityStillRefuses keeps authoritative session, process,
// and run identities enforced while the same names elsewhere are additive.
// All negative rows drive DeliverSessionNotice with fail-on-admit assertions.
func TestL3GRequiredIdentityStillRefuses(t *testing.T) {
	t.Run("opened_session_mismatch", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
		r["payload"].(map[string]any)["record"].(map[string]any)["session_id"] = "33333333-3333-4333-8333-333333333333"
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
	t.Run("route_process_mismatch", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
		r["payload"].(map[string]any)["record"].(map[string]any)["pid"] = h.snapshot.PID + 1
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
	t.Run("message_run_mismatch", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		l3gAppendAlignedRun(t, h, museTestUUID("l3g-required-run"), func(kind string, r map[string]any) {
			if kind == "assistant_message_committed" {
				r["payload"].(map[string]any)["run_id"] = museTestUUID("l3g-required-other")
			}
		})
		assertMuseNoInput(t, h, home)
	})
}

// TestL3GAdditiveProjectionCorpus attacks every object and array depth in the
// historical record grammar. Retained-frame envelopes are closed. The public prefix
// entry must admit the extra member; the single decode boundary must preserve
// exactly the original known values and retain only an additive count.
func TestL3GAdditiveProjectionCorpus(t *testing.T) {
	var add func(any) int
	add = func(value any) int {
		count := 0
		switch v := value.(type) {
		case map[string]any:
			for _, child := range v {
				count += add(child)
			}
			v["l3g_additive_probe"] = map[string]any{"session_id": "foreign", "pid": []any{false}}
			return count + 1
		case []any:
			for _, child := range v {
				count += add(child)
			}
		}
		return count
	}
	for i, raw := range museCorpus(t) {
		base, err := decodeMuseLogRecord(raw)
		if err != nil {
			t.Fatalf("capture %d: %v", i, err)
		}
		value, err := decodeMuseRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(base.known, value) {
			t.Fatalf("capture %d lost a known field at the boundary", i)
		}
		count := add(value)
		changed, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeMuseLogRecord(changed)
		if base.Frame != "" {
			if err == nil {
				t.Fatalf("capture %d admitted an unknown frame member", i)
			}
			continue
		}
		if err != nil {
			t.Fatalf("capture %d additive decode: %v", i, err)
		}
		if got.AdditiveFields != count {
			t.Fatalf("capture %d additive count=%d want=%d", i, got.AdditiveFields, count)
		}
		got.AdditiveFields = base.AdditiveFields
		if !reflect.DeepEqual(base, got) {
			t.Fatalf("capture %d additive content crossed the boundary", i)
		}
		if err := ValidateMuseStartupPrefix(l3gLogLines(t, value, value, value)); err != nil {
			t.Fatalf("capture %d additive public prefix: %v", i, err)
		}
	}
}
