package musepty

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

func museCorpus(t *testing.T) []json.RawMessage {
	return museVersionCorpus(t, "1.4.1")
}

func museVersionCorpus(t *testing.T, version string) []json.RawMessage {
	t.Helper()
	b, err := os.ReadFile("testdata/muse-" + version + "/corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var records []json.RawMessage
	if err = json.Unmarshal(b, &records); err != nil {
		t.Fatal(err)
	}
	return records
}

func TestMusePTYClosedGrammarCaptureCompleteness(t *testing.T) {
	seen := map[string]bool{}
	var visit func(json.RawMessage)
	visit = func(raw json.RawMessage) {
		v, err := decodeMuseRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		key := museGrammarKey(v)
		matches := 0
		for i, shape := range museGrammar[key] {
			if museShapeMatches(shape, v) {
				matches++
				seen[fmt.Sprintf("%s/%d", key, i)] = true
			}
		}
		// L3G required versus additive: a superset record also matches
		// subset-alternative entries, so at-least-one replaces
		// exactly-one. Every entry must still be witnessed below.
		if matches < 1 {
			t.Fatalf("capture matches no grammar entry: %s", raw)
		}
		if err = validateMuseRecord(raw); err != nil {
			t.Fatal(err)
		}
		m := v.(map[string]any)
		if children, ok := m["children"].([]any); ok {
			for _, c := range children {
				visit([]byte(c.(map[string]any)["record_json"].(string)))
			}
		}
	}
	for _, version := range []string{"1.4.1", "1.4.7", "1.4.10"} {
		for _, record := range museVersionCorpus(t, version) {
			visit(record)
		}
	}
	for _, record := range muse147SyntheticShapes(t) {
		visit(record)
	}
	for key, entries := range museGrammar {
		for i := range entries {
			if !seen[fmt.Sprintf("%s/%d", key, i)] {
				t.Errorf("uncaptured grammar entry %s/%d", key, i)
			}
		}
	}
	t.Logf("every capture or synthetic witness matches at least one entry; %d entries witnessed", len(seen))
}

// A bounded deterministic generator traverses every captured object/array.
// Seed controls the visit order, not coverage. Each object field is required.
func TestMusePTYClosedGrammarGeneratedNegatives(t *testing.T) {
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	records := museCorpus(t)
	rng := rand.New(rand.NewSource(14145031))
	rng.Shuffle(len(records), func(i, j int) { records[i], records[j] = records[j], records[i] })
	deadline := time.Now().Add(2 * time.Minute)
	count := 0
	capturedAlternatives := 0
	// r5: wrapper-attack skips, keyed by walk path. Pinned to an exact set
	// after the walk: a new zero-required grammar row fails loudly.
	skippedWrappers := map[string]bool{}
	check := func(value any, attack string) {
		t.Helper()
		if time.Now().After(deadline) {
			t.Fatal("bounded property deadline exceeded")
		}
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err = validateMuseRecord(raw); err == nil {
			t.Fatalf("grammar admitted %s: %s", attack, raw)
		}
		if err = os.WriteFile(h.snapshot.LogPath, append(append([]byte(nil), base...), append(raw, '\n')...), 0600); err != nil {
			t.Fatal(err)
		}
		assertMuseNoInput(t, h, home)
		count++
	}
	// Deduplicate identical shape entries. Completeness above still loads every
	// captured record; this property attacks every field of each distinct shape.
	seen := map[string]bool{}
	var visitRecord func(json.RawMessage)
	visitRecord = func(raw json.RawMessage) {
		var root map[string]any
		if err := json.Unmarshal(raw, &root); err != nil {
			t.Fatal(err)
		}
		key := museGrammarKey(root)
		decoded, err := decodeMuseRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		entry := ""
		for i, shape := range museGrammar[key] {
			if museShapeMatches(shape, decoded) {
				entry = fmt.Sprintf("%s/%d", key, i)
			}
		}
		if seen[entry] {
			return
		}
		seen[entry] = true
		// r5: the admitting variant selects wrapper-attack skips below,
		// exactly as decodeMuseLogRecord admits: the first shape that
		// matches with no known-field violation. A nil candidate (no
		// clean match) keeps every attack, preserving the pre-r5 behavior.
		shapes := museGrammar[key]
		var candidate *museShape
		for i := range shapes {
			if !museShapeMatches(shapes[i], decoded) {
				continue
			}
			if violated, _ := museCandidateKnownViolation(shapes, shapes[i], decoded, nil); violated {
				continue
			}
			candidate = &shapes[i]
			break
		}
		var walk func(any, string)
		walk = func(value any, path string) {
			switch v := value.(type) {
			case map[string]any:
				// L3G required versus additive (regression): an
				// undeclared member is additive and tolerated; the exact
				// closed refusal is superseded. Admission is asserted
				// here; end-to-end additive delivery is pinned by
				// TestAdditiveOptionalFieldTolerated.
				v["unknown_field"] = true
				additive, _ := json.Marshal(root)
				if strings.HasPrefix(key, "@frame/") {
					check(root, path+"/unknown-frame-member")
				} else if err := validateMuseRecord(additive); err != nil {
					t.Fatalf("grammar refused additive %s: %v", path, err)
				}
				delete(v, "unknown_field")
				keys := make([]string, 0, len(v))
				for k := range v {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					original := v[k]
					delete(v, k)
					dropped, _ := json.Marshal(root)
					if validateMuseRecord(dropped) != nil {
						check(root, path+"/drop/"+k)
					} else {
						capturedAlternatives++
					}
					v[k] = original
					v[k] = []any{"wrong_type"}
					if _, ok := original.([]any); ok {
						v[k] = false
					}
					check(root, path+"/type/"+k)
					v[k] = original
					if k == "schema_version" || k == "payload_schema_version" || k == "frame_schema_version" {
						v[k] = 1000 + original.(float64)
						check(root, path+"/future-version/"+k)
						v[k] = original
					}
					if k == "kind" || k == "payload_type" {
						v[k] = "future_unknown_kind"
						check(root, path+"/kind/"+k)
						v[k] = original
					}
					if k == "payload" {
						// Option A (r5 ruling on r4 §2 P1-P5): the wrapper
						// attack is vacuous where the admitting variant
						// declares ZERO required members at the attacked node
						// (fields:{}). Moving empty content under a key is
						// observationally additive — the known projection is
						// identical either way — so demanding refusal there
						// contradicts the held additive contract (design §6.2,
						// M56) and the test's own additive assertion one
						// statement below (r4 §2 P3). The skip keys on the
						// GRAMMAR node, never on the instance: a variant-0
						// record carrying extra additive members still skips,
						// while the sibling variant-1 record (inner payload
						// {kind}) still attacks and still refuses. Skipped
						// paths are pinned after the walk: a new
						// zero-required row fails loudly.
						if museGrammarPayloadRequiresNothing(candidate, museWrapperNodeSegments(key, path, k)) {
							skippedWrappers[path] = true
						} else {
							v[k] = map[string]any{"unknown_wrapper": original}
							check(root, path+"/wrapper")
							v[k] = original
						}
					}
					walk(original, path+"/"+k)
				}
			case []any:
				for i, x := range v {
					walk(x, fmt.Sprintf("%s/%d", path, i))
				}
			}
		}
		walk(root, key)
		if children, ok := root["children"].([]any); ok {
			for _, c := range children {
				visitRecord([]byte(c.(map[string]any)["record_json"].(string)))
			}
		}
	}
	for _, raw := range records {
		visitRecord(raw)
	}
	// r5: the skipped wrapper set is exact. The variant-0 retained inner
	// payload is the only fields:{} node in the grammar; a new
	// zero-required row — or a skip that stops firing — fails here, not
	// silently downstream.
	wantSkipped := map[string]bool{"runtime.retained_fact/retained_runtime_fact//payload/record": true}
	if !reflect.DeepEqual(skippedWrappers, wantSkipped) {
		t.Fatalf("skipped wrapper nodes = %q, want %q", museSortedKeys(skippedWrappers), museSortedKeys(wantSkipped))
	}
	t.Logf("seed=14145031; %d generated negatives refused through DeliverSessionNotice; %d shape entries", count, len(seen))
	t.Logf("%d field removals are another captured exact shape (not required across alternatives)", capturedAlternatives)
}

// museWrapperNodeSegments resolves the grammar-node segments of an attacked
// payload value: the walk path minus the record-key prefix, plus the key.
// Array indices pass through as segments; the grammar walk unions item
// shapes for them.
func museWrapperNodeSegments(key, path, leaf string) []string {
	rest := strings.TrimPrefix(path, key)
	var segs []string
	for _, s := range strings.Split(rest, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}
	return append(segs, leaf)
}

// museGrammarPayloadRequiresNothing reports whether every grammar shape
// reachable at segs under the admitting candidate is an object declaring
// zero required members (fields:{}). Arrays union their item shapes, so a
// mixed array keeps its attack. An undeclared field, a truncated descent,
// or an empty reachable set reports false (attack, as before): the skip
// fires only where the grammar proves the wrapper vacuous.
func museGrammarPayloadRequiresNothing(candidate *museShape, segs []string) bool {
	if candidate == nil {
		return false
	}
	current := []museShape{*candidate}
	for _, seg := range segs {
		var next []museShape
		for _, s := range current {
			switch s.Type {
			case "object":
				if f, ok := s.Fields[seg]; ok {
					next = append(next, f)
				}
			case "array":
				next = append(next, s.Items...)
			}
		}
		current = next
		if len(current) == 0 {
			return false
		}
	}
	for _, s := range current {
		if s.Type != "object" || len(s.Fields) != 0 {
			return false
		}
	}
	return len(current) > 0
}

func museSortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// museRetainedTopLevelOrigin returns the first captured retained fact whose
// nested payload is the top_level_turn.run_origin shape — the variant the
// grammar binds to fields:{}. Selection is by the grammar's own
// payload_type literal, never by the instance content.
func museRetainedTopLevelOrigin(t *testing.T) map[string]any {
	t.Helper()
	for _, raw := range museCorpus(t) {
		value, err := decodeMuseRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		record, ok := value.(map[string]any)
		if !ok || record["payload_type"] != "runtime.retained_fact" {
			continue
		}
		payload, ok := record["payload"].(map[string]any)
		if !ok {
			continue
		}
		nested, ok := payload["record"].(map[string]any)
		if !ok || nested["payload_type"] != "top_level_turn.run_origin" {
			continue
		}
		return record
	}
	t.Fatal("no captured top_level_turn.run_origin retained record")
	return nil
}

// TestMusePTYClosedGrammarEmptyPayloadWrapperAdmittedAndIgnored is the r5
// positive companion to the generator's wrapper-attack skip: at the single
// zero-required payload node, {"unknown_wrapper":{}} is ADMITTED, and the
// unknown member is IGNORED — dropped before the fold with only the
// additive count retained. Production entries: decodeMuseLogRecord (the
// single decode boundary) and ValidateMuseStartupPrefix (the startup
// readiness verdict).
func TestMusePTYClosedGrammarEmptyPayloadWrapperAdmittedAndIgnored(t *testing.T) {
	base := museRetainedTopLevelOrigin(t)
	raw, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	want, err := decodeMuseLogRecord(raw)
	if err != nil {
		t.Fatalf("captured empty-payload retained record refused: %v", err)
	}
	inner := base["payload"].(map[string]any)["record"].(map[string]any)
	wrapped := museCloneRecord(t, base)
	wrapped["payload"].(map[string]any)["record"].(map[string]any)["payload"] = map[string]any{"unknown_wrapper": inner["payload"]}
	changed, err := json.Marshal(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeMuseLogRecord(changed)
	if err != nil {
		t.Fatalf("zero-required wrapper refused: %v", err)
	}
	if got.AdditiveFields != want.AdditiveFields+1 {
		t.Fatalf("additive count=%d want=%d", got.AdditiveFields, want.AdditiveFields+1)
	}
	got.AdditiveFields = want.AdditiveFields
	// The whole decoded record — known tree, forwarded Payload bytes, and
	// every envelope identity — is identical: the unknown member is not
	// interpreted, not forwarded, and not surfaced as content or identity.
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("unknown wrapper member crossed the decode boundary")
	}
	// The startup readiness verdict is identical with and without the wrapper.
	if err := ValidateMuseStartupPrefix(l3gLogLines(t, base, base, base)); err != nil {
		t.Fatalf("base readiness: %v", err)
	}
	if err := ValidateMuseStartupPrefix(l3gLogLines(t, wrapped, wrapped, wrapped)); err != nil {
		t.Fatalf("wrapped readiness: %v", err)
	}
}

func museTestScreen(text string) string {
	upper := "── Voice input (⌥ + v to start) "
	return "\x1b[1;1H\x1b[J" + upper + strings.Repeat("─", 120-len([]rune(upper))) + "\r\n❯ " + strings.ReplaceAll(text, "\n", "\r\n  ") + "\r\n" + strings.Repeat("─", 120) + "\r\n  echo · ~ · Auto-review\r\n"
}

func TestMusePTYClosedScreenUnknownFooterRefuses(t *testing.T) {
	h, home := musePeer(t, "idle")
	h.mu.Lock()
	h.vt.observe([]byte(strings.Replace(museTestScreen(""), "  echo · ~ · Auto-review", "  Future decision surface", 1)))
	h.mu.Unlock()
	assertMuseNoInput(t, h, home)
}

func TestMusePTYCapturedVTControlsKnown(t *testing.T) {
	paths, err := filepath.Glob("testdata/muse-1.4.1/*.vt")
	if err != nil || len(paths) == 0 {
		t.Fatalf("missing VT captures: %v", err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			vt := newMuseVT(40, 120)
			vt.observe(raw)
			if vt.unknown || len(vt.pending) > 0 {
				t.Fatalf("captured VT is unknown: pending=%q", vt.pending)
			}
		})
	}
}

func TestMusePTYUnknownModeRefuses(t *testing.T) {
	h, home := musePeer(t, "idle")
	h.mu.Lock()
	h.vt.observe([]byte("\x1b[?9999h"))
	h.mu.Unlock()
	assertMuseNoInput(t, h, home)
}

func TestMusePTYLiteralRuleRoundTrip(t *testing.T) {
	h, home := musePeer(t, "unboxed")
	text := "literal separator\n──\nlast line"
	if err := h.DeliverSessionNotice(context.Background(), text); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(home, "peer-submit"))
	if err != nil || string(b) != "1:"+text {
		t.Fatalf("literal altered: %q %v", b, err)
	}
}

func TestMusePTYPartialMetadataFieldRefuses(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		if record["payload_type"] != "runtime.session.metadata" {
			continue
		}
		delete(record["payload"].(map[string]any)["record"].(map[string]any), "provider_id")
		appendMuseRecord(t, h, museOwnedRecord(h, record))
		assertMuseNoInput(t, h, home)
		return
	}
	t.Fatal("no captured metadata control")
}

func TestMusePTYRealLiteralRuleRoundTrip(t *testing.T) {
	h, ctx := museRealEchoHost(t, "")
	text := "literal separator\n──\nlast line"
	r, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: text})
	if err != nil || r == nil || r.Status != sm.ProviderRunIdle || r.Output != "echo: "+text {
		t.Fatalf("real literal output: %+v %v", r, err)
	}
}

func TestMusePTYUnknownVTFieldsRefuse(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"color_out_of_range", "\x1b[38;5;256m"}, {"color_wrong_type", "\x1b[38;5;+1m"},
		{"unknown_sgr_kind", "\x1b[999m"}, {"cursor_out_of_range", "\x1b[99;999H"},
		{"unknown_c0", "\x01"}, {"uncaptured_backspace", "\b"}, {"uncaptured_tab", "\t"}, {"unknown_osc", "\x1b]999;decision\a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte(tc.raw))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
}

func TestMusePTYPartialAppendWaitsForCompleteRecord(t *testing.T) {
	h, home := musePeer(t, "partial-log")
	for _, name := range []string{musePeerStartedEmit, musePeerStartedAck, musePeerPartialHalf, musePeerPartialAck} {
		if err := syscall.Mkfifo(filepath.Join(home, name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Two rendezvous sequence the partial append with RunOwner's
	// polls, so the partial window is handshakes, not sleeps, and a
	// RunOwner poll provably observes the partial tail before the
	// rest of the record arrives:
	//
	//   1. The peer emits started and waits. We verify the started
	//      record complete on disk, drain stale folds and count four
	//      fresh deliver folds before acking: the fourth read lands
	//      strictly after the started write (20ms poll spacing
	//      against sub-millisecond write/signal skew), so deliver
	//      provably observes the started run while the log is still
	//      complete and returns before the half lands. Without this
	//      wait the half can land first, deliver never returns until
	//      the rest arrives, and no RunOwner poll ever sees partial.
	//   2. The peer writes the first half and waits. We verify the
	//      partial tail on disk, drain and count four fresh RunOwner
	//      polls before acking: the fourth read lands strictly after
	//      the half write (25ms poll spacing) while the peer is
	//      still blocked, so that poll provably observes the
	//      partial tail.
	//
	// The helper goroutine reports through ackErr (never t.Fatal:
	// FailNow outside the test goroutine cannot end the test). Its
	// fold waits carry the watchdog bound; its FIFO opens block
	// until the peer rendezvous and leak past test end only on the
	// failure path, where RunOwner's context already bounds the
	// test. The test passes in every timing regime; the
	// counts above are what make the partial-tail observation deterministic.
	ackErr := make(chan error, 1)
	seqCtx, seqCancel := context.WithTimeout(t.Context(), museScriptedWatchdog)
	defer seqCancel()
	go func() {
		ackErr <- musePartialSequence(seqCtx, h, home)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "wait for durable append"})
	if err != nil || result == nil || result.Status != sm.ProviderRunIdle || result.Output != "peer reply" {
		t.Fatalf("partial append became an outcome: %+v %v", result, err)
	}
	if err := <-ackErr; err != nil {
		t.Fatalf("partial handshake: %v", err)
	}
}

// musePartialSequence runs the started/half rendezvous for
// TestMusePTYPartialAppendWaitsForCompleteRecord and reports the
// first failure. It runs on a helper goroutine: no t.Fatal.
func musePartialSequence(ctx context.Context, h *musePTYHost, home string) error {
	if err := musePartialRendezvousWait(home, musePeerStartedEmit); err != nil {
		return err
	}
	if err := musePartialRequireStartedComplete(h); err != nil {
		return err
	}
	museDrainNotify(h.foldNotify)
	for i := 0; i < 4; i++ {
		if err := museWaitNotifyErr(ctx, h.foldNotify, "started-observed fold"); err != nil {
			return err
		}
	}
	if err := musePartialRendezvousAck(home, musePeerStartedAck); err != nil {
		return err
	}
	if err := musePartialRendezvousWait(home, musePeerPartialHalf); err != nil {
		return err
	}
	if err := musePartialRequirePartialTail(h); err != nil {
		return err
	}
	museDrainNotify(h.foldNotify)
	for i := 0; i < 4; i++ {
		if err := museWaitNotifyErr(ctx, h.foldNotify, "partial-observed fold"); err != nil {
			return err
		}
	}
	return musePartialRendezvousAck(home, musePeerPartialAck)
}

// musePartialRendezvousWait blocks until the peer signals name.
func musePartialRendezvousWait(home, name string) error {
	f, err := os.OpenFile(filepath.Join(home, name), os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	var one [1]byte
	_, _ = f.Read(one[:])
	_ = f.Close()
	return nil
}

// musePartialRendezvousAck releases the peer's wait on name.
func musePartialRendezvousAck(home, name string) error {
	f, err := os.OpenFile(filepath.Join(home, name), os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, _ = f.Write([]byte{1})
	_ = f.Close()
	return nil
}

// musePartialRequireStartedComplete proves the started rendezvous
// fired after a complete started record hit the disk: the tail is
// terminated and its last line is a started event.
func musePartialRequireStartedComplete(h *musePTYHost) error {
	data, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		return err
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return fmt.Errorf("started rendezvous without complete tail")
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	var last struct {
		Payload struct {
			Event struct {
				Kind string `json:"kind"`
			} `json:"event"`
		} `json:"payload"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		return fmt.Errorf("started rendezvous tail undecodable: %v", err)
	}
	if last.Payload.Event.Kind != "started" {
		return fmt.Errorf("started rendezvous tail is %q, want started", last.Payload.Event.Kind)
	}
	return nil
}

// musePartialRequirePartialTail proves the half rendezvous fired
// while the tail record is still incomplete on disk.
func musePartialRequirePartialTail(h *musePTYHost) error {
	data, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		return err
	}
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return fmt.Errorf("half rendezvous without partial tail")
	}
	return nil
}
