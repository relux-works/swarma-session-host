package musepty

// Revision r6 structural properties: one bounded deterministic property test
// per strict structure, generated from the real captures. Every case refuses
// through the production DeliverSessionNotice entry point. Seeds are fixed
// and logged; each test carries an explicit deadline.

import (
	"encoding/json"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

const museR6PropertySeed = 2602001

func museR6Deadline(t *testing.T, d time.Duration) func() {
	t.Helper()
	deadline := time.Now().Add(d)
	return func() {
		t.Helper()
		if time.Now().After(deadline) {
			t.Fatalf("bounded property deadline exceeded after %s", d)
		}
	}
}

// Structure 1: the strict record decoder. Whitespace, control and surrogate
// variants injected into captured shapes must refuse. DEL inside a string
// value is deliberately excluded: valid UTF-8 and legal JSON, it never
// reaches the screen model or the outbound paste path (standing r5 bound).
func TestMusePTYR6DecoderProperty(t *testing.T) {
	guard := museR6Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	restore := func() {
		t.Helper()
		if err := os.WriteFile(h.snapshot.LogPath, base, 0600); err != nil {
			t.Fatal(err)
		}
	}
	shapes := []string{"session.opened.observed", "started", "runtime.session.route_facts", "terminal"}
	edgeBytes := []string{"\x0b", "\x0c", "\u0085", "\u00a0", "\u2000", "\x00", "\x1b"}
	escapes := []string{`"\ud800"`, `"\udfff"`, `"\ud83d\u0041"`, `"\udc00"`, `"\ud83d"`, `"\ud83"`, `"\uzzzz"`, `"\q"`}
	rng := rand.New(rand.NewSource(museR6PropertySeed))
	count := 0
	refuse := func(raw []byte, what string) {
		t.Helper()
		guard()
		if err := validateMuseRecord(raw); err == nil {
			t.Fatalf("decoder admitted %s: %q", what, raw)
		}
		if err := os.WriteFile(h.snapshot.LogPath, append(append([]byte(nil), base...), append(raw, '\n')...), 0600); err != nil {
			t.Fatal(err)
		}
		assertMuseNoInput(t, h, home)
		restore()
		count++
	}
	for i := 0; i < 40; i++ {
		record := museOwnedRecord(h, museFixtureRecord(shapes[rng.Intn(len(shapes))]))
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		switch rng.Intn(4) {
		case 0: // Non-JSON whitespace or control at a line edge.
			edge := edgeBytes[rng.Intn(len(edgeBytes))]
			if rng.Intn(2) == 0 {
				raw = append([]byte(edge), raw...)
			} else {
				raw = append(raw, edge...)
			}
			refuse(raw, "edge bytes")
		case 1: // Invalid UTF-8 or unescaped control at an interior position.
			pos := 1 + rng.Intn(len(raw)-1)
			mut := append([]byte(nil), raw...)
			switch rng.Intn(3) {
			case 0:
				mut[pos] = 0xff
			case 1:
				mut[pos] = 0x00
			default:
				mut[pos] = 0x1b
			}
			refuse(mut, "interior control")
		case 2: // Surrogate or escape corruption of a string value.
			withSentinel := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
			withSentinel["payload"].(map[string]any)["record"].(map[string]any)["credential_backend"] = "SENTINEL"
			raw, err := json.Marshal(withSentinel)
			if err != nil {
				t.Fatal(err)
			}
			raw = []byte(strings.Replace(string(raw), `"SENTINEL"`, escapes[rng.Intn(len(escapes))], 1))
			refuse(raw, "escape corruption")
		default: // A second record glued to the line is not one record.
			second, err := json.Marshal(museOwnedRecord(h, museFixtureRecord("started")))
			if err != nil {
				t.Fatal(err)
			}
			refuse(append(append(raw, ' '), second...), "glued records")
		}
	}
	t.Logf("seed=%d; %d decoder mutations refused through DeliverSessionNotice", museR6PropertySeed, count)
}

// Structure 2: the explicit session state machine. Dropped, duplicated,
// reordered and cross-key lifecycle records must refuse: an unmatched start,
// a terminal without its exact key, or a foreign stream is unknown state.
func TestMusePTYR6FoldProperty(t *testing.T) {
	guard := museR6Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	runIDs := []string{museFixtureRecord("started")["payload"].(map[string]any)["run_id"].(string), museTestUUID("prop-run-1"), museTestUUID("prop-run-2")}
	taskIDs := []string{museCorpusTaskRecord(t, "started")["payload"].(map[string]any)["task_id"].(string), museTestUUID("prop-task-1")}
	emitRun := func(records *[][]byte, kind, run, stream string) {
		t.Helper()
		r := museOwnedRecord(h, museFixtureRecord(kind))
		r["payload"].(map[string]any)["run_id"] = run
		r["stream"].(map[string]any)["id"] = stream
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		*records = append(*records, raw)
	}
	emitTask := func(records *[][]byte, event, run, task, stream string) {
		t.Helper()
		r := museOwnedRecord(h, museCorpusTaskRecord(t, event))
		r["payload"].(map[string]any)["run_id"] = run
		r["payload"].(map[string]any)["task_id"] = task
		r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		r["stream"].(map[string]any)["id"] = stream
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		*records = append(*records, raw)
	}
	rng := rand.New(rand.NewSource(museR6PropertySeed + 1))
	count := 0
	for i := 0; i < 48; i++ {
		guard()
		owned := h.NativeIdentity()
		a := runIDs[rng.Intn(len(runIDs))]
		b := runIDs[rng.Intn(len(runIDs))]
		for b == a {
			b = runIDs[rng.Intn(len(runIDs))]
		}
		x := taskIDs[rng.Intn(len(taskIDs))]
		var records [][]byte
		switch rng.Intn(12) {
		case 0: // Duplicate run start, one terminal.
			emitRun(&records, "started", a, owned)
			emitRun(&records, "started", a, owned)
			emitRun(&records, "terminal", a, owned)
		case 1: // Terminal for a run that never started.
			emitRun(&records, "started", a, owned)
			emitRun(&records, "terminal", b, owned)
		case 2: // Terminal before its start.
			emitRun(&records, "terminal", a, owned)
			emitRun(&records, "started", a, owned)
		case 3: // Task terminal under another run.
			emitRun(&records, "started", a, owned)
			emitRun(&records, "started", b, owned)
			emitTask(&records, "started", a, x, owned)
			emitTask(&records, "completed", b, x, owned)
			emitRun(&records, "terminal", a, owned)
			emitRun(&records, "terminal", b, owned)
		case 4: // Terminal from a foreign stream.
			emitRun(&records, "started", a, owned)
			emitRun(&records, "terminal", a, museTestUUID("foreign-session"))
		case 5: // Start from a foreign stream.
			emitRun(&records, "started", a, museTestUUID("foreign-session"))
		case 6: // Dropped terminal leaves an active run.
			emitRun(&records, "started", a, owned)
		case 7: // Dropped task terminal leaves pending work.
			emitRun(&records, "started", a, owned)
			emitTask(&records, "started", a, x, owned)
			emitRun(&records, "terminal", a, owned)
		case 8: // Task terminal without its start.
			emitRun(&records, "started", a, owned)
			emitTask(&records, "completed", a, x, owned)
			emitRun(&records, "terminal", a, owned)
		case 9: // Duplicate task start under the same key.
			emitRun(&records, "started", a, owned)
			emitTask(&records, "started", a, x, owned)
			emitTask(&records, "started", a, x, owned)
		case 10: // Task under an unobserved run.
			emitRun(&records, "started", a, owned)
			emitTask(&records, "started", museTestUUID("unobserved-run"), x, owned)
		default: // Queued inbox item without its drain.
			emitRun(&records, "started", a, owned)
			q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
			raw, err := json.Marshal(q)
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, raw)
			emitRun(&records, "terminal", a, owned)
		}
		log := append([]byte(nil), base...)
		for _, raw := range records {
			log = append(append(log, raw...), '\n')
		}
		if err := os.WriteFile(h.snapshot.LogPath, log, 0600); err != nil {
			t.Fatal(err)
		}
		assertMuseNoInput(t, h, home)
		if err := os.WriteFile(h.snapshot.LogPath, base, 0600); err != nil {
			t.Fatal(err)
		}
		count++
	}
	t.Logf("seed=%d; %d fold mutations refused through DeliverSessionNotice", museR6PropertySeed+1, count)
}

// Structure 3: the full-region screen match. Unknown or busy rows inserted
// anywhere in the region, parentless indented rows (glyph-led or plain,
// the round-6 orphan class), and corrupted OSC payloads, must refuse.
func TestMusePTYR6ScreenProperty(t *testing.T) {
	guard := museR6Deadline(t, 3*time.Minute)
	unknownRows := []string{
		"◈ Future action required", "◆ Future decision surface", "◇ Future work",
		"• Future queue state", "◆ Working", "◇ Thinking (soon)", "Future action required",
		"❯", "◆ echo", "↳ bare hook row", "› 1 Decision surface",
		"To continue this session, run muse resume elsewhere",
	}
	// r7: the sequence grammar admits indented rows only past a captured
	// parent, so hostile indented rows render parentless (first, or past
	// the header as a non-resume row). The plain rows are the round-6
	// panel orphan shapes, refused like the glyph-led ones.
	orphanIndented := []string{
		"  ◈ Future action required", "  ◆ Working (2s · esc to interrupt)",
		"  ◇ Thinking (1s · esc to interrupt)", "  • Queued input", "  ❯ smuggled turn",
		"   ◈ deep unknown", "     • deep queue claim",
		"  Future action required", "  Waiting for remote approval", "  ↳ unseen queued item",
	}
	busyRows := []string{
		"◆ Working (2s · esc to interrupt)", "◇ Thinking (11s · esc to interrupt)",
		"◈ Thinking (0s · esc to interrupt)", "• Queued input",
	}
	contentRows := []string{
		"◆ echo: harmless tail", "❯ harvest delayed line", "  status detail",
		"  ↳ second harvest", "  Muse Code 1.4.1", "   deep continuation", "",
	}
	rng := rand.New(rand.NewSource(museR6PropertySeed + 2))
	h, home := musePeer(t, "idle")
	count := 0
	render := func(rows []string) {
		t.Helper()
		guard()
		frame := "\x1b[1;1H\x1b[J" + strings.Join(rows, "\r\n")
		if len(rows) > 0 {
			frame += "\r\n"
		}
		frame += strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(frame))
		h.mu.Unlock()
		assertMuseNoInput(t, h, home)
		count++
	}
	for i := 0; i < 24; i++ {
		var rows []string
		for _, row := range contentRows[:rng.Intn(3)] {
			rows = append(rows, row)
		}
		injected := unknownRows[rng.Intn(len(unknownRows))]
		if rng.Intn(2) == 0 {
			injected = busyRows[rng.Intn(len(busyRows))]
		}
		// Insert the hostile row anywhere: above, below, or between the
		// content rows.
		at := rng.Intn(len(rows) + 1)
		rows = append(rows, "")
		copy(rows[at+1:], rows[at:])
		rows[at] = injected
		render(rows)
	}
	for i := 0; i < 24; i++ {
		// A parentless indented row refuses wherever the surrounding
		// content sits: first with content below, or past the header
		// as a non-resume row.
		rows := append([]string(nil), contentRows[:rng.Intn(3)]...)
		injected := orphanIndented[rng.Intn(len(orphanIndented))]
		if rng.Intn(2) == 0 {
			rows = append([]string{injected}, rows...)
		} else {
			rows = append([]string{museHeaderRow, injected}, rows...)
		}
		render(rows)
	}
	// Corrupted OSC payloads: bad bytes at sampled positions of ASCII bases
	// (any of the six bad bytes refuses at any ASCII position), truncated
	// multibyte runes, and lengths past the bound. Unrecognised OSC numbers
	// refuse as well. Single-byte corruption of a multibyte base can yield
	// another valid printable scalar, so multibyte bases are corrupted by
	// truncation only; the live spinner rune title keeps its own positive
	// coverage in TestMusePTYSpinnerNeverSubmits.
	payloadAlphabet := []string{"muse", "spinner", "title-123", "a"}
	badBytes := []byte{0x00, 0x1b, 0x7f, 0x80, 0x9b, 0xff}
	for i := 0; i < 24; i++ {
		guard()
		payload := []byte(payloadAlphabet[rng.Intn(len(payloadAlphabet))])
		payload[rng.Intn(len(payload))] = badBytes[rng.Intn(len(badBytes))]
		raw := append(append([]byte("\x1b]0;"), payload...), '\a')
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(museTestScreen("")))
		h.vt.observe(raw)
		if !h.vt.unknown {
			h.mu.Unlock()
			t.Fatalf("corrupted OSC payload admitted: %q", payload)
		}
		h.mu.Unlock()
		assertMuseNoInput(t, h, home)
		count++
	}
	for _, payload := range []string{"\xe2\xa0", "\xe2", "⠋\xff", "muse\xff"} {
		guard()
		raw := append(append([]byte("\x1b]0;"), payload...), '\a')
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(museTestScreen("")))
		h.vt.observe(raw)
		if !h.vt.unknown {
			h.mu.Unlock()
			t.Fatalf("truncated OSC payload admitted: %q", payload)
		}
		h.mu.Unlock()
		assertMuseNoInput(t, h, home)
		count++
	}
	for i := 0; i < 12; i++ {
		guard()
		raw := append([]byte("\x1b]0;"), append([]byte(strings.Repeat("t", 4097+rng.Intn(512))), '\a')...)
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(museTestScreen("")))
		h.vt.observe(raw)
		if !h.vt.unknown {
			h.mu.Unlock()
			t.Fatal("overlong OSC title admitted")
		}
		h.mu.Unlock()
		assertMuseNoInput(t, h, home)
		count++
	}
	for _, osc := range []string{"\x1b]1;title\a", "\x1b]2;title\a", "\x1b]8;;https://example.invalid\a", "\x1b]99;x\a"} {
		guard()
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(museTestScreen("")))
		h.vt.observe([]byte(osc))
		if !h.vt.unknown {
			h.mu.Unlock()
			t.Fatalf("unrecognised OSC admitted: %q", osc)
		}
		h.mu.Unlock()
		assertMuseNoInput(t, h, home)
		count++
	}
	t.Logf("seed=%d; %d screen mutations refused through DeliverSessionNotice", museR6PropertySeed+2, count)
}
