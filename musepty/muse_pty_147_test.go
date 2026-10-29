package musepty

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Replay the real captured log through the existing scripted PTY peer. The
// only changed fact is the route PID, which must name this child for discovery.
// The peer uses the existing terminal-query handshake and blocks on input;
// no sleeps, polling helpers or mutable test seams are needed.
func muse147StartupPeer(home string, reader *bufio.Reader) {
	data, err := os.ReadFile(filepath.Join(home, "peer-capture147"))
	if err != nil {
		panic(err)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'})
	var route map[string]any
	if err := json.Unmarshal(lines[2], &route); err != nil {
		panic(err)
	}
	route["payload"].(map[string]any)["record"].(map[string]any)["pid"] = os.Getpid()
	lines[2], err = json.Marshal(route)
	if err != nil {
		panic(err)
	}
	id := route["stream"].(map[string]any)["id"].(string)
	dir := filepath.Join(os.Getenv("XDG_DATA_HOME"), "muse/sessions/2026/10/13", id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "session.jsonl"), append(bytes.Join(lines, []byte{'\n'}), '\n'), 0600); err != nil {
		panic(err)
	}
	for {
		if _, err := reader.ReadByte(); err != nil {
			return
		}
	}
}

func TestMusePTY147CapturedStartupThroughHost(t *testing.T) {
	plan, home := musePeerPlan(t, "capture147", "")
	if err := os.WriteFile(filepath.Join(home, "peer-capture147"), muse147Fixture(t, "session.jsonl"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), museScriptedWatchdog)
	defer cancel()
	h, err := StartMusePTYHost(ctx, plan)
	if h != nil {
		t.Cleanup(func() { _ = h.Stop(context.Background()) })
	}
	if err != nil {
		t.Fatal(err)
	}
	f, err := h.fold()
	if err != nil || f == nil || !f.Opened || !f.StartupPrefixWitnessed || f.PID != h.snapshot.PID || h.NativeIdentity() != f.SessionID {
		t.Fatalf("host did not admit the captured startup: fold=%+v err=%v", f, err)
	}
}

func muse147Fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata/muse-1.4.7", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMusePTY147CaptureSHA256(t *testing.T) {
	for file, want := range map[string]string{
		"startup-echo.raw":                "eb61ef83884d4dba01e958164cd6bba43e5bb85b50af4c0f559c05d0e892719b",
		"corpus.json":                     "d25cf0ffc807e9528ad57fa972ae0c09c40b2b281b0d00920dda076181b8db07",
		"screen-after-submitted-turn.txt": "e4c2cec5cc0c6d568565ab767204bb4275fd48b24bd6e238a79e2755e0579dbd",
		"screen.txt":                      "363fc7f56ad8695dc3994b7fae4bcea42cd2445cd28544e0b4b9ac040892b948",
		"screen-meta-startup-plain.txt":   "086fc23bf3eb096df7e3eb77e2ff33a8bbc135e36777083b35fd29e61b7b4474",
		"screen-meta-startup-sgr.txt":     "8998bdef7667982618f2a6936b3f24e9cff1f19b417df4de7ec584932491070b",
		"screen_rows.json":                "d7010cae249eb543cfd995d7d18eea03375fafe2b7f59c132cc4223d8afd66ee",
		"session.jsonl":                   "0e639cc6429e42bc3950e8fa27d9a0c0ddeca3f5dc386c96fe17bd88e1034cd8",
		"submitted-tree/2026/10/14/01a136e3-960b-7b53-be21-9cfa1cd0ed9b/session.jsonl":                                               "0ecfa66101d7c2a39e6ec2436e6c85a9c91a956d783f48bd715759098454f970",
		"submitted-tree/2026/10/14/01a136e3-960b-7b53-be21-9cfa1cd0ed9b/subagent/2d8634f0-b75d-4816-9296-1293adcdea00/session.jsonl": "c03ee7e7173d7c8796f4e2a1b8f74f648e38916f9fecc020e9bbea269fee35dc",
		"submitted-tree/2026/10/14/01a136e3-960b-7b53-be21-9cfa1cd0ed9b/subagent/a2c39179-3089-4810-9c40-139151c60b90/session.jsonl": "43145cd45ae7d35a34d2bf74c4367c31591156d827338ce6bb7f2b10916df4ce",
		"synthetic-meta-shapes.json": "b052a71cb7245fd2076c78f6d347d98ee75c511840632406c018ca771630a68b",
	} {
		if got := fmt.Sprintf("%x", sha256.Sum256(muse147Fixture(t, file))); got != want {
			t.Errorf("%s sha256=%s want=%s", file, got, want)
		}
	}
	// The versioned corpus loader must read the same records as the raw log.
	raw := bytes.Split(bytes.TrimSuffix(append(muse147Fixture(t, "session.jsonl"), muse147Fixture(t, muse147SubmittedLog)...), []byte{'\n'}), []byte{'\n'})
	corpus := museVersionCorpus(t, "1.4.7")
	if len(raw) != len(corpus) {
		t.Fatal("corpus/log length mismatch")
	}
	for i := range raw {
		a, err := decodeMuseRecord(raw[i])
		if err != nil {
			t.Fatal(err)
		}
		b, err := decodeMuseRecord(corpus[i])
		if err != nil {
			t.Fatal(err)
		}
		aj, _ := json.Marshal(a)
		bj, _ := json.Marshal(b)
		if !bytes.Equal(aj, bj) {
			t.Fatalf("corpus/log record %d mismatch", i+1)
		}
	}
}

func TestMusePTY147StartupGateAndPIDDiscovery(t *testing.T) {
	path := "testdata/muse-1.4.7/session.jsonl"
	data := muse147Fixture(t, "session.jsonl")
	if err := ValidateMuseStartupPrefix(data); err != nil {
		t.Fatal(err)
	}
	f, err := readMuseLog(path)
	if err != nil {
		t.Fatal(err)
	}
	// Exactly the host's startup admission predicate, after the real fold.
	if !f.Opened || !f.StartupPrefixWitnessed || f.PID != 78801 || !f.idle() {
		t.Fatalf("captured startup not admitted: %+v", f)
	}
	root := t.TempDir()
	owned := filepath.Join(root, "2026/10/13", f.SessionID, "session.jsonl")
	if err := os.MkdirAll(filepath.Dir(owned), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(owned, data, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := findMuseLog(root, f.PID)
	if err != nil || got != owned {
		t.Fatalf("find owned log=%q err=%v", got, err)
	}
	if _, err := findMuseLog(root, f.PID+1); err == nil {
		t.Fatal("foreign PID admitted")
	}
}

func TestMusePTY141StartupGateStillAdmits(t *testing.T) {
	// The first complete historical stream in the real corpus contains the
	// version-1 permission frame and the session-name task lifecycle.
	corpus := museCorpus(t)
	var records [][]byte
	for _, raw := range corpus[:17] {
		var compact bytes.Buffer
		if err := json.Compact(&compact, raw); err != nil {
			t.Fatal(err)
		}
		records = append(records, compact.Bytes())
	}
	data := bytes.Join(records, []byte{'\n'})
	data = append(data, '\n')
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := readMuseLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Opened || !f.StartupPrefixWitnessed {
		t.Fatalf("historical startup refused: %+v", f)
	}
}

func muse147ChangedRecord(t *testing.T, line int, change func(map[string]any)) []byte {
	t.Helper()
	lines := bytes.Split(bytes.TrimSuffix(muse147Fixture(t, "session.jsonl"), []byte{'\n'}), []byte{'\n'})
	var r map[string]any
	if err := json.Unmarshal(lines[line], &r); err != nil {
		t.Fatal(err)
	}
	change(r)
	var err error
	lines[line], err = json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return append(bytes.Join(lines, []byte{'\n'}), '\n')
}

func TestMusePTY147ClosedLogNegativeControls(t *testing.T) {
	for _, tc := range []struct {
		name   string
		line   int
		change func(map[string]any)
	}{
		{"unknown_record_type", 3, func(r map[string]any) { r["payload_type"] = "session.future.observed" }},
		{"unknown_frame_member", 0, func(r map[string]any) { r["future_frame_member"] = true }},
		{"unknown_frame_child_member", 0, func(r map[string]any) { r["children"].([]any)[0].(map[string]any)["future_child_member"] = true }},
		{"future_frame_version", 0, func(r map[string]any) { r["frame_schema_version"] = float64(2) }},
		{"future_capture_version", 6, func(r map[string]any) { r["payload_schema_version"] = float64(2) }},
		{"unknown_capture_member", 6, func(r map[string]any) { r["payload"].(map[string]any)["future_capture_member"] = true }},
		{"unknown_branch_member", 3, func(r map[string]any) {
			r["payload"].(map[string]any)["record"].(map[string]any)["future_branch_member"] = true
		}},
		{"unknown_opened_member", 4, func(r map[string]any) {
			r["payload"].(map[string]any)["record"].(map[string]any)["future_opened_member"] = true
		}},
		{"foreign_capture_session", 6, func(r map[string]any) {
			r["payload"].(map[string]any)["session_stream"].(map[string]any)["id"] = museTestUUID("foreign-capture-session")
		}},
		{"foreign_capture_publication", 6, func(r map[string]any) {
			r["payload"].(map[string]any)["publication_id"] = museTestUUID("foreign-publication")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "session.jsonl")
			if err := os.WriteFile(path, muse147ChangedRecord(t, tc.line, tc.change), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readMuseLog(path); err == nil {
				t.Fatal("unknown or foreign shape admitted")
			}
		})
	}
}

func muse147Screen(t *testing.T, rows []string) *museVT {
	t.Helper()
	v := newMuseVT(len(rows)+1, 120)
	// Feed the captured still through the production terminal emulator.
	v.observe([]byte(strings.Join(rows, "\r\n")))
	if v.unknown || len(v.pending) != 0 {
		t.Fatal("captured screen contains unknown VT state")
	}
	return v
}

func TestMusePTY147CapturedScreenGrammar(t *testing.T) {
	var harvest struct {
		Screens []struct {
			Name string   `json:"name"`
			Rows []string `json:"rows"`
		} `json:"screens"`
	}
	if err := json.Unmarshal(muse147Fixture(t, "screen_rows.json"), &harvest); err != nil {
		t.Fatal(err)
	}
	for _, screen := range harvest.Screens {
		t.Run(screen.Name, func(t *testing.T) {
			v := muse147Screen(t, screen.Rows)
			expected := ""
			if screen.Name == "unsubmitted_composer" {
				expected = "harvest probe line"
			}
			want := screen.Name != "tmux-noflag-01"
			if got := v.safeComposer(expected, nil); got != want {
				t.Fatalf("safeComposer=%v want=%v", got, want)
			}
			if expected != "" && v.safeComposer("", nil) {
				t.Fatal("unsubmitted draft admitted as idle")
			}
		})
	}
}

func TestMusePTY147UnknownScreenRowRefuses(t *testing.T) {
	rows := strings.Split(strings.TrimSuffix(string(muse147Fixture(t, "screen.txt")), "\n"), "\n")
	for _, tc := range []struct{ name, row string }{
		{"unknown_row", "◈ Future action required"},
		{"unknown_notice", "  Including your Claude Code personal rules and 6 skills — future action required."},
		{"unknown_footer", "  echo · ~ · Future-review"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := append([]string(nil), rows...)
			at := 3
			if tc.name == "unknown_footer" {
				at = 9
			}
			changed[at] = tc.row
			if muse147Screen(t, changed).safeComposer("harvest probe line", nil) {
				t.Fatal("unknown screen row admitted")
			}
		})
	}
}
