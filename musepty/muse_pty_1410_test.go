package musepty

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const muse1410Dir = "testdata/muse-1.4.10"

func muse1410Fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(muse1410Dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func muse1410Lines(t *testing.T, name string) []string {
	t.Helper()
	return strings.Split(strings.TrimSuffix(string(muse1410Fixture(t, name)), "\n"), "\n")
}

// muse1410Fold folds a log written to a temp file from the given lines.
func muse1410Fold(t *testing.T, lines []string) (*museLogFold, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return readMuseLog(path)
}

// muse1410Seq returns the index of the record line with the given sequence.
func muse1410Seq(t *testing.T, lines []string, seq string) int {
	t.Helper()
	for i, line := range lines {
		if strings.Contains(line, `"sequence":`+seq+`,`) {
			return i
		}
	}
	t.Fatalf("no record with sequence %s", seq)
	return -1
}

// Claim: the checked-in 1.4.10 captures are exactly the reviewed bytes.
func TestMusePTY1410CaptureSHA256(t *testing.T) {
	want := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(muse1410Fixture(t, "SHA256SUMS"))), "\n") {
		digest, name, ok := strings.Cut(line, "  ")
		if !ok {
			t.Fatalf("malformed SHA256SUMS line %q", line)
		}
		want[name] = digest
	}
	pinned := map[string]string{
		"corpus.json":                      "19d4eed1108b99afa36a0091d8ac93f42ff52871b41d52714498b729eb309bb7",
		"resume.jsonl":                     "e4c166df8895d39fb2554a174444c6cccfada37f17f02766b18a7c54ecb3d94f",
		"screen-after-submitted-turn.txt":  "af9d1cd6a9baaaa8a4719dcbdd3155e52c880bee926b5c0f4bb8341bf23278fc",
		"screen-rules-warning-startup.txt": "6321d7d4709e335543f24945d794615ccb2b2dc2b3ccdbfcbaabb5077d6644e4",
		"screen.txt":                       "2f74d6f9f62eb93125fee20c542785aecaf3eea3c77afa50c23edc13c3335afd",
		"session.jsonl":                    "be6d4b48b2cf883e7276f56217cffaedc96840cb33c56d915064b2b39d2a981b",
	}
	if len(want) != len(pinned) {
		t.Fatalf("SHA256SUMS lists %d files, pinned %d", len(want), len(pinned))
	}
	for name, digest := range pinned {
		sum := sha256.Sum256(muse1410Fixture(t, name))
		if got := hex.EncodeToString(sum[:]); got != digest || want[name] != digest {
			t.Errorf("%s: sha256 %s, SHA256SUMS %s, pinned %s", name, got, want[name], digest)
		}
	}
}

// Claim: no 1.4.10 fixture carries operator paths, names or private context.
func TestMusePTY1410FixturesContainNoPrivateOperatorValues(t *testing.T) {
	username := regexp.MustCompile(`(?i)\biv\b`)
	files := 0
	err := filepath.WalkDir(muse1410Dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files++
		text := strings.ToLower(path + "\n" + string(data))
		for _, marker := range []string{"/users/", "/private/", "/var/folders/", "relux", "agents-infra", "~/.agents", "$home/.agents", "model_input_trace", "model_request_configured"} {
			if strings.Contains(text, marker) {
				t.Errorf("%s contains forbidden marker %q", path, marker)
			}
		}
		if username.MatchString(text) {
			t.Errorf("%s contains the operator's home-directory username", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Fatal("privacy scan found no fixtures")
	}
}

// Claim: the 1.4.10 startup log and the clean end-then-resume log fold
// through production, open the owned session and settle idle. Positive
// control for the refusal table below.
func TestMusePTY1410StartupAndResumeAdmission(t *testing.T) {
	for _, name := range []string{"session.jsonl", "resume.jsonl"} {
		t.Run(name, func(t *testing.T) {
			f, err := readMuseLog(filepath.Join(muse1410Dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if !f.Opened || f.Ended || !f.StartupPrefixWitnessed || !f.idle() || f.SessionStartPending {
				t.Fatalf("opened=%v ended=%v prefix=%v idle=%v pending=%v", f.Opened, f.Ended, f.StartupPrefixWitnessed, f.idle(), f.SessionStartPending)
			}
		})
	}
}

// Claim: the session-start command is admitted only before the first open
// or after an end, and its intake_v3/started records only for that pending
// command, once each. Each case changes one captured fact; the unmutated
// logs are admitted above.
func TestMusePTY1410SessionStartRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, log, want string
		mutate          func(t *testing.T, lines []string) []string
	}{
		{"start_triple_inside_open_epoch", "session.jsonl", "outside a startup or resume", func(t *testing.T, lines []string) []string {
			// Move the whole start triple (seq 5, 6, 7) after the open
			// (seq 8): the intake now arrives inside an open epoch.
			i := muse1410Seq(t, lines, "5")
			triple := append([]string(nil), lines[i:i+3]...)
			lines = append(lines[:i:i], lines[i+3:]...)
			j := muse1410Seq(t, lines, "8")
			return append(lines[:j+1:j+1], append(triple, lines[j+1:]...)...)
		}},
		{"second_start_after_resume_open", "resume.jsonl", "outside a startup or resume", func(t *testing.T, lines []string) []string {
			// A fresh session-start intake after the resumed epoch
			// opened: new envelope, command and sequence.
			i := muse1410Seq(t, lines, "138")
			extra := strings.Replace(lines[i], `"sequence":138,`, `"sequence":190,`, 1)
			extra = regexp.MustCompile(`"(id|command_id|causation_id)":"01a14d3e-24e8-7230-[0-9a-f-]+"`).ReplaceAllStringFunc(extra, func(m string) string {
				return m[:strings.Index(m, ":")+1] + `"01a14d3e-ffff-7000-8000-000000000003"`
			})
			return append(lines, extra)
		}},
		{"started_names_other_command", "session.jsonl", "names no pending session start", func(t *testing.T, lines []string) []string {
			i := muse1410Seq(t, lines, "7")
			lines[i] = regexp.MustCompile(`"command_id":"[0-9a-f-]+"`).ReplaceAllString(lines[i], `"command_id":"01a14d34-0000-7000-8000-000000000000"`)
			return lines
		}},
		{"intake_v3_after_open", "session.jsonl", "names no pending session start", func(t *testing.T, lines []string) []string {
			i := muse1410Seq(t, lines, "6")
			intake := lines[i]
			lines = append(lines[:i:i], lines[i+1:]...)
			j := muse1410Seq(t, lines, "8")
			return append(lines[:j+1:j+1], append([]string{intake}, lines[j+1:]...)...)
		}},
		{"started_twice", "session.jsonl", "recorded twice", func(t *testing.T, lines []string) []string {
			i := muse1410Seq(t, lines, "7")
			dup := strings.Replace(lines[i], `"sequence":7,`, `"sequence":70,`, 1)
			dup = regexp.MustCompile(`^\{"schema_version":1,"id":"[0-9a-f-]+"`).ReplaceAllString(dup, `{"schema_version":1,"id":"01a14d34-0000-7000-8000-000000000001"`)
			return append(lines[:i+1:i+1], append([]string{dup}, lines[i+1:]...)...)
		}},
		{"started_with_previous_stream", "session.jsonl", "session.started", func(t *testing.T, lines []string) []string {
			i := muse1410Seq(t, lines, "7")
			lines[i] = strings.Replace(lines[i], `"previous_session_stream":null`, `"previous_session_stream":{"kind":"session","id":"01a14d34-0000-7000-8000-000000000002"}`, 1)
			return lines
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines := tc.mutate(t, muse1410Lines(t, tc.log))
			_, err := muse1410Fold(t, lines)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("fold error %v, want %q", err, tc.want)
			}
		})
	}
}

func muse1410IdleScreen(t *testing.T, rows []string, logPath string) bool {
	t.Helper()
	f, err := readMuseLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	v := muse147Screen(t, rows)
	lastOutput := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	now := lastOutput.Add(musePTYQuiet)
	h := &musePTYHost{vt: v, exited: make(chan struct{}), snapshot: musePTYSnapshot{LogPath: logPath, SessionID: f.SessionID, PID: f.PID}, lastOutput: lastOutput, testFakeNow: &now}
	return v.safeComposer("", f) && h.idle("")
}

// Claim: the captured 1.4.10 stills, including the voice-input composer
// border and the soft-wrapped live-HOME rules warning, pass the production
// screen and host idle predicates with the captured idle startup fold.
func TestMusePTY1410ScreensIdle(t *testing.T) {
	logPath := filepath.Join(muse1410Dir, "session.jsonl")
	for _, name := range []string{"screen.txt", "screen-after-submitted-turn.txt", "screen-rules-warning-startup.txt"} {
		t.Run(name, func(t *testing.T) {
			if !muse1410IdleScreen(t, muse1410Lines(t, name), logPath) {
				t.Fatal("captured 1.4.10 still refused")
			}
		})
	}
}

// Claim: the rules warning is judged on its rejoined sentence and its place
// directly after the banner, never row by row. Positive controls: the
// capture and the same sentence rewrapped at another split point.
func TestMusePTY1410RulesWarningClosed(t *testing.T) {
	logPath := filepath.Join(muse1410Dir, "session.jsonl")
	captured := muse1410Lines(t, "screen-rules-warning-startup.txt")
	index := func(prefix string) int {
		for i, row := range captured {
			if strings.HasPrefix(row, prefix) {
				return i
			}
		}
		t.Fatalf("still has no row %q", prefix)
		return -1
	}
	warn, cont, notice := index("  warning: rules file at "), index("  startup context limit;"), index("  Including your Claude Code")
	edit := func(f func(rows []string) []string) []string {
		return f(append([]string(nil), captured...))
	}
	for _, tc := range []struct {
		name string
		rows []string
		want bool
	}{
		{"captured", captured, true},
		{"rewrapped", edit(func(rows []string) []string {
			rows[warn] = strings.TrimSuffix(rows[warn], " subagent delegation")
			rows[cont] = "  subagent delegation startup context limit; it will be truncated for this session"
			return rows
		}), true},
		{"changed_wording", edit(func(rows []string) []string {
			rows[cont] = "  startup context limit; it will not be truncated for this session"
			return rows
		}), false},
		{"extra_continuation", edit(func(rows []string) []string {
			return append(rows[:cont+1:cont+1], append([]string{"  and more"}, rows[cont+1:]...)...)
		}), false},
		{"warning_without_notice", edit(func(rows []string) []string {
			rows[notice] = "❯ hello"
			return rows
		}), false},
		{"warning_without_banner", edit(func(rows []string) []string {
			for i, row := range rows {
				if museIsChromeHeaderRow(row) {
					rows[i] = ""
				}
			}
			return rows
		}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := muse1410IdleScreen(t, tc.rows, logPath); got != tc.want {
				t.Fatalf("idle=%v, want %v", got, tc.want)
			}
		})
	}
}
