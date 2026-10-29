package musepty

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestMusePTYBindableTimeoutDescribesScreen(t *testing.T) {
	const logPath = "testdata/muse-1.4.7/session.jsonl"
	fold, err := readMuseLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	fixture := strings.Split(strings.TrimSuffix(string(muse147Fixture(t, "screen-meta-startup-plain.txt")), "\n"), "\n")
	for _, tc := range []struct {
		name, path, draft string
	}{
		{"absolute", "/Users/example/private workspace", "private draft"},
		{"abbreviated", "/h/f/ws", "private draft"},
		{"home", "~/private workspace", "private draft"},
		{"ellipsis", "…/private workspace", "private draft"},
		{"empty_composer", "/h/f/ws", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append([]string(nil), fixture...)
			rows[4] = "❯ " + tc.draft
			rows[6] = "  muse-wonder-1.0 · max · " + tc.path + " · Full access"
			lastOutput := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
			now := lastOutput.Add(musePTYQuiet)
			h := &musePTYHost{
				vt: muse147Screen(t, rows), exited: make(chan struct{}),
				snapshot:   musePTYSnapshot{LogPath: logPath, SessionID: fold.SessionID, PID: fold.PID},
				lastOutput: lastOutput, testFakeNow: &now,
			}
			// The existing factory timeout seam drives the timeout branch on a
			// fixed screen. No sleeps, peers, or test-side polling are involved.
			factory := NewMusePTYHostFactory()
			factory.bindableTimeout = time.Nanosecond
			err := factory.waitBindable(context.Background(), h)
			if err == nil {
				t.Fatal("unknown footer settled idle")
			}
			text := err.Error()
			first, _, _ := strings.Cut(text, "\n")
			if first != "Muse PTY did not settle idle within 1ns: the goal bind needs a quiet composer" {
				t.Fatalf("timeout first line changed: %s", first)
			}
			wantPredicates := fmt.Sprintf("fold_idle=true quiet=true ready=false composer_match=%t footer_admitted=false", tc.draft == "")
			failed := "ready, composer_match, footer_admitted"
			if tc.draft == "" {
				failed = "ready, footer_admitted"
			}
			for _, want := range []string{
				wantPredicates, "vt_unknown=false pending=0", "failed=[" + failed + "]",
				"row 2 [chrome_header]:   Muse Code 1.4.7",
				fmt.Sprintf("row 5 [composer]: <composer:%d chars>", utf8.RuneCountInString(tc.draft)),
				"row 7 [unknown]:   muse-wonder-1.0 · max · <path> · Full access",
			} {
				if !strings.Contains(text, want) {
					t.Errorf("missing %q in diagnostic:\n%s", want, text)
				}
			}
			if strings.Contains(text, tc.path) || tc.draft != "" && strings.Contains(text, tc.draft) {
				t.Fatal("diagnostic leaked a path or composer content")
			}
			rowCount := 0
			for _, line := range strings.Split(text, "\n") {
				if strings.HasPrefix(line, "row ") {
					rowCount++
					_, rendering, _ := strings.Cut(line, "]: ")
					if utf8.RuneCountInString(rendering) > 120 {
						t.Errorf("rendering exceeds 120 characters: %s", line)
					}
				}
			}
			if rowCount != 5 {
				t.Errorf("reported %d nonblank rows, want 5", rowCount)
			}
			t.Log(text)
		})
	}
}

func TestMusePTYDiagnosticMasksContentAndPartialComposer(t *testing.T) {
	const secret = "password=private-value"
	rows := []string{
		"  Muse Code 1.4.7",
		"❯ " + secret,
		"◆ echo: PRIVATE_ENV=private-value /private/file",
		"  private file contents",
		"unexpected secret: private-value",
		"── Voice input (⌥ + v to start) " + strings.Repeat("─", 120-utf8.RuneCountInString("── Voice input (⌥ + v to start) ")),
		"❯ " + secret,
		"  wrapped private draft",
	}
	v := muse147Screen(t, rows)
	lines := v.lines()
	for _, tc := range []struct {
		row           int
		id, rendering string
	}{
		{1, "user_turn", "<screen:24 chars>"},
		{2, "assistant_echo", "<screen:47 chars>"},
		{3, "indented", "<screen:23 chars>"},
		{4, "unknown", "<screen:32 chars>"},
		{6, "composer", "<composer:22 chars>"},
		{7, "composer", "<composer:21 chars>"},
	} {
		id, rendering := v.diagnosticRow(lines, tc.row)
		if id != tc.id || rendering != tc.rendering {
			t.Errorf("row %d: [%s] %s, want [%s] %s", tc.row+1, id, rendering, tc.id, tc.rendering)
		}
	}
}

func TestMusePTYDiagnosticUnadmittedFooterChrome(t *testing.T) {
	fixture := strings.Split(strings.TrimSuffix(string(muse147Fixture(t, "screen-meta-startup-plain.txt")), "\n"), "\n")
	for _, tc := range []struct {
		name, footer, want string
	}{
		{"model_suffix", "  muse-wonder-1.0-future · max · /h/f/ws · Auto-review", "  muse-wonder-1.0-future · max · <path> · Auto-review"},
		{"extra_field", "  muse-wonder-1.0 · ultra · /h/f/ws · Full access (yolo) · Future mode", "  muse-wonder-1.0 · ultra · <path> · Full access (yolo) · Future mode"},
		{"credential_mode", "  muse-wonder-1.0 · max · /h/f/ws · password=private-value", "  muse-wonder-1.0 · max · <path> · <label:22 chars>"},
		{"credential_effort", "  muse-wonder-1.0 · TOKEN=private-value · /h/f/ws · Full access", "  muse-wonder-1.0 · <label:19 chars> · <path> · Full access"},
		{"bearer_mode", "  muse-wonder-1.0 · max · /h/f/ws · Bearer private-value", "  muse-wonder-1.0 · max · <path> · <label:20 chars>"},
		{"echo", "  echo · ~/private workspace · Future-review", "  echo · <path> · Future-review"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append([]string(nil), fixture...)
			rows[6] = tc.footer
			v := muse147Screen(t, rows)
			class, rendering := v.diagnosticRow(v.lines(), 6)
			if class != "unknown" || rendering != tc.want {
				t.Errorf("got [%s] %s, want [unknown] %s", class, rendering, tc.want)
			}
		})
	}
}

func TestMusePTYDiagnosticBoundsRenderingAndMasksMalformedRule(t *testing.T) {
	const width = 160
	upper := "── Voice input (⌥ + v to start) "
	rows := []string{
		"  Muse Code 1.4.7",
		upper + strings.Repeat("─", width-utf8.RuneCountInString(upper)),
		"❯ private draft",
		strings.Repeat("─", width),
		"  muse-wonder-1.0 · max · /h/f/ws · Full access",
	}
	v := newMuseVT(8, width)
	v.observe([]byte(strings.Join(rows, "\r\n")))
	h := &musePTYHost{vt: v, exited: make(chan struct{})}
	text := h.bindableDiagnostic()
	if !strings.Contains(text, "fold_idle=false") || !strings.Contains(text, "failed=[fold_idle, ready, composer_match, footer_admitted]") {
		t.Errorf("fold failure not reported: %s", text)
	}
	if strings.Contains(text, "private draft") || strings.Contains(text, "open ") {
		t.Fatal("diagnostic leaked content or fold error")
	}
	for _, line := range strings.Split(text, "\n") {
		if _, rendering, found := strings.Cut(line, "]: "); found && utf8.RuneCountInString(rendering) > 120 {
			t.Errorf("rendering exceeds 120 characters: %s", line)
		}
	}
	if !strings.Contains(text, "…") {
		t.Fatal("wide rules were not truncated")
	}
	rows[1] = upper + "password=private-value /private/file"
	v = newMuseVT(8, width)
	v.observe([]byte(strings.Join(rows, "\r\n")))
	class, rendering := v.diagnosticRow(v.lines(), 1)
	if class != "unknown" || !strings.HasPrefix(rendering, "<screen:") {
		t.Errorf("malformed composer rule was echoed: [%s] %s", class, rendering)
	}
}

func TestMusePTYDiagnosticVTUnknownKindAndPendingPrivacy(t *testing.T) {
	for _, tc := range []struct {
		name, raw, kind string
	}{
		{"csi", "\x1b[12345;67890z", "CSI final=0x7a params=2"},
		{"osc", "\x1b]7501;password=private-value /private/file\x07", "OSC number=7501"},
		{"malformed_osc", "\x1b]password=private-value;?\x07", "OSC number=invalid"},
		{"title", "\x1b]0;password=private-value\n\x07", "OSC number=0"},
		{"latest", "\x1b[12345;67890z\x1b]7501;password=private-value\x07", "OSC number=7501"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newMuseVT(40, 120)
			v.observe([]byte(museTestScreen("")))
			v.observe([]byte("\x1b]0;private-title\x07"))
			v.observe([]byte(tc.raw))
			// An accepted query must not replace the latest unknown kind.
			v.observe([]byte("\x1b]7501;?\x1b\\"))
			partial := []byte("\x1b]7501;private-pending")
			v.observe(partial)
			h := &musePTYHost{vt: v, exited: make(chan struct{})}
			text := h.bindableDiagnostic()
			want := fmt.Sprintf("vt_unknown=true pending=%d unknown_kind=[%s]", len(partial), tc.kind)
			if !strings.Contains(text, want) {
				t.Errorf("missing VT rejection metadata: %s", text)
			}
			for _, private := range []string{"12345", "67890", "password", "private-value", "/private/file", "private-title", "private-pending"} {
				if strings.Contains(text, private) || strings.Contains(v.unknownKind, private) {
					t.Fatal("diagnostic or kind retained control values")
				}
			}
		})
	}
	v := newMuseVT(40, 120)
	v.observe([]byte(museTestScreen("")))
	v.observe([]byte("\x1b["))
	h := &musePTYHost{vt: v, exited: make(chan struct{})}
	text := h.bindableDiagnostic()
	if !strings.Contains(text, "vt_unknown=false pending=2") || strings.Contains(text, "unknown_kind=") {
		t.Fatal("pending-only VT diagnostic is incorrect")
	}
}
