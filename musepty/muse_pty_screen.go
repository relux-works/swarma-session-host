package musepty

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// museVT models the pinned TUI's cursor-addressed inline screen, not a stripped
// output history. Incomplete escape sequences/UTF-8 never constitute a frame.
// Unsupported screen operations invalidate it until a fresh host is started.
type museVT struct {
	cells                                     [][]rune
	row, col, savedRow, savedCol, top, bottom int
	pending                                   []byte
	unknown                                   bool
	unknownKind                               string // Kind only; never parameters or payloads.
	title                                     string
}

func newMuseVT(rows, cols int) *museVT {
	v := &museVT{bottom: rows - 1}
	for i := 0; i < rows; i++ {
		v.cells = append(v.cells, make([]rune, cols))
	}
	return v
}
func (v *museVT) linefeed() {
	if v.row == v.bottom {
		// Frame-local provenance: a row discarded from the top of
		// the screen is unknown state, never a parent. An indented
		// row admits only with its parent present in the current
		// frame; if the parent has scrolled off, delivery defers.
		copy(v.cells[v.top:v.bottom], v.cells[v.top+1:v.bottom+1])
		v.cells[v.bottom] = make([]rune, len(v.cells[0]))
	} else if v.row < len(v.cells)-1 {
		v.row++
	}
}
func (v *museVT) observe(data []byte) []byte {
	v.pending = append(v.pending, data...)
	var replies []byte
	for len(v.pending) > 0 {
		b := v.pending[0]
		if b == 27 {
			if len(v.pending) < 2 {
				break
			}
			switch v.pending[1] {
			case '[':
				end := 2
				for end < len(v.pending) && (v.pending[end] < 0x40 || v.pending[end] > 0x7e) {
					end++
				}
				if end == len(v.pending) {
					return replies
				}
				args := string(v.pending[2:end])
				op := v.pending[end]
				replies = append(replies, v.csi(args, op)...)
				v.pending = v.pending[end+1:]
				continue
			case ']':
				end := 2
				for end < len(v.pending) && v.pending[end] != 7 && !(v.pending[end] == 27 && end+1 < len(v.pending) && v.pending[end+1] == '\\') {
					end++
				}
				if end == len(v.pending) || v.pending[end] == 27 && end+1 == len(v.pending) {
					return replies
				}
				osc := string(v.pending[2:end])
				if strings.HasPrefix(osc, "0;") {
					// A title the host cannot fully read is unknown
					// state. Only a validated title is stored; anything
					// else refuses without becoming model state.
					if !museValidOSCTitle(osc[2:]) {
						v.unknownOSC(osc)
					} else {
						v.title = osc[2:]
					}
				} else if osc != "7501;?" && !museVTColorQuery(osc) {
					v.unknownOSC(osc)
				}
				// The captured 1.4.7 OSC 7501 capability query is consumed
				// without a reply or screen mutation (ST or BEL terminator).
				n := 1
				if v.pending[end] == 27 {
					n = 2
				}
				v.pending = v.pending[end+n:]
				continue
			case '7':
				v.savedRow, v.savedCol = v.row, v.col
			case '8':
				v.row, v.col = v.savedRow, v.savedCol
			case 'M':
				if v.row > v.top {
					v.row--
				} else {
					copy(v.cells[v.top+1:v.bottom+1], v.cells[v.top:v.bottom])
					v.cells[v.top] = make([]rune, len(v.cells[0]))
				}
			default:
				v.unknown = true
				v.unknownKind = fmt.Sprintf("ESC final=0x%02x", v.pending[1])
			}
			v.pending = v.pending[2:]
			continue
		}
		switch b {
		case '\r':
			v.col = 0
		case '\n':
			v.linefeed()
		default:
			if b >= 32 {
				if !utf8.FullRune(v.pending) {
					return replies
				}
				r, n := utf8.DecodeRune(v.pending)
				// Only a positively printable rune becomes a cell. DEL, C1,
				// invalid UTF-8 and every other non-printing rune outside a
				// recognised escape sequence is unknown state, never a
				// printable cell.
				if r == utf8.RuneError && n == 1 || !unicode.IsPrint(r) {
					v.unknown = true
					v.unknownKind = "nonprinting"
				} else {
					if v.col >= len(v.cells[0]) {
						v.col = 0
						v.linefeed()
					}
					v.cells[v.row][v.col] = r
					v.col++
				}
				v.pending = v.pending[n:]
				continue
			}
			v.unknown = true // No uncaptured C0 control is a harmless absence.
			v.unknownKind = fmt.Sprintf("C0 byte=0x%02x", b)
		}
		v.pending = v.pending[1:]
	}
	return replies
}

func (v *museVT) unknownCSI(raw string, op byte) {
	count := 0
	if raw != "" {
		count = strings.Count(raw, ";") + 1
	}
	v.unknown = true
	v.unknownKind = fmt.Sprintf("CSI final=0x%02x params=%d", op, count)
}

func (v *museVT) unknownOSC(osc string) {
	v.unknown = true
	v.unknownKind = "OSC number=invalid"
	number, _, found := strings.Cut(osc, ";")
	if !found || number == "" {
		return
	}
	for _, r := range number {
		if r < '0' || r > '9' {
			return
		}
	}
	if n, err := strconv.Atoi(number); err == nil {
		v.unknownKind = fmt.Sprintf("OSC number=%d", n)
	}
}

func (v *museVT) csi(raw string, op byte) []byte {
	if op == 'c' && raw == "" {
		return []byte("\x1b[?1;2c")
	}
	if op == 'n' {
		if raw == "6" {
			return []byte(fmt.Sprintf("\x1b[%d;%dR", v.row+1, min(v.col+1, len(v.cells[0]))))
		}
		if raw == "5" {
			return []byte("\x1b[0n")
		}
	}
	// Exact controls captured from 1.4.1 and 1.4.7, not arbitrary toggles.
	if !museVTControl(raw, op) {
		v.unknownCSI(raw, op)
		return nil
	}
	if op == 'm' || op == 'h' || op == 'l' || op == 'q' || op == 'u' {
		return nil
	}
	parts := strings.Split(raw, ";")
	nums := make([]int, len(parts))
	for i, s := range parts {
		if s != "" {
			n, e := strconv.Atoi(s)
			if e != nil {
				v.unknownCSI(raw, op)
				return nil
			}
			nums[i] = n
		}
	}
	n := nums[0]
	if n == 0 {
		n = 1
	}
	switch op {
	case 'H', 'f':
		if len(nums) != 2 || nums[0] < 1 || nums[0] > len(v.cells) || nums[1] < 1 || nums[1] > len(v.cells[0]) {
			v.unknownCSI(raw, op)
			return nil
		}
		v.row = max(0, min(len(v.cells)-1, n-1))
		c := 1
		if len(nums) > 1 && nums[1] > 0 {
			c = nums[1]
		}
		v.col = max(0, min(len(v.cells[0])-1, c-1))
	case 'A':
		v.row = max(0, v.row-n)
	case 'B':
		v.row = min(len(v.cells)-1, v.row+n)
	case 'C':
		v.col = min(len(v.cells[0])-1, v.col+n)
	case 'D':
		v.col = max(0, v.col-n)
	case 'G':
		v.col = min(len(v.cells[0])-1, n-1)
	case 'd':
		v.row = min(len(v.cells)-1, n-1)
	case 'E':
		v.row = min(len(v.cells)-1, v.row+n)
		v.col = 0
	case 'F':
		v.row = max(0, v.row-n)
		v.col = 0
	case 'J':
		if nums[0] == 2 {
			for i := range v.cells {
				clear(v.cells[i])
			}
			// A full clear starts a new view: nothing above the
			// redrawn frame is proven, and every indented row in
			// the redrawn frame needs its parent in that frame.
			v.unknown = false
			v.unknownKind = ""
		} else if nums[0] == 0 {
			clear(v.cells[v.row][min(v.col, len(v.cells[0])):])
			for i := v.row + 1; i < len(v.cells); i++ {
				clear(v.cells[i])
			}
		} else {
			v.unknownCSI(raw, op)
		}
	case 'K':
		switch nums[0] {
		case 0:
			clear(v.cells[v.row][min(v.col, len(v.cells[0])):])
		case 1:
			clear(v.cells[v.row][:min(v.col+1, len(v.cells[0]))])
		case 2:
			clear(v.cells[v.row])
		default:
			v.unknownCSI(raw, op)
		}
	case 'r':
		bottom := len(v.cells)
		if len(nums) > 1 && nums[1] > 0 {
			bottom = nums[1]
		}
		if nums[0] < 1 && raw != "" || n < 1 || bottom > len(v.cells) || n >= bottom {
			v.unknownCSI(raw, op)
		} else {
			v.top, v.bottom = n-1, bottom-1
			v.row, v.col = 0, 0
		}
	case 's':
		v.savedRow, v.savedCol = v.row, v.col
	case 't': // Captured window-save/restore controls.
	default:
		v.unknownCSI(raw, op)
	}
	return nil
}
func museVTRowText(cells []rune) string {
	c := append([]rune(nil), cells...)
	for j, r := range c {
		if r == 0 {
			c[j] = ' '
		}
	}
	return strings.TrimRight(string(c), " ")
}

func (v *museVT) lines() []string {
	lines := make([]string, len(v.cells))
	for i, cells := range v.cells {
		lines[i] = museVTRowText(cells)
	}
	return lines
}
func museText(s string) string { return strings.Join(strings.Fields(s), " ") }

// museCommittedWidth is the closed width allowlist for the
// off-frame path (r12): printable ASCII plus the non-ASCII runes the
// real captured committed output contains, each with its display
// width. U+2502 BOX DRAWINGS LIGHT VERTICAL is the only non-ASCII
// rune in the captured committed output (corpus record
// fd935169-307d-4da9-9141-790b045f0399, "echo: Echo this literal:
// │"), width 1. Any other rune has no established rendering: the
// line is unknown and the path fails closed. There are no range
// tables: a comment calling a range list closed does not make its
// catch-all admission closed.
func museCommittedWidth(r rune) (int, bool) {
	if r >= 0x20 && r <= 0x7E {
		return 1, true
	}
	if r == 0x2502 {
		return 1, true
	}
	return 0, false
}

// museDisplayWidth measures the terminal columns a committed line
// occupies under the closed allowlist: every rune must have an
// established width or the whole line is unestablished and cannot
// back the screen.
func museDisplayWidth(line string) (int, bool) {
	width := 0
	for _, r := range line {
		w, ok := museCommittedWidth(r)
		if !ok {
			return 0, false
		}
		width += w
	}
	return width, true
}

// museRenderCommittedOutput renders committed assistant text the way the
// pinned TUI wraps it: the first line under the echo marker, later lines
// as indented continuations. A line wider than the viewport has no
// captured rendering (no captured committed line exceeds 81 columns)
// and fails closed: the run cannot back the screen. Blank lines render
// as blank rows, which the transcript scan skips on both sides.
func museRenderCommittedOutput(text string, cols int) ([]string, bool) {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for i, line := range lines {
		if width, ok := museDisplayWidth(line); !ok || width > cols-2 {
			return nil, false
		}
		var row string
		if i == 0 {
			row = "◆ " + line
		} else {
			row = "  " + line
		}
		if strings.TrimSpace(row) == "" {
			continue
		}
		out = append(out, row)
	}
	return out, true
}

// museLogBackedRowsOK reports whether the leading off-frame rows are a
// contiguous suffix of the rendered committed output of one run the
// fold shows as terminated. Anything else — no matching committed
// output, a run that never terminated, a gap, a reordered or foreign
// line, or rows mixed across runs — is unknown state. A nil fold
// backs nothing. No cross-frame rows are retained: the durable log is
// the only provenance.
func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {
	if len(rows) == 0 || fold == nil {
		return false
	}
	for run, state := range fold.RunLifecycle {
		if state != museRunTerminated {
			continue
		}
		text, ok := fold.Messages[run]
		if !ok || text == "" {
			continue
		}
		rendered, ok := museRenderCommittedOutput(text, cols)
		if !ok || len(rows) > len(rendered) {
			continue
		}
		tail := rendered[len(rendered)-len(rows):]
		match := true
		for i := range rows {
			if rows[i] != tail[i] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func (v *museVT) safeComposer(expected string, fold *museLogFold) bool {
	if v.unknown || len(v.pending) > 0 || !v.composerMatches(expected) {
		return false
	}
	lines := v.lines()
	// In 1.4.1, menus and status live below the lower composer rule. The
	// transcript above the upper rule is content, including arbitrary words
	// such as "permissions" or "thinking". Never search that transcript.
	_, begin, end, _ := v.composerRows()
	live := append([]string(nil), lines[end:]...)
	// Provenance before chrome (r15): a leading row is transcript
	// content when the durable log positively backs it as part of a
	// committed-output suffix, whatever its text. Only rows the log
	// does not back may be classified as chrome by position and
	// context below. An indented first row has no parent in the
	// current frame. It and the indented rows chained under it are
	// admitted only as a contiguous suffix of one terminated run's
	// rendered committed output; rows with an in-frame parent keep
	// the sequence rule below. A first row the content table calls
	// indented — including the exact header text — joins that
	// backed-prefix check first; when the check fails, an orphan
	// indented row still refuses here, while positional chrome falls
	// through to the sequence walk. No cross-frame rows, no glyph
	// lists, no timers: the durable log is the only off-frame
	// provenance.
	var transcript []string
	for i := 0; i <= begin-2; i++ {
		if strings.TrimSpace(lines[i]) == "" {
			continue
		}
		transcript = append(transcript, lines[i])
	}
	backed := -1
	if len(transcript) > 0 && museScreenContentID(transcript[0]) == museScreenRowIndented {
		last := 0
		for last+1 < len(transcript) && museScreenContentID(transcript[last+1]) == museScreenRowIndented {
			last++
		}
		if museLogBackedRowsOK(transcript[:last+1], len(v.cells[0]), fold) {
			backed = last
		} else if museScreenRowStateAt(transcript[0], 0, museRowStart) == museScreenRowIndented {
			return false
		}
	}
	// The whole region between the last transcript row and the composer
	// must match, row by row, top-down: the closed screen table proves
	// the kind of every nonblank row above the upper composer rule, and
	// the sequence grammar proves the parent of every indented row. A
	// busy row or a row the table does not match refuses without
	// poisoning the model: the TUI redraws, and the next frame is judged
	// on its own. A content row never hides the row above it; the scan
	// continues past every match.
	warningEnd := museRulesWarningEnd(transcript)
	prevState := museRowStart
	at := -1
	for i := 0; i <= begin-2; i++ {
		s := lines[i]
		if strings.TrimSpace(s) == "" {
			continue
		}
		at++
		class, matches := museClassifyScreenRow(s)
		if matches != 1 {
			return false
		}
		if class == museScreenRowBusy {
			return false
		}
		state := museScreenRowStateAt(s, at, prevState)
		if at >= 1 && at <= warningEnd {
			state = museRowChromeWarning
		}
		if at <= backed {
			// Log-backed provenance (r15): a backed row is
			// transcript content whatever its text, so the
			// positional chrome classification above does not
			// apply to it. The closed row-transition table
			// still judges the resulting states below.
			state = museScreenContentID(s)
		}
		if !museRowTransitionOK(prevState, state, s, at <= backed) {
			return false
		}
		prevState = state
		if state == "" {
			// A content row with no content entry is reachable only
			// when a narrowing mutant demotes a busy row: the
			// table admits the empty state explicitly (see
			// museRowTransitionOK) and the weakened gate reads it
			// as bare transcript below.
			prevState = museScreenRowUserTurn
		}
	}
	screen := strings.ToLower(strings.Join(live, "\n"))
	dialog := false
	for _, marker := range []string{"do you trust", "permissions", "allow once", "allow always", "press enter to", "enter to confirm", "enter to select", "enter to choose", "up/down to choose", "[y/n]", "approve", "confirm"} {
		if strings.Contains(screen, marker) {
			dialog = true
		}
	}
	// A numbered cursor row is a decision surface even with unfamiliar wording.
	for _, line := range live {
		s := strings.TrimSpace(line)
		for _, prefix := range []string{">", "›", "❯"} {
			if strings.HasPrefix(s, prefix) {
				tail := strings.TrimSpace(strings.TrimPrefix(s, prefix))
				if len(tail) > 0 && tail[0] >= '0' && tail[0] <= '9' {
					dialog = true
				}
			}
		}
	}
	if dialog {
		return false
	}
	if strings.Contains(screen, "esc to interrupt") || strings.Contains(screen, "thinking") {
		return false
	}
	for _, r := range v.title {
		if r >= 0x2800 && r <= 0x28ff {
			return false
		}
	}
	// Closed screen grammar: one captured status footer, then blank rows.
	// Menus, new status surfaces and partial redraws cannot count as idle.
	footer := false
	for _, line := range lines[end+1:] {
		if line == "" {
			continue
		}
		if footer || !museIdleFooter(line) {
			return false
		}
		footer = true
	}
	return footer
}

// The TUI shows versioned-short ids such as muse-wonder-1.0, which the
// spawn registry deliberately does not register (it owns contributor ids
// and aliases). Match the Muse display-id shape instead: a lowercase ASCII
// name, optional full variant, major.minor, and an optional contributor or
// full suffix. An unversioned alias or an arbitrary word is not a model row.
var museFooterModel = regexp.MustCompile(`^  muse-[a-z][a-z0-9]*(-full)?-[0-9]+\.[0-9]+(-(contributor|full))?$`)

func museIdleFooter(line string) bool {
	parts := strings.Split(line, " · ")
	// Auto-review is the only captured mode. The yolo posture's footer label
	// (including any full-access wording) is unobserved and must still refuse.
	switch len(parts) {
	case 3:
		return parts[0] == "  echo" && parts[1] != "" && parts[2] == "Auto-review"
	case 4:
		if !museFooterModel.MatchString(parts[0]) || parts[2] == "" || parts[3] != "Auto-review" {
			return false
		}
		// Current Muse effort union, as defined by
		// pkg/remoteconfig/reasoningeffort.MuseWonderVocabulary. Spark's
		// high/xhigh/max is a subset; none and retired ultra are not efforts.
		switch parts[1] {
		case "minimal", "low", "medium", "high", "xhigh", "max":
			return true
		}
	}
	return false
}

// Screen entry ids for the sequence grammar. Busy entries never reach the
// sequence check: their own row already refuses the frame.
const (
	museScreenRowUserTurn = "user_turn"
	museScreenRowEcho     = "assistant_echo"
	museScreenRowIndented = "indented"
)

// Row states for the sequence grammar (r13 states, r14 table). Chrome
// rows — the pinned header and the resume notice, the only chrome in
// the transcript region of any capture — are distinct states, never
// generic indented transcript. Transcript rows keep their content ids
// as states. museRowStart is the state before the first
// transcript-region row.
const (
	museRowChromeHeader  = "chrome_header"
	museRowChromeNotice  = "chrome_notice"
	museRowChromeWarning = "chrome_warning"
	museRowStart         = "start"
)

// Observed in 1.4.10 with a live HOME whose rules exceed the delegation
// budget: one startup warning directly after the banner, soft-wrapped at
// the terminal width, so its row split depends on the width and the path.
// The byte counts and the path are data; the wording is closed and is
// judged on the rejoined sentence, never row by row.
var museRulesWarningText = regexp.MustCompile(`^warning: rules file at \S.* produced [0-9]+ bytes, over the [0-9]+ byte subagent delegation startup context limit; it will be truncated for this session$`)

// museRulesWarningEnd returns the index of the last nonblank transcript row
// of the 1.4.10 rules warning, or -1 when the region does not open with the
// banner followed by that warning. The warning starts at the row after the
// banner and takes every following indented row up to the rules notice, the
// first unindented row or the end; the rejoined text must match the closed
// sentence exactly, so a partial redraw or changed wording admits nothing.
func museRulesWarningEnd(transcript []string) int {
	if len(transcript) < 2 || !museIsChromeHeaderRow(transcript[0]) || !strings.HasPrefix(transcript[1], "  warning: rules file at ") {
		return -1
	}
	end := 1
	for end+1 < len(transcript) {
		next := transcript[end+1]
		if !strings.HasPrefix(next, "  ") || strings.HasPrefix(next, "   ") || museRulesNoticeRow.MatchString(next) {
			break
		}
		end++
	}
	parts := make([]string, 0, end)
	for _, row := range transcript[1 : end+1] {
		parts = append(parts, strings.TrimSpace(row))
	}
	if !museRulesWarningText.MatchString(strings.Join(parts, " ")) {
		return -1
	}
	return end
}

// The captured 1.4.1 chrome header, the only indented row the captures
// show with no parent. It stays the historical witness: chrome admission
// itself is structural (museIsChromeHeaderRow), never a release list, so
// a newer build's banner parses exactly like this one.
const museHeaderRow = "  Muse Code 1.4.1"

// museIsChromeHeaderRow reports whether a screen row is the TUI chrome
// header: two spaces, "Muse Code ", and a dotted release triple of ASCII
// digits. Any well-formed triple admits; anything else (a missing or
// malformed version, trailing text) is not chrome and falls through to
// the content classification, where a parentless first row still refuses
// without log backing. No release allowlist exists to age out.
func museIsChromeHeaderRow(row string) bool {
	rest, ok := strings.CutPrefix(row, "  Muse Code ")
	if !ok {
		return false
	}
	parts := strings.Split(rest, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for i := 0; i < len(part); i++ {
			if part[i] < '0' || part[i] > '9' {
				return false
			}
		}
	}
	return true
}

var museResumeNoticeRow = regexp.MustCompile(`^  resumed session [0-9a-f-]+$`)

// Observed in 1.4.7: one startup notice directly after the banner.
// The skill count is data; wording, indentation and position are closed.
var museRulesNoticeRow = regexp.MustCompile(`^  Including your Claude Code personal rules and [0-9]+ skills — manage with /settings\.$`)

// museScreenContentID names the content entry a row matched. Empty means no
// content entry matched: with exactly one classifier match of content class
// that is reachable only under a narrowing mutant.
func museScreenContentID(row string) string {
	for _, entry := range museScreenRows {
		if entry.Class != museScreenRowContent {
			continue
		}
		if entry.Pattern.MatchString(row) {
			return entry.ID
		}
	}
	return ""
}

// museScreenRowStateAt names the sequence-grammar state of one
// transcript-region row by POSITION and context (r14), not by text
// alone. The header text is chrome only as the first nonblank row of
// the region (at == 0); the resume notice text is chrome only as the
// next nonblank row after the chrome header. The same text anywhere
// else is transcript content (an indented continuation) and never
// breaks its legitimate continuation. Since r15 this positional
// decision runs only for rows the durable log does not back:
// safeComposer proves log-backed provenance first, and a backed row
// keeps its content id whatever its text. Blank rows never reach
// this function: the caller skips them, so at counts nonblank rows
// and prevState is the previous nonblank row's state (museRowStart
// for the first). Empty means no content entry matched: with exactly
// one classifier match of content class that is reachable only under
// a narrowing mutant.
func museScreenRowStateAt(row string, at int, prevState string) string {
	if at == 0 && museIsChromeHeaderRow(row) {
		return museRowChromeHeader
	}
	if prevState == museRowChromeHeader && museResumeNoticeRow.MatchString(row) {
		return museRowChromeNotice
	}
	if (prevState == museRowChromeHeader || prevState == museRowChromeWarning) && museRulesNoticeRow.MatchString(row) {
		return museRowChromeNotice
	}
	return museScreenContentID(row)
}

// museRowTransitions is the closed admit-table for the screen
// row-sequence grammar (r14). Every nonblank transcript-region row
// goes through exactly one check — the pair (previous row state, this
// row state) must be admitted here — and no state skips it. The table
// is derived from the real captures (the five harvested stills plus
// every checked-in VT final frame, walked in
// TestMusePTYR7ScreenSequenceCompleteness): each entry below is
// witnessed, and no other pair admits. Four narrowness points stay
// outside the literal map in museRowTransitionOK so a mutant can
// weaken exactly one member: the empty state (reachable only when a
// narrowing mutant demotes a busy row) admits; a first-row indented
// continuation admits only with log backing (the r10
// scrolled-transcript path — backed is the caller's log-backed prefix
// verdict); and the two refused chrome-to-continuation edges are
// explicit refusals with the row in scope. An opener
// (turn, echo) claims no parent, so a headerless first row admits
// when the header has scrolled off; a first-row notice text is
// transcript by position and needs backing like any other
// continuation. Chrome states are unreachable past their captured
// positions by construction (museScreenRowStateAt), so pairs like
// (turn, chrome_header) cannot occur and are absent, not admitted.
var museRowTransitions = map[string]map[string]bool{
	museRowStart: {
		museRowChromeHeader:   true,
		museScreenRowUserTurn: true,
		museScreenRowEcho:     true,
	},
	museRowChromeHeader: {
		museRowChromeNotice:   true,
		museRowChromeWarning:  true,
		museScreenRowUserTurn: true,
	},
	museRowChromeWarning: {
		museRowChromeWarning: true,
		museRowChromeNotice:  true,
	},
	museRowChromeNotice: {
		museScreenRowUserTurn: true,
	},
	museScreenRowUserTurn: {
		museScreenRowEcho:     true,
		museScreenRowIndented: true,
	},
	museScreenRowEcho: {
		museScreenRowUserTurn: true,
		museScreenRowIndented: true,
	},
	museScreenRowIndented: {
		museScreenRowUserTurn: true,
		museScreenRowEcho:     true,
		museScreenRowIndented: true,
	},
}

// museRowTransitionOK reports whether the pair (prevState, state) is an
// admitted row-sequence edge. row is the current row's text: narrowing
// mutants admit exactly one sentinel row past an otherwise-refused
// edge. backed is the caller's log-backed prefix verdict for a
// first-row continuation.
func museRowTransitionOK(prevState, state, row string, backed bool) bool {
	if state == "" {
		// Mutant-demoted busy row (see safeComposer): no content
		// entry matched, so no table entry can name it.
		return true
	}
	if prevState == museRowStart && state == museScreenRowIndented {
		return backed
	}
	if prevState == museRowChromeNotice && state == museScreenRowIndented {
		return false
	}
	if prevState == museRowChromeHeader && state == museScreenRowIndented {
		return false
	}
	return museRowTransitions[prevState][state]
}
func (v *museVT) composerMatches(expected string) bool {
	if v.unknown || len(v.pending) > 0 {
		return false
	}
	rows, _, _, found := v.composerRows()
	if !found {
		return false
	}
	// A physical row boundary can split a word. Consume only whitespace
	// that actually exists in the expected paste; never invent a space at
	// a soft wrap. Within a row, whitespace normalization remains exact.
	rest := museText(expected)
	for _, row := range rows {
		part := museText(row)
		if !strings.HasPrefix(rest, part) {
			return false
		}
		rest = strings.TrimSpace(strings.TrimPrefix(rest, part))
	}
	return rest == ""
}

// The 1.4.7 submitted-turn still contains no SGR attributes; the VT stores
// characters only. This exact hint is therefore indistinguishable from a
// draft with identical text. Do not generalize to other hint wording.
const museComposerPlaceholder = "Type @ to search and insert workspace file paths"

// Pinned inline composer geometry: prompt at column 0, continuation at
// column 2, and a lower rule occupying every terminal column at column 0.
// Transcript and literal continuation rows cannot terminate the composer.
func (v *museVT) composerRows() (rows []string, begin, end int, found bool) {
	lines := v.lines()
	lower := strings.Repeat("─", len(v.cells[0]))
	for i := len(lines) - 1; i >= 1; i-- {
		if !strings.HasPrefix(lines[i], "❯ ") && lines[i] != "❯" {
			continue
		}
		upper := lines[i-1]
		if !strings.HasPrefix(upper, "── Voice input (⌥ + v to start) ") || len([]rune(upper)) != len(v.cells[0]) || strings.TrimPrefix(upper, "── Voice input (⌥ + v to start) ") != strings.Repeat("─", len(v.cells[0])-len([]rune("── Voice input (⌥ + v to start) "))) {
			continue
		}
		rows = []string{strings.TrimPrefix(strings.TrimPrefix(lines[i], "❯"), " ")}
		for j := i + 1; j < len(lines); j++ {
			if lines[j] == lower {
				if lines[i] == "❯ "+museComposerPlaceholder {
					emptyTail := true
					for _, row := range rows[1:] {
						emptyTail = emptyTail && row == ""
					}
					if emptyTail {
						// Normalize at the shared read boundary: matches,
						// safeComposer and every host idle check see empty.
						rows = []string{""}
					}
				}
				return rows, i, j, true
			}
			if lines[j] != "" && !strings.HasPrefix(lines[j], "  ") {
				return nil, 0, 0, false
			}
			rows = append(rows, strings.TrimPrefix(lines[j], "  "))
		}
	}
	return nil, 0, 0, false
}

// Captured parameter grammars. Cursor coordinates/scroll margins vary with
// geometry; they remain decimal integers bounded by the screen in csi.
func museVTControl(raw string, op byte) bool {
	switch op {
	case 'm':
		switch raw {
		case "0", "1", "2", "22", "39", "49", "59", "39;49":
			return true
		}
		// Color indices are captured integer data, not new control kinds. The
		// pinned stream uses these four layouts; other SGR layouts stay unknown.
		parts := strings.Split(raw, ";")
		color := func(s string) bool {
			if s == "" {
				return false
			}
			for _, r := range s {
				if r < '0' || r > '9' {
					return false
				}
			}
			n, e := strconv.Atoi(s)
			return e == nil && n >= 0 && n <= 255
		}
		if len(parts) == 3 {
			return parts[0] == "38" && parts[1] == "5" && color(parts[2])
		}
		if len(parts) == 4 {
			return parts[0] == "38" && parts[1] == "5" && color(parts[2]) && parts[3] == "49" || parts[0] == "39" && parts[1] == "48" && parts[2] == "5" && color(parts[3])
		}
		if len(parts) == 6 {
			return parts[0] == "38" && parts[1] == "5" && color(parts[2]) && parts[3] == "48" && parts[4] == "5" && color(parts[5])
		}

	case 'h', 'l':
		return raw == "?25" || raw == "?1004" || raw == "?2004" || op == 'l' && raw == "?1049"
	case 'q':
		return raw == "0 "
	case 'u':
		return raw == "<1" || raw == "<" || raw == ">3" || raw == "?"
	case 't':
		return raw == "22;0" || raw == "23;0"
	case 'A', 'G':
		return raw == "1"
	case 'K':
		// Bare EL0 is captured in 1.4.7. Explicit 0K/1K remain outside
		// the captured grammar; the existing 2K behavior is unchanged.
		return raw == "" || raw == "2"
	case 'J':
		return raw == ""
	case 'H', 'r':
		if op == 'r' && raw == "" {
			return true
		}
		parts := strings.Split(raw, ";")
		if len(parts) != 2 {
			return false
		}
		for _, p := range parts {
			if p == "" {
				return false
			}
			for _, r := range p {
				if r < '0' || r > '9' {
					return false
				}
			}
		}
		return true
	}
	return false
}

// An OSC title payload must be readable in full: valid UTF-8, printable
// runes only (no C0/C1 controls, no DEL, no embedded escape), and bounded.
// The checked-in captures do carry OSC 0 titles (spinner runes with a
// request counter, e.g. "⠋ 001"); the 4096-byte bound stays stated, not
// harvested, since no terminal title needs more.
func museValidOSCTitle(title string) bool {
	if len(title) > 4096 || !utf8.ValidString(title) {
		return false
	}
	for _, r := range title {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func museVTColorQuery(osc string) bool {
	if osc == "10;?" || osc == "11;?" {
		return true
	}
	for i := 0; i < 16; i++ {
		if osc == fmt.Sprintf("4;%d;?", i) {
			return true
		}
	}
	return false
}
