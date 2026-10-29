package musepty

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// bindableDiagnostic reads only predicate results and the current VT screen.
// Never render fold errors, launch inputs, OSC titles, or pending PTY bytes:
// those can contain paths, environment values, or credentials. The manager
// already carries the returned error into provider_launch_failed's cause.
func (h *musePTYHost) bindableDiagnostic() string {
	fold, err := h.fold()
	h.mu.Lock()
	defer h.mu.Unlock()
	lines := h.vt.lines()
	_, _, end, found := h.vt.composerRows()
	footer := false
	if found {
		footer = true
		count := 0
		for _, line := range lines[end+1:] {
			if line == "" {
				continue
			}
			count++
			footer = footer && museIdleFooter(line)
		}
		footer = footer && count == 1
	}
	checks := []struct {
		name string
		ok   bool
	}{
		{"fold_idle", err == nil && fold != nil && fold.idle()},
		{"quiet", h.quietLocked()},
		{"ready", h.readyLocked(fold, "")},
		{"composer_match", h.vt.composerMatches("")},
		{"footer_admitted", footer},
	}
	var out strings.Builder
	var failed []string
	out.WriteString("settle predicates:")
	for _, check := range checks {
		fmt.Fprintf(&out, " %s=%t", check.name, check.ok)
		if !check.ok {
			failed = append(failed, check.name)
		}
	}
	fmt.Fprintf(&out, " vt_unknown=%t pending=%d", h.vt.unknown, len(h.vt.pending))
	if h.vt.unknown {
		fmt.Fprintf(&out, " unknown_kind=[%s]", h.vt.unknownKind)
	}
	fmt.Fprintf(&out, "; failed=[%s]", strings.Join(failed, ", "))
	for i, row := range lines {
		if strings.TrimSpace(row) == "" {
			continue
		}
		class, rendering := h.vt.diagnosticRow(lines, i)
		runes := []rune(rendering)
		if len(runes) > 120 {
			rendering = string(runes[:119]) + "…"
		}
		fmt.Fprintf(&out, "\nrow %d [%s]: %s", i+1, class, rendering)
	}
	return out.String()
}

// diagnosticRow keeps only structurally identified chrome. Arbitrary content
// (including unknown rows) is represented by length, so transcript/file text
// and unlabelled secrets cannot escape through this diagnostic. Composer
// masking also covers incomplete geometry that composerRows must refuse.
func (v *museVT) diagnosticRow(lines []string, at int) (string, string) {
	row := lines[at]
	upper := "── Voice input (⌥ + v to start) "
	lower := strings.Repeat("─", len(v.cells[0]))
	begin, end := -1, len(lines)
	for i := len(lines) - 1; i >= 1; i-- {
		if strings.HasPrefix(lines[i-1], upper) && (lines[i] == "❯" || strings.HasPrefix(lines[i], "❯ ")) {
			begin = i
			for j := i + 1; j < len(lines); j++ {
				if lines[j] == lower {
					end = j
					break
				}
			}
			break
		}
	}
	if begin >= 0 && at >= begin && at < end {
		content := strings.TrimPrefix(row, "  ")
		if at == begin {
			content = strings.TrimPrefix(strings.TrimPrefix(row, "❯"), " ")
		}
		return "composer", fmt.Sprintf("<composer:%d chars>", utf8.RuneCountInString(content))
	}
	if begin >= 0 && (at == begin-1 || at == end) {
		if at == end || row == upper+strings.Repeat("─", max(0, len(v.cells[0])-utf8.RuneCountInString(upper))) {
			return "composer_rule", row
		}
		return "unknown", fmt.Sprintf("<screen:%d chars>", utf8.RuneCountInString(row))
	}
	if begin >= 0 && at > end {
		class := "unknown"
		if museIdleFooter(row) {
			class = "footer"
		}
		return class, museDiagnosticFooter(row)
	}
	if museIsChromeHeaderRow(row) {
		return museRowChromeHeader, row
	}
	if museRulesNoticeRow.MatchString(row) {
		return museRowChromeNotice, row
	}
	id, matches := "unknown", 0
	for _, entry := range museScreenRows {
		if entry.Pattern.MatchString(row) {
			id = entry.ID
			matches++
		}
	}
	if matches != 1 {
		id = "unknown"
	}
	if class, n := museClassifyScreenRow(row); n == 1 && class == museScreenRowBusy {
		return id, row // Closed progress/queue chrome, never arbitrary content.
	}
	return id, fmt.Sprintf("<screen:%d chars>", utf8.RuneCountInString(row))
}

// Footer path fields are masked as whole fields, including spaces and any
// abbreviation (~/, /h/f/ws, or ellipsis). Only model/effort/mode label shapes
// are rendered; arbitrary text and secret assignments remain masked.
var (
	museDiagnosticModel  = regexp.MustCompile(`^  muse-[a-z0-9.-]+$`)
	museDiagnosticLabel  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9 ()-]{0,47}$`)
	museDiagnosticSecret = regexp.MustCompile(`(?i)\b(?:bearer|authorization|password|secret|token|credential|cookie|api[- ]?key)\b`)
)

func museDiagnosticFooter(row string) string {
	parts := strings.Split(row, " · ")
	pathAt := -1
	if len(parts) >= 3 && parts[0] == "  echo" {
		pathAt = 1
	} else if len(parts) >= 4 && museDiagnosticModel.MatchString(parts[0]) {
		pathAt = 2
	}
	if pathAt >= 0 {
		parts[pathAt] = "<path>"
		for i := 0; i < len(parts); i++ {
			if i == pathAt {
				continue
			}
			if i > 0 && !museDiagnosticLabel.MatchString(parts[i]) || museDiagnosticSecret.MatchString(parts[i]) {
				parts[i] = fmt.Sprintf("<label:%d chars>", utf8.RuneCountInString(parts[i]))
			}
		}
		return strings.Join(parts, " · ")
	}
	return fmt.Sprintf("<screen:%d chars>", utf8.RuneCountInString(row))
}
