package sessionmanager

import (
	"strings"
	"sync"
)

// claudeOutputScanner turns the raw Claude PTY byte stream into a normalized
// rolling text window and resolves marker waiters against it. Normalization
// strips ANSI escape/control sequences, lowercases printable text, and
// collapses whitespace runs so a marker cannot be split by cursor movement,
// styling, or line wrapping between words.
type claudeOutputScanner struct {
	mu      sync.Mutex
	state   int
	buffer  []byte
	base    int64
	pending bool
	waiters map[*claudeOutputWaiter]struct{}
}

const (
	claudeScanText = iota
	claudeScanEscape
	claudeScanCSI
	claudeScanOSC
	claudeScanOSCEscape
)

// claudeScannerWindow bounds the retained normalized text. Goal directives
// stay far below this size, so a waiter registered before its command is
// written can never lose its acknowledgement to window trimming.
const claudeScannerWindow = 64 * 1024

type claudeOutputMatch struct {
	marker   string
	failure  bool
	observed string
	evidence InputEvidenceClass
}

// claudeConditionedMarker acknowledges only rendered output that names a
// specific payload: an event prefix (for example "goal set:") followed by a
// tolerant match of the expected key text. The live TUI paints lines through
// cursor positioning and can drop characters from the linearized stream, so
// the key is verified as an ordered subsequence with bounded coverage instead
// of an exact substring. A zero threshold uses the general condition default;
// identity-bearing keys (revision tokens) set a higher bar.
type claudeConditionedMarker struct {
	prefix    string
	key       string
	threshold float64
}

type claudeOutputWaiter struct {
	success     []string
	conditioned []claudeConditionedMarker
	failure     []string
	from        int64
	done        chan claudeOutputMatch
}

func newClaudeOutputScanner() *claudeOutputScanner {
	return &claudeOutputScanner{waiters: make(map[*claudeOutputWaiter]struct{})}
}

// watch registers marker interest strictly after the already-seen output, so
// echoes and acknowledgements from earlier commands can never satisfy a new
// waiter. Markers must already be normalized (lowercase, single spaces).
func (s *claudeOutputScanner) watch(success, failure []string) *claudeOutputWaiter {
	return s.watchConditioned(success, nil, failure)
}

func (s *claudeOutputScanner) watchConditioned(
	success []string,
	conditioned []claudeConditionedMarker,
	failure []string,
) *claudeOutputWaiter {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiter := &claudeOutputWaiter{
		success:     append([]string(nil), success...),
		conditioned: append([]claudeConditionedMarker(nil), conditioned...),
		failure:     append([]string(nil), failure...),
		from:        s.base + int64(len(s.buffer)),
		done:        make(chan claudeOutputMatch, 1),
	}
	s.waiters[waiter] = struct{}{}
	return waiter
}

// sawMarker reports whether a marker is present anywhere in the retained
// window. Unlike watch it looks backwards, which is what a post-launch check
// needs: the message it looks for was printed while the launch was still
// starting up, so waiting for it again would wait forever.
func (s *claudeOutputScanner) sawMarker(marker string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found := firstClaudeMarker(string(s.buffer), []string{marker})
	return found
}

func (s *claudeOutputScanner) cancel(waiter *claudeOutputWaiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.waiters, waiter)
}

// observe consumes one raw PTY chunk.
func (s *claudeOutputScanner) observe(chunk []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range chunk {
		s.consume(b)
	}
	s.resolveLocked()
	if excess := len(s.buffer) - claudeScannerWindow; excess > 0 {
		s.buffer = append(s.buffer[:0], s.buffer[excess:]...)
		s.base += int64(excess)
	}
}

// text returns the normalized window; tests and diagnostics only.
func (s *claudeOutputScanner) text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.buffer)
}

func (s *claudeOutputScanner) consume(b byte) {
	switch s.state {
	case claudeScanText:
		switch {
		case b == 0x1b:
			s.state = claudeScanEscape
		case b == '\n' || b == '\r' || b == '\t' || b == ' ':
			s.appendSpace()
		case b >= 0x20 && b < 0x7f:
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			s.buffer = append(s.buffer, b)
			s.pending = false
		case b >= 0x80:
			// Keep multi-byte UTF-8 content verbatim; markers are ASCII, so
			// exact glyph fidelity does not matter for matching.
			s.buffer = append(s.buffer, b)
			s.pending = false
		}
	case claudeScanEscape:
		switch b {
		case '[':
			s.state = claudeScanCSI
		case ']':
			s.state = claudeScanOSC
		default:
			s.state = claudeScanText
		}
	case claudeScanCSI:
		if b >= 0x40 && b <= 0x7e {
			s.state = claudeScanText
		}
	case claudeScanOSC:
		switch b {
		case 0x07:
			s.state = claudeScanText
		case 0x1b:
			s.state = claudeScanOSCEscape
		}
	case claudeScanOSCEscape:
		if b == '\\' {
			s.state = claudeScanText
		} else {
			s.state = claudeScanOSC
		}
	}
}

func (s *claudeOutputScanner) appendSpace() {
	if s.pending || len(s.buffer) == 0 {
		return
	}
	s.buffer = append(s.buffer, ' ')
	s.pending = true
}

func (s *claudeOutputScanner) resolveLocked() {
	if len(s.waiters) == 0 {
		return
	}
	window := string(s.buffer)
	for waiter := range s.waiters {
		start := waiter.from - s.base
		if start < 0 {
			start = 0
		}
		if start >= int64(len(window)) {
			continue
		}
		segment := window[start:]
		if marker, ok := firstClaudeMarker(segment, waiter.failure); ok {
			waiter.done <- claudeOutputMatch{marker: marker, failure: true}
			delete(s.waiters, waiter)
			continue
		}
		if marker, ok := firstClaudeMarker(segment, waiter.success); ok {
			waiter.done <- claudeOutputMatch{marker: marker, observed: claudeLiteralObserved(segment, marker)}
			delete(s.waiters, waiter)
			continue
		}
		if match, ok := firstClaudeConditionedEvidence(segment, waiter.conditioned); ok {
			waiter.done <- match
			delete(s.waiters, waiter)
		}
	}
}

// claudeConditionCoverageThreshold is the fraction of the expected condition
// key that must appear, in order, immediately after a conditioned event
// prefix. Live captures show the TUI can drop a few percent of characters
// during incremental repaints (~94% observed coverage); a different goal's
// text cannot sustain this coverage inside the bounded slice.
const claudeConditionCoverageThreshold = 0.75

// claudeConditionedPrefixCoverageThreshold is the ordered-subsequence coverage
// the letters of a conditioned event prefix must reach in the window just
// before its colon. The same cell-diff repaint that drops key characters drops
// prefix characters: a live capture  rendered "Goal set:"
// as "Goal s<CSI 13 G>t:" because the "e" cell already held an "e" from the
// previous frame, and an exact prefix search then timed out every launch on
// that board. The letters only distinguish the acknowledgement from other
// colon-bearing text; the revision key that follows is the identity check, so
// this bar can sit low ("goalset" survives two dropped letters).
const claudeConditionedPrefixCoverageThreshold = 0.6

// claudeConditionedPrefixSlack bounds how many extra characters may sit inside
// the prefix window. It is tight on purpose: the command echo reads
// "/goal tbg-<key>" with no colon, and a wide window would let the letters of
// "goal" plus some later objective text pass for a prefix.
const claudeConditionedPrefixSlack = 2

func firstClaudeConditionedMarker(segment string, markers []claudeConditionedMarker) (string, bool) {
	match, ok := firstClaudeConditionedEvidence(segment, markers)
	return match.marker, ok
}

// The legacy predicate/marker stays unchanged. The evidence adds only a bounded
// slice of actual normalized native bytes; expected cells are never restored.
func firstClaudeConditionedEvidence(segment string, markers []claudeConditionedMarker) (claudeOutputMatch, bool) {
	if len(markers) == 0 {
		return claudeOutputMatch{}, false
	}
	stripped := strings.ReplaceAll(segment, " ", "")
	for _, marker := range markers {
		prefix := strings.ReplaceAll(marker.prefix, " ", "")
		if prefix == "" || marker.key == "" {
			continue
		}
		threshold := marker.threshold
		if threshold <= 0 {
			threshold = claudeConditionCoverageThreshold
		}
		limit := len(marker.key) + len(marker.key)/4 + 24
		for _, sliceStart := range conditionedPrefixAnchors(stripped, prefix) {
			sliceEnd := sliceStart + limit
			if sliceEnd > len(stripped) {
				sliceEnd = len(stripped)
			}
			if orderedSubsequenceCoverage(marker.key, stripped[sliceStart:sliceEnd]) >= threshold {
				// Keep the earliest end attaining the same original matcher score.
				// Extra bytes after that evidence are not part of the observation.
				coverage := orderedSubsequenceCoverage(marker.key, stripped[sliceStart:sliceEnd])
				for sliceEnd > sliceStart && orderedSubsequenceCoverage(marker.key, stripped[sliceStart:sliceEnd-1]) == coverage {
					sliceEnd--
				}
				start := sliceStart - len(prefix)
				if strings.HasSuffix(prefix, ":") {
					start -= claudeConditionedPrefixSlack
				}
				observed := claudeStrippedSpan(segment, max(0, start), sliceEnd)
				// A later command echo never supplies native evidence, including
				// missing cells. The legacy marker/predicate remains unchanged;
				// typed admission rechecks only this actual acknowledgement span.
				if echo := strings.Index(observed, "/goal"); echo >= 0 {
					observed = strings.TrimSpace(observed[:echo])
				}
				class := InputEvidenceClaudePTYLossyConditioned
				if strings.Contains(strings.ReplaceAll(observed, " ", ""), prefix+marker.key) {
					class = InputEvidenceClaudePTYConditioned
				}
				return claudeOutputMatch{marker: marker.prefix + " " + marker.key, observed: observed, evidence: class}, true
			}
		}
	}
	return claudeOutputMatch{}, false
}

func claudeStrippedSpan(segment string, start, end int) string {
	positions := make([]int, 0, len(segment))
	for i := range len(segment) {
		if segment[i] != ' ' {
			positions = append(positions, i)
		}
	}
	if start < 0 || end <= start || end > len(positions) {
		return ""
	}
	return segment[positions[start] : positions[end-1]+1]
}

func claudeLiteralObserved(segment, marker string) string {
	stripped := strings.ReplaceAll(segment, " ", "")
	needle := strings.ReplaceAll(marker, " ", "")
	start := strings.Index(stripped, needle)
	if start < 0 {
		return ""
	}
	return claudeStrippedSpan(segment, start, start+len(needle))
}

// conditionedPrefixAnchors returns every offset in stripped at which a
// conditioned key may begin: just past a colon whose preceding window covers
// the prefix letters. The colon stays an exact anchor -- it is what keeps the
// plain command echo from resolving its own acknowledgement -- while the
// letters before it tolerate the dropped cells of an incremental repaint. A
// prefix that does not end in a colon is located exactly.
func conditionedPrefixAnchors(stripped, prefix string) []int {
	var anchors []int
	letters, anchored := strings.CutSuffix(prefix, ":")
	if !anchored || letters == "" {
		for offset := 0; offset < len(stripped); {
			index := strings.Index(stripped[offset:], prefix)
			if index < 0 {
				break
			}
			anchors = append(anchors, offset+index+len(prefix))
			offset += index + len(prefix)
		}
		return anchors
	}
	window := len(letters) + claudeConditionedPrefixSlack
	for index := 0; index < len(stripped); index++ {
		if stripped[index] != ':' {
			continue
		}
		start := index - window
		if start < 0 {
			start = 0
		}
		if orderedSubsequenceCoverage(letters, stripped[start:index]) >= claudeConditionedPrefixCoverageThreshold {
			anchors = append(anchors, index+1)
		}
	}
	return anchors
}

// orderedSubsequenceCoverage reports which fraction of needle appears in
// haystack as an in-order (possibly gapped) byte subsequence. It is the
// longest-common-subsequence length normalized by the needle length: a greedy
// scan would collapse on the insert/delete corruption the TUI repaints
// produce. Inputs are bounded (needle by the condition-key cap, haystack by
// the conditioned-marker slice), so the quadratic cost stays trivial.
func orderedSubsequenceCoverage(needle, haystack string) float64 {
	if needle == "" || haystack == "" {
		return 0
	}
	previous := make([]int, len(haystack)+1)
	current := make([]int, len(haystack)+1)
	for i := 1; i <= len(needle); i++ {
		for j := 1; j <= len(haystack); j++ {
			switch {
			case needle[i-1] == haystack[j-1]:
				current[j] = previous[j-1] + 1
			case previous[j] >= current[j-1]:
				current[j] = previous[j]
			default:
				current[j] = current[j-1]
			}
		}
		previous, current = current, previous
	}
	return float64(previous[len(haystack)]) / float64(len(needle))
}

// normalizeClaudeMarkerText applies the scanner's text normalization to a
// marker source: ASCII letters are lowercased and ASCII whitespace runs
// collapse to single spaces, so a marker built from raw provider-condition
// text compares against the normalized window on equal terms.
func normalizeClaudeMarkerText(raw string) string {
	var builder strings.Builder
	pendingSpace := false
	for _, r := range raw {
		switch r {
		case ' ', '\t', '\n', '\r':
			pendingSpace = builder.Len() > 0
		default:
			if pendingSpace {
				builder.WriteByte(' ')
				pendingSpace = false
			}
			if r >= 'A' && r <= 'Z' {
				r += 'a' - 'A'
			}
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func firstClaudeMarker(segment string, markers []string) (string, bool) {
	// The Claude TUI often paints box/banner text through cursor positioning
	// instead of literal spaces, so a marker is also matched with all
	// whitespace removed from both sides.
	stripped := strings.ReplaceAll(segment, " ", "")
	for _, marker := range markers {
		if marker == "" {
			continue
		}
		if strings.Contains(segment, marker) ||
			strings.Contains(stripped, strings.ReplaceAll(marker, " ", "")) {
			return marker, true
		}
	}
	return "", false
}
