package sessionmanager

import (
	"strings"
	"testing"
)

// claudeCellDiffAckCapture preserves a captured PTY repaint's control bytes.
// Captured on 2026-09-11; objective and goal metadata are fictionalized at equal
// byte width. Claude Code 2.1.268 acknowledgement/correlation and control bytes are unchanged;
// the renderer painted with cursor-to-column skips ("Goal s<CSI 13 G>t:",
// "babf9<CSI 26 G>750e8", "[boa<CSI 58 G>d goal") wherever the cell already
// held the same character from the previous frame. Linearized, that is
// "goal st:tbg-babf9750e8263dc85381a1a78fe580 [boad goal ...".
const claudeCellDiffAckCapture = "\x1b[38;2;80;80;80m\xe2\x9d\xaf\x1b[39m \x1b[38;2;255;255;255m/goal tbg-babf9a750e8d263dc85381a1a78fe580 [board goal PRIMARY-GOAL-000000-EXAMPL rev 11 - reference only, not the \x1b[39m   \r\x1b[1B\x1b[38;2;255;255;255mcondition - objective \"All three objectives of this goal are delivered on main TASK-260101-aaaaaa / STO...\"] The bound \x1b[39m \r\x1b[1B\x1b[38;2;255;255;255mparent session has handled this exact alignment revision.\x1b[39m                                                               \r\x1b[1B\x1b[49m\x1b[38;2;153;153;153m  \xe2\x8e\xbf  \x1b[39mGoal s\x1b[13Gt:\x1b[16Gtbg-babf9\x1b[26G750e8\x1b[32G263dc85381a1a78fe580 [boa\x1b[58Gd goal PRIMARY-GOAL-000000-EXAMPL\x1b[92Grev\x1b[96G11\x1b[99G-\x1b[101Greference\x1b[111Gonly,\x1b[117Gnot\r\x1b[1B     the condition - objective \"All three objectives of this goa"

// claudeCellDiffAckEchoOnly is the capture cut just before the
// acknowledgement line: the directive echo alone, as rendered.
const claudeCellDiffAckEchoOnly = "\x1b[38;2;80;80;80m\xe2\x9d\xaf\x1b[39m \x1b[38;2;255;255;255m/goal tbg-babf9a750e8d263dc85381a1a78fe580 [board goal PRIMARY-GOAL-000000-EXAMPL rev 11 - reference only, not the \x1b[39m   \r\x1b[1B\x1b[38;2;255;255;255mcondition - objective \"All three objectives of this goal are delivered on main TASK-260101-aaaaaa / STO...\"] The bound \x1b[39m \r\x1b[1B\x1b[38;2;255;255;255mparent session has handled this exact alignment revision.\x1b[39m"

// Fixed captured native token. Canonical derivation is independently pinned in
// private claudecompat; these tests exercise actual output/scanner behavior.
func capturedGoalToken(t *testing.T) string {
	t.Helper()
	return "tbg-babf9a750e8d263dc85381a1a78fe580"
}

func waiterResolved(t *testing.T, waiter *claudeOutputWaiter) (claudeOutputMatch, bool) {
	t.Helper()
	// observe resolves synchronously into a buffered channel. An empty channel
	// after it returns proves that this input did not acknowledge the goal.
	select {
	case match := <-waiter.done:
		return match, true
	default:
		return claudeOutputMatch{}, false
	}
}

// An absent event stays unresolved after unrelated input, while the same
// waiter immediately receives the synchronous failure event when it arrives.
func TestClaudeOutputWaiterObservationNeverArrives(t *testing.T) {
	scanner := newClaudeOutputScanner()
	waiter := scanner.watch([]string{"acknowledged"}, []string{"unknown command"})
	if _, ok := waiterResolved(t, waiter); ok {
		t.Fatal("an absent event resolved the waiter")
	}
	scanner.observe([]byte("unrelated output\r\n"))
	if _, ok := waiterResolved(t, waiter); ok {
		t.Fatal("unrelated output resolved the waiter")
	}
	scanner.observe([]byte("unknown command\r\n"))
	match, ok := waiterResolved(t, waiter)
	if !ok || !match.failure || match.marker != "unknown command" {
		t.Fatalf("failure event = %+v, resolved = %v", match, ok)
	}
}

// TestClaudeGoalAckResolvesWhenTheRepaintDropsPrefixCells is the regression
// witness for dropped prefix cells: the captured cursor/control bytes resolve
// the waiter armed for that revision, even though the prefix reached the
// scanner as "goal st:".
func TestClaudeGoalAckResolvesWhenTheRepaintDropsPrefixCells(t *testing.T) {
	revision := capturedGoalToken(t)
	scanner := newClaudeOutputScanner()
	waiter := scanner.watchConditioned(nil, claudeGoalConditionedMarkers(revision), []string{"unknown command"})
	scanner.observe([]byte(claudeCellDiffAckCapture))

	if !strings.Contains(scanner.text(), "goal st:tbg-babf9750e8263dc85381a1a78fe580") {
		t.Fatalf("fixture no longer reproduces the dropped-cell rendering; window = %q", scanner.text())
	}
	match, ok := waiterResolved(t, waiter)
	if !ok {
		t.Fatal("the captured acknowledgement with dropped prefix cells did not resolve the waiter")
	}
	if match.failure {
		t.Fatalf("the acknowledgement resolved as a failure: %+v", match)
	}
}

// TestClaudeGoalAckEchoNeverResolvesTheWaiter is the forgery control: the
// directive echo carries the full token right after "/goal", and neither the
// echo alone nor an echo repainted after unrelated colon-bearing text (the
// status clock) may satisfy the conditioned markers. Tolerating dropped
// prefix letters must not loosen this.
func TestClaudeGoalAckEchoNeverResolvesTheWaiter(t *testing.T) {
	revision := capturedGoalToken(t)
	markers := claudeGoalConditionedMarkers(revision)

	echo := newClaudeOutputScanner()
	waiter := echo.watchConditioned(nil, markers, nil)
	echo.observe([]byte(claudeCellDiffAckEchoOnly))
	if _, ok := waiterResolved(t, waiter); ok {
		t.Fatal("the directive echo alone resolved the acknowledgement waiter")
	}

	// A clock in the status line puts a colon a few characters before the
	// repainted echo; the letters before that colon are nothing like a prefix.
	clocked := newClaudeOutputScanner()
	waiter = clocked.watchConditioned(nil, markers, nil)
	clocked.observe([]byte("\x1b[38;2;153;153;153m\xe2\x9c\xbb brewed for 0s \xc2\xb7 done 10:10 pm\x1b[39m\r\n"))
	clocked.observe([]byte(claudeCellDiffAckEchoOnly))
	if _, ok := waiterResolved(t, waiter); ok {
		t.Fatal("a clock colon followed by the echo repaint resolved the acknowledgement waiter")
	}

	// The echo with its own slash cell dropped still reads "goal tbg-...":
	// no colon, so no anchor.
	slashless := newClaudeOutputScanner()
	waiter = slashless.watchConditioned(nil, markers, nil)
	slashless.observe([]byte(strings.Replace(claudeCellDiffAckEchoOnly, "/goal tbg", "\x1b[3Ggoal tbg", 1)))
	if _, ok := waiterResolved(t, waiter); ok {
		t.Fatal("the echo with a dropped slash resolved the acknowledgement waiter")
	}
}

// TestClaudeGoalAckDifferentRevisionStillRejectedUnderTolerantPrefix keeps
// the identity half of the contract: an intact "Goal set:" line that names
// another revision's token does not resolve this revision's waiter, and the
// intact rendering of the right token still does (positive control for the
// exact-prefix path the tolerant anchor must not have broken).
func TestClaudeGoalAckDifferentRevisionStillRejectedUnderTolerantPrefix(t *testing.T) {
	revision := capturedGoalToken(t)
	first := "tbg-93ba2c99de8d285c69cadef6b0bca634"
	markers := claudeGoalConditionedMarkers(revision)

	other := newClaudeOutputScanner()
	waiter := other.watchConditioned(nil, markers, nil)
	other.observe([]byte("  \xe2\x8e\xbf  Goal set: " + first + " fixture condition" + "\r\n"))
	if _, ok := waiterResolved(t, waiter); ok {
		t.Fatal("another revision's intact acknowledgement resolved this revision's waiter")
	}

	intact := newClaudeOutputScanner()
	waiter = intact.watchConditioned(nil, markers, nil)
	intact.observe([]byte("  \xe2\x8e\xbf  Goal set: " + revision + " fixture condition" + "\r\n"))
	if _, ok := waiterResolved(t, waiter); !ok {
		t.Fatal("the intact acknowledgement of the requested revision was rejected")
	}
}

// TestConditionedPrefixAnchorsRequireTheColon pins the anchor rule directly:
// dropped letters are tolerated, a missing colon is not, and a prefix without
// a colon is still located exactly.
func TestConditionedPrefixAnchorsRequireTheColon(t *testing.T) {
	cases := []struct {
		name     string
		stripped string
		prefix   string
		want     []int
	}{
		{name: "intact", stripped: "xx⎿goalset:tbg-1", prefix: "goalset:", want: []int{len("xx⎿goalset:")}},
		{name: "one letter dropped", stripped: "⎿goalst:tbg-1", prefix: "goalset:", want: []int{len("⎿goalst:")}},
		{name: "two letters dropped", stripped: "⎿galst:tbg-1", prefix: "goalset:", want: []int{len("⎿galst:")}},
		{name: "colon dropped", stripped: "⎿goalsettbg-1", prefix: "goalset:", want: nil},
		{name: "echo has no colon", stripped: "❯/goaltbg-babf9a750e8d263dc85381a1a78fe580[boardgoal", prefix: "goalset:", want: nil},
		{name: "clock colon", stripped: "brewedfor0s·done10:10pm❯/goaltbg-1", prefix: "goalset:", want: nil},
		{name: "short prefix tolerates one drop", stripped: "⎿gal:tbg-1", prefix: "goal:", want: []int{len("⎿gal:")}},
		{name: "short prefix rejects the echo", stripped: "❯/goaltbg-1", prefix: "goal:", want: nil},
		{name: "colon-free prefix is exact", stripped: "xxgoalclearedyy", prefix: "goalcleared", want: []int{len("xxgoalcleared")}},
		{name: "colon prefix with no colon in the stream", stripped: "xxgoalclearedyy", prefix: "goalcleared:", want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := conditionedPrefixAnchors(tc.stripped, tc.prefix)
			if len(got) != len(tc.want) {
				t.Fatalf("anchors = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("anchors = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
