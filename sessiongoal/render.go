package sessiongoal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// RevisionToken retains the existing immutable ID@revision native token. It
// leads the label so truncated terminal repaint cannot remove ack identity.
func RevisionToken(ref core.RevisionRef) string {
	sum := sha256.Sum256([]byte(ref.ID + "@" + strconv.Itoa(ref.Revision)))
	return "tbg-" + hex.EncodeToString(sum[:16])
}

// labelSafeText relocates the existing PTY acknowledgement-forgery boundary:
// remove event-prefix punctuation, quotes and controls; collapse whitespace.
func labelSafeText(text string) string {
	var b strings.Builder
	space := false
	for _, r := range text {
		switch {
		case r == ':' || r == '[' || r == ']' || r == '"':
			continue
		case r < 0x20 || r == 0x7f:
			space = b.Len() > 0
		case unicode.IsSpace(r):
			space = b.Len() > 0
		default:
			if space {
				b.WriteRune(' ')
				space = false
			}
			b.WriteRune(r)
		}
	}
	return b.String()
}
func claudePayload(r Revision) string {
	label := "[session goal " + labelSafeText(r.Ref.ID) + " rev " + strconv.Itoa(r.Ref.Revision)
	excerpt := labelSafeText(r.Content.Objective)
	if runes := []rune(excerpt); len(runes) > 80 {
		excerpt = strings.TrimRight(string(runes[:80]), " ") + "..."
	}
	if excerpt != "" {
		label += ` - reference only, not the condition - objective "` + excerpt + `"`
	}
	return RevisionToken(r.Ref) + " " + label + "] " + strings.TrimSpace(r.Content.Condition)
}
func codexText(r Revision, clear bool) string {
	if clear {
		return fmt.Sprintf("[session goal] Goal %s revision %d is %s; this session has no active assignment goal.", r.Ref.ID, r.Ref.Revision, r.State)
	}
	budget := fmt.Sprintf("%d tokens", r.Content.Budget.Tokens)
	if r.Content.Budget.Unlimited {
		budget = "none"
	}
	return fmt.Sprintf("[session goal] Goal %s revision %d is active for this session.\nCondition: %s\nObjective: %s\nToken budget: %s\nWork toward the condition. If you are blocked, make your final message start with BLOCKED: and name the exact blocker. Turn end or a native done banner does not accept or clear this assignment; only the goal holder accepts the outcome.", r.Ref.ID, r.Ref.Revision, strings.TrimSpace(r.Content.Condition), strings.TrimSpace(r.Content.Objective), budget)
}
func render(r Revision, provider, phase string, clear bool, current *core.NativeGoalObservation) (core.RenderedInput, core.AckExpectation, error) {
	command := RevisionToken(r.Ref) + "/" + phase
	expected := core.AckExpectation{Kind: "provider-ack", Correlation: command}
	if !clear && !validContent(r.Content) {
		return core.RenderedInput{}, expected, ErrInvalidCommand
	}
	switch provider {
	case "codex":
		return core.RenderedInput{CodexTurn: &core.CodexTurnCommand{CommandID: command, Start: &core.CodexTurnStart{Text: codexText(r, clear)}}}, expected, nil
	case "claude":
		expected.Kind = "claude-goal-ack"
		c := &core.ClaudeGoalCommand{CommandID: command}
		if clear {
			c.Clear = &core.ClaudeGoalClear{Text: "/goal clear"}
		} else {
			c.Set = &core.ClaudeGoalSet{Text: "/goal " + claudePayload(r), Token: RevisionToken(r.Ref)}
		}
		return core.RenderedInput{ClaudeGoal: c}, expected, nil
	case "muse":
		c := &core.MuseGoalCommand{CommandID: command}
		if clear {
			c.Clear = &core.MuseGoalClear{}
		} else {
			if current == nil {
				return core.RenderedInput{}, expected, core.ErrHostCapabilityUnavailable
			}
			objective := RevisionToken(r.Ref) + " " + strings.TrimSpace(r.Content.Condition)
			// Exact existing native set/edit choice; never infer native completion.
			if current.Objective != "" && current.Objective != objective && current.Status == "active" {
				c.Edit = &core.MuseGoalEdit{Objective: objective}
			} else {
				c.Set = &core.MuseGoalSet{Objective: objective}
			}
		}
		return core.RenderedInput{MuseGoal: c}, expected, nil
	default:
		return core.RenderedInput{}, expected, core.ErrHostCapabilityUnavailable
	}
}

// Command identity identifies a native effect, independently of the immutable
// revision token in its human label. Restoration after clear cannot borrow the
// prior apply receipt; retries retain this exact stored command identity.
func deliveryIdentity(input core.InputSubmission, serial uint64) (core.InputSubmission, error) {
	id := input.Expected.Correlation + "/delivery/" + strconv.FormatUint(serial, 10)
	input.Expected.Correlation = id
	switch {
	case input.Payload.CodexTurn != nil:
		input.Payload.CodexTurn.CommandID = id
	case input.Payload.ClaudeGoal != nil:
		input.Payload.ClaudeGoal.CommandID = id
	case input.Payload.MuseGoal != nil:
		input.Payload.MuseGoal.CommandID = id
	default:
		return input, ErrInvalidCommand
	}
	return input, nil
}
