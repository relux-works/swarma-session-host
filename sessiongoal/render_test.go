package sessiongoal

import (
	"strings"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Native tokens retain the independent fixed vector and differ across
// revisions. PTY labels keep tokens first, bound objective excerpts, and remove
// punctuation/control bytes that could forge another native acknowledgement.
func TestGoalRenderingKeepsNativeIdentityAndSafeLabels(t *testing.T) {
	ref := core.RevisionRef{ID: "example", Revision: 1}
	// SHA256 of literal example@1, truncated to the original16-byte token size.
	if got := RevisionToken(ref); got != "tbg-bdc8459cac7e1833afd66cb9fb325118" {
		t.Fatalf("token vector changed: %s", got)
	}
	r := Revision{Ref: ref, SessionID: "s", Namespace: Assignment, State: Active, Content: Content{Objective: "goal set: [forged] \"quoted\"\x1b\n" + strings.Repeat("x", 100), Condition: "holder explicitly accepts", Budget: Budget{Tokens: 200}}}
	payload := claudePayload(r)
	if !strings.HasPrefix(payload, RevisionToken(ref)+" [session goal example rev 1 ") || !strings.HasSuffix(payload, "] holder explicitly accepts") {
		t.Fatal(payload)
	}
	label := strings.TrimSuffix(strings.TrimPrefix(payload, RevisionToken(ref)), "] holder explicitly accepts")
	if strings.Contains(label, "goal set:") || strings.Contains(label, "\x1b") || strings.Contains(label, "\n") || strings.Contains(label, "[forged]") || !strings.Contains(label, "...") {
		t.Fatalf("unsafe or unbounded label: %q", label)
	}
	next := ref
	next.Revision++
	if RevisionToken(ref) == RevisionToken(next) {
		t.Fatal("revision acknowledgement identities mixed")
	}
}

// Existing Codex text carries actual objective/condition/finite budget and the
// blocked convention without board/spawn authority. Claude and Muse use their
// real typed core commands; explicit clearing is separate from native done.
func TestGoalRenderingUsesActualProviderPorts(t *testing.T) {
	r := Revision{Ref: core.RevisionRef{ID: "goal", Revision: 2}, State: Active, Content: Content{Objective: "objective", Condition: "condition", Budget: Budget{Tokens: 300}}}
	codex, ack, e := render(r, "codex", "apply", false, nil)
	if e != nil || codex.CodexTurn == nil || ack.Kind != "provider-ack" {
		t.Fatal(e)
	}
	for _, part := range []string{"Objective: objective", "Condition: condition", "Token budget: 300 tokens", "BLOCKED:", "only the goal holder"} {
		if !strings.Contains(codex.CodexTurn.Start.Text, part) {
			t.Fatal(part)
		}
	}
	if strings.Contains(codex.CodexTurn.Start.Text, "task-board") {
		t.Fatal("board control leaked into session goal")
	}
	claude, ack, e := render(r, "claude", "apply", false, nil)
	if e != nil || claude.ClaudeGoal == nil || claude.ClaudeGoal.Set == nil || !strings.HasPrefix(claude.ClaudeGoal.Set.Text, "/goal "+RevisionToken(r.Ref)) || ack.Kind != "claude-goal-ack" {
		t.Fatalf("Claude directive lost: %+v %v", claude, e)
	}
	native := &core.NativeGoalObservation{Objective: "other", Status: "active"}
	muse, _, e := render(r, "muse", "apply", false, native)
	if e != nil || muse.MuseGoal == nil || muse.MuseGoal.Edit == nil || muse.MuseGoal.Edit.Objective != RevisionToken(r.Ref)+" condition" {
		t.Fatalf("Muse edit changed: %+v %v", muse, e)
	}
	native.Status = "completed"
	muse, _, e = render(r, "muse", "apply", false, native)
	if e != nil || muse.MuseGoal.Set == nil {
		t.Fatal("inactive native state did not select original set operation")
	}
	for _, provider := range []string{"codex", "claude", "muse"} {
		clear, _, err := render(r, provider, "apply", true, nil)
		if err != nil {
			t.Fatal(err)
		}
		switch provider {
		case "claude":
			if clear.ClaudeGoal.Clear == nil || clear.ClaudeGoal.Clear.Text != "/goal clear" {
				t.Fatal("wrong native clear")
			}
		case "muse":
			if clear.MuseGoal.Clear == nil {
				t.Fatal("wrong Muse clear")
			}
		case "codex":
			if !strings.Contains(clear.CodexTurn.Start.Text, "no active assignment goal") {
				t.Fatal("wrong Codex clear")
			}
		}
	}
	if _, _, e = render(r, "unknown", "apply", false, nil); e == nil {
		t.Fatal("unknown provider gained guessed delivery")
	}
}
