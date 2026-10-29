package sessionrun

import (
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

// The original terminal rule accepts only an explicit completed terminal
// without error. Open enum values and negative usage cannot attest success.
func TestMuseNativeTerminalFolding(t *testing.T) {
	for _, terminal := range []string{"completed", "failed", "cancelled", "future-terminal"} {
		e := core.MSPTurnCompletedNotification{Terminal: terminal, TurnID: "turn", Usage: &core.MSPTokenUsage{InputTokens: 4, OutputTokens: 3}}
		r := museRunResult(e, " final ")
		if (r.Status == core.ProviderRunComplete) != (terminal == "completed") || r.Output != "final" || r.Usage.TotalTokens != 7 {
			t.Fatalf("terminal %s: %+v", terminal, r)
		}
	}
	e := core.MSPTurnCompletedNotification{Terminal: "completed", Error: &core.MSPTurnError{Message: "refused"}}
	if r := museRunResult(e, ""); r.Status != core.ProviderRunFailed || r.Detail != "refused" {
		t.Fatalf("error attested success: %+v", r)
	}
	e.Error = nil
	e.Usage = &core.MSPTokenUsage{InputTokens: -1}
	if r := museRunResult(e, ""); r.Status != core.ProviderRunFailed || r.ProviderStatus != "usage_malformed" {
		t.Fatalf("negative usage: %+v", r)
	}
}

// Item identity/order/revision is immutable evidence: lower revisions do not
// erase newer text, equal conflicting revisions and turn moves fail closed.
func TestMuseNativeFinalItemFolding(t *testing.T) {
	f := newFinals()
	a := agentMessage{ItemID: "a", Revision: 1, Kind: "agentMessage", TurnID: "turn", Text: "BLOCKED: prerequisite"}
	if err := f.apply(a); err != nil {
		t.Fatal(err)
	}
	b := a
	b.ItemID = "b"
	b.Text = ""
	if err := f.apply(b); err != nil {
		t.Fatal(err)
	}
	if output, final := f.render("turn"); output != "BLOCKED: prerequisite\n" || final != "BLOCKED: prerequisite" {
		t.Fatalf("empty erased final: %q %q", output, final)
	}
	a.Revision = 2
	a.Text = "newer"
	if err := f.apply(a); err != nil {
		t.Fatal(err)
	}
	old := a
	old.Revision = 1
	old.Text = "stale"
	if err := f.apply(old); err != nil {
		t.Fatal(err)
	}
	if _, final := f.render("turn"); final != "newer" {
		t.Fatal("lower revision replaced final")
	}
	a.Text = "contradiction"
	if err := f.apply(a); err == nil {
		t.Fatal("equal revision contradiction accepted")
	}
	a.TurnID = "other"
	a.Revision = 3
	if err := f.apply(a); err == nil {
		t.Fatal("item moved turns")
	}
}
