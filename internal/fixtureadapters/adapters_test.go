package fixtureadapters

import (
	"bytes"
	"testing"

	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// The synthetic acknowledgement must bind notice ID, reference and every opaque
// selection value. Unrelated, incomplete and malformed output is not success.
func TestAcknowledgementsBindFullIntent(t *testing.T) {
	notice := sessionhost.Notice{ID: "one", Reference: "fixture/ref"}
	selection := sessionhost.Selection{Vendor: "new-vendor", Model: "new-model", Reasoning: "new-effort"}
	for _, tc := range []struct {
		name string
		edit func(*Envelope)
	}{
		{"valid", func(*Envelope) {}},
		{"id", func(e *Envelope) { e.Notice.ID = "other" }},
		{"reference", func(e *Envelope) { e.Notice.Reference = "other" }},
		{"vendor", func(e *Envelope) { e.Selection.Vendor = "other" }},
		{"model", func(e *Envelope) { e.Selection.Model = "other" }},
		{"reasoning", func(e *Envelope) { e.Selection.Reasoning = "other" }},
	} {
		t.Run("json/"+tc.name, func(t *testing.T) {
			envelope := Envelope{Kind: "applied", Notice: notice, Selection: selection}
			tc.edit(&envelope)
			var output bytes.Buffer
			if err := localipc.Write(&output, envelope); err != nil {
				t.Fatal(err)
			}
			got, err := (JSON{}).Applied(output.Bytes(), notice, selection)
			if err != nil || got != (tc.name == "valid") {
				t.Fatalf("ack binding mismatch: applied=%v err=%v", got, err)
			}
		})
	}
	for _, tc := range []struct {
		name, output       string
		applied, malformed bool
	}{
		{"valid", "applied\tone\tfixture/ref\tnew-vendor\tnew-model\tnew-effort\n", true, false},
		{"model", "applied\tone\tfixture/ref\tnew-vendor\tother\tnew-effort\n", false, false},
		{"partial", "applied\tone\tfixture/ref\tnew-vendor\tnew-model\tnew-effort", false, false},
		{"malformed", "unrelated-output\n", false, true},
	} {
		t.Run("line/"+tc.name, func(t *testing.T) {
			got, err := (Lines{}).Applied([]byte(tc.output), notice, selection)
			if got != tc.applied || (err != nil) != tc.malformed {
				t.Fatalf("ack result: %v %v", got, err)
			}
		})
	}
	for _, tc := range []struct {
		output    string
		malformed bool
	}{
		{`{"kind":"applied"`, false}, {"malformed\n", true},
	} {
		got, err := (JSON{}).Applied([]byte(tc.output), notice, selection)
		if got || (err != nil) != tc.malformed {
			t.Fatalf("JSON partial/malformed accepted: %v %v", got, err)
		}
	}
}
