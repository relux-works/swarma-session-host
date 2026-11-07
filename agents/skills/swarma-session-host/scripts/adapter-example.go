// Run from the repository root with: go run ./agents/skills/swarma-session-host/scripts/adapter-example.go
// This example constructs configuration and checks bytes in memory; it never opens a Host or runner.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// exampleAdapter reuses the landed line codec and supplies local input evidence.
// The new name is not registered with the command-line adapter switch.
type exampleAdapter struct {
	fixtureadapters.Lines
	state sessionhost.InputState
}

func (exampleAdapter) Name() string                         { return "example-fixture" }
func (a exampleAdapter) InputState() sessionhost.InputState { return a.state }

var _ sessionhost.Adapter = exampleAdapter{}

func main() {
	adapter := exampleAdapter{state: sessionhost.InputUnknown}
	cfg := sessionhost.Config{Adapter: adapter}
	if cfg.Adapter.InputState() != sessionhost.InputUnknown {
		panic("missing idle evidence must remain unknown")
	}

	// Explicit construction is the compiled seam; no registry or activation occurs.
	adapter.state = sessionhost.InputIdle
	cfg.Adapter = adapter
	notice := sessionhost.Notice{ID: "one", Reference: "fixture/one"}
	selection := sessionhost.Selection{Vendor: "example-vendor", Model: "example-model", Reasoning: "example-effort"}
	encoded, err := cfg.Adapter.Encode(notice, selection)
	if err != nil {
		panic(err)
	}
	acknowledgement := append([]byte("applied\t"), encoded...)
	require := func(label string, output []byte, n sessionhost.Notice, s sessionhost.Selection, want, malformed bool) {
		got, err := cfg.Adapter.Applied(output, n, s)
		if got != want || (err != nil) != malformed {
			panic(fmt.Sprintf("%s: applied=%v error=%v", label, got, err))
		}
		result := struct {
			Case      string `json:"case"`
			Applied   bool   `json:"applied"`
			Malformed bool   `json:"malformed"`
		}{label, got, err != nil}
		data, err := json.Marshal(result)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(data))
	}
	require("matching", acknowledgement, notice, selection, true, false)
	changed := selection
	changed.Model = "other-model"
	require("different-model", acknowledgement, notice, changed, false, false)
	require("incomplete-frame", bytes.TrimSuffix(acknowledgement, []byte{'\n'}), notice, selection, false, false)
	require("malformed-frame", []byte("unrelated-output\n"), notice, selection, false, true)
}
