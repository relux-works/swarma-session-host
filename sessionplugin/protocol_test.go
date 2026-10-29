package sessionplugin

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

// The standalone schema admits these wire shapes. The plugin must reject
// selectors with its own provider grammar before ledger or provider effects;
// empty tails and a value-position "resume" remain valid at this boundary.
// Nonempty valid tails can still be refused by C1a's managed composer policy.
func TestNativeTailSelectorRefusal(t *testing.T) {
	for _, runtime := range []string{"codex_cli", "claude_code", "muse"} {
		latest, identity, incomplete := []string{"resume", "--last"}, []string{"resume", "11111111-1111-4111-8111-111111111111"}, []string{"resume"}
		if runtime == "claude_code" {
			latest, identity, incomplete = []string{"--continue"}, []string{"--resume", "11111111-1111-4111-8111-111111111111"}, []string{"--resume"}
		}
		for _, tc := range []struct {
			name  string
			tail  []string
			valid bool
		}{
			{"empty", []string{}, true},
			{"value", []string{"--model", "resume"}, true},
			{"selector", latest, false},
			{"selector-id", identity, false},
			{"incomplete-selector", incomplete, false},
		} {
			t.Run(runtime+"/"+tc.name, func(t *testing.T) {
				q := request(1)
				q["runtime"], q["native_tail"] = runtime, tc.tail
				body := marshal(q)
				if err := schema.Validate("launch.schema.json", body); err != nil {
					t.Fatalf("wire control: %v", err)
				}
				got, err := Validate("launch", body)
				if tc.valid {
					if err != nil || !reflect.DeepEqual(got.NativeTail, tc.tail) {
						t.Fatalf("valid tail changed or refused: %#v, %v", got.NativeTail, err)
					}
					return
				}
				e, host := testEngine(t)
				_, _, err = e.Handle(context.Background(), "launch", body)
				var refusal Refusal
				if !errors.As(err, &refusal) || refusal.Code != "launch_plan_invalid" || len(refusal.Details) != 0 || refusal.Exit() != 6 {
					t.Fatalf("selector refusal: %#v, %v", refusal, err)
				}
				if host.starts != 0 {
					t.Fatal("selector started provider")
				}
				if _, err = os.Lstat(e.Dir); !os.IsNotExist(err) {
					t.Fatalf("selector touched ledger: %v", err)
				}
			})
		}
	}
}
