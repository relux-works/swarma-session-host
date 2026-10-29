//go:build unix

package sessiongoal

import (
	"context"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
	"testing"
)

// Actual transport+Service+receipt binding use the concrete proof. Trusted
// admission/source fixtures do not qualify a real bridge or native provider.
func TestLegacyProofActualApplyAndNoEffectRefusals(t *testing.T) {
	for _, name := range []string{"entitled", "missing_source", "changed_metadata", "invalid_version", "old_holder", "wrong_generation", "agent"} {
		t.Run(name, func(t *testing.T) {
			s, root, stored, wire, source := legacyProofFixture(t)
			// This consumer currently requires matching wire and stored references.
			wire.GoalID = stored.Ref.ID
			wire.Revision = stored.Ref.Revision
			source.fact.RawRevision = legacyRaw(t, wire)
			proof := LegacyProof{Source: source}
			principal := "launcher"
			switch name {
			case "missing_source":
				proof.Source = nil
			case "changed_metadata":
				wire.Reason = "changed after retention"
			case "invalid_version":
				wire.ContractVersion = "other"
			case "old_holder":
				s.authority.(*authorityFixture).facts.Holder = "new-holder"
				s.authority.(*authorityFixture).facts.Generation = 2
			case "wrong_generation":
				source.fact.Revision.HolderGeneration++
			case "agent":
				principal = "agent"
			}
			writer := newWriter()
			binding, err := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", writer, budgetFixture{})
			if err != nil {
				t.Fatal(err)
			}
			runtime := &protocolRuntimeFixture{session: core.SessionView{SessionID: "session-a", Owner: core.OwnerKey("parent/primary"), Provider: "codex"}, binding: binding}
			handler, err := NewProtocol8Handler(s, runtime, proof)
			if err != nil {
				t.Fatal(err)
			}
			before := bytesAt(root)
			reply := protocolCall(handler, "/v1/goals/apply", `{"revision":`+string(legacyRaw(t, wire))+`,"requester":{"pid":1}}`, principal)
			if name == "entitled" {
				if reply.Code != 200 || writer.submits != 1 || !strings.Contains(reply.Body.String(), `"applied":true`) {
					t.Fatalf("valid concrete apply: %d %s submits=%d", reply.Code, reply.Body, writer.submits)
				}
			} else if reply.Code != 409 || writer.submits != 0 || bytesAt(root) != before {
				t.Fatalf("refused effect: %d %s submits=%d", reply.Code, reply.Body, writer.submits)
			}
		})
	}
}
