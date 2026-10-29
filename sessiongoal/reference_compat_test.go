package sessiongoal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Generated durable refs pass the real archival accounting codec unchanged;
// invalid refs and stale native identity refuse. This does not admit restore or
// qualify a native provider, bridge caller, or live accounting image supplier.
func TestGoalGeneratedReferenceRoundTripsActualMuseAccounting(t *testing.T) {
	s, _, _ := fixture(t)
	receipt, err := s.Handle(caller("launcher"), command("codec", 0))
	if err != nil {
		t.Fatal(err)
	}
	record, err := os.ReadFile(filepath.Join("..", "sessionmanager", "testdata", "legacy-records", "muse-native-initial.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(record, &fields); err != nil {
		t.Fatal(err)
	}
	raw := fields["provider_host"]
	codec := core.NewLegacyRecordCodec()
	state, err := codec.DecodeMuseAccounting(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	state.GoalRef = &core.LegacyGoalRef{ID: receipt.Ref.ID, Revision: receipt.Ref.Revision}
	encoded, err := codec.EncodeMuseAccounting(context.Background(), raw, state)
	if err != nil {
		t.Fatalf("generated service ref rejected by actual accounting codec: %s: %v", receipt.Ref.ID, err)
	}
	got, err := codec.DecodeMuseAccounting(context.Background(), encoded)
	if err != nil || got.GoalRef == nil || *got.GoalRef != *state.GoalRef {
		t.Fatalf("ref changed across codec: %+v %v", got.GoalRef, err)
	}
	if got.SessionID != state.SessionID || got.WorkspaceRoot != state.WorkspaceRoot || got.GoalTransport != state.GoalTransport {
		t.Fatal("native identity changed")
	}
	for _, name := range []string{"old_id", "zero_revision", "stale_session", "stale_workspace", "stale_transport"} {
		t.Run(name, func(t *testing.T) {
			invalid := state
			ref := *state.GoalRef
			invalid.GoalRef = &ref
			switch name {
			case "old_id":
				invalid.GoalRef.ID = "sg-0123456789abcdef0123456789abcdef"
			case "zero_revision":
				invalid.GoalRef.Revision = 0
			case "stale_session":
				invalid.SessionID += "-stale"
			case "stale_workspace":
				invalid.WorkspaceRoot += "-stale"
			case "stale_transport":
				invalid.GoalTransport = "text"
			}
			rejected, e := codec.EncodeMuseAccounting(context.Background(), raw, invalid)
			if e == nil || len(rejected) != 0 {
				t.Fatalf("invalid accounting replacement admitted: %s: %v", name, e)
			}
			if strings.HasPrefix(name, "stale_") && !errors.Is(e, core.ErrLegacyViewStale) {
				t.Fatalf("wrong stale refusal: %v", e)
			}
		})
	}
}

// The same session/namespace retains one deterministic digest, while namespace
// and session changes cannot reuse it. The new prefix is codec-compatible.
func TestGoalReferenceAllocationKeepsScopedDigest(t *testing.T) {
	seen := map[string]bool{}
	for _, pair := range []struct {
		session   string
		namespace Namespace
	}{{"session-a", Assignment}, {"session-a", Working}, {"session-b", Assignment}} {
		sum := sha256.Sum256([]byte(string(Owner(pair.session, pair.namespace))))
		want := "GOAL-SG-" + strings.ToUpper(hex.EncodeToString(sum[:16]))
		got := referenceID(pair.session, pair.namespace)
		if got != want || seen[got] || referenceID(pair.session, pair.namespace) != got {
			t.Fatalf("allocation changed digest/scope: %q", got)
		}
		seen[got] = true
	}
}

// An isolated historical sg journal is refused without rewriting any bytes or
// reaching native input, including through an already-open Service instance.
func TestGoalHistoricalSGJournalRefusesWithoutMigrationOrInput(t *testing.T) {
	s, a, root := fixture(t)
	receipt, err := s.Handle(caller("launcher"), command("historical", 0))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(string(Owner("session-a", Assignment))))
	old := "sg-" + hex.EncodeToString(sum[:16])
	historical := strings.ReplaceAll(bytesAt(root), receipt.Ref.ID, old)
	if err = os.WriteFile(filepath.Join(root, "journal.json"), []byte(historical), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(root, a); !errors.Is(err, ErrJournalInvalid) {
		t.Fatalf("historical journal admitted: %v", err)
	}
	writer := newWriter()
	binding, err := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", writer, budgetFixture{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Reconcile(Owner("session-a", Assignment), binding); !errors.Is(err, ErrJournalInvalid) || writer.submits != 0 || bytesAt(root) != historical {
		t.Fatalf("historical journal changed or delivered: %v submits=%d", err, writer.submits)
	}
}
