package sessiongoal

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// This source supplies explicitly trusted fixture data, not bridge custody,
// authentication or a replacement validation verdict.
type legacySourceFixture struct {
	fact                 LegacySourceFact
	binding              LegacySourceBinding
	calls                int
	skip, twice, swallow bool
	err                  error
}

func (s *legacySourceFixture) WithVerifiedLegacySource(ctx context.Context, b LegacySourceBinding, fn func(LegacySourceFact) error) error {
	s.calls++
	s.binding = b
	if s.skip {
		return s.err
	}
	err := fn(s.fact)
	if s.twice {
		err = errors.Join(err, fn(s.fact))
	}
	if s.swallow {
		return s.err
	}
	return errors.Join(s.err, err)
}
func legacyProofFixture(t *testing.T) (*Service, string, Revision, LegacyRevision, *legacySourceFixture) {
	t.Helper()
	s, _, root := fixture(t)
	wire := legacyParentFixture()
	c := command("retained-source-envelope", 0)
	c.Content.Objective = wire.Objective
	c.Content.Condition = wire.ProviderCondition
	if _, err := s.Handle(caller("launcher"), c); err != nil {
		t.Fatal(err)
	}
	h, err := s.Snapshot("session-a", Assignment)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	return s, root, h.Latest, wire, &legacySourceFixture{fact: LegacySourceFact{RawRevision: raw, Revision: h.Latest}}
}
func legacyRaw(t *testing.T, r LegacyRevision) json.RawMessage {
	t.Helper()
	b, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

// The actual Service produces the immutable revision. Concrete proof validates
// all original fields and the retained pair, and writes neither journal nor input.
func TestLegacyProofCompleteRetainedPair(t *testing.T) {
	_, root, stored, wire, source := legacyProofFixture(t)
	before := bytesAt(root)
	p := LegacyProof{Source: source}
	if err := p.VerifyLegacyRevision(context.Background(), legacyRaw(t, wire), stored); err != nil {
		t.Fatal(err)
	}
	want := LegacySourceBinding{SessionID: stored.SessionID, Namespace: stored.Namespace, SourceEnvelopeID: stored.SourceEnvelopeID, Ref: stored.Ref, Principal: stored.Principal, Holder: stored.Holder, HolderGeneration: stored.HolderGeneration}
	if !reflect.DeepEqual(source.binding, want) || source.calls != 1 || bytesAt(root) != before {
		t.Fatal("wrong binding or proof side effect")
	}
	if _, err := p.ReflectionContract(); err != nil {
		t.Fatal(err)
	}
	// Identical JSON with different whitespace/member order remains the same fact.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(source.fact.RawRevision, &fields); err != nil {
		t.Fatal(err)
	}
	reordered, err := json.MarshalIndent(fields, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err = p.VerifyLegacyRevision(context.Background(), reordered, stored); err != nil {
		t.Fatal(err)
	}
	if bytesAt(root) != before {
		t.Fatal("proof changed intent")
	}
}

// Every retained field is compared, including fields that the pure parent
// validator intentionally treats as opaque metadata; ID/objective equality is insufficient.
func TestLegacyProofRejectsCompleteShapeAndHostMismatch(t *testing.T) {
	for _, name := range []string{"reason", "timestamp", "mutation_authority", "scope", "budget", "envelope", "session", "namespace", "principal", "holder", "generation", "host_budget", "host_condition", "host_ref", "duplicate_member"} {
		t.Run(name, func(t *testing.T) {
			_, root, stored, wire, source := legacyProofFixture(t)
			before := bytesAt(root)
			raw := legacyRaw(t, wire)
			switch name {
			case "reason":
				wire.Reason = "different retained reason"
			case "timestamp":
				wire.UpdatedAt = wire.UpdatedAt.Add(1)
			case "mutation_authority":
				wire.MutationAuthority = legacyGoalAuthoritySystemAuthorized
			case "scope":
				wire.Scope.RequestedIDs = []string{"TASK-000000-OTHER"}
			case "budget":
				wire.Budget = &legacyGoalBudget{Kind: legacyGoalBudgetNone}
			case "envelope":
				source.fact.Revision.SourceEnvelopeID = "foreign-envelope"
			case "session":
				source.fact.Revision.SessionID = "foreign-session"
			case "namespace":
				source.fact.Revision.Namespace = Working
			case "principal":
				source.fact.Revision.Principal = "foreign-principal"
			case "holder":
				source.fact.Revision.Holder = "foreign-holder"
			case "generation":
				source.fact.Revision.HolderGeneration++
			case "host_budget":
				source.fact.Revision.Content.Budget.Tokens++
			case "host_condition":
				source.fact.Revision.Content.Condition = "different condition"
			case "host_ref":
				source.fact.Revision.Ref.Revision++
			case "duplicate_member":
				raw = []byte(strings.Replace(string(raw), `"reason":`, `"reason":"discarded duplicate","reason":`, 1))
			}
			if name != "duplicate_member" {
				raw = legacyRaw(t, wire)
			}
			err := (LegacyProof{Source: source}).VerifyLegacyRevision(context.Background(), raw, stored)
			if !errors.Is(err, ErrLegacyProvenanceMismatch) || bytesAt(root) != before {
				t.Fatalf("wrong refusal/effect: %v", err)
			}
		})
	}
}

// Missing data and broken supplier callbacks fail closed. The source cannot
// turn the concrete validator's refusal into success by swallowing an error.
func TestLegacyProofMissingAndBrokenSourceRefuses(t *testing.T) {
	for _, name := range []string{"absent", "no_callback", "twice", "swallowed_mismatch", "source_error", "cancelled_context", "invalid_contract"} {
		t.Run(name, func(t *testing.T) {
			_, root, stored, wire, source := legacyProofFixture(t)
			before := bytesAt(root)
			p := LegacyProof{Source: source}
			ctx := context.Background()
			want := ErrLegacyProvenanceMismatch
			switch name {
			case "absent":
				p.Source = nil
				want = ErrLegacyProvenanceUnavailable
			case "no_callback":
				source.skip = true
				want = ErrLegacyProvenanceUnavailable
			case "twice":
				source.twice = true
			case "swallowed_mismatch":
				source.swallow = true
				source.fact.Revision.Content.Budget.Tokens++
			case "source_error":
				source.err = ErrForbidden
				want = ErrForbidden
			case "cancelled_context":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "invalid_contract":
				wire.ContractVersion = "unsupported"
				want = nil
			}
			err := p.VerifyLegacyRevision(ctx, legacyRaw(t, wire), stored)
			if (want != nil && !errors.Is(err, want)) || (want == nil && !legacyTestCode(err, legacyGoalErrorInvalidContract)) || bytesAt(root) != before {
				t.Fatalf("wrong refusal/effect: %v", err)
			}
			if name == "invalid_contract" && source.calls != 0 {
				t.Fatal("invalid record reached source")
			}
		})
	}
	if _, err := (LegacyProof{}).ReflectionContract(); !errors.Is(err, ErrLegacyProvenanceUnavailable) {
		t.Fatal(err)
	}
}

// A genuine cleared event can be reflected. Holder acceptance has no historical
// cancellation event and is refused; neither proof nor native status creates one.
func TestLegacyProofStateDispositionNeverInventsCompletion(t *testing.T) {
	s, root, _, wire, source := legacyProofFixture(t)
	c := Command{EnvelopeID: "explicit-clear", SessionID: "session-a", Namespace: Assignment, Action: Clear, ExpectedRevision: 1}
	if _, err := s.Handle(caller("launcher"), c); err != nil {
		t.Fatal(err)
	}
	h, err := s.Snapshot("session-a", Assignment)
	if err != nil {
		t.Fatal(err)
	}
	wire.State = legacyBoardGoalStateCleared
	source.fact = LegacySourceFact{RawRevision: legacyRaw(t, wire), Revision: h.Latest}
	before := bytesAt(root)
	if err = (LegacyProof{Source: source}).VerifyLegacyRevision(context.Background(), source.fact.RawRevision, h.Latest); err != nil {
		t.Fatal(err)
	}
	h.Latest.State = Accepted
	h.Latest.Acceptance = &Acceptance{Kind: HolderAcceptance, Evidence: "explicit acceptance"}
	wire.State = legacyBoardGoalStateCancelled
	if err = (LegacyValidator{}).ValidateRevision(&wire); err != nil {
		t.Fatal("original cancelled record must remain valid", err)
	}
	if err = (LegacyProof{Source: source}).VerifyLegacyRevision(context.Background(), legacyRaw(t, wire), h.Latest); !errors.Is(err, ErrLegacyStateUnmapped) || bytesAt(root) != before {
		t.Fatalf("fabricated cancellation or effect: %v", err)
	}
}
