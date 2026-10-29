package sessiongoal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

var (
	ErrLegacyProvenanceUnavailable = errors.New("authenticated retained legacy source is unavailable")
	ErrLegacyProvenanceMismatch    = errors.New("legacy source does not match the retained immutable session revision")
	ErrLegacyStateUnmapped         = errors.New("legacy and host goal states have no matching historical event")
)

// LegacySourceBinding comes exclusively from the stored host revision, never
// from request owner/PID/UID claims. Source must authenticate the retained pair
// for this exact binding and fence it for the callback, independently from the
// transport's separate current caller/grant/controller admission.
type LegacySourceBinding struct {
	SessionID        string
	Namespace        Namespace
	SourceEnvelopeID string
	Ref              core.RevisionRef
	Principal        string
	Holder           string
	HolderGeneration uint64
}

// LegacySourceFact is an authenticated immutable pair retained when the raising
// source accepted the typed host revision. It includes every original wire
// field and every host revision field; neither can be reconstructed from IDs.
// This package does not implement a registrar, signatures or external custody.
type LegacySourceFact struct {
	RawRevision json.RawMessage
	Revision    Revision
}
type LegacySource interface {
	WithVerifiedLegacySource(context.Context, LegacySourceBinding, func(LegacySourceFact) error) error
}

// LegacyProof is the concrete original validator plus complete pair comparison.
// No custom success predicate/validator is injected. Source supplies verified
// data, not an acceptance verdict; an absent source fails before any effect.
type LegacyProof struct{ Source LegacySource }

// ReflectionContract is available only for a configured concrete proof. A
// nonnil source is a wiring precondition, not qualification of a live bridge.
func (p LegacyProof) ReflectionContract() (LegacyReflectionContract, error) {
	if absent(p.Source) {
		return LegacyReflectionContract{}, ErrLegacyProvenanceUnavailable
	}
	return (LegacyValidator{}).ReflectionContract(), nil
}
func (p LegacyProof) VerifyLegacyRevision(ctx context.Context, raw json.RawMessage, stored Revision) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Snapshot mutable caller pointers before consulting any external source.
	encoded, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(encoded, &stored); err != nil {
		return err
	}
	raw = append(json.RawMessage(nil), raw...)
	revision, err := (LegacyValidator{}).DecodeRevision(raw)
	if err != nil {
		return err
	}
	if stored.Namespace != Assignment || stored.SessionID == "" || stored.SourceEnvelopeID == "" || stored.Ref.Revision < 1 || stored.Ref.ID == "" || strings.TrimSpace(stored.Principal) == "" || strings.TrimSpace(stored.Holder) == "" || stored.HolderGeneration == 0 || !validContent(stored.Content) {
		return ErrLegacyProvenanceMismatch
	}
	if stored.State == Accepted || revision.State == legacyBoardGoalStateCancelled {
		return ErrLegacyStateUnmapped
	}
	if (stored.State != Active && stored.State != Cleared) || (string(revision.State) != string(stored.State)) || stored.Acceptance != nil {
		return ErrLegacyStateUnmapped
	}
	if absent(p.Source) {
		return ErrLegacyProvenanceUnavailable
	}
	shape, err := legacyRevisionShape(raw)
	if err != nil {
		return err
	}
	binding := LegacySourceBinding{SessionID: stored.SessionID, Namespace: stored.Namespace, SourceEnvelopeID: stored.SourceEnvelopeID, Ref: stored.Ref, Principal: stored.Principal, Holder: stored.Holder, HolderGeneration: stored.HolderGeneration}
	called := false
	var operationErr error
	sourceErr := p.Source.WithVerifiedLegacySource(ctx, binding, func(fact LegacySourceFact) error {
		if called {
			operationErr = errors.Join(operationErr, ErrLegacyProvenanceMismatch)
			return operationErr
		}
		called = true
		if err := ctx.Err(); err != nil {
			operationErr = err
			return err
		}
		// The complete immutable host revision must agree, including content budget,
		// state, principal, envelope and holder generation. No reconstructed defaults.
		if !reflect.DeepEqual(fact.Revision, stored) {
			operationErr = ErrLegacyProvenanceMismatch
			return operationErr
		}
		sourceRaw := append(json.RawMessage(nil), fact.RawRevision...)
		if _, err := (LegacyValidator{}).DecodeRevision(sourceRaw); err != nil {
			operationErr = err
			return err
		}
		expected, err := legacyRevisionShape(sourceRaw)
		if err != nil {
			operationErr = err
			return err
		}
		if !reflect.DeepEqual(shape, expected) {
			operationErr = ErrLegacyProvenanceMismatch
			return operationErr
		}
		return nil
	})
	if !called && sourceErr == nil {
		return ErrLegacyProvenanceUnavailable
	}
	return errors.Join(sourceErr, operationErr)
}

// This private representation compares the entire JSON shape while allowing
// member order/whitespace differences. All keys, values, array order and even
// duplicate-member occurrences are retained; no field is projected away.
type legacyShapeMember struct {
	Key   string
	Value any
}
type legacyShapeObject struct{ Members []legacyShapeMember }
type legacyShapeArray struct{ Items []any }

func legacyRevisionShape(raw []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, err := legacyShapeValue(d)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, ErrLegacyProvenanceMismatch
	}
	return value, nil
}
func legacyShapeValue(d *json.Decoder) (any, error) {
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := legacyShapeObject{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			value, err := legacyShapeValue(d)
			if err != nil {
				return nil, err
			}
			object.Members = append(object.Members, legacyShapeMember{Key: key.(string), Value: value})
		}
		if _, err = d.Token(); err != nil {
			return nil, err
		}
		sort.SliceStable(object.Members, func(i, j int) bool { return object.Members[i].Key < object.Members[j].Key })
		return object, nil
	case json.Delim('['):
		array := legacyShapeArray{}
		for d.More() {
			value, err := legacyShapeValue(d)
			if err != nil {
				return nil, err
			}
			array.Items = append(array.Items, value)
		}
		if _, err = d.Token(); err != nil {
			return nil, err
		}
		return array, nil
	default:
		return token, nil
	}
}
