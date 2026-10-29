package sessionmanager

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"io"
	"reflect"
	"strings"
)

const CuratorProvenanceSourceKind = "curator-fragment-provenance"
const ErrorSessionCuratorIdentityStale = "session_curator_identity_stale"

func extractCuratorProvenance(sources []LaunchPlanSource) (agentic.CuratorContextProvenance, bool, error) {
	provenance, present, err := decodeCuratorProvenance(sources)
	if err != nil || !present {
		return provenance, present, err
	}
	if err := validateCuratorProvenance(provenance); err != nil {
		return agentic.CuratorContextProvenance{}, true, err
	}
	return provenance, true, nil
}

func decodeCuratorProvenance(sources []LaunchPlanSource) (agentic.CuratorContextProvenance, bool, error) {
	var encoded string
	found := false
	curatorSelected := false
	for _, source := range sources {
		if source.Kind == "curator-fragment" || source.Kind == "curator-fragment-pins" {
			curatorSelected = true
		}
		if source.Kind != CuratorProvenanceSourceKind {
			continue
		}
		if found {
			return agentic.CuratorContextProvenance{}, true, fmt.Errorf("duplicate %s plan source", CuratorProvenanceSourceKind)
		}
		found = true
		encoded = source.Path
	}
	if !found {
		if curatorSelected {
			return agentic.CuratorContextProvenance{}, true, fmt.Errorf("Curator sources have no provenance")
		}
		return agentic.CuratorContextProvenance{}, false, nil
	}
	// ambiguous provenance refuses before the
	// typed decode. Go's decoder keeps the last duplicate object member
	// silently (encoding/json/decode.go), and the permissions wire
	// decoder collapses through a member map first, so
	// {"locked":true,...,"locked":false,"source":"profile"} would
	// validate as an unlocked grant. A repeated member at ANY level
	// makes the lock state unreadable, never last-wins. r5: the
	// comparison folds case like the decoder (EqualFold), so
	// "Permissions" after "permissions" is the same refusal.
	if err := rejectDuplicateProvenanceMembers(encoded); err != nil {
		return agentic.CuratorContextProvenance{}, true, fmt.Errorf("decoding the %s plan source: %w", CuratorProvenanceSourceKind, err)
	}
	var provenance agentic.CuratorContextProvenance
	decoder := json.NewDecoder(bytes.NewBufferString(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&provenance); err != nil {
		return agentic.CuratorContextProvenance{}, true, fmt.Errorf("decoding the %s plan source: %w", CuratorProvenanceSourceKind, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return agentic.CuratorContextProvenance{}, true, fmt.Errorf("trailing Curator provenance data")
	}
	return provenance, true, nil
}

func rejectDuplicateProvenanceMembers(encoded string) error {
	type frame struct {
		object    bool
		expectKey bool
		seen      []string
	}
	var stack []frame
	decoder := json.NewDecoder(strings.NewReader(encoded))
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil
		}
		switch value := token.(type) {
		case json.Delim:
			switch value {
			case '{', '[':
				stack = append(stack, frame{object: value == '{', expectKey: true})
			default:
				if len(stack) == 0 {
					return nil
				}
				stack = stack[:len(stack)-1]
				if len(stack) == 0 {
					return nil
				}
				if top := &stack[len(stack)-1]; top.object {
					top.expectKey = true
				}
			}
		case string:
			if len(stack) == 0 {
				return nil
			}
			if top := &stack[len(stack)-1]; top.object {
				if top.expectKey {
					for _, prior := range top.seen {
						if strings.EqualFold(prior, value) {
							return fmt.Errorf("duplicate member %q in Curator provenance", value)
						}
					}
					top.seen = append(top.seen, value)
					top.expectKey = false
				} else {
					top.expectKey = true
				}
			}
		default:
			if len(stack) == 0 {
				return nil
			}
			if top := &stack[len(stack)-1]; top.object {
				top.expectKey = true
			}
		}
	}
}

func validateCuratorProvenance(p agentic.CuratorContextProvenance) error {
	f := p.Fragment
	context := &agentic.CuratorContext{
		Revision: f.Revision, Environment: f.Environment, Profile: f.Profile,
		Precedence: f.Precedence, Env: f.Env, MCP: f.MCP, PathPrepend: f.PathPrepend,
		Permissions:     f.Permissions,
		CredentialStore: f.CredentialStore,
		RootContext:     f.RootContext,
	}
	if f.SystemPrompt != nil {
		context.SystemPrompt = &agentic.CuratorSystemPromptContext{
			Path: f.SystemPrompt.Path, Channels: f.SystemPrompt.Channels, Intent: p.SystemPromptIntent,
		}
	} else if p.SystemPromptIntent != "" {
		return fmt.Errorf("prompt intent without a system-prompt fragment")
	}
	if p.ProfileName != f.Profile.Name || p.LockSHA256 != f.Profile.LockSHA256 {
		return fmt.Errorf("inconsistent Curator provenance")
	}
	if agentic.CuratorFragmentUsesXDGHomes(f.Environment) {
		// The XDG-home environments carry no single managed-home
		// variable: it is empty by design, the managed home is the shared
		// XDG parent, and the verified four-home set carries the rest.
		homes, err := context.MuseHomes()
		if err != nil || p.ManagedHomeVariable != "" || p.ManagedHome == "" ||
			p.ManagedHome != homes.Parent || p.MuseHomes == nil || *p.MuseHomes != homes {
			return fmt.Errorf("inconsistent Curator provenance")
		}
	} else if p.ManagedHomeVariable == "" || p.ManagedHome == "" || f.Env[p.ManagedHomeVariable] != p.ManagedHome {
		return fmt.Errorf("inconsistent Curator provenance")
	}
	return agentic.ValidateCuratorContext(context, p.ManagedHome)
}

func curatorProvenanceMismatchMember(stored, fresh agentic.CuratorContextProvenance) string {
	switch {
	case stored.ProfileName != fresh.ProfileName:
		return "profile"
	case stored.LockSHA256 != fresh.LockSHA256:
		return "profile lock"
	case stored.ManagedHomeVariable != fresh.ManagedHomeVariable:
		return "managed-home variable"
	case stored.ManagedHome != fresh.ManagedHome:
		return "managed home"
	case !reflect.DeepEqual(stored.MuseHomes, fresh.MuseHomes):
		return "muse home set"
	case stored.SystemPromptIntent != fresh.SystemPromptIntent:
		return "system-prompt intent"
	}
	fragment, current := stored.Fragment, fresh.Fragment
	switch {
	case fragment.Environment != current.Environment:
		return "fragment environment"
	case fragment.Profile != current.Profile:
		return "fragment profile pin"
	case fragment.Precedence != current.Precedence:
		return "fragment precedence"
	case !reflect.DeepEqual(fragment.Env, current.Env):
		return "fragment home entry"
	case !reflect.DeepEqual(fragment.SystemPrompt, current.SystemPrompt):
		return "fragment system-prompt channel"
	case !reflect.DeepEqual(fragment.MCP, current.MCP):
		return "fragment MCP channel"
	case fragment.PathPrepend != current.PathPrepend:
		return "fragment path prepend"
	case !reflect.DeepEqual(fragment.Permissions, current.Permissions):
		return "fragment permissions"
	default:
		return "fragment value"
	}
}

func validCuratorDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == strings.ToLower(value)
}

type curatorIdentityReadError struct {
	public error
	cause  error
}

func unreadableCuratorIdentityError(reason string, cause error) error {
	return &curatorIdentityReadError{public: staleCuratorIdentityError(reason), cause: cause}
}

func staleCuratorIdentityError(reason string) error {
	return &PublicError{Code: ErrorSessionCuratorIdentityStale, Message: reason}
}

func (e *curatorIdentityReadError) Error() string { return e.public.Error() }

func (e *curatorIdentityReadError) Unwrap() []error { return []error{e.public, e.cause} }
