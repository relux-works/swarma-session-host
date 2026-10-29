package sessiongoal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// LegacyRevision and LegacyHead are the complete original published wire data.
// They carry no caller authority and provide no tracking store or board access.
type LegacyRevision = legacyBoardGoalRevision
type LegacyHead = legacyBoardGoalHead
type LegacyRef = core.LegacyGoalRef
type LegacyOwner = core.Protocol8SessionOwner

// LegacyReflectionContract describes the pure validator's actual vocabulary.
// A runtime must install that validator/proof before advertising these facts.
type LegacyReflectionContract struct {
	ValidationVersion       int      `json:"validation_version"`
	RevisionContractVersion string   `json:"revision_contract_version"`
	ParentGoalKind          string   `json:"parent_goal_kind"`
	ParentSuccessPredicates []string `json:"parent_success_predicates"`
	NativeReflections       []string `json:"native_reflections"`
	States                  []string `json:"states"`
}

// LegacyValidator implements the original pure revision, commit and native
// status rules. Validation of record-shaped authority is not authentication.
type LegacyValidator struct{}

func (LegacyValidator) ReflectionContract() LegacyReflectionContract {
	return LegacyReflectionContract{ValidationVersion: 2, RevisionContractVersion: legacyBoardGoalContractVersion, ParentGoalKind: string(legacyGoalKindAlignment), ParentSuccessPredicates: []string{string(legacyGoalSuccessAlignmentHandled), string(legacyGoalSuccessRoutingDutyClear)}, NativeReflections: []string{string(legacyBoardGoalNativeRequired), string(legacyBoardGoalNativeNotSupported)}, States: []string{string(legacyBoardGoalStateActive), string(legacyBoardGoalStateCleared), string(legacyBoardGoalStateCancelled)}}
}
func (LegacyValidator) ValidateRevision(revision *LegacyRevision) error {
	return legacyValidateBoardGoalRevision(revision)
}
func (LegacyValidator) ValidateCommit(revision *LegacyRevision, head *LegacyHead) error {
	return legacyValidateBoardGoalCommit(revision, head)
}
func (LegacyValidator) ValidateNativeStatus(revision *LegacyRevision, status string) error {
	return legacyValidateBoardGoalNativeStatus(revision, legacyBoardGoalNativeStatus(status))
}
func (v LegacyValidator) DecodeRevision(raw json.RawMessage) (LegacyRevision, error) {
	var revision LegacyRevision
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&revision); err != nil {
		return revision, legacyGoalError(legacyGoalErrorInvalidContract, "invalid legacy goal revision JSON", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return revision, legacyGoalError(legacyGoalErrorInvalidContract, "one legacy goal revision is required", err)
	}
	return revision, v.ValidateRevision(&revision)
}

func legacyOwnerEqual(a, b LegacyOwner) bool {
	return a.Kind == b.Kind && strings.EqualFold(strings.TrimSpace(a.ID), strings.TrimSpace(b.ID))
}
func legacyRefEqual(a, b LegacyRef) bool {
	return strings.EqualFold(strings.TrimSpace(a.ID), strings.TrimSpace(b.ID)) && a.Revision == b.Revision
}

type legacyValidationError struct {
	public *core.PublicError
	cause  error
}

func (e *legacyValidationError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %v", e.public.Error(), e.cause)
	}
	return e.public.Error()
}
func (e *legacyValidationError) Unwrap() []error {
	if e.cause != nil {
		return []error{e.public, e.cause}
	}
	return []error{e.public}
}
func legacyGoalError(code legacyGoalErrorCode, message string, cause error) error {
	return &legacyValidationError{public: &core.PublicError{Code: string(code), Message: message}, cause: cause}
}
func legacyPrimaryGoalProviderConditionError(message string) error {
	return &core.PublicError{Code: "primary_goal_provider_condition_invalid", Message: message}
}
