package sessionmanager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// GoalReflectionContract is the original protocol vocabulary declaration. It
// is supplied by the actual configured validator, not inferred from transport
// authentication or a native completion banner.
type GoalReflectionContract struct {
	ValidationVersion       int      `json:"validation_version"`
	RevisionContractVersion string   `json:"revision_contract_version"`
	ParentGoalKind          string   `json:"parent_goal_kind"`
	ParentSuccessPredicates []string `json:"parent_success_predicates"`
	NativeReflections       []string `json:"native_reflections"`
	States                  []string `json:"states"`
}

// ControlStatusDetails comes from the real materialization catalog and goal
// validator. No legacy vocabulary is advertised when that validator is absent.
type ControlStatusDetails struct {
	MaterializationCount            int
	QuarantinedMaterializationCount int
	GoalReflectionContract          *GoalReflectionContract
}

// Protocol8GoalReflectionVocabulary is the original published vocabulary, for
// a supplier which actually enforces it. Returning these data is not validation
// or authorization and does not install a validator in a running host.
func Protocol8GoalReflectionVocabulary() GoalReflectionContract {
	return GoalReflectionContract{ValidationVersion: 2, RevisionContractVersion: "board-goal-v2", ParentGoalKind: "alignment", ParentSuccessPredicates: []string{"alignment_handled", "routing_duty_clear"}, NativeReflections: []string{"required", "not-supported"}, States: []string{"active", "cleared", "cancelled"}}
}
func copyGoalReflectionContract(src *GoalReflectionContract) *GoalReflectionContract {
	if src == nil {
		return nil
	}
	out := *src
	out.ParentSuccessPredicates = append([]string(nil), src.ParentSuccessPredicates...)
	out.NativeReflections = append([]string(nil), src.NativeReflections...)
	out.States = append([]string(nil), src.States...)
	return &out
}

// Original quarantine inventory counts every SES directory, including one with
// unreadable metadata; a read failure is not absence. Empty means unconfigured.
func quarantinedSessionCount(root string) (int, error) {
	if root == "" {
		return 0, nil
	}
	if !filepath.IsAbs(root) {
		return 0, fmt.Errorf("absolute quarantine directory required")
	}
	entries, e := os.ReadDir(root)
	if errors.Is(e, os.ErrNotExist) {
		return 0, nil
	}
	if e != nil {
		return 0, e
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "SES-") {
			count++
		}
	}
	return count, nil
}
