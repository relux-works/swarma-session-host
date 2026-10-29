//go:build unix

package sessionmanager

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Status uses real host inventory and the configured catalog/validator facts.
// The fixture verifies transfer/ownership, not the external catalog algorithm.
// Missing validator facts cannot silently advertise legacy validation support.
func TestControlStatusUsesActualInventoryAndVocabulary(t *testing.T) {
	h, _ := controlFixture(t)
	root := t.TempDir()
	for _, name := range []string{"SES-one", "SES-two", "ordinary"} {
		if e := os.Mkdir(filepath.Join(root, name), 0700); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.WriteFile(filepath.Join(root, "SES-file"), []byte("fixture"), 0600); e != nil {
		t.Fatal(e)
	}
	count, e := quarantinedSessionCount(root)
	if e != nil || count != 2 {
		t.Fatalf("actual quarantine inventory %d %v", count, e)
	}
	h.quarantinedCount = count
	vocabulary := Protocol8GoalReflectionVocabulary()
	if len(vocabulary.NativeReflections) != 2 || vocabulary.NativeReflections[1] != "not-supported" {
		t.Fatal("published native reflection spelling changed")
	}
	calls := 0
	s, e := NewControlServer(h, func(*ControlServer) http.Handler { return http.NotFoundHandler() }, WithControlServerVersion("fixture-version"), WithControlStatusDetails(func() ControlStatusDetails {
		calls++
		return ControlStatusDetails{MaterializationCount: 3, QuarantinedMaterializationCount: 4, GoalReflectionContract: &vocabulary}
	}))
	if e != nil {
		t.Fatal(e)
	}
	status := s.Status()
	if status.SessionCount != len(h.sessions) || status.QuarantinedCount != 2 || status.MaterializationCount != 3 || status.QuarantinedMaterializationCount != 4 || status.GoalReflectionContract == nil || status.GoalReflectionContract.ValidationVersion != 2 || calls != 1 {
		t.Fatalf("status facts %+v calls%d", status, calls)
	}
	status.GoalReflectionContract.States[0] = "mutated"
	if vocabulary.States[0] != "active" {
		t.Fatal("status aliases supplier vocabulary")
	}
	raw, e := json.Marshal(s.Status())
	if e != nil || !strings.Contains(string(raw), `"quarantined_materialization_count":4`) || !strings.Contains(string(raw), `"parent_success_predicates":["alignment_handled","routing_duty_clear"]`) {
		t.Fatalf("original JSON vocabulary lost %s %v", raw, e)
	}
	s.details = nil
	if s.Status().GoalReflectionContract != nil {
		t.Fatal("missing validator fabricated support")
	}
	if _, e = quarantinedSessionCount(filepath.Join(root, "SES-file")); e == nil {
		t.Fatal("inventory read failure became zero")
	}
}
