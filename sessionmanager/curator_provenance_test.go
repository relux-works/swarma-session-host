package sessionmanager

import (
	"encoding/json"
	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"strings"
	"testing"
)

// These original pure decoder roots preserve ambiguity refusal before typed decode.
// They use literal fixture provenance and perform no Curator or credential operations.
func curatorIdentityProvenance() agentic.CuratorContextProvenance {
	lock := strings.Repeat("a", 64)
	return agentic.CuratorContextProvenance{
		ProfileName:         "test",
		LockSHA256:          lock,
		ManagedHomeVariable: "CODEX_HOME",
		ManagedHome:         "/managed/home",
		Fragment: agentic.CuratorFragmentIdentity{
			Revision:    agentic.CuratorLaunchFragmentV1,
			Environment: "codex_cli",
			Profile:     agentic.CuratorProfilePin{Name: "test", LockSHA256: lock},
			Precedence:  agentic.CuratorPrecedence{Winner: "higher-weight", Placement: "winner-last"},
			Env:         map[string]string{"CODEX_HOME": "/managed/home"},
		},
	}
}

func curatorIdentitySources(t *testing.T, provenance agentic.CuratorContextProvenance) []LaunchPlanSource {
	t.Helper()
	encoded, err := json.Marshal(provenance)
	if err != nil {
		t.Fatal(err)
	}
	return []LaunchPlanSource{
		{Kind: "curator-fragment", Path: provenance.ManagedHome, Scope: "profile=" + provenance.ProfileName},
		{Kind: "curator-fragment-pins", Path: "lock=" + provenance.LockSHA256, Scope: "digest=fixture"},
		{Kind: CuratorProvenanceSourceKind, Path: string(encoded)},
	}
}

func TestRejectDuplicateProvenanceMembers(t *testing.T) {
	for _, row := range []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"empty object admits", `{}`, false},
		{"distinct members admit", `{"a":1,"b":[1,2],"c":{"d":null}}`, false},
		{"sibling repeats admit", `[{"a":1},{"a":1}]`, false},
		{"top-level repeat refuses", `{"a":1,"a":2}`, true},
		{"same-value repeat refuses", `{"a":1,"a":1}`, true},
		{"nested repeat refuses", `{"a":{"b":1,"b":2}}`, true},
		{"nested repeat refuses at depth", `{"a":{"b":{"c":1,"c":2}}}`, true},
		{"repeat inside array refuses", `{"a":[{"b":1,"b":2}]}`, true},
		{"permissions conflict refuses", `{"permissions":{"mode":"native","locked":true,"source":"global","locked":false}}`, true},
		// r5 case-fold aliases: encoding/json maps member names onto
		// struct fields case-insensitively with Unicode simple
		// folding, so any EqualFold pair in one object is the same
		// ambiguity as an exact repeat.
		{"case-variant repeat refuses", `{"a":1,"A":2}`, true},
		{"permissions alias refuses", `{"permissions":{"mode":"native","locked":true,"source":"global"},"Permissions":{"mode":"native","locked":false,"source":"profile"}}`, true},
		{"long-s alias refuses", "{\"permi\u017fsions\":{\"mode\":\"native\"},\"permissions\":{\"mode\":\"native\"}}", true},
		{"kelvin alias refuses", "{\"loc\u212aed\":1,\"locked\":2}", true},
		{"permissions inner case variant refuses", `{"permissions":{"mode":"native","locked":true,"source":"global","Locked":false}}`, true},
		{"sibling case variants admit", `[{"a":1},{"A":1}]`, false},
		{"malformed passes to decode", `{bad`, false},
		{"truncated passes to decode", `{"a":`, false},
		{"trailing data passes to decode", `{"a":1} {"a":1}`, false},
		{"top-level scalar passes to decode", `1`, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			err := rejectDuplicateProvenanceMembers(row.raw)
			if row.wantErr && err == nil {
				t.Fatalf("rejectDuplicateProvenanceMembers(%s) = nil, want the duplicate refusal", row.raw)
			}
			if !row.wantErr && err != nil {
				t.Fatalf("rejectDuplicateProvenanceMembers(%s) = %v, want nil", row.raw, err)
			}
		})
	}
}

func TestExtractCuratorProvenanceRefusesFoldedPermissionsMember(t *testing.T) {
	encoded, err := json.Marshal(coreLockedClaudeDaemonProvenance())
	if err != nil {
		t.Fatal(err)
	}
	const anchor = `"permissions":{"mode":"native","locked":true,"source":"global"}`
	if strings.Count(string(encoded), anchor) != 1 {
		t.Fatalf("permissions anchor occurs %d times, want exactly once", strings.Count(string(encoded), anchor))
	}
	spliced := strings.Replace(string(encoded), anchor,
		`"permissions":{"mode":"native","locked":true,"source":"global","Locked":false}`, 1)
	_, _, err = extractCuratorProvenance([]LaunchPlanSource{{Kind: CuratorProvenanceSourceKind, Path: spliced}})
	if err == nil || !strings.Contains(err.Error(), `duplicate member "Locked"`) {
		t.Fatalf("extractCuratorProvenance = %v; want the duplicate-member refusal naming \"Locked\"", err)
	}
}

func coreLockedClaudeDaemonProvenance() agentic.CuratorContextProvenance {
	lock := strings.Repeat("a", 64)
	return agentic.CuratorContextProvenance{
		ProfileName:         "test",
		LockSHA256:          lock,
		ManagedHomeVariable: "CLAUDE_CONFIG_DIR",
		ManagedHome:         "/managed/home",
		Fragment: agentic.CuratorFragmentIdentity{
			Revision:    agentic.CuratorLaunchFragmentV3,
			Environment: "claude_code",
			Profile:     agentic.CuratorProfilePin{Name: "test", LockSHA256: lock},
			Precedence:  agentic.CuratorPrecedence{Winner: "higher-weight", Placement: "winner-last"},
			Env:         map[string]string{"CLAUDE_CONFIG_DIR": "/managed/home"},
			Permissions: &agentic.CuratorPermissions{
				Mode: agentic.CuratorPermissionNative, Locked: true, Source: agentic.CuratorPermissionSourceGlobal,
			},
		},
	}
}
