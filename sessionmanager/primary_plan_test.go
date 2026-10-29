package sessionmanager

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

// The original decode controls retain complete managed-client and additive composer payloads.
func TestDecodePrimarySessionLaunchPlanAcceptsManagedClientContract(t *testing.T) {
	project := t.TempDir()
	executable := filepath.Join(project, "codex")
	document := map[string]any{
		"contract":       PrimarySessionLaunchPlanContract,
		"schema_version": 1,
		"status":         "ok",
		"producer":       map[string]any{"version": "test", "commit": "abc"},
		"provider":       "codex",
		"project_dir":    project,
		"executable":     executable,
		"launch_variants": map[string]any{
			"interactive":    map[string]any{"argv": []string{"--no-alt-screen", "prompt"}},
			"managed_host":   map[string]any{"kind": "codex-app-server", "argv": []string{"app-server"}},
			"managed_client": map[string]any{"argv": []string{"--no-alt-screen", "prompt"}},
		},
		"resolved": map[string]any{
			"model":     map[string]any{"value": "gpt-test", "source": "test"},
			"reasoning": map[string]any{"value": "high", "source": "test"},
			"yolo":      map[string]any{"value": false, "source": "native"},
			"sandbox":   map[string]any{"value": "workspace-write", "source": "test"},
			"profile":   map[string]any{"value": nil, "source": "native"},
			"approval":  map[string]any{"value": "on-request", "source": "test"},
			"mcp":       map[string]any{"servers": []any{}, "sources": []string{}},
		},
		"required_env_names": []string{},
		"sources":            []any{},
	}
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := DecodePrimarySessionLaunchPlan(data)
	if err != nil {
		t.Fatal(err)
	}
	if plan.LaunchVariants.ManagedHost.Kind != "codex-app-server" ||
		len(plan.LaunchVariants.ManagedClient.Argv) != 2 ||
		plan.LaunchVariants.ManagedClient.Argv[1] != "prompt" {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestDecodePrimarySessionLaunchPlanAcceptsNewerComposerResolvedKeys(t *testing.T) {
	project := t.TempDir()
	document := map[string]any{
		"contract":       PrimarySessionLaunchPlanContract,
		"schema_version": 1,
		"status":         "ok",
		"producer":       map[string]any{"version": "v1.6.1", "commit": "302a445"},
		"provider":       "claude",
		"project_dir":    project,
		"executable":     filepath.Join(project, "claude"),
		"launch_variants": map[string]any{
			"interactive":    map[string]any{"argv": []string{"prompt"}},
			"managed_host":   map[string]any{"kind": "claude-pty", "argv": []string{"--print"}},
			"managed_client": map[string]any{"argv": []string{"prompt"}},
		},
		"resolved": map[string]any{
			"model":            map[string]any{"value": "claude-test", "source": "test"},
			"reasoning":        map[string]any{"value": nil, "source": "native"},
			"yolo":             map[string]any{"value": true, "source": "test"},
			"sandbox":          map[string]any{"value": nil, "source": "not_applicable"},
			"profile":          map[string]any{"value": nil, "source": "not_applicable"},
			"approval":         map[string]any{"value": "bypass-permissions", "source": "test"},
			"mcp":              map[string]any{"servers": []any{}, "sources": []string{}},
			"pi_compatibility": map[string]any{"value": nil, "source": ""},
		},
		"required_env_names": []string{},
		"sources":            []any{},
	}
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := DecodePrimarySessionLaunchPlan(data)
	if err != nil {
		t.Fatalf("newer composer plan refused: %v", err)
	}
	if plan.Resolved.Model.Value != "claude-test" {
		t.Fatalf("plan = %#v", plan)
	}
}
