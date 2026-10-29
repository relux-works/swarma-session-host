package testsupport

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func CuratorMuseV4Case(t testing.TB, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata/curator-launch-env-fragment-v4", name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// CuratorMuseV4Object rebinds only the profile pin and XDG parent of the
// upstream valid-muse-no-root case; all v4 policy members remain from upstream.
func CuratorMuseV4Object(t testing.TB, parent, profile string) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(CuratorMuseV4Case(t, "valid-muse-no-root.json"), &object); err != nil {
		t.Fatal(err)
	}
	object["profile"] = map[string]string{"name": profile, "lock_sha256": strings.Repeat("a", 64)}
	env := map[string]string{}
	for name, value := range object["env"].(map[string]any) {
		env[name] = value.(string)
	}
	for _, leaf := range []string{"config", "data", "state", "cache"} {
		env["XDG_"+strings.ToUpper(leaf)+"_HOME"] = filepath.Join(parent, leaf)
	}
	object["env"] = env
	return object
}
