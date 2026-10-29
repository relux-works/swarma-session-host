package musepty

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"github.com/relux-works/swarma-session-host/musepty/internal/testsupport"
)

func museTestFragmentDoc(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	object := testsupport.CuratorMuseV4Object(t, t.TempDir(), "operator")
	object["permissions"].(map[string]any)["source"] = "default"
	if mutate != nil {
		mutate(object)
	}
	document, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func museTestFragmentCode(t *testing.T, err error) string {
	t.Helper()
	var fragmentErr *MuseFragmentError
	if !errors.As(err, &fragmentErr) {
		t.Fatalf("error = %v, want a typed fragment refusal", err)
	}
	return fragmentErr.Code
}

func TestDecodeMuseCuratorFragmentAcceptsValidV4(t *testing.T) {
	document := museTestFragmentDoc(t, nil)
	fragment, err := DecodeMuseCuratorFragment(document)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if fragment.Profile.Name != "operator" || fragment.Profile.LockSHA256 != strings.Repeat("a", 64) {
		t.Fatalf("profile = %#v", fragment.Profile)
	}
	if len(fragment.Env) != 5 || MuseFragmentManagedHome(fragment) == "" {
		t.Fatalf("env = %#v", fragment.Env)
	}
	if !strings.HasSuffix(MuseFragmentManagedHome(fragment), "/data") {
		t.Fatalf("managed home = %q, want XDG_DATA_HOME", MuseFragmentManagedHome(fragment))
	}
	sum := sha256.Sum256(document)
	if want := hex.EncodeToString(sum[:]); MuseFragmentDigest(document) != want {
		t.Fatalf("digest mismatch")
	}
}

func TestDecodeMuseCuratorFragmentRefusesEachClass(t *testing.T) {
	cases := []struct {
		name     string
		document func(t *testing.T) []byte
		code     string
	}{
		{"malformed", func(t *testing.T) []byte { return []byte(`{`) }, MuseFragmentMalformed},
		{"trailing", func(t *testing.T) []byte {
			// A second JSON value, not whitespace: trailing whitespace
			// is valid JSON and the decoder rightly accepts it.
			return append(museTestFragmentDoc(t, nil), []byte(`{}`)...)
		}, MuseFragmentMalformed},
		{"unknown-version", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) { object["fragment"] = "launch-env-fragment-v1" })
		}, MuseFragmentVersionUnknown},
		{"wrong-environment", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) { object["environment"] = "codex_cli" })
		}, MuseFragmentMalformed},
		{"missing-profile-name", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["profile"] = map[string]string{"name": "", "lock_sha256": strings.Repeat("a", 64)}
			})
		}, MuseFragmentProfileMissing},
		{"missing-profile-lock", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["profile"] = map[string]string{"name": "operator", "lock_sha256": ""}
			})
		}, MuseFragmentProfileMissing},
		{"unknown-descriptor", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["env"].(map[string]string)["WHATEVER_HOME"] = "/tmp/x"
			})
		}, MuseFragmentDescriptorUnknown},
		{"home-member", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["env"].(map[string]string)["HOME"] = "/tmp/x"
			})
		}, MuseFragmentMalformed},
		{"missing-parent", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				delete(object["env"].(map[string]string), MuseXDGCacheHome)
			})
		}, MuseFragmentMalformed},
		{"relative-parent", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["env"].(map[string]string)[MuseXDGDataHome] = "relative/data"
			})
		}, MuseFragmentMalformed},
		{"wrong-leaf-parent", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				env := object["env"].(map[string]string)
				env[MuseXDGDataHome] = env[MuseXDGDataHome] + "/nested"
			})
		}, MuseFragmentMalformed},
		{"root-parent", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["env"] = map[string]string{
					MuseXDGDataHome:   "/data",
					MuseXDGConfigHome: "/config",
					MuseXDGCacheHome:  "/cache",
					MuseXDGStateHome:  "/state",
				}
			})
		}, MuseFragmentMalformed},
		{"missing-permissions", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				delete(object, "permissions")
			})
		}, MuseFragmentMalformed},
		{"unknown-member", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["muse_future_member"] = "x"
			})
		}, MuseFragmentMalformed},
		{"bad-permissions-mode", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["permissions"].(map[string]any)["mode"] = "admin"
			})
		}, MuseFragmentMalformed},
		{"bad-permissions-source", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["permissions"].(map[string]any)["source"] = "fleet"
			})
		}, MuseFragmentMalformed},
		{"permissions-lock-mismatch", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["permissions"].(map[string]any)["locked"] = true
			})
		}, MuseFragmentMalformed},
		{"permissions-yolo-default", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["permissions"].(map[string]any)["mode"] = "yolo"
			})
		}, MuseFragmentMalformed},
		{"permissions-unknown-member", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["permissions"].(map[string]any)["level"] = "x"
			})
		}, MuseFragmentMalformed},
		{"permissions-missing-locked", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				delete(object["permissions"].(map[string]any), "locked")
			})
		}, MuseFragmentMalformed},
		{"mcp-on-muse", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["mcp"] = map[string]any{"path": "/tmp/mcp.json", "env_names": []string{}, "channels": []any{}}
			})
		}, MuseFragmentMalformed},
		{"system-prompt-on-muse", func(t *testing.T) []byte {
			return museTestFragmentDoc(t, func(object map[string]any) {
				object["system_prompt"] = map[string]any{"path": "/tmp/prompt.md", "channels": []any{}}
			})
		}, MuseFragmentMalformed},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := DecodeMuseCuratorFragment(testCase.document(t))
			if err == nil {
				t.Fatalf("decode succeeded, want %s", testCase.code)
			}
			if code := museTestFragmentCode(t, err); code != testCase.code {
				t.Fatalf("code = %q, want %q", code, testCase.code)
			}
		})
	}
}

func TestMergeMuseFragmentEnvWinsXDGHomesAndKeepsHome(t *testing.T) {
	fragment, err := DecodeMuseCuratorFragment(museTestFragmentDoc(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	parent := []string{"HOME=/operator", "PATH=/usr/bin:/bin", "XDG_DATA_HOME=/stale", "TERM=xterm", "TBH_CREDENTIAL_BACKEND=ambient"}
	merged := MergeMuseFragmentEnv(parent, fragment)
	seen := map[string]int{}
	for _, entry := range merged {
		name, _, _ := strings.Cut(entry, "=")
		seen[name]++
	}
	for _, name := range museFragmentEnvNames {
		if seen[name] != 1 {
			t.Fatalf("%s occurs %d times", name, seen[name])
		}
	}
	if got := museTestEnvValue(merged, "HOME"); got != "/operator" {
		t.Fatalf("HOME = %q", got)
	}
	if got := museTestEnvValue(merged, "XDG_DATA_HOME"); got != fragment.Env[MuseXDGDataHome] {
		t.Fatalf("XDG_DATA_HOME = %q, want the fragment value", got)
	}
	if got := museTestEnvValue(merged, "TBH_CREDENTIAL_BACKEND"); got != "file" {
		t.Fatalf("credential backend = %q", got)
	}
	if got := museTestEnvValue(merged, "TERM"); got != "xterm" {
		t.Fatalf("TERM = %q", got)
	}
}

func museTestEnvValue(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		if value, ok := strings.CutPrefix(env[i], key+"="); ok {
			return value
		}
	}
	return ""
}

func TestApplyMuseBinarySelectionFloatsSelectedDirectory(t *testing.T) {
	env := []string{"PATH=/usr/bin:/bin", "HOME=/operator"}
	unchanged, err := ApplyMuseBinarySelection(env, "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(unchanged, "\x00") != strings.Join(env, "\x00") {
		t.Fatalf("empty selection rewrote the env: %q", unchanged)
	}
	selected, err := ApplyMuseBinarySelection(env, "/opt/frozen/muse")
	if err != nil {
		t.Fatal(err)
	}
	if got := museTestEnvValue(selected, "PATH"); got != "/opt/frozen:/usr/bin:/bin" {
		t.Fatalf("PATH = %q", got)
	}
	if _, err := ApplyMuseBinarySelection(env, "relative/muse"); err == nil {
		t.Fatalf("relative selection accepted")
	}
	if _, err := ApplyMuseBinarySelection(env, "/"); err == nil {
		t.Fatalf("root selection accepted")
	}
}

func TestValidateMuseOperatorNativeArgsRefusesResumeOnly(t *testing.T) {
	if err := ValidateMuseOperatorNativeArgs(nil); err != nil {
		t.Fatalf("empty: %v", err)
	}
	if err := ValidateMuseOperatorNativeArgs([]string{"--model", "x", "resumed", "resume-later"}); err != nil {
		t.Fatalf("near-miss tokens: %v", err)
	}
	if err := ValidateMuseOperatorNativeArgs([]string{"resume", "abc"}); err == nil || !strings.Contains(err.Error(), "manager-composed") {
		t.Fatalf("error = %v", err)
	}
	if err := ValidateMuseOperatorNativeArgs([]string{"--", "resume"}); err == nil {
		t.Fatalf("positional resume accepted")
	}
}

func TestBuildMuseLaunchRequestComposesColdAndResume(t *testing.T) {
	fragment, err := DecodeMuseCuratorFragment(museTestFragmentDoc(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	params := MuseLaunchRequestParams{
		Model:        "echo",
		WorkDir:      t.TempDir(),
		Env:          []string{"HOME=/operator", "PATH=/usr/bin:/bin"},
		Fragment:     fragment,
		OperatorArgs: []string{"--workspace-extra"},
	}
	cold, err := BuildMuseLaunchRequest(params)
	if err != nil {
		t.Fatalf("cold: %v", err)
	}
	if cold.System != "muse" || !cold.PermissionMode.IsZero() && cold.PermissionMode != agentic.PermissionModeNative {
		t.Fatalf("cold posture = %q", cold.PermissionMode)
	}
	if len(cold.NativeArgs) != 2 || cold.NativeArgs[0] != MuseTrustWorkspaceArg {
		t.Fatalf("cold native args = %q", cold.NativeArgs)
	}
	if museTestEnvValue(cold.Env, "HOME") != "/operator" {
		t.Fatalf("cold HOME = %q", museTestEnvValue(cold.Env, "HOME"))
	}
	params.ResumeSessionID = "019f8e50-1234-7abc-8def-0123456789ab"
	resumed, err := BuildMuseLaunchRequest(params)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(resumed.NativeArgs) != 4 || resumed.NativeArgs[0] != "resume" || resumed.NativeArgs[1] != params.ResumeSessionID || resumed.NativeArgs[2] != MuseTrustWorkspaceArg {
		t.Fatalf("resume native args = %q", resumed.NativeArgs)
	}
	params.ResumeSessionID = ""
	params.Yolo = true
	params.ToolRelease = "9.9.9"
	yolo, err := BuildMuseLaunchRequest(params)
	if err != nil {
		t.Fatalf("yolo: %v", err)
	}
	if yolo.PermissionMode != agentic.PermissionModeYolo || yolo.ToolRelease != "9.9.9" {
		t.Fatalf("yolo = (%q, %q)", yolo.PermissionMode, yolo.ToolRelease)
	}
	params.Yolo = false
	params.Fragment = nil
	if _, err := BuildMuseLaunchRequest(params); err == nil {
		t.Fatalf("nil fragment accepted")
	}
	params.Fragment = fragment
	params.Env = []string{"PATH=/usr/bin:/bin"}
	if _, err := BuildMuseLaunchRequest(params); err == nil || !strings.Contains(err.Error(), "HOME") {
		t.Fatalf("missing HOME accepted: %v", err)
	}
}

func TestBuildMuseLaunchRequestRefusesYoloWithLockedPermissions(t *testing.T) {
	fragment, err := DecodeMuseCuratorFragment(museTestFragmentDoc(t, func(object map[string]any) {
		object["permissions"] = map[string]any{"mode": "native", "locked": true, "source": "global"}
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = BuildMuseLaunchRequest(MuseLaunchRequestParams{
		Model: "echo", WorkDir: t.TempDir(), Env: []string{"HOME=" + t.TempDir()},
		Fragment: fragment, Yolo: true,
	})
	if code := museTestFragmentCode(t, err); code != MuseFragmentPermissionsLocked {
		t.Fatalf("code = %q, want %q", code, MuseFragmentPermissionsLocked)
	}
	if !strings.Contains(err.Error(), "global") || !strings.Contains(err.Error(), "--yolo is not allowed") {
		t.Fatalf("error = %v, want the locked source and --yolo refusal", err)
	}
}

func TestBuildMuseLaunchRequestKeepsNativeWithLockedPermissions(t *testing.T) {
	fragment, err := DecodeMuseCuratorFragment(museTestFragmentDoc(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	params := MuseLaunchRequestParams{
		Model: "echo", WorkDir: t.TempDir(), Env: []string{"HOME=" + t.TempDir()},
		Fragment: fragment, OperatorArgs: []string{"--workspace-extra"},
	}
	unlocked, err := BuildMuseLaunchRequest(params)
	if err != nil {
		t.Fatal(err)
	}
	fragment.Permissions = &agentic.CuratorPermissions{
		Mode: agentic.CuratorPermissionNative, Locked: true, Source: agentic.CuratorPermissionSourceGlobal,
	}
	locked, err := BuildMuseLaunchRequest(params)
	if err != nil {
		t.Fatalf("locked native: %v", err)
	}
	if locked.PermissionMode != agentic.PermissionModeNative {
		t.Fatalf("posture = %q, want native", locked.PermissionMode)
	}
	unlockedBytes, err := json.Marshal(unlocked)
	if err != nil {
		t.Fatal(err)
	}
	lockedBytes, err := json.Marshal(locked)
	if err != nil {
		t.Fatal(err)
	}
	if string(lockedBytes) != string(unlockedBytes) {
		t.Fatalf("locked native request differs from unlocked native request")
	}
}

func TestMuseLaunchRequestFromPayloadRefusesYoloWithLockedPermissions(t *testing.T) {
	document := museTestFragmentDoc(t, func(object map[string]any) {
		object["permissions"] = map[string]any{"mode": "native", "locked": true, "source": "global"}
	})
	fragment, err := DecodeMuseCuratorFragment(document)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(MuseSessionLaunch{
		Contract: MuseSessionLaunchContract, Model: "echo", WorkDir: t.TempDir(),
		Env: []string{"HOME=" + t.TempDir()}, Yolo: true,
		Fragment: document, FragmentSHA256: MuseFragmentDigest(document),
		Profile: fragment.Profile.Name, ProfileLock: fragment.Profile.LockSHA256,
		ManagedHome: MuseFragmentManagedHome(fragment),
	})
	if err != nil {
		t.Fatal(err)
	}
	launch, err := DecodeMuseSessionLaunch(encoded)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	_, err = MuseLaunchRequestFromPayload(launch, "")
	if code := museTestFragmentCode(t, err); code != MuseFragmentPermissionsLocked {
		t.Fatalf("code = %q, want %q", code, MuseFragmentPermissionsLocked)
	}
}
