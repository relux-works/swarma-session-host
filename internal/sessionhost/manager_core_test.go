package sessionhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	managerschema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

func coreDocuments(t *testing.T, q map[string]any, version int) ([]byte, []byte) {
	t.Helper()
	p := q["profile"].(map[string]any)
	f := map[string]any{"fragment": fmt.Sprintf("launch-env-fragment-v%d", version), "environment": q["runtime"], "profile": map[string]any{"name": p["name"], "lock_sha256": p["lock_sha256"]}, "precedence": map[string]any{"winner": "higher-weight", "placement": "winner-last"}, "env": map[string]any{"CODEX_HOME": "/owned/inert/home"}}
	if version >= 2 {
		f["permissions"] = map[string]any{"mode": "native", "locked": false, "source": "default"}
	}
	if version >= 4 {
		f["root_context"] = map[string]any{"exclusive": false}
		f["credential_store"] = map[string]any{"selection": "native"}
	}
	fragment, _ := json.Marshal(f)
	canonical, err := managerschema.CanonicalJSON(fragment)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	p["fragment_digest"] = "sha256:" + hex.EncodeToString(digest[:])
	n := q["notification"].(map[string]any)
	w := n["workflow"].(map[string]any)
	selected, _ := json.Marshal(map[string]any{"schema": "curator.launcher-config/2", "config_schema": 8, "config_path": "fixture-selected.json", "extensions": map[string]any{"notification": map[string]any{"backend": "host", "contract": NotificationContract, "workflow_reference": w["reference"], "workflow_sha256": w["sha256"]}}})
	return selected, fragment
}

// The consumed Core subset accepts all four currently emitted fragment versions
// and hashes all opaque composition fields. It does not replace full composition
// validation, read any indicated file or execute the real Core command.
func TestManagerCoreCorrelation(t *testing.T) {
	for version := 1; version <= 4; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			_, _, q := memberOperationFixture(t, "launch")
			selected, fragment := coreDocuments(t, q, version)
			parsed, err := parseMemberOperation("launch", memberOperationBytes(t, q))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := correlateManagerCore(selected, fragment, parsed); err != nil {
				t.Fatal("valid emitted version refused", err)
			}
			for name, change := range map[string]func(*memberOperationRequest){
				"workflow-reference": func(q *memberOperationRequest) { q.Notification.Workflow.Reference = "other" },
				"workflow-hash":      func(q *memberOperationRequest) { q.Notification.Workflow.SHA256 = strings.Repeat("d", 64) },
				"fragment-digest":    func(q *memberOperationRequest) { q.Profile.FragmentDigest = "sha256:" + strings.Repeat("d", 64) },
				"profile":            func(q *memberOperationRequest) { q.Profile.Name = "other" },
				"lock":               func(q *memberOperationRequest) { q.Profile.Lock = strings.Repeat("d", 64) },
				"runtime":            func(q *memberOperationRequest) { q.Runtime = "claude_code" },
			} {
				t.Run(name, func(t *testing.T) {
					other := parsed
					change(&other)
					if _, err := correlateManagerCore(selected, fragment, other); !errors.Is(err, errMemberRecordUnavailable) {
						t.Fatal("mismatched Core projection admitted", err)
					}
				})
			}
			for name, bad := range map[string][]byte{"duplicate": []byte(strings.Replace(string(selected), `"backend":"host"`, `"backend":"host","back\u0065nd":"host"`, 1)), "off": []byte(strings.Replace(string(selected), `"backend":"host"`, `"backend":"off"`, 1)), "unknown": []byte(strings.Replace(string(selected), `"config_schema":8`, `"unknown":true,"config_schema":8`, 1)), "null": []byte(`null`), "old-schema": []byte(strings.Replace(string(selected), `"config_schema":8`, `"config_schema":7`, 1)), "oversize": append(append([]byte(nil), selected...), []byte(strings.Repeat(" ", managerschema.Limit))...)} {
				t.Run(name, func(t *testing.T) {
					if _, err := correlateManagerCore(bad, fragment, parsed); err == nil {
						t.Fatal("invalid selection admitted")
					}
				})
			}
		})
	}
}

// This subprocess is an owned inert Go fixture, never Core/native/provider. It
// exercises actual bounded process output, nonzero and cancellation behavior.
func TestManagerCoreCommandChild(t *testing.T) {
	switch os.Getenv("HOST_CORE_COMMAND_FIXTURE") {
	case "ok":
		fmt.Print(`{"fixture":true}`)
		os.Exit(0)
	case "bad-exit":
		fmt.Print(`{"fixture":true}`)
		os.Exit(3)
	case "large":
		fmt.Print(strings.Repeat("x", managerschema.Limit+1))
		os.Exit(0)
	case "wait":
		time.Sleep(10 * time.Second)
		os.Exit(0)
	}
}
func TestManagerCoreCommandBound(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"ok", "bad-exit", "large", "wait"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestManagerCoreCommandChild$")
			cmd.Env = []string{"HOST_CORE_COMMAND_FIXTURE=" + mode, "GORACE=atexit_sleep_ms=0"}
			b, err := runManagerCore(cmd)
			if mode == "ok" {
				if err != nil || string(b) != `{"fixture":true}` {
					t.Fatal("valid output refused", err)
				}
			} else if err == nil || b != nil {
				t.Fatal("bad process output admitted", mode, err)
			}
			if cmd.Process != nil {
				t.Logf("owned inert Core-fixture pid=%d exited=%v", cmd.Process.Pid, cmd.ProcessState != nil && cmd.ProcessState.Exited())
			}
		})
	}
}

func managerBridgeFixture(t *testing.T) (*NotificationManager, map[string]any, *[]byte) {
	t.Helper()
	o, s, q := memberOperationFixture(t, "launch")
	selected, fragment := coreDocuments(t, q, 4)
	m := &NotificationManager{owner: o, records: s, attachments: make(map[*ManagerAttachment]struct{}), admission: inertManagerAdmission{}}
	m.read = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if dir != q["cwd"] {
			return nil, errors.New("wrong actual workspace")
		}
		switch {
		case reflect.DeepEqual(args, []string{"config", "launcher"}):
			return append([]byte(nil), selected...), nil
		case reflect.DeepEqual(args, []string{"env", "resolve", "codex_cli", "--profile", "swarm-orch", "--format", "json"}):
			return append([]byte(nil), fragment...), nil
		default:
			return nil, errors.New("unexpected Core command or repair")
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Close(ctx)
	})
	return m, q, &selected
}

// Model only the missing ordinary wire-handler admission in these owned inert
// tests. Production has no default admission and therefore cannot activate a
// broker from public options. This fixture attests no native runtime assignment.
type inertManagerAdmission struct{}

func (inertManagerAdmission) checkManagerOperation(ctx context.Context, _ *Host, _ string, _ []byte) error {
	return ctx.Err()
}
