package musepty

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
	musebuilds "github.com/relux-works/swarma-session-host/musebuilds"
	"github.com/relux-works/swarma-session-host/musepty/internal/testsupport"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

func musePTYMaterializePersonalContext(t *testing.T, launch *MuseSessionLaunch) string {
	t.Helper()
	fragment, err := DecodeMuseCuratorFragment(launch.Fragment)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(fragment.Env[MuseXDGConfigHome], "muse")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "AGENTS.md"), []byte("<!--\ncurator-root-context-v2\n-->\nManaged root context.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var object, source map[string]any
	if err := json.Unmarshal(launch.Fragment, &object); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(testsupport.CuratorMuseV4Case(t, "valid-muse-root.json"), &source); err != nil {
		t.Fatal(err)
	}
	object["root_context"] = source["root_context"]
	launch.Fragment, err = json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	launch.FragmentSHA256 = MuseFragmentDigest(launch.Fragment)
	return fragment.Env[MuseXDGConfigHome]
}

func musePTYAssertPersonalContextOption(t *testing.T, argv []string, want bool) {
	t.Helper()
	index := slices.Index(argv, "-c")
	if !want {
		if index >= 0 || slices.Contains(argv, "context.foreign_personal_skills=false") {
			t.Fatalf("unexpected config option: %q", argv)
		}
		return
	}
	if index < 0 || index+1 >= len(argv) || argv[index+1] != "context.foreign_personal_skills=false" {
		t.Fatalf("missing consecutive config pair: %q", argv)
	}
}

func TestMusePTYPersonalContextPayloadRoundTripAcceptsConfigPairAndSeal(t *testing.T) {
	home := t.TempDir()
	script := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'Muse Code 1.4.7 (1.4.7-R6101.1)\\n'; exit 0; fi\nexit 1\n"
	binary := testsupport.WriteExecutable(t, "muse", []byte(script), 0o755)
	launch := museFactoryPeerLaunch(t, home)
	launch.Env = []string{"HOME=" + home, "PATH=" + filepath.Dir(binary)}
	launch.ToolRelease = "1.4.7"
	musePTYMaterializePersonalContext(t, launch)
	wire, err := json.Marshal(launch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMuseSessionLaunch(wire)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ToolRelease != launch.ToolRelease {
		t.Fatalf("decoded release = %q, want %q", decoded.ToolRelease, launch.ToolRelease)
	}
	for _, resume := range []string{"", "019f8e50-1234-7abc-8def-0123456789ab"} {
		t.Run("resume="+resume, func(t *testing.T) {
			request, err := MuseLaunchRequestFromPayload(decoded, resume)
			if err != nil {
				t.Fatal(err)
			}
			if request.ToolRelease != "1.4.7" {
				t.Fatalf("rebuilt release = %q", request.ToolRelease)
			}
			plan, err := BuildMusePTYLaunchPlan(request)
			if err != nil {
				t.Fatalf("PTY refused config pair: %v", err)
			}
			argv := plan.plan.Argv
			index := slices.Index(argv, "-c")
			if index < 0 || index+1 >= len(argv) || argv[index+1] != "context.foreign_personal_skills=false" {
				t.Fatalf("missing consecutive config pair: %q", argv)
			}
			if !slices.Contains(argv, "--trust-workspace") {
				t.Fatalf("workspace trust lost: %q", argv)
			}
			if resume != "" && !slices.Contains(argv, resume) {
				t.Fatalf("resume selection lost: %q", argv)
			}
			if err := plan.plan.VerifyBeforeExec(); err != nil {
				t.Fatalf("seal refused config pair: %v", err)
			}
		})
	}
}

func TestMusePTYNativeIsolationMismatchPayloadRebuildAndResume(t *testing.T) {
	home := t.TempDir()
	script := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'Muse Code 1.4.7 (1.4.7-R6200)\\n'; exit 0; fi\nexit 1\n"
	binary := testsupport.WriteExecutable(t, "muse", []byte(script), 0o755)
	launch := museFactoryPeerLaunch(t, home)
	launch.Env = []string{"HOME=" + home, "PATH=" + filepath.Dir(binary)}
	launch.ToolRelease = "1.4.7"
	musePTYMaterializePersonalContext(t, launch)
	for _, resume := range []string{"", "019f8e50-1234-7abc-8def-0123456789ab"} {
		t.Run("resume="+resume, func(t *testing.T) {
			request, err := MuseLaunchRequestFromPayload(launch, resume)
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			for _, permission := range []agentic.PermissionMode{agentic.PermissionModeNative, agentic.PermissionModeYolo} {
				request.PermissionMode = permission
				plan, err := BuildMusePTYLaunchPlan(request, &stderr)
				if plan != nil || !errors.Is(err, muse.ErrForeignContextExclusionUnqualified) {
					t.Fatalf("%s required exclusion = %v", permission, err)
				}
				if stderr.Len() != 0 {
					t.Fatalf("required exclusion emitted fallback: %q", stderr.String())
				}
			}
		})
	}
}

func TestMusePTYNativeRealReleaseMismatchStillRefuses(t *testing.T) {
	home := t.TempDir()
	script := "#!/bin/sh\nprintf 'Muse Code 1.4.2 (1.4.2-R4684.1)\\n'\n"
	binary := testsupport.WriteExecutable(t, "muse", []byte(script), 0o755)
	launch := museFactoryPeerLaunch(t, home)
	launch.Env = []string{"HOME=" + home, "PATH=" + filepath.Dir(binary)}
	// The assertion would enable isolation, but the binary attests a different
	// release. This is not the optional isolation-build refusal.
	launch.ToolRelease = "1.4.7"
	musePTYMaterializePersonalContext(t, launch)
	request, err := MuseLaunchRequestFromPayload(launch, "")
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if _, err := BuildMusePTYLaunchPlan(request, &stderr); !errors.Is(err, muse.ErrMuseToolReleaseMismatch) {
		t.Fatalf("real release disagreement = %v, want refusal", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("real disagreement triggered optional fallback: %q", stderr.String())
	}
}

// The factory never starts the peer when its required exclusion is unqualified.
func TestMusePTYFactoryNativeIsolationMismatchRefusesRequiredV4Context(t *testing.T) {
	home := museFactoryPeerHome(t, "idle")
	if err := os.WriteFile(filepath.Join(home, "peer-version"), []byte("Muse Code 1.4.7 (1.4.7-R6200)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	launch := museFactoryPeerLaunch(t, home)
	launch.ToolRelease = "1.4.7"
	musePTYMaterializePersonalContext(t, launch)
	wire, err := json.Marshal(launch)
	if err != nil {
		t.Fatal(err)
	}
	host, err := NewMusePTYHostFactory().Launch(context.Background(), sm.ProviderLaunchRequest{Launch: museTestLaunchSpec(wire)})
	if host != nil || !errors.Is(err, muse.ErrForeignContextExclusionUnqualified) {
		t.Fatalf("factory required exclusion = %v", err)
	}
}

func TestMusePTYFrozenNativePersonalContextResumeBuildGate(t *testing.T) {
	for _, build := range []string{"1.4.7-R6101.1", "1.4.7-R6200"} {
		t.Run(build, func(t *testing.T) {
			f := newMuseL3PFixture(t, 0)
			_, seedEnv := f.install(museL3PBuild142)
			record, id, dataHome, _ := museL3PSeedResumeSession(t, f, seedEnv, "personal-context-seed")
			_, env := f.install(build)
			tuple := f.prepare(env, musebuilds.RouteResume, "personal-context-resume")
			home := museL3PPeerHome(t, "idle")
			request := museL3PReq(home, dataHome, musebuilds.FrozenIdentity{}, id)
			request.ToolRelease = "1.4.7"
			object := testsupport.CuratorMuseV4Object(t, filepath.Dir(dataHome), "operator")
			var source map[string]any
			if err := json.Unmarshal(testsupport.CuratorMuseV4Case(t, "valid-muse-root.json"), &source); err != nil {
				t.Fatal(err)
			}
			object["root_context"] = source["root_context"]
			document, err := json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			request.Context, err = DecodeMuseCuratorFragment(document)
			if err != nil {
				t.Fatal(err)
			}
			request.Home = filepath.Dir(dataHome)
			// Exercise the production tuple injector, not a manually edited plan.
			composed, err := musePTYComposeFreshLeg(request, tuple, f.hostConfig())
			if build != "1.4.7-R6101.1" {
				if composed != nil || !errors.Is(err, muse.ErrForeignContextExclusionUnqualified) {
					t.Fatalf("frozen required exclusion = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("frozen native resume composition: %v", err)
			}
			wantIsolation := build == "1.4.7-R6101.1"
			musePTYAssertPersonalContextOption(t, composed.plan.Argv, wantIsolation)
			wantRelease := ""
			if wantIsolation {
				wantRelease = "1.4.7"
			}
			if composed.ToolRelease() != wantRelease || !composed.frozenSet || composed.frozenTuple != tuple {
				t.Fatalf("frozen binding/release changed: release = %q, tuple = %+v", composed.ToolRelease(), composed.frozenTuple)
			}
			seal, err := composed.plan.ExportSeal()
			if err != nil {
				t.Fatal(err)
			}
			imported, err := BuildMusePTYLaunchPlanFromImport(seal, composed.plan, tuple)
			if err != nil {
				t.Fatalf("frozen resume import: %v", err)
			}
			imported.frozenStore = f.hostConfig()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			host, err := ResumeMusePTYHost(ctx, imported, record)
			if err != nil {
				t.Fatalf("frozen native resume launch: %v", err)
			}
			t.Cleanup(func() { _ = host.Stop(context.Background()) })
			musePTYAssertPersonalContextOption(t, host.command.Args, wantIsolation)
			if host.NativeIdentity() != id {
				t.Fatalf("resumed identity = %q, want %q", host.NativeIdentity(), id)
			}
		})
	}
}
