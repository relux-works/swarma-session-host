package musepty

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// museFactoryPeerHome stages a scripted-peer home: the test binary
// symlinked as muse, the named peer mode, and the frame handshake fifo.
func museFactoryPeerHome(t *testing.T, mode string) string {
	t.Helper()
	home := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(binary, filepath.Join(home, "muse")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "peer-mode"), []byte(mode), 0600); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(home, musePeerFrameFIFO), 0600); err != nil {
		t.Fatal(err)
	}
	return home
}

// museFactoryPeerLaunch builds a valid cold-start payload over a peer
// home: the fragment names peer XDG parents and the env resolves the
// symlinked test binary.
func museFactoryPeerLaunch(t *testing.T, home string) *MuseSessionLaunch {
	t.Helper()
	document := museTestFragmentDoc(t, func(object map[string]any) {
		env := object["env"].(map[string]string)
		env[MuseXDGDataHome] = filepath.Join(home, "data")
		env[MuseXDGConfigHome] = filepath.Join(home, "config")
		env[MuseXDGCacheHome] = filepath.Join(home, "cache")
		env[MuseXDGStateHome] = filepath.Join(home, "state")
	})
	fragment, err := DecodeMuseCuratorFragment(document)
	if err != nil {
		t.Fatal(err)
	}
	return &MuseSessionLaunch{
		Contract:       MuseSessionLaunchContract,
		Model:          "echo",
		WorkDir:        home,
		Env:            []string{"HOME=" + home, "PATH=" + home, "TERM=xterm-256color"},
		Fragment:       document,
		FragmentSHA256: MuseFragmentDigest(document),
		Profile:        fragment.Profile.Name,
		ProfileLock:    fragment.Profile.LockSHA256,
		ManagedHome:    MuseFragmentManagedHome(fragment),
	}
}

func museFactoryPeerLaunchBytes(t *testing.T, home string) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(museFactoryPeerLaunch(t, home))
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// museFactoryPeerStart launches through the factory and settles the
// scripted peer to idle, mirroring the R2 peer dance.
func museFactoryPeerStart(t *testing.T, home string, launch json.RawMessage, prior *sm.ProviderHostRecord) *musePTYProvenanceHost {
	t.Helper()
	factory := NewMusePTYHostFactory()
	if factory.Provider() != "muse" {
		t.Fatalf("provider = %q", factory.Provider())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request := sm.ProviderLaunchRequest{Launch: museTestLaunchSpec(launch), Prior: prior}
	host, err := factory.Launch(ctx, request)
	if err != nil {
		t.Fatalf("factory launch: %v", err)
	}
	provenance, ok := host.(*musePTYProvenanceHost)
	if !ok {
		t.Fatalf("host = %T", host)
	}
	t.Cleanup(func() { _ = provenance.Stop(context.Background()) })
	museInjectTestSeams(t, provenance.musePTYHost)
	museSettleScriptedIdle(t, provenance.musePTYHost, home)
	return provenance
}

func TestMusePTYFactoryLaunchesColdPeerAndBindsTextGoal(t *testing.T) {
	home := museFactoryPeerHome(t, "idle")
	provenance := museFactoryPeerStart(t, home, museFactoryPeerLaunchBytes(t, home), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if state, err := provenance.State(ctx); err != nil || state != sm.ProviderHostRunning {
		t.Fatalf("state = %v, %v", state, err)
	}
	if provenance.NativeIdentity() == "" {
		t.Fatalf("native identity empty")
	}
	snapshot, err := museTestSnapshot(provenance)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Contract       string `json:"contract"`
		SessionID      string `json:"session_id"`
		Profile        string `json:"profile"`
		ProfileLock    string `json:"profile_lock_sha256"`
		ManagedHome    string `json:"managed_home"`
		FragmentSHA256 string `json:"fragment_sha256"`
	}
	if err := json.Unmarshal(snapshot, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Contract != "task-board.muse-pty-host-v1" || decoded.SessionID == "" {
		t.Fatalf("snapshot = %+v", decoded)
	}
	if decoded.Profile == "" || decoded.ProfileLock == "" || decoded.ManagedHome == "" || decoded.FragmentSHA256 == "" {
		t.Fatalf("provenance missing: %+v", decoded)
	}
	revision, _ := productionParityRevisions(t)
	if err := provenance.DeliverGoalText(ctx, revision); err != nil {
		t.Fatalf("text goal: %v", err)
	}
	if state, _ := provenance.State(ctx); state != sm.ProviderHostRunning {
		t.Fatalf("goal delivery stopped the host")
	}
}

func TestMusePTYFactoryResumesRecordedPeerSession(t *testing.T) {
	home := museFactoryPeerHome(t, "idle")
	provenance := museFactoryPeerStart(t, home, museFactoryPeerLaunchBytes(t, home), nil)
	snapshot, err := museTestSnapshot(provenance)
	if err != nil {
		t.Fatal(err)
	}
	id := provenance.NativeIdentity()
	inner := provenance.musePTYHost
	_ = provenance.Stop(context.Background())

	log, err := os.OpenFile(inner.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	endBytes, err := json.Marshal(museOwnedRecord(inner, museFixtureRecord("session.end")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Write(append(endBytes, '\n')); err != nil {
		t.Fatal(err)
	}
	_ = log.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resumed, err := NewMusePTYHostFactory().Launch(ctx, sm.ProviderLaunchRequest{
		Launch: museTestLaunchSpec(museFactoryPeerLaunchBytes(t, home)),
		Prior:  &sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(snapshot)},
	})
	if err != nil {
		t.Fatalf("factory resume: %v", err)
	}
	defer resumed.Stop(context.Background())
	identity, ok := resumed.(sm.ProviderNativeIdentity)
	if !ok || identity.NativeIdentity() != id {
		t.Fatalf("resumed identity mismatch")
	}
}

// museFactoryPeerRestartWithPrior stops a factory-started peer, ends its log
// epoch, and relaunches through the factory with the prior snapshot,
// returning the resumed host and the native identity it must preserve.
func museFactoryPeerRestartWithPrior(t *testing.T, home string, provenance *musePTYProvenanceHost) (sm.ProviderHost, string) {
	t.Helper()
	snapshot, err := museTestSnapshot(provenance)
	if err != nil {
		t.Fatal(err)
	}
	id := provenance.NativeIdentity()
	inner := provenance.musePTYHost
	_ = provenance.Stop(context.Background())

	log, err := os.OpenFile(inner.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	endBytes, err := json.Marshal(museOwnedRecord(inner, museFixtureRecord("session.end")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Write(append(endBytes, '\n')); err != nil {
		t.Fatal(err)
	}
	_ = log.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resumed, err := NewMusePTYHostFactory().Launch(ctx, sm.ProviderLaunchRequest{
		Launch: museTestLaunchSpec(museFactoryPeerLaunchBytes(t, home)),
		Prior:  &sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(snapshot)},
	})
	if err != nil {
		t.Fatalf("factory resume: %v", err)
	}
	return resumed, id
}

// TestMusePTYFactoryAcceptsEveryModuleVerifiedBuild launches through the
// factory with each module-verified peer answer: the default and the second
// verified row, cold and resumed. A host-local release pin would refuse the
// second row after its plan seals; success on both rows proves admission now
// follows the module seal alone. The synthetic leg is a well-formed build no
// release table ever named; since agents-management v0.5.58 (Muse L0) the
// module seals any well-formed build, so it launches like the others.
func TestMusePTYFactoryAcceptsEveryModuleVerifiedBuild(t *testing.T) {
	t.Run("1.4.1-cold", func(t *testing.T) {
		home := museFactoryPeerHome(t, "idle")
		provenance := museFactoryPeerStart(t, home, museFactoryPeerLaunchBytes(t, home), nil)
		if provenance.NativeIdentity() == "" {
			t.Fatal("native identity empty")
		}
	})
	t.Run("1.4.2-cold-and-resume", func(t *testing.T) {
		home := museFactoryPeerHome(t, "idle")
		if err := os.WriteFile(filepath.Join(home, "peer-version"), []byte("Muse Code 1.4.2 (1.4.2-R4684.1)\n"), 0600); err != nil {
			t.Fatal(err)
		}
		provenance := museFactoryPeerStart(t, home, museFactoryPeerLaunchBytes(t, home), nil)
		resumed, id := museFactoryPeerRestartWithPrior(t, home, provenance)
		defer resumed.Stop(context.Background())
		identity, ok := resumed.(sm.ProviderNativeIdentity)
		if !ok || identity.NativeIdentity() != id {
			t.Fatalf("resumed identity mismatch")
		}
	})
	t.Run("synthetic-1.4.4-admitted-by-module-seal", func(t *testing.T) {
		home := museFactoryPeerHome(t, "idle")
		// Synthetic, never captured: a well-formed release the module once
		// refused as unverified. agents-management v0.5.58 (Muse L0) retired
		// the verified-release table and seals any well-formed build, so the
		// leg this test promised to convert now launches: the host still adds
		// no admission of its own.
		if err := os.WriteFile(filepath.Join(home, "peer-version"), []byte("Muse Code 1.4.4 (1.4.4-R5000.1)\n"), 0600); err != nil {
			t.Fatal(err)
		}
		provenance := museFactoryPeerStart(t, home, museFactoryPeerLaunchBytes(t, home), nil)
		if provenance.NativeIdentity() == "" {
			t.Fatal("native identity empty")
		}
	})
}

func TestMusePTYFactoryRefusesPeerThatNeverSettles(t *testing.T) {
	home := museFactoryPeerHome(t, "busy")
	factory := NewMusePTYHostFactory()
	// The settle bound is a compliant one-second wall: the busy peer never
	// idles, so the factory must refuse on the bound rather than hang.
	factory.bindableTimeout = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	host, err := factory.Launch(ctx, sm.ProviderLaunchRequest{Launch: museTestLaunchSpec(museFactoryPeerLaunchBytes(t, home))})
	if err == nil {
		_ = host.Stop(context.Background())
		t.Fatalf("launch settled on a busy peer")
	}
	if !strings.Contains(err.Error(), "did not settle idle") {
		t.Fatalf("error = %v", err)
	}
}

func TestMusePTYFactoryRefusesBadLaunches(t *testing.T) {
	home := museFactoryPeerHome(t, "idle")
	good := museFactoryPeerLaunchBytes(t, home)
	cases := []struct {
		name   string
		launch json.RawMessage
		prior  *sm.ProviderHostRecord
		fork   bool
	}{
		{"empty-payload", nil, nil, false},
		{"malformed-payload", json.RawMessage(`{`), nil, false},
		{"foreign-prior", good, &sm.ProviderHostRecord{Provider: "claude", Snapshot: museTestCoreSnapshot(good)}, false},
		{"unreadable-prior", good, &sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(json.RawMessage(`{`))}, false},
		{"prior-without-identity", good, &sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(json.RawMessage(`{"contract":"task-board.muse-pty-host-v1"}`))}, false},
		{"fork-required", good, nil, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			host, err := NewMusePTYHostFactory().Launch(ctx, sm.ProviderLaunchRequest{
				Launch:        museTestLaunchSpec(testCase.launch),
				Prior:         testCase.prior,
				PriorMustFork: testCase.fork,
			})
			if err == nil {
				_ = host.Stop(context.Background())
				t.Fatalf("launch succeeded")
			}
		})
	}
}

func TestMusePTYFactoryRestoreDetachesWithoutPTY(t *testing.T) {
	home := museFactoryPeerHome(t, "idle")
	provenance := museFactoryPeerStart(t, home, museFactoryPeerLaunchBytes(t, home), nil)
	snapshot, err := museTestSnapshot(provenance)
	if err != nil {
		t.Fatal(err)
	}
	_ = provenance.Stop(context.Background())

	factory := NewMusePTYHostFactory()
	restored, err := factory.Restore(context.Background(), sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(snapshot)})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if state, err := restored.State(ctx); err != nil || state != sm.ProviderHostStopped {
		t.Fatalf("detached state = %v, %v", state, err)
	}
	roundTrip, err := museTestSnapshot(restored)
	if err != nil {
		t.Fatal(err)
	}
	if string(roundTrip) != string(snapshot) {
		t.Fatalf("detached snapshot differs")
	}
	revision, _ := productionParityRevisions(t)
	if err := restored.(interface {
		DeliverGoalText(context.Context, string) error
	}).DeliverGoalText(ctx, revision); err == nil {
		t.Fatalf("detached goal accepted")
	}
	if err := restored.Stop(ctx); err != nil {
		t.Fatalf("detached stop: %v", err)
	}
	for _, record := range []sm.ProviderHostRecord{
		{Provider: "claude", Snapshot: museTestCoreSnapshot(snapshot)},
		{Provider: "muse"},
		{Provider: "muse", Snapshot: museTestCoreSnapshot(json.RawMessage(`{`))},
		{Provider: "muse", Snapshot: museTestCoreSnapshot(json.RawMessage(`{"contract":"task-board.muse-pty-host-v1"}`))},
	} {
		if _, err := factory.Restore(context.Background(), record); !errors.Is(err, sm.ErrProviderHostNotFound) {
			t.Fatalf("restore(%+v) = %v, want not-found", record, err)
		}
	}
}

func TestDecodeMuseSessionLaunchChecksIntegrity(t *testing.T) {
	home := t.TempDir()
	valid := func(t *testing.T) *MuseSessionLaunch {
		t.Helper()
		document := museTestFragmentDoc(t, nil)
		fragment, err := DecodeMuseCuratorFragment(document)
		if err != nil {
			t.Fatal(err)
		}
		return &MuseSessionLaunch{
			Contract:       MuseSessionLaunchContract,
			Model:          "echo",
			WorkDir:        home,
			Env:            []string{"HOME=" + home, "PATH=/usr/bin:/bin"},
			Fragment:       document,
			FragmentSHA256: MuseFragmentDigest(document),
			Profile:        fragment.Profile.Name,
			ProfileLock:    fragment.Profile.LockSHA256,
			ManagedHome:    MuseFragmentManagedHome(fragment),
		}
	}
	encode := func(t *testing.T, launch *MuseSessionLaunch) []byte {
		t.Helper()
		encoded, err := json.Marshal(launch)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	if _, err := DecodeMuseSessionLaunch(encode(t, valid(t))); err != nil {
		t.Fatalf("valid: %v", err)
	}
	cases := map[string]func(*MuseSessionLaunch){
		"contract":      func(launch *MuseSessionLaunch) { launch.Contract = "task-board.claude-session-launch-v1" },
		"model":         func(launch *MuseSessionLaunch) { launch.Model = "" },
		"workdir":       func(launch *MuseSessionLaunch) { launch.WorkDir = "relative" },
		"env":           func(launch *MuseSessionLaunch) { launch.Env = nil },
		"fragment":      func(launch *MuseSessionLaunch) { launch.Fragment = nil },
		"sha":           func(launch *MuseSessionLaunch) { launch.FragmentSHA256 = strings.Repeat("0", 64) },
		"profile":       func(launch *MuseSessionLaunch) { launch.Profile = "forged" },
		"operator-args": func(launch *MuseSessionLaunch) { launch.OperatorArgs = []string{"resume", "x"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			launch := valid(t)
			mutate(launch)
			if _, err := DecodeMuseSessionLaunch(encode(t, launch)); err == nil {
				t.Fatalf("decode succeeded")
			}
		})
	}
	if _, err := DecodeMuseSessionLaunch([]byte(`{`)); err == nil {
		t.Fatalf("malformed JSON accepted")
	}
}

func TestMuseLaunchRequestFromPayloadShapesNativeAndYolo(t *testing.T) {
	home := museFactoryPeerHome(t, "idle")
	launch := museFactoryPeerLaunch(t, home)
	if _, err := MuseLaunchRequestFromPayload(nil, ""); err == nil {
		t.Fatalf("nil payload accepted")
	}
	native, err := MuseLaunchRequestFromPayload(launch, "")
	if err != nil {
		t.Fatal(err)
	}
	nativePlan, err := BuildMusePTYLaunchPlan(native)
	if err != nil {
		t.Fatalf("native plan: %v", err)
	}
	if err := nativePlan.plan.VerifyBeforeExec(); err != nil {
		t.Fatalf("native verify: %v", err)
	}
	if !museTestArgvHas(nativePlan.plan.Argv, MuseTrustWorkspaceArg) {
		t.Fatalf("native argv = %q", nativePlan.plan.Argv)
	}
	if museTestArgvHas(nativePlan.plan.Argv, "--yolo") {
		t.Fatalf("native argv carries yolo: %q", nativePlan.plan.Argv)
	}
	if museTestEnvValue(nativePlan.plan.Env, "MUSE_NO_AUTO_UPDATE") != "1" {
		t.Fatalf("updater pin missing from plan env")
	}
	if home := museTestEnvValue(nativePlan.plan.Env, "HOME"); home == "" {
		t.Fatalf("plan env lost HOME")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	release, err := ProbeMuseToolRelease(ctx, native.Env)
	if err != nil || release == "" {
		t.Fatalf("probe = %q, %v", release, err)
	}
	launch.Yolo = true
	launch.ToolRelease = release
	yolo, err := MuseLaunchRequestFromPayload(launch, "")
	if err != nil {
		t.Fatal(err)
	}
	yoloPlan, err := BuildMusePTYLaunchPlan(yolo)
	if err != nil {
		t.Fatalf("yolo plan: %v", err)
	}
	if !museTestArgvHas(yoloPlan.plan.Argv, "--yolo") {
		t.Fatalf("yolo argv = %q", yoloPlan.plan.Argv)
	}
	resumed, err := MuseLaunchRequestFromPayload(launch, "019f8e50-1234-7abc-8def-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	resumePlan, err := BuildMusePTYLaunchPlan(resumed)
	if err != nil {
		t.Fatalf("resume plan: %v", err)
	}
	selected := false
	for i, arg := range resumePlan.plan.Argv {
		if arg == "resume" && i+1 < len(resumePlan.plan.Argv) && resumePlan.plan.Argv[i+1] == "019f8e50-1234-7abc-8def-0123456789ab" {
			selected = true
		}
	}
	if !selected || !museTestArgvHas(resumePlan.plan.Argv, MuseTrustWorkspaceArg) {
		t.Fatalf("resume argv = %q", resumePlan.plan.Argv)
	}
}

func museTestArgvHas(argv []string, want string) bool {
	for _, arg := range argv {
		if arg == want {
			return true
		}
	}
	return false
}
