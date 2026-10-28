package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// installFixture is one synthetic launcher install directory: a `muse`
// launcher, an optional `.muse-version` sidecar and versioned `muse-bin-`
// entries. Every executable is written to a temp name, synced, closed,
// permissioned and renamed into place, so no reader ever observes a partial
// file. Nothing here is executed by discovery; entries shaped as scripts
// exist so the no-execution test can observe they were not run.
type installFixture struct {
	t   *testing.T
	dir string
}

// newInstall creates an empty install directory and returns it with
// symlinked parents resolved, so physical-path assertions hold on hosts
// where the temp root itself passes through a link.
func newInstall(t *testing.T) *installFixture {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &installFixture{t: t, dir: dir}
}

// writeFileSync durably publishes data at path with the given mode: temp
// file in the same directory, full write, fsync, close, chmod, rename.
func (f *installFixture) writeFileSync(path string, data []byte, mode os.FileMode) {
	f.t.Helper()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".writing-*")
	if err != nil {
		f.t.Fatal(err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		f.t.Fatal(err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		f.t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		f.t.Fatal(err)
	}
}

// writeLauncher publishes an executable `muse` launcher with the given
// content.
func (f *installFixture) writeLauncher(content string) string {
	f.t.Helper()
	path := filepath.Join(f.dir, launcherName)
	f.writeFileSync(path, []byte(content), 0o755)
	return path
}

// writeSidecar publishes the `.muse-version` selector with the exact bytes
// given. Callers control trailing newlines byte for byte.
func (f *installFixture) writeSidecar(content string) {
	f.t.Helper()
	f.writeFileSync(filepath.Join(f.dir, sidecarName), []byte(content), 0o644)
}

// writeCandidate publishes an executable `muse-bin-<build>` entry whose
// bytes name the build, so distinct builds hash distinctly.
func (f *installFixture) writeCandidate(build string) string {
	f.t.Helper()
	path := filepath.Join(f.dir, pinnedNamePrefix+build)
	f.writeFileSync(path, []byte("#!/bin/sh\necho "+build+"\n"), 0o755)
	return path
}

// plainScript bodies a launcher or candidate that succeeds silently when
// run. Discovery must resolve it, never run it.
const plainScript = "#!/bin/sh\nexit 0\n"

// env returns a launch environment whose PATH holds exactly this install.
func (f *installFixture) env() []string {
	return []string{"PATH=" + f.dir}
}

// requireCode fails unless err chains to a discovery refusal with the exact
// code. Refusal identity is the code, never message prose.
func requireCode(t *testing.T, err error, code string) *DiscoveryError {
	t.Helper()
	if err == nil {
		t.Fatalf("want refusal %s, got success", code)
	}
	refusal, ok := AsDiscoveryError(err)
	if !ok {
		t.Fatalf("want refusal %s, got untyped %v", code, err)
	}
	if refusal.Code != code {
		t.Fatalf("want refusal %s, got %s (%v)", code, refusal.Code, err)
	}
	stages := map[string]string{
		CodeNoPath: StagePath, CodeLauncherNotFound: StagePath, CodeLauncherUnusable: StagePath,
		CodeLauncherResolution: StageLauncher, CodeSidecarUnreadable: StageSidecar,
		CodeSidecarMalformed: StageSidecar, CodeSidecarTargetRejected: StageSidecar,
		CodeInstallScanFailed: StageScan, CodeInstallNameUnrecognized: StageScan,
		CodeNoInstalledBuild: StageScan, CodeSelectionAmbiguous: StageSelect,
		CodeSelectionUnusable: StageSource, CodeDiscoveryChanged: StageSelect,
		CodeUnsupportedPlatform: StageStart,
	}
	if stage, ok := stages[code]; ok && refusal.Stage != stage {
		t.Fatalf("want stage %s for %s, got %s (%v)", stage, code, refusal.Stage, err)
	}
	return refusal
}

// requireSuccess fails unless err is nil and returns the discovery.
func requireSuccess(t *testing.T, discovery Discovery, err error) Discovery {
	t.Helper()
	if err != nil {
		t.Fatalf("want success, got %v", err)
	}
	return discovery
}

// resolveSuccess resolves the install behind env and fails unless discovery
// succeeds, returning the discovery.
func resolveSuccess(t *testing.T, env []string) Discovery {
	t.Helper()
	discovery, err := ResolveNewest(context.Background(), env)
	if err != nil {
		t.Fatalf("want success, got %v", err)
	}
	return discovery
}

// skipIfRoot skips permission-refusal cells when the test runs as root: a
// superuser reads through unpermissioned files, so no EACCES exists to
// refuse. Every killer carrying such a cell also carries cells that run in
// every environment.
func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permission refusals need a non-root user")
	}
}
