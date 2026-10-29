//go:build unix

package sessionmanager

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
)

// TestManagerLifecycleLeavesNoEmptyAttachmentDirectory drives the production
// attach/detach path for a provider host that exposes no attachment target —
// the no-target shape that must never allocate an empty attachments directory.
//
// Nothing in the lifecycle may create the directory speculatively, so the
// assertion is that it never exists: not after the daemon-style directory
// preparation, not after an attach, not after a detach.
func TestManagerLifecycleLeavesNoEmptyAttachmentDirectory(t *testing.T) {
	layout := legacyPhysicalLayout(t, "attachment-dir-no-proxy")
	if err := legacyPhysicalPrepareDirectories(t, layout); err != nil {
		t.Fatal(err)
	}
	legacyPhysicalAssertAttachmentDirAbsent(t, layout.AttachmentsDir, "manager state preparation")

	factory := legacyPhysicalNewFactory(t, "codex")
	manager, err := legacyPhysicalNewHost(t, layout, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	attachment, err := manager.Attach(context.Background(), AttachRequest{
		Owner:    OwnerKey("parent/primary"),
		Provider: "codex",
	})
	if err != nil {
		t.Fatal(err)
	}
	legacyPhysicalAssertAttachmentDirAbsent(t, layout.AttachmentsDir, "attach")

	if _, err := manager.Detach(context.Background(), attachment.Session.SessionID, attachment.AttachmentID); err != nil {
		t.Fatal(err)
	}
	legacyPhysicalAssertAttachmentDirAbsent(t, layout.AttachmentsDir, "detach")

	if _, err := manager.Stop(context.Background(), attachment.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	legacyPhysicalAssertAttachmentDirAbsent(t, layout.AttachmentsDir, "stop")
}

// TestAttachmentDirectoryIsRemovedOnStopButSurvivesDetachWithLiveProxy pins
// both halves of the ownership rule for a host that DOES open a proxy socket.
//
// Detach deliberately keeps the session's proxy alive for a later reattach
// (TestProviderProxySurvivesDetachAndManagerRestartUntilSessionStop), so the
// directory that holds that live socket must survive detach too — "removed
// when empty" is not "removed on detach". Stop closes the proxy, and the
// directory goes with the last socket in it.
func TestAttachmentDirectoryIsRemovedOnStopButSurvivesDetachWithLiveProxy(t *testing.T) {
	provider, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	go legacyPhysicalServeProxyEcho(provider)

	layout := legacyPhysicalLayout(t, "attachment-dir-proxy")
	if err := legacyPhysicalPrepareDirectories(t, layout); err != nil {
		t.Fatal(err)
	}
	factory := &legacyPhysicalProxyFactory{
		t: t,
		host: &legacyPhysicalRunner{
			state:  ProviderHostRunning,
			target: "ws://" + provider.Addr().String(),
		},
	}
	manager, err := legacyPhysicalNewHost(t, layout, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	attachment, err := manager.Attach(context.Background(), AttachRequest{
		Owner:    OwnerKey("parent/primary"),
		Provider: factory.Provider(),
	})
	if err != nil {
		t.Fatal(err)
	}
	socketPath := legacyPhysicalProxySocketPath(t, attachment.ManagerEndpoint)
	if filepath.Dir(socketPath) != layout.AttachmentsDir {
		t.Fatalf("proxy socket %q is not inside %q", socketPath, layout.AttachmentsDir)
	}
	if _, err := os.Stat(layout.AttachmentsDir); err != nil {
		t.Fatalf("attach did not create the attachments directory: %v", err)
	}

	if _, err := manager.Detach(context.Background(), attachment.Session.SessionID, attachment.AttachmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("detach removed a live proxy socket: %v", err)
	}
	if _, err := os.Stat(layout.AttachmentsDir); err != nil {
		t.Fatalf("detach removed the directory holding a live proxy socket: %v", err)
	}

	if _, err := manager.Stop(context.Background(), attachment.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stop left the proxy socket behind: %v", err)
	}
	legacyPhysicalAssertAttachmentDirAbsent(t, layout.AttachmentsDir, "stop")
}

// TestDoctorRemovesStaleAttachmentDirsAndPreservesOccupiedOnes drives
// Manager.Doctor -> repair -> sweepStaleTransports -> sweepStaleAttachmentDirs,
// the operator entry point named in the acceptance criteria.
//
// Four seeded directories separate the classes the sweep must tell apart:
// empty (removed), holding only a dead socket the socket sweep clears first
// (removed), holding a LIVE listening socket (preserved), and holding a plain
// file this sweep does not own (preserved). The live one is a real listener,
// not a leftover inode, so a sweep narrowed to os.RemoveAll would destroy a
// working attachment and fail here.
func TestDoctorRemovesStaleAttachmentDirsAndPreservesOccupiedOnes(t *testing.T) {
	layout := legacyPhysicalLayout(t, "attachment-dir-doctor")
	if err := legacyPhysicalPrepareDirectories(t, layout); err != nil {
		t.Fatal(err)
	}
	manager, err := legacyPhysicalNewHost(t, layout)
	if err != nil {
		t.Fatal(err)
	}
	socketRoot := filepath.Dir(layout.SocketPath)

	emptyDir := legacyPhysicalSeedAttachmentDir(t, socketRoot, "attachments-0000dead")
	deadSocketDir := legacyPhysicalSeedAttachmentDir(t, socketRoot, "attachments-1111beef")
	deadSocket := filepath.Join(deadSocketDir, "SES-dead-attachment.sock")
	deadListener, err := net.Listen("unix", deadSocket)
	if err != nil {
		t.Fatal(err)
	}
	legacyPhysicalLeaveUnixSocketOnClose(t, deadListener)
	if err := deadListener.Close(); err != nil {
		t.Fatal(err)
	}

	liveDir := legacyPhysicalSeedAttachmentDir(t, socketRoot, "attachments-2222cafe")
	liveSocket := filepath.Join(liveDir, "SES-live-attachment.sock")
	liveListener, err := net.Listen("unix", liveSocket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = liveListener.Close() })

	occupiedDir := legacyPhysicalSeedAttachmentDir(t, socketRoot, "attachments-3333f00d")
	occupiedFile := filepath.Join(occupiedDir, "not-a-socket")
	if err := os.WriteFile(occupiedFile, []byte("owned by something else\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// A directory the sweep has no claim on: empty, but not an attachments-<hex>
	// directory. The sweep owns one naming class and must leave the rest of the
	// shared socket root alone.
	foreignDir := legacyPhysicalSeedAttachmentDir(t, socketRoot, "not-an-attachment-dir")

	report, err := manager.Doctor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(foreignDir); err != nil {
		t.Fatalf("sweep removed a directory outside the attachments-<hex> class: %v", err)
	}
	if report.Current.StaleAttachmentDirsRemoved != 2 {
		t.Fatalf("stale_attachment_dirs_removed = %d, want 2: %#v",
			report.Current.StaleAttachmentDirsRemoved, report.Current)
	}
	if report.Current.LiveAttachmentDirsPreserved != 1 {
		t.Fatalf("live_attachment_dirs_preserved = %d, want 1: %#v",
			report.Current.LiveAttachmentDirsPreserved, report.Current)
	}
	for _, removed := range []string{emptyDir, deadSocketDir} {
		if _, err := os.Stat(removed); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale attachment directory %s survived the sweep: %v", removed, err)
		}
	}
	if _, err := os.Stat(liveSocket); err != nil {
		t.Fatalf("sweep destroyed a live attachment socket: %v", err)
	}
	if _, err := os.Stat(liveDir); err != nil {
		t.Fatalf("sweep removed the directory of a live attachment: %v", err)
	}
	if _, err := os.Stat(occupiedFile); err != nil {
		t.Fatalf("sweep destroyed a file it does not own: %v", err)
	}
}

// TestDaemonStartupRepairRemovesStaleAttachmentDirs covers the second entry
// point named in the acceptance criteria: the automatic sweep the daemon runs
// before it listens. The count is read back off the startup half of the doctor
// report, which is what an operator sees.
func TestDaemonStartupRepairRemovesStaleAttachmentDirs(t *testing.T) {
	layout := legacyPhysicalLayout(t, "attachment-dir-startup")
	if err := legacyPhysicalPrepareDirectories(t, layout); err != nil {
		t.Fatal(err)
	}
	socketRoot := filepath.Dir(layout.SocketPath)
	staleDir := legacyPhysicalSeedAttachmentDir(t, socketRoot, "attachments-4444abcd")

	liveDir := legacyPhysicalSeedAttachmentDir(t, socketRoot, "attachments-5555abcd")
	liveListener, err := net.Listen("unix", filepath.Join(liveDir, "SES-startup-live.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = liveListener.Close() })

	manager, err := legacyPhysicalNewHost(t, layout)
	if err != nil {
		t.Fatal(err)
	}
	daemon, err := legacyPhysicalNewServer(manager)
	if err != nil {
		t.Fatal(err)
	}
	cancel, daemonErr := legacyPhysicalStartServer(t, daemon)
	defer func() {
		cancel()
		if err := <-daemonErr; err != nil {
			t.Errorf("daemon shutdown: %v", err)
		}
	}()

	client, err := legacyPhysicalDial(layout)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	report, err := client.Doctor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Startup.StaleAttachmentDirsRemoved != 1 {
		t.Fatalf("startup stale_attachment_dirs_removed = %d, want 1: %#v",
			report.Startup.StaleAttachmentDirsRemoved, report.Startup)
	}
	if report.Startup.LiveAttachmentDirsPreserved != 1 {
		t.Fatalf("startup live_attachment_dirs_preserved = %d, want 1: %#v",
			report.Startup.LiveAttachmentDirsPreserved, report.Startup)
	}
	if _, err := os.Stat(staleDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup repair left the stale attachment directory: %v", err)
	}
	if _, err := os.Stat(liveDir); err != nil {
		t.Fatalf("startup repair removed a live attachment directory: %v", err)
	}

	entries, raw := readOperatorLogEntries(t, layout.OperatorLogFile)
	if !operatorLogContains(entries, "session_repair_finished", map[string]string{
		"trigger":                       "startup",
		"stale_attachment_dirs_removed": "1",
	}) {
		t.Fatalf("operator log missing the attachment directory repair count:\n%s", raw)
	}
}

// TestAttachmentDirSweepReportsReadFailureInsteadOfRemoving pins the rule that
// a failed read is not an absence of attachments: an unreadable directory is
// reported, never treated as empty and removed.
func TestAttachmentDirSweepReportsReadFailureInsteadOfRemoving(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read a 0000 directory, so the failure cannot be provoked")
	}
	parent := t.TempDir()
	unreadable := legacyPhysicalSeedAttachmentDir(t, parent, "attachments-6666dead")
	if err := os.WriteFile(filepath.Join(unreadable, "SES-hidden.sock"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unreadable, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o700) })

	var report RepairReport
	err := sweepStaleAttachmentDirs(context.Background(), []string{parent}, &report)
	if err == nil {
		t.Fatalf("unreadable attachment directory was not reported: %#v", report)
	}
	if report.StaleAttachmentDirsRemoved != 0 {
		t.Fatalf("unreadable directory was counted as swept: %#v", report)
	}
	if _, statErr := os.Stat(unreadable); statErr != nil {
		t.Fatalf("unreadable attachment directory was removed: %v", statErr)
	}
}

func legacyPhysicalSeedAttachmentDir(t *testing.T, parent, name string) string {
	t.Helper()
	path := filepath.Join(parent, name)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func legacyPhysicalAssertAttachmentDirAbsent(t *testing.T, dir, stage string) {
	t.Helper()
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("attachments directory %s exists after %s: %v", dir, stage, err)
	}
}

// TestDetachPreservesAttachmentDirectoryHoldingAnotherOwnersSocket reaches the
// half of pruneEmptyProxyRootLocked that the same-manager cases cannot: a
// manager that holds no proxy of its own is not thereby entitled to delete the
// directory. The socket root is shared, and a replacement manager instance —
// or another session restored by one — can still be serving an attachment
// inside it while this manager's own proxy table is empty.
func TestDetachPreservesAttachmentDirectoryHoldingAnotherOwnersSocket(t *testing.T) {
	layout := legacyPhysicalLayout(t, "attachment-dir-foreign-socket")
	if err := legacyPhysicalPrepareDirectories(t, layout); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(layout.AttachmentsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	foreignSocket := filepath.Join(layout.AttachmentsDir, "SES-owned-by-another-manager.sock")
	listener, err := net.Listen("unix", foreignSocket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	factory := legacyPhysicalNewFactory(t, "codex")
	manager, err := legacyPhysicalNewHost(t, layout, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	attachment, err := manager.Attach(context.Background(), AttachRequest{
		Owner:    OwnerKey("parent/primary"),
		Provider: "codex",
	})
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	// The neutral API exposes published endpoints rather than a private proxy table.
	proxies := len(attachment.ManagerEndpoint)
	manager.mu.Unlock()
	if proxies != 0 {
		t.Fatalf("fixture precondition failed: manager holds %d proxies, want 0", proxies)
	}

	if _, err := manager.Detach(context.Background(), attachment.Session.SessionID, attachment.AttachmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(foreignSocket); err != nil {
		t.Errorf("detach destroyed a socket this manager does not own: %v", err)
	}
	if _, err := os.Stat(layout.AttachmentsDir); err != nil {
		t.Errorf("detach removed an occupied attachments directory: %v", err)
	}

	if _, err := manager.Stop(context.Background(), attachment.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(foreignSocket); err != nil {
		t.Errorf("stop destroyed a socket this manager does not own: %v", err)
	}
	if _, err := os.Stat(layout.AttachmentsDir); err != nil {
		t.Errorf("stop removed an occupied attachments directory: %v", err)
	}
}
