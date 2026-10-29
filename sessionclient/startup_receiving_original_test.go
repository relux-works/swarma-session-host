//go:build linux

package sessionclient_test

import (
	"context"
	client "github.com/relux-works/swarma-session-host/sessionclient"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"testing"
	"time"
)

// Original5721 assertion bodies are retained with explicit setup/API ports.
// These tests run actual authenticated7da host/default/client/proxy composition;
// provider launch and verified authority are controlled isolated boundaries.
func TestConnectOrStartAndAttachRecoversFromUnknownFieldRejection(t *testing.T) {
	declareOutsideShell(t)
	restoreTimeouts := client.StartupReceivingOriginalTimeouts()
	t.Cleanup(restoreTimeouts)

	layout := receivingTestLayout(t, "unknown-field-holder")
	holder := layout.startup.StartHolder(t, "unknown-field")
	target, _ := receivingEcho(t)
	factory := newReceivingFactory(layout, target)
	daemon := newReceivingManager(t, layout, factory)
	daemon.source.adoptAllowed = true
	var (
		cancelDaemon context.CancelFunc
		daemonErr    <-chan error
	)
	t.Cleanup(func() {
		if cancelDaemon != nil {
			cancelDaemon()
			if err := <-daemonErr; err != nil {
				t.Errorf("replacement daemon shutdown: %v", err)
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	client, attachment, err := connectReceivingAndAttach(ctx, layout, func() error {
		cancelDaemon, daemonErr = startReceivingControl(t, daemon)
		return nil
	}, factory.request(true))
	if err != nil {
		t.Fatalf("unknown-field recovery surfaced an error: %v", err)
	}
	defer client.Close()
	if attachment.Session.State != core.SessionRunning || factory.launchCount() != 1 {
		t.Fatalf("replacement attachment = %#v launches=%d", attachment, factory.launchCount())
	}
	status, err := client.StartupStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.PID == holder.PID() || status.ProtocolVersion != core.ControlProtocolVersion() {
		t.Fatalf("replacement status = %#v, old pid=%d", status, holder.PID())
	}
	holder.WaitForExit(t, 2*time.Second)
}

func TestProviderProxySurvivesDetachAndManagerRestartUntilSessionStop(t *testing.T) {
	target, provider := receivingEcho(t)
	defer provider.Close()
	layout := receivingTestLayout(t, "provider-proxy-lifecycle")
	factory := newReceivingFactory(layout, target)
	manager := newReceivingManager(t, layout, factory)
	if err := manager.LoadAndReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	attachment, err := manager.Attach(context.Background(), factory.request(false))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := attachment.ManagerEndpoint
	socketPath := providerProxySocketPath(t, endpoint)
	assertProviderProxyRoundTrip(t, socketPath, "before-detach")

	if _, err := manager.Detach(attachment.Session.SessionID, attachment.AttachmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("session proxy did not survive detach: %v", err)
	}
	assertProviderProxyRoundTrip(t, socketPath, "after-detach")

	reattached, err := manager.Attach(context.Background(), factory.request(false))
	if err != nil {
		t.Fatal(err)
	}
	if reattached.ManagerEndpoint != endpoint {
		t.Fatalf("reattach endpoint = %q, want stable %q", reattached.ManagerEndpoint, endpoint)
	}
	if _, err := manager.Detach(reattached.Session.SessionID, reattached.AttachmentID); err != nil {
		t.Fatal(err)
	}

	// Simulate the old manager process releasing its listener. A replacement
	// manager must recreate the same session-scoped path while restoring the
	// durable provider host, before any client attaches.
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := newReceivingManager(t, layout, factory)
	if err := restarted.LoadAndReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("restart did not restore session proxy: %v", err)
	}
	assertProviderProxyRoundTrip(t, socketPath, "after-restart")

	restoredAttachment, err := restarted.Attach(context.Background(), factory.request(false))
	if err != nil {
		t.Fatal(err)
	}
	if restoredAttachment.ManagerEndpoint != endpoint {
		t.Fatalf("restored endpoint = %q, want stable %q", restoredAttachment.ManagerEndpoint, endpoint)
	}
	if _, err := restarted.Detach(restoredAttachment.Session.SessionID, restoredAttachment.AttachmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Stop(context.Background(), restoredAttachment.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("session proxy survived explicit stop: %v", err)
	}
}
