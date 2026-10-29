//go:build unix

package sessionmanager

import (
	"os"
	"path/filepath"
	"testing"
)

// Minimal original helper closure; real durable host/codec/notices,
// isolated filesystem and in-process control only, no provider process.
func legacyPhysicalLayout(t *testing.T, _ string) Layout {
	t.Helper()
	root := t.TempDir()
	socketRoot := os.Getenv("SESSIONHOST_TEST_SOCKET_ROOT")
	if socketRoot == "" {
		socketRoot = os.TempDir()
	}
	sockets, err := os.MkdirTemp(socketRoot, "p")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(sockets); err != nil {
			t.Error(err)
		}
	})
	state := filepath.Join(root, "managers", "fixture")
	return Layout{Namespace: "physical-fixture", StateDir: state, SessionsDir: filepath.Join(state, "sessions"), LockFile: filepath.Join(state, "manager.lock"), SocketPath: filepath.Join(sockets, "c.sock"), AttachmentsDir: filepath.Join(sockets, "attachments-1234abcd"), EndpointFile: filepath.Join(state, "endpoint.json"), TokenFile: filepath.Join(state, "token"), OperatorLogFile: filepath.Join(state, "logs", "sessiond.log"), NoticesDir: filepath.Join(state, "notices")}
}
func legacyPhysicalNewHost(t *testing.T, layout Layout, factories ...ProviderHostFactory) (*SessionHost, error) {
	t.Helper()
	notices, err := NewSessionNoticeStore(layout)
	if err != nil {
		return nil, err
	}
	h, err := NewHost(HostOptions{Layout: layout, Config: hostCommitProbeConfig{}, Codec: NewLegacyRecordCodec(), Notices: notices, Factories: factories})
	if err == nil {
		t.Cleanup(h.Close)
	}
	return h, err
}
