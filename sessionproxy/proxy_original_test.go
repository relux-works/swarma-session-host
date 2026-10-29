package sessionproxy

import (
	"io"
	"net"
	"os"
	"strings"
	"testing"
)

// Preserves the original Unix proxy round-trip, private mode and Close cleanup
// assertions using an actual owned loopback listener, not a provider process.
func TestProviderProxyHidesProviderEndpointAndRemovesSocketOnClose(t *testing.T) {
	proxyDir, err := os.MkdirTemp("/tmp", "tb-attachment-proxy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(proxyDir) })

	provider, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	go func() {
		connection, acceptErr := provider.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_, _ = io.Copy(connection, connection)
	}()

	proxy, err := New(
		proxyDir,
		"SES-1234567890abcdef",
		"ws://"+provider.Addr().String(),
	)
	if err != nil {
		t.Fatal(err)
	}
	path := strings.TrimPrefix(proxy.Endpoint(), "unix://")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("proxy mode = %o, want 0600", info.Mode().Perm())
	}
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write([]byte("manager-proxy")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len("manager-proxy"))
	if _, err := io.ReadFull(connection, buffer); err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	if string(buffer) != "manager-proxy" {
		t.Fatalf("echo = %q", buffer)
	}
	if err := proxy.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("proxy socket still exists: %v", err)
	}
}
