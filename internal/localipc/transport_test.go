package localipc

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Closed, bounded frames accept one object; unknown members, extra values,
// truncated JSON and oversized input are rejected rather than partly applied.
func TestFrames(t *testing.T) {
	for _, tc := range []struct {
		input string
		valid bool
	}{
		{"{\"name\":\"fixture\"}\n", true},
		{"{\"name\":\"fixture\",\"extra\":1}\n", false},
		{"{} {}\n", false}, {"{\"name\":\n", false},
		{strings.Repeat("x", MaxFrame+1), false},
	} {
		var value struct {
			Name string `json:"name"`
		}
		err := Decode(strings.NewReader(tc.input), &value)
		if (err == nil) != tc.valid {
			t.Fatalf("frame validity mismatch (bytes=%d): %v", len(tc.input), err)
		}
	}
	var b bytes.Buffer
	if err := Write(&b, struct {
		Name string `json:"name"`
	}{"fixture"}); err != nil {
		t.Fatal(err)
	}
	var read struct {
		Name string `json:"name"`
	}
	if err := Read(bufio.NewReaderSize(&b, MaxFrame), &read); err != nil || read.Name != "fixture" {
		t.Fatalf("frame round trip: %v %+v", err, read)
	}
}

// The real local Unix socket returns the current kernel UID on this platform;
// this proves OS-user authentication only, not hostile same-UID isolation.
func TestPeerUIDAndPrivateEndpoint(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "session-peer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	listener, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, conn := range []*net.UnixConn{client, server} {
		uid, err := PeerUID(conn)
		if err != nil || uid != uint32(os.Getuid()) {
			t.Fatalf("peer UID: %d %v", uid, err)
		}
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket mode: %v %v", info, err)
	}
	if duplicate, err := Listen(path); err == nil {
		duplicate.Close()
		t.Fatal("occupied endpoint replaced")
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if public, err := Listen(filepath.Join(dir, "public.sock")); err == nil {
		public.Close()
		t.Fatal("non-private socket directory admitted")
	}
}
