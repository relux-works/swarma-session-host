package localipc

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

// Both ends of a real same-process Unix connection expose the actual UID/PID;
// malformed SDK size/version/range fixtures fail closed without guessed values.
func TestPeerIdentity(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "session-peer-id-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	l, err := Listen(filepath.Join(dir, "s.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c, err := net.DialUnix("unix", nil, l.Addr().(*net.UnixAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, err := l.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, conn := range []*net.UnixConn{c, s} {
		uid, pid, err := PeerIdentity(conn)
		if err != nil || uid != uint32(os.Getuid()) || pid != os.Getpid() {
			t.Fatalf("PEER_KERNEL_IDENTITY: %d %d %v", uid, pid, err)
		}
	}
	for _, tc := range []struct {
		name                string
		size, want, version uint32
		groups              int16
		pidSize             uint32
		pid                 int32
		valid               bool
	}{
		{"valid", 76, 76, 0, 1, 4, 123, true}, {"short", 75, 76, 0, 1, 4, 123, false},
		{"version", 76, 76, 1, 1, 4, 123, false}, {"negativeGroups", 76, 76, 0, -1, 4, 123, false},
		{"manyGroups", 76, 76, 0, 17, 4, 123, false}, {"pidSize", 76, 76, 0, 1, 3, 123, false},
		{"zeroPID", 76, 76, 0, 1, 4, 0, false}, {"negativePID", 76, 76, 0, 1, 4, -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if validPeerIdentity(tc.size, tc.want, tc.version, tc.groups, tc.pidSize, tc.pid) != tc.valid {
				t.Fatal("PEER_CREDENTIAL_VALIDATION_" + tc.name)
			}
		})
	}
	c.Close()
	if _, _, err := PeerIdentity(c); err == nil {
		t.Fatal("PEER_CLOSED_FD")
	}
}
