//go:build linux && amd64

package localipc

import (
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestLinuxConnectedPeerIdentity(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "linux-peer-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	l, err := Listen(filepath.Join(dir, "p.sock"))
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
			t.Fatal(uid, pid, err)
		}
	}
	c.Close()
	if _, _, err := PeerIdentity(c); err == nil {
		t.Fatal("closed descriptor accepted")
	}
}

func TestLinuxConnectedPeerRejectsDatagram(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[1])
	f := os.NewFile(uintptr(fds[0]), "datagram-peer")
	c, err := net.FileConn(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := PeerIdentity(c.(*net.UnixConn)); err == nil {
		t.Fatal("datagram peer accepted")
	}
}
