//go:build linux && amd64

package localipc

import (
	"errors"
	"net"
	"syscall"
	"unsafe"
)

// PeerIdentity reads one SO_PEERCRED structure from the connected descriptor.
// PID/UID equality does not prove an incarnation, image or distinct same-UID agent.
func PeerIdentity(conn *net.UnixConn) (uint32, int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var cred syscall.Ucred
	size := uint32(unsafe.Sizeof(cred))
	var errno syscall.Errno
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		var kind int
		kind, socketErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TYPE)
		if socketErr != nil {
			return
		}
		if kind != syscall.SOCK_STREAM {
			socketErr = errors.New("invalid_peer_socket")
			return
		}
		if _, socketErr = syscall.Getpeername(int(fd)); socketErr != nil {
			return
		}
		_, _, errno = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.SOL_SOCKET, syscall.SO_PEERCRED, uintptr(unsafe.Pointer(&cred)), uintptr(unsafe.Pointer(&size)), 0)
	})
	if err != nil {
		return 0, 0, err
	}
	if socketErr != nil {
		return 0, 0, socketErr
	}
	if errno != 0 {
		return 0, 0, errno
	}
	if size != uint32(unsafe.Sizeof(cred)) || cred.Pid <= 0 || cred.Uid == ^uint32(0) {
		return 0, 0, errors.New("invalid_peer_identity")
	}
	return cred.Uid, int(cred.Pid), nil
}
