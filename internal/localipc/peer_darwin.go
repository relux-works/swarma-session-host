package localipc

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

// PeerUID authenticates an OS user, not individual agents sharing that user.
func PeerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	// Darwin's SOL_LOCAL / LOCAL_PEERCRED returns struct xucred.
	var cred struct {
		Version uint32
		UID     uint32
		GroupsN int16
		Groups  [16]uint32
	}
	size := uint32(unsafe.Sizeof(cred))
	var socketErr syscall.Errno
	err = raw.Control(func(fd uintptr) {
		_, _, socketErr = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, 0, 1,
			uintptr(unsafe.Pointer(&cred)), uintptr(unsafe.Pointer(&size)), 0)
	})
	if err != nil {
		return 0, err
	}
	if socketErr != 0 {
		return 0, socketErr
	}
	if size != uint32(unsafe.Sizeof(cred)) || cred.Version != 0 || cred.GroupsN < 0 || cred.GroupsN > 16 {
		return 0, fmt.Errorf("invalid peer credentials")
	}
	return cred.UID, nil
}
