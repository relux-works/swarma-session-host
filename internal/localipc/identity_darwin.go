package localipc

import (
	"errors"
	"net"
	"syscall"
	"unsafe"
)

// PeerIdentity obtains both credentials from this connected descriptor while
// RawConn.Control holds it open. Same UID is OS-user identity only.
// Darwin SDK sys/un.h: SOL_LOCAL=0, LOCAL_PEERCRED=1, LOCAL_PEERPID=2;
// sys/ucred.h: xucred version0, uid_t32, short group count, 16 gid_t32.
func PeerIdentity(conn *net.UnixConn) (uint32, int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var cred struct {
		Version uint32
		UID     uint32
		GroupsN int16
		Groups  [16]uint32
	}
	credSize := uint32(unsafe.Sizeof(cred))
	var pid int32
	pidSize := uint32(unsafe.Sizeof(pid))
	var socketErr syscall.Errno
	err = raw.Control(func(fd uintptr) {
		_, _, socketErr = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, 0, 1, uintptr(unsafe.Pointer(&cred)), uintptr(unsafe.Pointer(&credSize)), 0)
		if socketErr == 0 {
			_, _, socketErr = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, 0, 2, uintptr(unsafe.Pointer(&pid)), uintptr(unsafe.Pointer(&pidSize)), 0)
		}
	})
	if err != nil {
		return 0, 0, err
	}
	if socketErr != 0 {
		return 0, 0, socketErr
	}
	if !validPeerIdentity(credSize, uint32(unsafe.Sizeof(cred)), cred.Version, cred.GroupsN, pidSize, pid) {
		return 0, 0, errors.New("invalid_peer_identity")
	}
	return cred.UID, int(pid), nil
}
func validPeerIdentity(credSize, wantSize, version uint32, groups int16, pidSize uint32, pid int32) bool {
	return credSize == wantSize && version == 0 && groups >= 0 && groups <= 16 && pidSize == 4 && pid > 0
}
