package localipc

import (
	"net"
	"syscall"
)

func PeerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *syscall.Ucred
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		cred, socketErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if err != nil {
		return 0, err
	}
	if socketErr != nil {
		return 0, socketErr
	}
	return cred.Uid, nil
}
