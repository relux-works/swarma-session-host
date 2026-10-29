package sessionmanager

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func readProviderProcessIdentity(pid int) (providerProcessIdentity, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return providerProcessIdentity{}, err
	}
	if int(info.Proc.P_pid) != pid || info.Proc.P_starttime.Sec <= 0 {
		return providerProcessIdentity{}, fmt.Errorf("unreadable provider process birth time")
	}
	return providerProcessIdentity{PID: pid, UID: info.Eproc.Pcred.P_ruid, Start: fmt.Sprintf("%d:%d", info.Proc.P_starttime.Sec, info.Proc.P_starttime.Usec), PGID: int(info.Eproc.Pgid)}, nil
}
