//go:build darwin && cgo

package runner

/*
#include <libproc.h>
*/
import "C"
import "unsafe"

// Only called for the original directly owned child, before any reaper starts.
// PROC_PIDTBSDINFO also describes zombies; this capture does not claim liveness.
func captureChildBirth(pid int) (childBirth, error) {
	if pid <= 0 || int(C.int(pid)) != pid {
		return childBirth{}, errChildBirthUnavailable
	}
	// Nonzero arg includes zombies in XNU's PROC_PIDTBSDINFO lookup. With
	// arg=0 an already exited but unreaped child is reported unavailable.
	var info C.struct_proc_bsdinfo
	n := C.proc_pidinfo(C.int(pid), C.PROC_PIDTBSDINFO, 1, unsafe.Pointer(&info), C.int(C.sizeof_struct_proc_bsdinfo))
	return validateChildBirth(pid, int(info.pbi_pid), n == C.sizeof_struct_proc_bsdinfo, uint64(info.pbi_start_tvsec), uint64(info.pbi_start_tvusec))
}
