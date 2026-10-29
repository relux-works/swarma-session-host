//go:build darwin && cgo

package admission

/*
#include <libproc.h>
#include <sys/sysctl.h>
#include <string.h>
// Apple XNU bsd/sys/codesign.h declares this ABI; the macOS SDK omits the header.
extern int csops(pid_t, unsigned int, void *, size_t);
static int read_identity(int pid, struct proc_bsdinfo *info, unsigned char *hash, char *boot, size_t *bootlen, unsigned int hashop) {
 struct proc_bsdinfo before;
 if (proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &before, sizeof(before)) != sizeof(before)) return -1;
 if (csops(pid, hashop, hash, 20) != 0) return -1; // CS_OPS_CDHASH
 if (sysctlbyname("kern.bootsessionuuid", boot, bootlen, NULL, 0) != 0) return -1;
 if (proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, info, sizeof(*info)) != sizeof(*info)) return -1;
 if (before.pbi_pid != info->pbi_pid || before.pbi_uid != info->pbi_uid || before.pbi_start_tvsec != info->pbi_start_tvsec || before.pbi_start_tvusec != info->pbi_start_tvusec) return -1;
 // SZOMB is a readable exited process, not live evidence.
 if (info->pbi_status == 5) return -1;
 unsigned char after[20];
 if (csops(pid, hashop, after, 20) != 0 || memcmp(hash, after, 20) != 0) return -1;
 return 0;
}
*/
import "C"
import (
	"context"
	"encoding/hex"
	"errors"
	"time"
	"unsafe"
)

// ObserveProcess reads a process supplied by the caller's admitted scope. M3-S
// exercises it only on owned synthetic processes; it neither discovers nor kills.
func ObserveProcess(ctx context.Context, pid int) (Process, error) {
	return observeProcess(ctx, pid, 5) // CS_OPS_CDHASH
}

// The private operation argument permits a control of an actual failing csops
// syscall; public observation always uses CS_OPS_CDHASH.
func observeProcess(ctx context.Context, pid int, hashop uint32) (Process, error) {
	if pid <= 0 || int(C.int(pid)) != pid || ctx.Err() != nil {
		return Process{}, errors.New("process_unavailable")
	}
	var info C.struct_proc_bsdinfo
	var hash [20]byte
	var boot [64]byte
	n := C.size_t(len(boot))
	if C.read_identity(C.int(pid), &info, (*C.uchar)(unsafe.Pointer(&hash[0])), (*C.char)(unsafe.Pointer(&boot[0])), &n, C.uint(hashop)) != 0 || ctx.Err() != nil || info.pbi_start_tvusec >= 1000000 || info.pbi_pid != C.uint32_t(pid) {
		return Process{}, errors.New("process_unavailable")
	}
	// Require the published canonical UUID including its terminator.
	if n != 37 || boot[36] != 0 {
		return Process{}, errors.New("process_unavailable")
	}
	p := Process{PID: pid, UID: uint32(info.pbi_uid), BootUUID: string(boot[:36]), StartedAt: time.Unix(int64(info.pbi_start_tvsec), int64(info.pbi_start_tvusec)*1000).UTC().Format(ProcessTime), CDHash: hex.EncodeToString(hash[:])}
	if !validProcess(p) {
		return Process{}, errors.New("process_unavailable")
	}
	return p, nil
}
