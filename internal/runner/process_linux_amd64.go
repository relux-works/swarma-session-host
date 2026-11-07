package runner

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// The required GET_INFO and namespace ioctl ABI is present in upstream v6.13.
// Runtime support is determined by capabilities, never a release string. Use
// the 64-byte version-0 ABI, not the build machine's evolving header size.
const (
	pidfsMagic            = 0x50494446
	procfsMagic           = 0x9fa0
	nsfsMagic             = 0x6e736673
	pidfdGetInfo          = 0xc040ff0b
	pidfdGetPIDNamespace  = 0xff05
	pidfdGetUserNamespace = 0xff09
)

type pidfdInfo struct {
	Mask, CgroupID                                                           uint64
	PID, TGID, PPID, RUID, RGID, EUID, EGID, SUID, SGID, FSUID, FSGID, Spare uint32
}

func getPIDFDInfo(fd int) (pidfdInfo, error) {
	if unsafe.Sizeof(pidfdInfo{}) != 64 {
		return pidfdInfo{}, errChildBirthUnsupported
	}
	info := pidfdInfo{Mask: 3}
	_, _, e := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), pidfdGetInfo, uintptr(unsafe.Pointer(&info)))
	if e != 0 {
		return info, e
	}
	if info.Mask&3 != 3 {
		return info, errChildBirthUnavailable
	}
	return info, nil
}

// pidfd_open is used ONLY for self preflight/runner scope. Child capture uses
// the atomic CLONE_PIDFD result, never a numeric-PID reacquisition.
const sysPIDFDOpen = 434 // Linux/amd64

func openSelfPIDFD() (int, error) {
	fd, _, e := syscall.Syscall(sysPIDFDOpen, uintptr(os.Getpid()), 0, 0)
	if e != 0 {
		return -1, e
	}
	return int(fd), nil
}

func preflightLinuxSelf() error {
	fd, err := openSelfPIDFD()
	if err != nil {
		return classifyLinuxCapture(err)
	}
	defer syscall.Close(fd)
	if !setCloseOnExec(fd) {
		return errChildBirthUnavailable
	}
	if _, err = fdInode(fd, pidfsMagic); err != nil {
		return classifyLinuxCapture(err)
	}
	info, err := getPIDFDInfo(fd)
	if err != nil {
		return classifyLinuxCapture(err)
	}
	if info.PID != uint32(os.Getpid()) || info.TGID != info.PID || info.EUID != uint32(os.Geteuid()) {
		return errChildBirthUnavailable
	}
	for _, cmd := range []uintptr{pidfdGetPIDNamespace, pidfdGetUserNamespace} {
		ns, _, e := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), cmd, 0)
		if e != 0 {
			return classifyLinuxCapture(e)
		}
		ok := setCloseOnExec(int(ns))
		_, nsErr := fdInode(int(ns), nsfsMagic)
		syscall.Close(int(ns))
		if !ok {
			return errChildBirthUnavailable
		}
		if nsErr != nil {
			return classifyLinuxCapture(nsErr)
		}
	}
	return nil
}

// pollPIDFD is a zero-time capability/lifecycle observation, never called by
// the identify snapshot. It is not an independent live-admission verifier.
func pollPIDFD(fd int) (uint16, error) {
	p := struct {
		FD              int32
		Events, Revents int16
	}{FD: int32(fd), Events: 1}
	_, _, e := syscall.Syscall(syscall.SYS_POLL, uintptr(unsafe.Pointer(&p)), 1, 0)
	if e != 0 {
		return 0, e
	}
	return uint16(p.Revents), nil
}

// This names an observation budget, not a realtime bound on mandatory cleanup.
const preflightObservationTimeout = 1000 * time.Millisecond
const preflightProbeBinary = "/bin/sleep"

func preflightIdentifyV2() error { return preflightLinuxCapture() }

// Hooks are per invocation and used only by fault-injection qualification.
// They cannot be selected by Config, Plan, environment or a wire request.
type linuxPreflightHooks struct {
	now     func() time.Time
	stage   func(string) error
	capture func(int, int) linuxOriginal
	wait    func(*os.Process) (*os.ProcessState, error)
}

func preflightLinuxCapture() error {
	return preflightLinuxCaptureWith(linuxPreflightHooks{})
}

func preflightLinuxCaptureWith(h linuxPreflightHooks) error {
	if h.now == nil {
		h.now = time.Now
	}
	if h.capture == nil {
		h.capture = captureLinuxOriginal
	}
	if h.wait == nil {
		h.wait = (*os.Process).Wait
	}
	deadline := h.now().Add(preflightObservationTimeout)
	check := func() error {
		if !h.now().Before(deadline) {
			return errChildBirthUnavailable
		}
		return nil
	}
	stage := func(name string, f func() error) error {
		if err := check(); err != nil {
			return err
		}
		if h.stage != nil {
			if err := h.stage(name); err != nil {
				return err
			}
		}
		if err := check(); err != nil {
			return err
		}
		if err := f(); err != nil {
			return err
		}
		return check()
	}
	if err := stage("self", preflightLinuxSelf); err != nil {
		return err
	}
	if err := stage("peer", func() error { return classifyPreflightPeer(deadline) }); err != nil {
		return err
	}

	// One owner, at most one probe child and exactly one Wait attempt, including
	// partial capture, expiry and uncertain Wait. Never reacquire a numeric PID.
	var p *os.Process
	var original linuxOriginal
	waitAttempted := false
	defer func() {
		if p != nil && !waitAttempted {
			_ = p.Kill()
			waitAttempted = true
			_, _ = h.wait(p)
		}
		if original.release != nil {
			original.release()
		}
	}()
	pidfd := -1
	if err := stage("spawn", func() error {
		devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			return errChildBirthUnavailable
		}
		defer devnull.Close()
		p, err = os.StartProcess(preflightProbeBinary, []string{"sleep", "5"}, &os.ProcAttr{
			Env: []string{}, Files: []*os.File{devnull, devnull, devnull}, Sys: &syscall.SysProcAttr{PidFD: &pidfd},
		})
		// Own even a partial atomic handle before any post-spawn deadline check.
		original.release = (&linuxFDSet{fds: []int{pidfd}}).close
		if err != nil {
			return classifyLinuxCapture(err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := stage("capture", func() error {
		original = h.capture(p.Pid, pidfd)
		return original.err
	}); err != nil {
		return err
	}
	if err := stage("live", func() error {
		if flags, err := pollPIDFD(pidfd); err != nil || flags != 0 {
			return errChildBirthUnavailable
		}
		return preflightFDInfo(pidfd, p.Pid)
	}); err != nil {
		return err
	}
	if err := stage("kill", p.Kill); err != nil {
		return err
	}
	if err := stage("zombie", func() error {
		for {
			if err := check(); err != nil {
				return err
			}
			flags, err := pollPIDFD(pidfd)
			if err != nil {
				return classifyLinuxCapture(err)
			}
			if err := check(); err != nil {
				return err
			}
			if flags != 0 {
				if flags != 1 {
					return errChildBirthUnavailable
				}
				break // POLLIN only, before our sole Wait
			}
			remaining := deadline.Sub(h.now())
			if remaining <= 0 {
				return errChildBirthUnavailable
			}
			time.Sleep(min(time.Millisecond, remaining))
		}
		return preflightFDInfo(pidfd, p.Pid)
	}); err != nil {
		return err
	}
	if err := stage("wait", func() error {
		waitAttempted = true // consumed even on error; cleanup must never retry
		if _, err := h.wait(p); err != nil {
			return errChildBirthUnavailable
		}
		return nil
	}); err != nil {
		return err
	}
	if err := stage("reaped", func() error {
		if flags, err := pollPIDFD(pidfd); err != nil || flags != 0x11 {
			return errChildBirthUnavailable
		}
		if err := preflightFDInfo(pidfd, -1); err != nil {
			return err
		}
		inode, err := fdInode(pidfd, pidfsMagic)
		if err != nil || strconv.FormatUint(inode, 10) != original.identity.PIDFSInode {
			return errChildBirthUnavailable
		}
		return nil
	}); err != nil {
		return err
	}
	return check() // no success after the absolute observation deadline
}

func classifyPreflightPeer(deadline time.Time) error {
	if err := preflightLinuxPeer(deadline); err != nil {
		return classifyLinuxCapture(err)
	}
	return nil
}

func preflightFDInfo(fd, pid int) error {
	b, err := os.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", fd))
	if err != nil || !procPIDFDMatches(string(b), pid) {
		return errChildBirthUnavailable
	}
	return nil
}

func preflightLinuxPeer(deadline time.Time) error {
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	// Keep both original endpoints open during the round-trip; each FileConn
	// owns a separate duplicate. Originals close exactly once through os.File.
	files := []*os.File{os.NewFile(uintptr(pair[0]), "pidfs-peer-0"), os.NewFile(uintptr(pair[1]), "pidfs-peer-1")}
	defer files[0].Close()
	defer files[1].Close()
	var conns [2]*net.UnixConn
	for i, f := range files {
		c, err := net.FileConn(f)
		if err != nil {
			return err
		}
		conn, ok := c.(*net.UnixConn)
		if !ok {
			c.Close()
			return errChildBirthUnavailable
		}
		defer conn.Close()
		conns[i] = conn
		if err := conn.SetDeadline(deadline); err != nil {
			return err
		}
		uid, pid, err := localipc.PeerIdentity(conn)
		if err != nil || pid != os.Getpid() || uid != uint32(os.Geteuid()) {
			return errChildBirthUnavailable
		}
	}
	// Private round-trip only. Both directions share the entry deadline.
	for i := range conns {
		if _, err := conns[i].Write([]byte{0x5a}); err != nil {
			return err
		}
		var b [1]byte
		if _, err := io.ReadFull(conns[1-i], b[:]); err != nil || b[0] != 0x5a {
			return errChildBirthUnavailable
		}
	}
	return nil
}

// Every retained descriptor has one serialized terminal owner. Reset numbers
// before closing so a repeated release cannot close an unrelated reused fd.
type linuxFDSet struct {
	once sync.Once
	fds  []int
}

func (s *linuxFDSet) close() {
	s.once.Do(func() {
		for i, fd := range s.fds {
			s.fds[i] = -1
			if fd >= 0 {
				_ = syscall.Close(fd)
			}
		}
	})
}

func fdInode(fd int, magic int64) (uint64, error) {
	var fs syscall.Statfs_t
	var st syscall.Stat_t
	if unsafe.Sizeof(st.Ino) != 8 {
		return 0, errChildBirthUnsupported
	}
	if err := syscall.Fstatfs(fd, &fs); err != nil {
		return 0, err
	}
	if fs.Type != magic {
		return 0, errChildBirthUnsupported
	}
	if err := syscall.Fstat(fd, &st); err != nil {
		return 0, err
	}
	if st.Ino == 0 {
		return 0, errChildBirthUnavailable
	}
	return st.Ino, nil
}
func closeOnExec(fd int) bool {
	flags, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
	return e == 0 && flags&syscall.FD_CLOEXEC != 0
}

func setCloseOnExec(fd int) bool {
	flags, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
	if e != 0 {
		return false
	}
	_, _, e = syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFD, flags|syscall.FD_CLOEXEC)
	return e == 0 && closeOnExec(fd)
}

// Every component stays beneath a pinned genuine procfs directory. No final or
// intermediate symlink is accepted, except the explicitly opened nsfs links.
func readProcAt(root int, path string, limit int) (string, error) {
	parts := strings.Split(path, "/")
	dir := root
	for i, name := range parts {
		flags := syscall.O_RDONLY | syscall.O_CLOEXEC | syscall.O_NOFOLLOW
		if i < len(parts)-1 {
			flags |= syscall.O_DIRECTORY
		}
		fd, err := syscall.Openat(dir, name, flags, 0)
		if dir != root {
			syscall.Close(dir)
		}
		if err != nil {
			return "", err
		}
		if _, err = fdInode(fd, procfsMagic); err != nil {
			syscall.Close(fd)
			return "", err
		}
		if i == len(parts)-1 {
			f := os.NewFile(uintptr(fd), "proc-evidence")
			defer f.Close()
			b, err := io.ReadAll(io.LimitReader(f, int64(limit+1)))
			if err != nil || len(b) > limit {
				return "", errChildBirthUnavailable
			}
			return string(b), nil
		}
		dir = fd
	}
	return "", errChildBirthUnavailable
}
func procPIDFDMatches(text string, pid int) bool {
	seenPID, seenNS := false, false
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "Pid:":
			if seenPID || len(fields) != 2 || fields[1] != strconv.Itoa(pid) {
				return false
			}
			seenPID = true
		case "NSpid:":
			// A second namespace level means this procfs is not our admitted scope.
			if seenNS || len(fields) != 2 || fields[1] != strconv.Itoa(pid) {
				return false
			}
			seenNS = true
		}
	}
	return seenPID && seenNS
}

// The component supports the named single-trust-unit spawn expectation only.
// This is an effective-UID check, never pidfs st_uid or an O/S/C policy.
func sameChildCredentials(i pidfdInfo, pid int, expectedEUID uint32) bool {
	return i.PID == uint32(pid) && i.TGID == uint32(pid) && i.PPID == uint32(os.Getpid()) && i.EUID == expectedEUID
}

func classifyLinuxCapture(err error) error {
	if errors.Is(err, syscall.ENOTTY) || errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, errChildBirthUnsupported) {
		return errChildBirthUnsupported
	}
	return errChildBirthUnavailable
}

func startOriginalProcess(selected bool, binary string, args []string, attr *os.ProcAttr) (*os.Process, linuxOriginal, error) {
	if !selected {
		p, err := os.StartProcess(binary, args, attr)
		return p, linuxOriginal{err: errChildBirthUnsupported}, err
	}
	// Selected Start already completed preflight before Listen. Never repeat it
	// here. Reject unsupported scope/credential intent before workload creation.
	if attr.Sys != nil && (attr.Sys.Cloneflags != 0 || attr.Sys.Unshareflags != 0 || attr.Sys.Credential != nil ||
		len(attr.Sys.UidMappings) != 0 || len(attr.Sys.GidMappings) != 0 || len(attr.Sys.AmbientCaps) != 0 || attr.Sys.Chroot != "" || attr.Sys.Ptrace) {
		return nil, linuxOriginal{err: errChildBirthUnsupported}, errChildBirthUnsupported
	}
	pidfd := -1
	childAttr := *attr
	sys := syscall.SysProcAttr{}
	if attr.Sys != nil {
		sys = *attr.Sys
	}
	sys.PidFD = &pidfd
	childAttr.Sys = &sys
	p, err := os.StartProcess(binary, args, &childAttr)
	if err != nil {
		if pidfd >= 0 {
			syscall.Close(pidfd)
			pidfd = -1
		}
		return nil, linuxOriginal{}, err
	}
	result := captureLinuxOriginal(p.Pid, pidfd)
	return p, result, nil
}
func captureLinuxOriginal(pid, pidfd int) (result linuxOriginal) {
	held := &linuxFDSet{}
	if pidfd >= 0 {
		held.fds = append(held.fds, pidfd)
	}
	result.release = held.close
	// Even partial capture stays under the original terminal cleanup duty. Failure
	// never discards the child, retries capture, adopts a PID or changes v1 birth.
	result.err = errChildBirthUnavailable
	fail := func(err error) linuxOriginal { result.err = classifyLinuxCapture(err); return result }
	if pid <= 0 || pid > 2147483647 || pidfd < 0 {
		return fail(errChildBirthUnsupported)
	}
	if !setCloseOnExec(pidfd) {
		return result
	}
	inode, err := fdInode(pidfd, pidfsMagic)
	if err != nil {
		return fail(err)
	}
	before, err := getPIDFDInfo(pidfd)
	if err != nil {
		return fail(err)
	}
	if !sameChildCredentials(before, pid, uint32(os.Geteuid())) {
		return result
	}
	ownPIDFD, err := openSelfPIDFD()
	if err != nil {
		return fail(err)
	}
	held.fds = append(held.fds, ownPIDFD)
	if !setCloseOnExec(ownPIDFD) {
		return result
	}
	if _, err = fdInode(ownPIDFD, pidfsMagic); err != nil {
		return fail(err)
	}
	ownInfo, err := getPIDFDInfo(ownPIDFD)
	if err != nil {
		return fail(err)
	}
	if ownInfo.PID != uint32(os.Getpid()) || ownInfo.TGID != ownInfo.PID || ownInfo.EUID != uint32(os.Geteuid()) {
		return result
	}
	proc, err := syscall.Open("/proc", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fail(err)
	}
	defer syscall.Close(proc)
	if _, err = fdInode(proc, procfsMagic); err != nil {
		return fail(err)
	}
	// self is a kernel procfs magic link; its target must equal our locally known
	// PID. Opening it relative to the pinned mount avoids a fresh /proc lookup.
	self, err := syscall.Openat(proc, "self", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fail(err)
	}
	defer syscall.Close(self)
	if _, err = fdInode(self, procfsMagic); err != nil {
		return fail(err)
	}
	status, err := readProcAt(self, "status", 16384)
	if err != nil {
		return fail(err)
	}
	selfPID, selfTGID := "", ""
	for _, line := range strings.Split(status, "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			if f[0] == "Pid:" {
				selfPID = f[1]
			}
			if f[0] == "Tgid:" {
				selfTGID = f[1]
			}
		}
	}
	if selfPID != strconv.Itoa(os.Getpid()) || selfTGID != selfPID {
		return result
	}
	fdinfo, err := readProcAt(self, fmt.Sprintf("fdinfo/%d", pidfd), 4096)
	if err != nil {
		return fail(err)
	}
	if !procPIDFDMatches(fdinfo, pid) {
		return result
	}
	boot, err := readProcAt(proc, "sys/kernel/random/boot_id", 64)
	if err != nil {
		return fail(err)
	}
	if len(boot) != 37 || boot[36] != '\n' || !canonicalBootID(boot[:36]) {
		return result
	}
	var scopes [2]string
	for j, cmd := range []uintptr{pidfdGetPIDNamespace, pidfdGetUserNamespace} {
		// Namespace FDs come from the original pidfd, never /proc/<child-pid>.
		childFD, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(pidfd), cmd, 0)
		if errno != 0 {
			return fail(errno)
		}
		held.fds = append(held.fds, int(childFD))
		if !setCloseOnExec(int(childFD)) {
			return result
		}
		childInode, err := fdInode(int(childFD), nsfsMagic)
		if err != nil {
			return fail(err)
		}
		ownNS, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(ownPIDFD), cmd, 0)
		if errno != 0 {
			return fail(errno)
		}
		ownFD := int(ownNS)
		held.fds = append(held.fds, ownFD)
		if !setCloseOnExec(ownFD) {
			return result
		}
		ownInode, err := fdInode(ownFD, nsfsMagic)
		if err != nil {
			return fail(err)
		}
		var a, b syscall.Stat_t
		if syscall.Fstat(int(childFD), &a) != nil || syscall.Fstat(ownFD, &b) != nil || a.Dev != b.Dev || childInode != ownInode {
			return result
		}
		scopes[j] = strconv.FormatUint(childInode, 10)
	}
	after, err := getPIDFDInfo(pidfd)
	if err != nil {
		return fail(err)
	}
	if !sameChildCredentials(after, pid, uint32(os.Geteuid())) {
		return result
	}
	result.identity = LinuxPIDFSIdentity{"linux-pidfs-v1", boot[:36], strconv.FormatUint(inode, 10), scopes[0], scopes[1]}
	result.err = nil
	return result
}
