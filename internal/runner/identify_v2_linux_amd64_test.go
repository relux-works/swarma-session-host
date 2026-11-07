package runner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func startV2KernelRunner(t *testing.T) (*Server, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	cfg := testConfig(t, "echo")
	cfg.IdentifyV2 = true
	s, err := startWithReaperGate(cfg, captureChildBirth, waitOriginalChild, func() { <-gate })
	if err != nil {
		open()
		t.Fatal(err)
	}
	t.Cleanup(func() { open(); s.Close() })
	trackTestRunner(t, s)
	if s.linux.err != nil {
		open()
		if os.Getenv("SESSION_HOST_REQUIRE_LINUX_PIDFS") == "1" {
			t.Fatalf("REQUIRED_LINUX_PIDFS_NOT_QUALIFIED: %v", s.linux.err)
		}
		t.Skipf("LINUX_PIDFS_NOT_QUALIFIED: %v", s.linux.err)
	}
	return s, open
}
func v2RequestFor(s *Server) IdentifyV2Request {
	return IdentifyV2Request{identifyRequestSchemaV2, s.cfg.Execution, s.instance}
}
func v2Call(t *testing.T, s *Server) IdentifyV2Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := identifyV2ForQualification(ctx, s.cfg.Socket, os.Getpid(), uint32(os.Getuid()), v2RequestFor(s))
	if err != nil {
		t.Fatalf("v2 ownership exchange: %+v %v", r, err)
	}
	return r
}
func rawV2(t *testing.T, s *Server, b []byte) ([]byte, error) {
	t.Helper()
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: s.cfg.Socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err = c.Write(b); err != nil {
		return nil, err
	}
	return bufio.NewReader(c).ReadBytes('\n')
}
func ownedPIDFDs(t *testing.T, id LinuxPIDFSIdentity) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	var fds []int
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		ino, err := fdInode(fd, pidfsMagic)
		if err == nil && strconv.FormatUint(ino, 10) == id.PIDFSInode {
			fds = append(fds, fd)
		}
	}
	return fds
}
func ownedNamespaceFDs(t *testing.T, id LinuxPIDFSIdentity) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	var result []int
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		ino, err := fdInode(fd, nsfsMagic)
		if err == nil && (strconv.FormatUint(ino, 10) == id.PIDNamespaceInode || strconv.FormatUint(ino, 10) == id.UserNamespaceInode) {
			if !closeOnExec(fd) {
				t.Fatal("retained namespace without CLOEXEC")
			}
			result = append(result, fd)
		}
	}
	return result
}

func pidfdEvents(t *testing.T, fd int) uint32 {
	t.Helper()
	p := struct {
		FD              int32
		Events, Revents int16
	}{FD: int32(fd), Events: 1}
	_, _, e := syscall.Syscall(syscall.SYS_POLL, uintptr(unsafe.Pointer(&p)), 1, 0)
	if e != 0 {
		t.Fatal(e)
	}
	return uint32(uint16(p.Revents))
}
func logPIDFDEvidence(t *testing.T, phase string, fd int) {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", fd))
	if err != nil {
		t.Fatal(err)
	}
	info, infoErr := getPIDFDInfo(fd)
	inode, inodeErr := fdInode(fd, pidfsMagic)
	if inodeErr != nil {
		t.Fatal(inodeErr)
	}
	var fs syscall.Statfs_t
	if err = syscall.Fstatfs(fd, &fs); err != nil {
		t.Fatal(err)
	}
	t.Logf("phase=%s poll=%#x magic=%#x inode=%d info_mask=%#x info_error=%v fdinfo=%q", phase, pidfdEvents(t, fd), fs.Type, inode, info.Mask, infoErr, b)
	for _, key := range []string{"Pid:", "NSpid:"} {
		want := key + "\t" + strconv.FormatUint(uint64(info.PID), 10)
		if phase == "reaped" {
			want = key + "\t-1"
		}
		if !strings.Contains(string(b), want+"\n") {
			t.Fatalf("%s: missing %q in %q", phase, want, b)
		}
	}
}

func originalStartTicks(t *testing.T, pid int) uint64 {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	close := strings.LastIndexByte(string(b), ')')
	if close < 0 {
		t.Fatal("malformed owned-child stat")
	}
	fields := strings.Fields(string(b[close+1:]))
	if len(fields) <= 19 {
		t.Fatal("short owned-child stat")
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || ticks == 0 {
		t.Fatal("owned-child ticks", err)
	}
	return ticks
}

// Real kernel evidence with the runner's ordinary reaper paused before locks.
// No fixture consumes its wait status or changes PID limits/namespaces/UIDs.
func TestLinuxV2KernelLifecycle(t *testing.T) {
	s, open := startV2KernelRunner(t)
	if unsafe.Sizeof(pidfdInfo{}) != 64 {
		t.Fatal("pidfd_info ABI")
	}
	r := v2Call(t, s)
	id := r.Child.Identity
	liveTicks := originalStartTicks(t, s.pid)
	fds := ownedPIDFDs(t, id)
	if len(fds) != 2 {
		t.Fatalf("spawn plus stdlib duplicate pidfds: %v", fds)
	}
	namespaces := ownedNamespaceFDs(t, id)
	if len(namespaces) != 4 {
		t.Fatalf("child and self scope pins: %v", namespaces)
	}
	for _, fd := range fds {
		if !closeOnExec(fd) {
			t.Fatal("pidfd CLOEXEC")
		}
	}
	duplicate, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fds[0]), syscall.F_DUPFD_CLOEXEC, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	held := int(duplicate)
	defer syscall.Close(held)
	logPIDFDEvidence(t, "live", held)
	if pidfdEvents(t, held) != 0 {
		t.Fatal("live pidfd readable")
	}
	other, release := startV2KernelRunner(t)
	otherID := v2Call(t, other).Child.Identity
	if otherID.PIDFSInode == id.PIDFSInode || otherID.BootID != id.BootID || otherID.PIDNamespaceInode != id.PIDNamespaceInode {
		t.Fatal("independent incarnation/scope mismatch")
	}
	release()
	// Existing v1 Linux contract continues to refuse on this same endpoint.
	b, _ := encodeIdentify(identifyMatchingRequest(s))
	reply, err := rawV2(t, s, b)
	if err != nil {
		t.Fatal(err)
	}
	v1, err := decodeIdentifyResponse(reply)
	if err != nil || v1.Reason == nil || *v1.Reason != "unsupported_platform" {
		t.Fatalf("v1 changed: %+v %v", v1, err)
	}
	if err = s.child.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for pidfdEvents(t, held)&syscall.EPOLLIN == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no exit readiness")
		}
		time.Sleep(time.Millisecond)
	}
	if pidfdEvents(t, held)&syscall.EPOLLHUP != 0 {
		t.Fatal("reaped despite held reaper gate")
	}
	logPIDFDEvidence(t, "zombie", held)
	if ticks := originalStartTicks(t, s.pid); ticks != liveTicks {
		t.Fatal("observed ticks changed while owned", liveTicks, ticks)
	}
	zombie := v2Call(t, s)
	if *zombie.Child != *r.Child {
		t.Fatal("zombie changed immutable identity")
	}
	open()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("reap")
	}
	s.processMu.Lock() // terminal release occurs under this same lock after publication
	for _, fd := range append(fds, namespaces...) {
		_, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
		if e != syscall.EBADF {
			t.Errorf("custody fd not closed: %d %v", fd, e)
		}
	}
	s.processMu.Unlock()
	after, _ := s.identifyV2Snapshot(v2RequestFor(s))
	if after.Reason == nil || *after.Reason != "child_reaped" {
		t.Fatal(after)
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d/stat", s.pid)); !os.IsNotExist(err) {
		t.Fatal("owned-child proc entry survived reap", err)
	}
	v1Reply, err := rawV2(t, s, b)
	v1After, decodeErr := decodeIdentifyResponse(v1Reply)
	if err != nil || decodeErr != nil || v1After.Reason == nil || *v1After.Reason != "child_reaped" {
		t.Fatal("v1 reap precedence", v1After, err, decodeErr)
	}
	if pidfdEvents(t, held)&syscall.EPOLLHUP == 0 {
		t.Fatal("no reap HUP")
	}
	logPIDFDEvidence(t, "reaped", held)
	ino, err := fdInode(held, pidfsMagic)
	if err != nil || strconv.FormatUint(ino, 10) != id.PIDFSInode {
		t.Fatal("held pidfd identity changed after reap", err)
	}
	if _, err = getPIDFDInfo(held); !errors.Is(err, syscall.ESRCH) {
		t.Fatal("reaped pidfd info", err)
	}
	t.Logf("ticks_observed_unchanged=%d (not identity evidence)", liveTicks)
	t.Logf("qualified live/zombie/reap pid=%d identity=%+v independent_pid=%d", s.pid, id, other.pid)
}

func TestLinuxV2ControllerAndClosedTransport(t *testing.T) {
	s, open := startV2KernelRunner(t)
	defer open()
	owner := testClient(t, s.cfg)
	claim := call(t, owner, Request{Op: "claim", Execution: s.cfg.Execution, Epoch: 1}, "ok")
	write := Request{Op: "write", Execution: s.cfg.Execution, Epoch: claim.Epoch, Sequence: 1, Data: []byte("owned inert payload\n")}
	first := call(t, owner, write, "written")
	before := v2Call(t, s)
	after := call(t, owner, write, "written")
	if first.Writes != 1 || after.Writes != 1 || before.Child.PID != s.pid {
		t.Fatal("receipt changed")
	}
	req := v2RequestFor(s)
	b, _ := encodeIdentifyV2(req)
	control := []byte(`{"op":"stop"}` + "\n")
	reply, err := rawV2(t, s, append(bytes.Clone(b), control...))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeIdentifyV2Response(reply); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.done:
		t.Fatal("pipelined control dispatched")
	default:
	}
	for _, bad := range [][]byte{bytes.Replace(b, []byte("{"), []byte(`{"op":"stop",`), 1), bytes.Replace(b, []byte("expected_instance"), []byte("instance"), 1), bytes.Replace(b, []byte("request-v2"), []byte("request-v3"), 1)} {
		reply, _ = rawV2(t, s, bad)
		if len(reply) != 0 {
			t.Fatal("malformed offer got reply")
		}
	}
	req.ExpectedInstance = "ffffffffffffffffffffffffffffffff"
	bad, _ := encodeIdentifyV2(req)
	reply, err = rawV2(t, s, bad)
	if err != nil {
		t.Fatal(err)
	}
	refusal, err := decodeIdentifyV2Response(reply)
	if err != nil || refusal.Reason == nil || *refusal.Reason != "instance_mismatch" || refusal.Child != nil {
		t.Fatal(refusal, err)
	}
	// No second attempt, missing deadline and canceled context refuse locally.
	for _, ctx := range []context.Context{context.Background(), func() context.Context { c, cancel := context.WithCancel(context.Background()); cancel(); return c }()} {
		if _, err := identifyV2ForQualification(ctx, s.cfg.Socket, os.Getpid(), uint32(os.Getuid()), v2RequestFor(s)); !errors.Is(err, errIdentifyUnavailable) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := identifyV2ForQualification(ctx, s.cfg.Socket, os.Getpid()+1, uint32(os.Getuid()), v2RequestFor(s)); !errors.Is(err, errIdentifyConflict) {
		t.Fatal("wrong connected peer", err)
	}
}

// A missing or non-pidfs facility is permanently refused. Partial resources
// are retained until the original cleanup release and are closed once only.
func TestLinuxV2MissingEvidence(t *testing.T) {
	missing := captureLinuxOriginal(os.Getpid(), -1)
	defer missing.release()
	if !errors.Is(missing.err, errChildBirthUnsupported) {
		t.Fatal(missing.err)
	}
	fd, err := syscall.Open(os.DevNull, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	bad := captureLinuxOriginal(os.Getpid(), fd)
	if bad.err == nil || validLinuxIdentity(bad.identity) {
		t.Fatal("non-pidfs accepted")
	}
	if _, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0); e != 0 {
		t.Fatal("partial handle released early")
	}
	bad.release()
	replacement, err := syscall.Open(os.DevNull, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(replacement)
	bad.release() // Must not close a reused descriptor number.
	if _, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(replacement), syscall.F_GETFD, 0); e != 0 {
		t.Fatal("double-close reused descriptor")
	}
	if procPIDFDMatches("Pid:\t123\nNSpid:\t123\t2\n", 123) || procPIDFDMatches("Pid:\t124\nNSpid:\t124\n", 123) {
		t.Fatal("wrong proc namespace accepted")
	}
}

// A stalled reply cannot hold processMu, stop or reap. The copied identity may
// arrive after reap; fresh live proof remains mandatory for any real admission.
func TestLinuxV2DelayedReplyDoesNotPinReap(t *testing.T) {
	s, open := startV2KernelRunner(t)
	entered, replyGate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(replyGate) }) }
	defer release()
	s.mu.Lock()
	s.identifyBeforeReply = func() { close(entered); <-replyGate }
	s.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := identifyV2ForQualification(ctx, s.cfg.Socket, os.Getpid(), uint32(os.Getuid()), v2RequestFor(s))
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reply gate")
	}
	s.child.Kill()
	open()
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("reply blocked reap")
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Selected capture must not silently discard namespace/credential intent.
func TestLinuxV2SpawnRejectsScopeChanges(t *testing.T) {
	for _, sys := range []*syscall.SysProcAttr{
		{Cloneflags: syscall.CLONE_NEWPID}, {Unshareflags: syscall.CLONE_NEWNS},
		{Credential: &syscall.Credential{Uid: uint32(os.Getuid())}},
		{UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}}},
		{AmbientCaps: []uintptr{1}},
	} {
		p, _, err := startOriginalProcess(true, "/bin/true", []string{"true"}, &os.ProcAttr{Sys: sys})
		if p != nil || !errors.Is(err, errChildBirthUnsupported) {
			t.Fatal("unsupported profile reached spawn", err)
		}
	}
}

func TestLinuxV2CapabilityAndNamespaceCustody(t *testing.T) {
	if err := preflightLinuxCapture(); err != nil {
		if os.Getenv("SESSION_HOST_REQUIRE_LINUX_PIDFS") == "1" {
			t.Fatal("REQUIRED_LINUX_PIDFS_NOT_QUALIFIED", err)
		}
		t.Skip("LINUX_PIDFS_NOT_QUALIFIED", err)
	}
	fd, err := openSelfPIDFD()
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if !setCloseOnExec(fd) {
		t.Fatal("pidfd CLOEXEC")
	}
	for _, scope := range []struct {
		name string
		cmd  uintptr
	}{{"pid", pidfdGetPIDNamespace}, {"user", pidfdGetUserNamespace}} {
		ns, _, e := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), scope.cmd, 0)
		if e != 0 {
			t.Fatal(e)
		}
		childNS := int(ns)
		if !setCloseOnExec(childNS) {
			syscall.Close(childNS)
			t.Fatal("namespace CLOEXEC")
		}
		ino, err := fdInode(childNS, nsfsMagic)
		var own syscall.Stat_t
		statErr := syscall.Stat("/proc/self/ns/"+scope.name, &own)
		t.Logf("scope=%s nsfs_inode=%d proc_inode=%d cloexec=%v", scope.name, ino, own.Ino, closeOnExec(childNS))
		if err != nil || statErr != nil || ino != own.Ino {
			syscall.Close(childNS)
			t.Fatal(err, statErr, ino, own.Ino)
		}
		owner := &linuxFDSet{fds: []int{childNS}}
		owner.close()
		if owner.fds[0] != -1 {
			t.Fatal("namespace owner did not reset")
		}
		replacement, err := syscall.Open(os.DevNull, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		owner.close()
		if !closeOnExec(replacement) {
			t.Fatal("reused fd was closed")
		}
		syscall.Close(replacement)
	}
	// Set-and-verify also covers a descriptor initially missing CLOEXEC.
	ordinary, err := syscall.Open(os.DevNull, syscall.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(ordinary)
	if closeOnExec(ordinary) || !setCloseOnExec(ordinary) {
		t.Fatal("CLOEXEC set/verify")
	}
}

func TestLinuxV2EffectiveUIDExpectation(t *testing.T) {
	info := pidfdInfo{PID: 123, TGID: 123, PPID: uint32(os.Getpid()), EUID: 4242, RUID: 3131}
	if !sameChildCredentials(info, 123, 4242) || sameChildCredentials(info, 123, 3131) {
		t.Fatal("euid expectation used another credential")
	}
	info.PID = 124
	if sameChildCredentials(info, 123, 4242) {
		t.Fatal("wrong pid accepted")
	}
}
