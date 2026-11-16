package sessionhost

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

// debugProbeReport is written by the inert probe child about itself: its own
// argv, environment and every inherited descriptor with type, device/inode and
// the readable head of a regular file. Only synthetic fixture data is involved.
type debugProbeFD struct {
	FD   int    `json:"fd"`
	Type string `json:"type"`
	Dev  uint64 `json:"dev"`
	Ino  uint64 `json:"ino"`
	Head string `json:"head,omitempty"`
}
type debugProbeReport struct {
	Argv  []string       `json:"argv"`
	Env   []string       `json:"env"`
	FDs   []debugProbeFD `json:"fds"`
	Grant string         `json:"grant_sha256,omitempty"`
}

const debugProbeReportName = "debug-probe-report.json"

func debugProbeType(mode uint32) string {
	switch mode & syscall.S_IFMT {
	case syscall.S_IFREG:
		return "regular"
	case syscall.S_IFIFO:
		return "fifo"
	case syscall.S_IFSOCK:
		return "socket"
	case syscall.S_IFCHR:
		return "char"
	case syscall.S_IFDIR:
		return "dir"
	}
	return "other"
}

// TestDebugProbeProcess is the inert child. As "subordinate" it inventories
// itself; as "manager" it additionally reads one grant line from FD 3 and
// reports only its digest; as "detector" it is handed harmless sentinels.
func TestDebugProbeProcess(t *testing.T) {
	mode := os.Getenv("SESSION_HOST_DEBUG_PROBE")
	if mode == "" {
		return
	}
	report := debugProbeReport{Argv: os.Args, Env: os.Environ()}
	if mode == "manager" {
		line, err := bufio.NewReader(os.NewFile(3, "grant-delivery")).ReadString('\n')
		if err != nil {
			os.Exit(9)
		}
		sum := sha256.Sum256([]byte(strings.TrimSuffix(line, "\n")))
		report.Grant = hex.EncodeToString(sum[:])
	}
	dir, err := os.Open("/dev/fd")
	if err != nil {
		os.Exit(10)
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		os.Exit(11)
	}
	for _, name := range names {
		fd, err := strconv.Atoi(name)
		if err != nil || fd == int(dir.Fd()) {
			continue
		}
		var st syscall.Stat_t
		if syscall.Fstat(fd, &st) != nil {
			continue
		}
		entry := debugProbeFD{FD: fd, Type: debugProbeType(uint32(st.Mode)), Dev: uint64(st.Dev), Ino: uint64(st.Ino)}
		if entry.Type == "regular" {
			buffer := make([]byte, 64)
			if n, err := syscall.Pread(fd, buffer, 0); err == nil {
				entry.Head = string(buffer[:n])
			}
		}
		report.FDs = append(report.FDs, entry)
	}
	dir.Close()
	data, err := json.Marshal(report)
	if err != nil {
		os.Exit(12)
	}
	if os.WriteFile(debugProbeReportName+".tmp", data, 0600) != nil || os.Rename(debugProbeReportName+".tmp", debugProbeReportName) != nil {
		os.Exit(13)
	}
	os.Exit(0)
}

func debugProbeDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "debug-probe-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
func debugProbeRead(t *testing.T, dir string) debugProbeReport {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(filepath.Join(dir, debugProbeReportName))
		if err == nil {
			var report debugProbeReport
			if json.Unmarshal(data, &report) != nil {
				t.Fatal("probe report unreadable")
			}
			return report
		}
		if time.Now().After(deadline) {
			t.Fatal("probe child wrote no report")
		}
		time.Sleep(time.Millisecond)
	}
}

// debugSubordinate spawns the probe through the actual runner, exactly as the
// native child would be started: explicit plan, explicit environment, stdio only.
func debugSubordinate(t *testing.T) debugProbeReport {
	t.Helper()
	dir := debugProbeDir(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	rc := runner.Config{Socket: filepath.Join(dir, "r.sock"), Execution: "probe-execution", IOTimeout: 200 * time.Millisecond,
		Plan: runner.Plan{Binary: binary, Args: []string{"-test.run=^TestDebugProbeProcess$"}, Cwd: dir, Env: []string{"SESSION_HOST_DEBUG_PROBE=subordinate", "GORACE=atexit_sleep_ms=0"}}}
	s, err := runner.Start(rc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return debugProbeRead(t, dir)
}

// debugControlChild starts the detector or manager fixture directly with
// explicitly inherited descriptors; the parent closes its copies after start.
func debugControlChild(t *testing.T, mode string, files ...*os.File) debugProbeReport {
	t.Helper()
	return debugProbeChild(t, mode, nil, files...)
}

// debugProbeChild is debugControlChild with an optional deliberate plant: a
// non-nil stdin becomes the child's descriptor 0, so a leak check can be shown
// to look at the standard descriptor numbers too. The parent closes its copy.
func debugProbeChild(t *testing.T, mode string, stdin *os.File, files ...*os.File) debugProbeReport {
	t.Helper()
	dir := debugProbeDir(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestDebugProbeProcess$", "-test.count=1")
	cmd.Dir = dir
	cmd.Env = []string{"SESSION_HOST_DEBUG_PROBE=" + mode, "GORACE=atexit_sleep_ms=0"}
	cmd.ExtraFiles = files
	if stdin != nil {
		// Assigned only when present: a nil *os.File must not become a non-nil reader.
		cmd.Stdin = stdin
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		f.Close()
	}
	if stdin != nil {
		stdin.Close()
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("%s child: %v", mode, err)
	}
	return debugProbeRead(t, dir)
}

type debugInode struct{ dev, ino uint64 }

func debugInodeOf(t *testing.T, f *os.File) debugInode {
	t.Helper()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("no inode evidence")
	}
	return debugInode{uint64(st.Dev), uint64(st.Ino)}
}

// debugDescriptorDir names the directory that lists this process's descriptors.
func debugDescriptorDir() string {
	if runtime.GOOS == "linux" {
		return "/proc/self/fd"
	}
	return "/dev/fd"
}

// debugOwnFD is one live descriptor of this test process.
type debugOwnFD struct {
	FD    int
	Type  string
	Inode debugInode
}

// debugOwnDescriptors is the parent-side census: a names-only listing of the
// descriptor directory, then one Fstat per listed number. No path lookup is
// made, so a descriptor that cannot be stat-ed by name does not abort it.
// The listing's own descriptor is left out by its number, and any further
// descriptor the listing holds on the same directory by that directory's
// identity; no descriptor number is exempt, 0, 1 and 2 are reported like any
// other. A number closed between listing and Fstat (EBADF) is not a live
// descriptor and is dropped; any other Fstat error fails the test.
func debugOwnDescriptors(t *testing.T) []debugOwnFD {
	t.Helper()
	dir, err := os.Open(debugDescriptorDir())
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	listing := int(dir.Fd())
	var self syscall.Stat_t
	if err := syscall.Fstat(listing, &self); err != nil {
		t.Fatal(err)
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		t.Fatal(err)
	}
	var own []debugOwnFD
	for _, name := range names {
		fd, err := strconv.Atoi(name)
		if err != nil || fd == listing {
			continue
		}
		var st syscall.Stat_t
		if err := syscall.Fstat(fd, &st); err != nil {
			if errors.Is(err, syscall.EBADF) {
				continue
			}
			t.Fatalf("descriptor census: fstat %d: %v", fd, err)
		}
		typ := debugProbeType(uint32(st.Mode))
		if typ == "dir" && st.Dev == self.Dev && st.Ino == self.Ino {
			continue
		}
		own = append(own, debugOwnFD{FD: fd, Type: typ, Inode: debugInode{uint64(st.Dev), uint64(st.Ino)}})
	}
	return own
}

// debugDupCloseOnExec duplicates a descriptor and marks the copy close-on-exec
// under the fork lock, so no concurrently started child can inherit it.
func debugDupCloseOnExec(fd int) (int, error) {
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	copied, err := syscall.Dup(fd)
	if err == nil {
		syscall.CloseOnExec(copied)
	}
	return copied, err
}

// debugHeldEndpoint keeps a private endpoint open in this process for the rest
// of the test. A device/inode pair identifies an object only while that object
// is open: a pipe's pair may be given to a later pipe once every end is closed.
type debugHeldEndpoint struct {
	fd    int
	inode debugInode
}

// debugHoldEndpoint retains a close-on-exec duplicate of f until test cleanup
// and records the identity read from that duplicate.
func debugHoldEndpoint(t *testing.T, f *os.File) debugHeldEndpoint {
	t.Helper()
	raw, err := f.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	held, dupErr := -1, error(nil)
	if err := raw.Control(func(fd uintptr) { held, dupErr = debugDupCloseOnExec(int(fd)) }); err != nil {
		t.Fatal(err)
	}
	if dupErr != nil {
		t.Fatal(dupErr)
	}
	t.Cleanup(func() { syscall.Close(held) })
	var st syscall.Stat_t
	if err := syscall.Fstat(held, &st); err != nil {
		t.Fatal(err)
	}
	return debugHeldEndpoint{held, debugInode{uint64(st.Dev), uint64(st.Ino)}}
}

// live requires the retained endpoint to be, right now, an open FIFO with the
// recorded identity; a comparison against it is then a comparison with a live
// object and not with a retired number.
func (e debugHeldEndpoint) live(t *testing.T) {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Fstat(e.fd, &st); err != nil {
		t.Fatalf("retained private endpoint is no longer open: %v", err)
	}
	if debugProbeType(uint32(st.Mode)) != "fifo" || (debugInode{uint64(st.Dev), uint64(st.Ino)}) != e.inode {
		t.Fatal("retained private endpoint changed type or identity")
	}
}

// duplicate returns another descriptor on the retained endpoint, for a
// deliberate plant handed to a detector child.
func (e debugHeldEndpoint) duplicate(t *testing.T) *os.File {
	t.Helper()
	fd, err := debugDupCloseOnExec(e.fd)
	if err != nil {
		t.Fatal(err)
	}
	return os.NewFile(uintptr(fd), "planted-private-endpoint")
}

// debugPrivateDescriptor reports the first descriptor of the probe report that
// shares an identity with a private object. Every reported number is compared,
// the standard descriptors included.
func debugPrivateDescriptor(report debugProbeReport, private []debugInode) (int, bool) {
	for _, fd := range report.FDs {
		for _, p := range private {
			if fd.Dev == p.dev && fd.Ino == p.ino {
				return fd.FD, true
			}
		}
	}
	return 0, false
}
func debugProbeTypes(r debugProbeReport) []string {
	var types []string
	for _, fd := range r.FDs {
		if fd.FD > 2 {
			types = append(types, fd.Type)
		}
	}
	sort.Strings(types)
	return types
}

// debugAssertSubordinateClean requires the actual inert child to hold neither
// grant bytes nor any descriptor sharing an inode with a host-private object,
// and to see exactly the descriptor shape of the no-custody baseline.
func debugAssertSubordinateClean(t *testing.T, report, baseline debugProbeReport, secrets []string, private []debugInode) {
	t.Helper()
	for _, s := range append(report.Argv, report.Env...) {
		for _, secret := range secrets {
			if secret != "" && strings.Contains(s, secret) {
				t.Fatal("grant or body bytes reached subordinate argv/environment")
			}
		}
	}
	if fd, found := debugPrivateDescriptor(report, private); found {
		t.Fatalf("subordinate inherited a private descriptor at fd %d", fd)
	}
	for _, fd := range report.FDs {
		for _, secret := range secrets {
			if secret != "" && strings.Contains(fd.Head, secret) {
				t.Fatal("subordinate can read grant bytes")
			}
		}
	}
	if got, want := debugProbeTypes(report), debugProbeTypes(baseline); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("subordinate descriptors %v differ from baseline %v", got, want)
	}
}

// GrantAndDescriptorCustodyAtChild: the intended manager fixture receives its
// grant over the explicitly routed private descriptor and attaches with it,
// while the subordinate spawned by the real runner with that custody open sees
// no grant bytes and no journal or delivery descriptor at any fd number; the
// detector-control child proves the same probe does see a deliberately passed
// sentinel file and pipe, so an empty probe cannot pass silently.
func TestGrantAndDescriptorCustodyAtChild(t *testing.T) {
	baseline := debugSubordinate(t)
	h, _ := debugEffectFixture(t)
	journal := debugInodeOf(t, h.debug.journal.file)
	deliveryRead, deliveryWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer deliveryRead.Close()
	defer deliveryWrite.Close()
	delivery := debugInodeOf(t, deliveryWrite)
	token := debugToken(t, h, "view", "writer_control")
	h.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"private-tail-canary"}}`))
	subordinate := debugSubordinate(t)
	debugAssertSubordinateClean(t, subordinate, baseline, []string{token, "private-tail-canary"}, []debugInode{journal, delivery})
	// Detector control: the identical probe must report the sentinel descriptors.
	sentinel, err := os.CreateTemp(debugProbeDir(t), "sentinel-")
	if err != nil {
		t.Fatal(err)
	}
	defer sentinel.Close()
	if _, err := sentinel.WriteString("sentinel-fd-canary"); err != nil {
		t.Fatal(err)
	}
	pipeRead, pipeWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pipeWrite.Close()
	sentinelInode, pipeInode := debugInodeOf(t, sentinel), debugInodeOf(t, pipeRead)
	sentinelCopy, err := os.Open(sentinel.Name())
	if err != nil {
		t.Fatal(err)
	}
	detector := debugControlChild(t, "detector", sentinelCopy, pipeRead)
	seenFile, seenPipe := false, false
	for _, fd := range detector.FDs {
		if fd.Dev == sentinelInode.dev && fd.Ino == sentinelInode.ino && strings.Contains(fd.Head, "sentinel-fd-canary") && fd.FD == 3 {
			seenFile = true
		}
		if fd.Dev == pipeInode.dev && fd.Ino == pipeInode.ino && fd.Type == "fifo" && fd.FD == 4 {
			seenPipe = true
		}
	}
	if !seenFile || !seenPipe {
		t.Fatalf("detector control did not see its sentinels: %+v", detector.FDs)
	}
	for _, p := range []debugInode{journal, delivery} {
		for _, fd := range detector.FDs {
			if fd.Dev == p.dev && fd.Ino == p.ino {
				t.Fatal("detector control received a private descriptor")
			}
		}
	}
	// Intended delivery: the host writes into the private pipe and closes its
	// end; the manager child then reads the grant from its routed FD 3.
	debugWant(t, h.debugDeliverGrant(token, deliveryWrite), "")
	manager := debugControlChild(t, "manager", deliveryRead)
	sum := sha256.Sum256([]byte(token))
	if manager.Grant != hex.EncodeToString(sum[:]) {
		t.Fatal("intended manager did not receive the delivered grant")
	}
	for _, fd := range manager.FDs {
		if fd.Dev == journal.dev && fd.Ino == journal.ino {
			t.Fatal("manager delivery also leaked the journal descriptor")
		}
	}
	for _, s := range append(manager.Argv, manager.Env...) {
		if strings.Contains(s, token) {
			t.Fatal("grant travelled through manager argv/environment")
		}
	}
	debugWant(t, h.debugAttachView(token), "")
	read, err := h.debugRead(token, 0)
	debugWant(t, err, "")
	if !bytes.Contains(read.Event, []byte("private-tail-canary")) {
		t.Fatal("delivered grant could not attach and read")
	}
	debugWant(t, h.debugDeliverGrant("absent-token", deliveryWrite), "attach_grant_revoked")
}

// GrantDescriptorReplacementAndCleanup: after delivery and attach, journal
// reopen with host replacement, a failed private setup, a rejected second
// attach and detach cleanup, the real subordinate probe stays clean, stale
// tokens are V05, fresh intended delivery still attaches, and no grant bytes
// appear in ordinary snapshot, workspace or temporary artifacts.
// Claim: a subordinate started while this process still holds every private
// delivery endpoint open inherits none of them, the journal or the replaced
// object, at any descriptor number; the same endpoint planted as a detector
// child's descriptor 0 is found by the same check. Limit: the retained
// endpoints are close-on-exec duplicates kept by the test, so this proves
// non-inheritance of a live private object, not that the host closes its end.
func TestGrantDescriptorReplacementAndCleanup(t *testing.T) {
	baseline := debugSubordinate(t)
	h, _ := debugEffectFixture(t)
	// Every delivery pipe stays open here until cleanup: once all its ends are
	// closed its identity may be reused by an unrelated new pipe, and a saved
	// pair would then match a descriptor that was never inherited.
	var held []debugHeldEndpoint
	clean := func(secrets []string, private []debugInode) {
		t.Helper()
		for _, endpoint := range held {
			endpoint.live(t)
		}
		report := debugSubordinate(t)
		for _, endpoint := range held {
			endpoint.live(t)
		}
		debugAssertSubordinateClean(t, report, baseline, secrets, private)
	}
	deliver := func(h *Host) (string, debugInode) {
		t.Helper()
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		endpoint := debugHoldEndpoint(t, w)
		held = append(held, endpoint)
		inode := endpoint.inode
		token := debugToken(t, h, "view", "writer_control")
		debugWant(t, h.debugDeliverGrant(token, w), "")
		report := debugControlChild(t, "manager", r)
		sum := sha256.Sum256([]byte(token))
		if report.Grant != hex.EncodeToString(sum[:]) {
			t.Fatal("fresh intended delivery failed")
		}
		debugWant(t, h.debugAttachView(token), "")
		return token, inode
	}
	first, firstPipe := deliver(h)
	private := []debugInode{debugInodeOf(t, h.debug.journal.file), firstPipe}
	secrets := []string{first}
	clean(secrets, private)
	// Negative beside the clean census above: the same live endpoint, handed
	// to a detector child as its descriptor 0, must be reported there and must
	// be found by the check the subordinate just passed.
	plant := debugProbeChild(t, "detector", held[0].duplicate(t))
	atStdin := false
	for _, fd := range plant.FDs {
		if fd.FD == 0 && fd.Type == "fifo" && fd.Dev == firstPipe.dev && fd.Ino == firstPipe.ino {
			atStdin = true
		}
	}
	if _, found := debugPrivateDescriptor(plant, private); !atStdin || !found {
		t.Fatalf("planted private endpoint at descriptor 0 was not found: %+v", plant.FDs)
	}
	// Journal reopen with host replacement invalidates the old grant.
	h.debug.close()
	h.state.Epoch++
	debugWant(t, h.openDebug(true), "")
	_, err := h.debugRead(first, 0)
	debugWant(t, err, "attach_grant_revoked")
	second, secondPipe := deliver(h)
	private = append(private, debugInodeOf(t, h.debug.journal.file), secondPipe)
	secrets = append(secrets, second)
	clean(secrets, private)
	// Partial setup failure: a replaced journal object refuses and leaves no
	// descriptor to its target behind.
	broken, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(broken, "debug-attach"), 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(broken, "target.journal")
	if err := os.WriteFile(target, []byte("replaced-object-canary"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(broken, "debug-attach", "input.journal")); err != nil {
		t.Fatal(err)
	}
	if _, err := openDebugJournal(broken, true); err == nil || err.Error() != "input_storage_unavailable" {
		t.Fatalf("replaced object opened: %v", err)
	}
	targetFile, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	private = append(private, debugInodeOf(t, targetFile))
	targetFile.Close()
	clean(append(secrets, "replaced-object-canary"), private)
	// Rejected attach and cleanup.
	rejected := debugToken(t, h, "view")
	debugWant(t, h.debugAttachView(rejected), "attach_in_use")
	debugWant(t, h.debugControl(second, "detach", h.state.Debug.WriterRevision), "")
	debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", h.state.Epoch}, rejected), "")
	_, err = h.debugRead(second, 0)
	debugWant(t, err, "attach_grant_revoked")
	clean(append(secrets, rejected), private)
	entries, err := os.ReadDir(h.store.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(h.store.dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range append(secrets, rejected) {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatalf("grant bytes in ordinary artifact %s", entry.Name())
			}
		}
	}
	third, _ := deliver(h)
	if h.debug.viewer != third {
		t.Fatal("fresh delivery after cleanup did not attach")
	}
}

// ExportAndSinkRedaction / RecoverableRefusalIsNotFd4Event (host portion):
// every reachable ordinary sink (snapshot file, status response, summary,
// rendered private results, notifier intents) is free of grant, body, option
// and path canaries, while the private journal holds the body and the view
// alone shows the tail; recoverable private refusals render only their fixed
// closed envelope, a later plain detach reports no refusal, and no raw
// transcript export exists.
func TestExportAndSinkRedactionHostPortion(t *testing.T) {
	h, e := debugEffectFixture(t)
	// Answers require the host's current native thread to be the request's.
	h.state.Protocol = &ProtocolState{View: ap.View{NativeSession: "thread"}}
	var notifications [][]byte
	h.debugNotify = func(n debugNotification) {
		b, _ := json.Marshal(n)
		notifications = append(notifications, b)
	}
	h.debug.tail.observe([]byte(`{"id":5,"method":"item/tool/requestUserInput","params":{"threadId":"thread","turnId":"turn-1","itemId":"item","questions":[{"id":"q","header":"Delete?","question":"question-body-canary","options":[{"label":"option-label-canary","description":"d"}]}]}}`))
	offered := []string{"option-label-canary", "other-option-canary"}
	interaction, err := h.debugObserveInteraction("thread", "turn-1", 5, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	next := cloneState(h.state)
	debugWant(t, h.debugTerminalLocked(&next, "notice-1", "thread", "turn-1", "completed"), "")
	debugWant(t, h.persistLocked(next), "")
	h.debugAnnounceLocked(0)
	token, revision := debugWriter(t, h, "view", "writer_control", "text", "native_response")
	_, err = h.debugInput(token, debugInputIntent{Kind: "answer_interaction", ID: "answer", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: interaction, RequestID: 5, OptionsDigest: debugOptionsDigest(offered), Option: "option-label-canary"})
	debugWant(t, err, "")
	debugWant(t, h.debugInteractionResolved(interaction), "")
	_, err = h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "reply", Text: "reply-body-canary", ExpectedRevision: revision, ExpectedTurn: "turn-1"})
	debugWant(t, err, "")
	h.state.InputState = InputBusy
	debugWant(t, debugText(h, token, "busy", "busy-body-canary", revision, "turn-1"), "input_busy")
	h.state.InputState = InputIdle
	rendered, err := debugRefusal("input_busy")
	debugWant(t, err, "")
	closing, err := debugRefusal("input_storage_unavailable")
	debugWant(t, err, "")
	summary, err := h.debugSummaryJSON()
	debugWant(t, err, "")
	status, err := json.Marshal(Response{Code: "ok", State: h.Snapshot()})
	debugWant(t, err, "")
	journalPath := filepath.Join(h.debug.journal.dir, "input.journal")
	journal, err := os.ReadFile(journalPath)
	debugWant(t, err, "")
	// Journal bodies are stored as encoded frame members (base64 of the typed
	// intent), so the private custody check decodes each retained frame rather
	// than scanning raw file bytes; the grant must be absent raw and decoded.
	var replyBody, answerBody []byte
	for _, e := range debugEntries(t, h) {
		switch e.Kind {
		case "follow_up":
			replyBody = e.Body
		case "answer_interaction":
			answerBody = e.Body
		}
		if bytes.Contains(e.Body, []byte(token)) {
			t.Fatal("private journal frame stored the grant")
		}
	}
	if !bytes.Contains(replyBody, []byte("reply-body-canary")) {
		t.Fatalf("private journal lost the follow-up body: %s", replyBody)
	}
	if !bytes.Contains(answerBody, []byte("option-label-canary")) {
		t.Fatalf("private journal lost the answer option: %s", answerBody)
	}
	if bytes.Contains(journal, []byte(token)) {
		t.Fatal("private journal file stored the grant")
	}
	view, err := h.debugRead(token, 0)
	debugWant(t, err, "")
	if !bytes.Contains(view.Event, []byte("question-body-canary")) {
		t.Fatal("view does not show the admitted question")
	}
	canaries := []string{token, "reply-body-canary", "option-label-canary", "other-option-canary", "question-body-canary", "busy-body-canary", journalPath, h.store.dir}
	sinks := map[string][]byte{"snapshot": debugDisk(t, h), "status": status, "summary": summary, "refusal": rendered, "closing": closing}
	for i, n := range notifications {
		sinks["notification-"+strconv.Itoa(i)] = n
	}
	for name, sink := range sinks {
		for _, canary := range canaries {
			if bytes.Contains(sink, []byte(canary)) {
				t.Fatalf("%s exposes %q: %s", name, canary, sink)
			}
		}
	}
	if len(notifications) != 1 || !bytes.Contains(notifications[0], []byte(`"kind":"terminal_observed"`)) {
		t.Fatalf("notifier intent: %v", notifications)
	}
	if string(rendered) != `{"contract":"host-debug-attach/1","ok":false,"code":"input_busy","details":{}}`+"\n" || string(closing) != `{"contract":"host-debug-attach/1","ok":false,"code":"input_storage_unavailable","details":{}}`+"\n" {
		t.Fatalf("private envelope changed: %s %s", rendered, closing)
	}
	var parsed debugSummary
	if err := json.Unmarshal(summary, &parsed); err != nil || parsed.Results.Pending != 1 || parsed.Inputs["native_accepted"] != 1 || parsed.Inputs["write_known"] != 1 || parsed.Interactions["resolved"] != 1 || parsed.Rejections["input_busy"] != 1 || parsed.OldestDebtSeconds < 0 {
		t.Fatalf("summary counters: %s", summary)
	}
	if e.count() != 2 {
		t.Fatal("effects differ from accepted inputs")
	}
	debugWant(t, h.debugControl(token, "detach", h.state.Debug.WriterRevision), "")
	for _, entry := range mustReadDir(t, h.store.dir) {
		if entry.Name() != "debug-attach" && bytes.Contains(mustReadFile(t, filepath.Join(h.store.dir, entry.Name())), []byte("reply-body-canary")) {
			t.Fatalf("body in ordinary artifact %s", entry.Name())
		}
	}
}

func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}
func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
