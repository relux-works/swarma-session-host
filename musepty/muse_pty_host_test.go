package musepty

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"golang.org/x/term"
)

// A separate Go process speaks a PTY, including CPR, paste rendering and log
// appends. Tests use the production planner/start/delivery/terminal paths.
func init() {
	base := filepath.Base(os.Args[0])
	if (base != "muse" && !strings.HasPrefix(base, "muse-bin-")) || len(os.Args) < 2 {
		return
	}
	if os.Args[1] == "--version" {
		if b, e := os.ReadFile(filepath.Join(os.Getenv("HOME"), "peer-version")); e == nil {
			fmt.Print(string(b))
			os.Exit(0)
		}
		// L3P frozen copies attest their own filename build, so
		// every fixture build probes clean without HOME state;
		// the PATH `muse` keeps its historical fixed answer.
		if build, ok := strings.CutPrefix(base, "muse-bin-"); ok {
			if release, _, found := strings.Cut(build, "-R"); found && release != "" {
				fmt.Printf("Muse Code %s (%s)\n", release, build)
				os.Exit(0)
			}
		}
		fmt.Println("Muse Code 1.4.1 (1.4.1-R4503.1)")
		os.Exit(0)
	}
	if os.Args[1] == "--help" {
		// agents-management v0.5.58 binds yolo to bounded help evidence:
		// the selected binary must declare --yolo as its own option.
		// A peer-help file overrides the text for stale-evidence tests.
		if b, e := os.ReadFile(filepath.Join(os.Getenv("HOME"), "peer-help")); e == nil {
			fmt.Print(string(b))
			os.Exit(0)
		}
		fmt.Print("Usage: muse [options]\n\nOptions:\n  --yolo  Skip every permission prompt\n")
		os.Exit(0)
	}
	home := os.Getenv("HOME")
	mode, _ := os.ReadFile(filepath.Join(home, "peer-mode"))
	if len(mode) == 0 {
		return
	} // not the scripted peer
	// L3P newest-failure leg: the binary is healthy (it answered
	// every probe) but the session never starts.
	if string(mode) == "exit-early" {
		os.Exit(3)
	}
	_, _ = term.MakeRaw(int(os.Stdin.Fd()))
	fmt.Print("\x1b[5n")
	reader := bufio.NewReader(os.Stdin)
	status := ""
	for !strings.HasSuffix(status, "n") {
		b, e := reader.ReadByte()
		if e != nil {
			os.Exit(2)
		}
		status += string(b)
	}
	if status != "\x1b[0n" {
		os.Exit(3)
	}
	_ = os.WriteFile(filepath.Join(os.Getenv("HOME"), "peer-dsr"), []byte(status), 0600)
	fmt.Print("\x1b[6n")
	response := ""
	for !strings.HasSuffix(response, "R") {
		b, e := reader.ReadByte()
		if e != nil {
			os.Exit(2)
		}
		response += string(b)
	}
	if !strings.HasPrefix(response, "\x1b[") {
		os.Exit(3)
	}
	_ = os.WriteFile(filepath.Join(home, "peer-cpr"), []byte(response), 0600)
	if string(mode) == "capture147" {
		muse147StartupPeer(home, reader)
		os.Exit(0)
	}
	id := museTestUUID("peer-session")
	resuming := false
	for i, arg := range os.Args {
		if arg == "resume" && i+1 < len(os.Args) {
			id = os.Args[i+1]
			resuming = true
		}
	}
	dir := filepath.Join(os.Getenv("XDG_DATA_HOME"), "muse", "sessions", "2026", "10", "01", id)
	_, plantStat := os.Stat(filepath.Join(home, "peer-plant-foreign"))
	armed := plantStat == nil
	if !armed {
		_ = os.MkdirAll(dir, 0700)
	}
	if armed {
		// Tier-1 fix-forward r3 for nested-foreign-primary-only: the
		// test moved the whole session tree aside (outside the
		// sessions root) before resume, so <session> is absent while
		// discovery polls. Complete the staged tree with a nested
		// foreign log naming this peer's own process, then publish
		// the whole tree with one atomic rename: every listing that
		// contains <session> also contains foreign/ with the
		// complete poison, so every admitting traversal under the
		// mutant reads the poison first and fails fast with the
		// typed prefix refusal. The triple mirrors the unknown-third
		// mode record for record, so its verdict is the pinned
		// unknown-record one. Depth 6 keeps the test's static
		// depth-7 fixture intact. Any failure here exits loudly: a
		// silent partial plant would pass clean and survive weakened.
		staged := filepath.Join(home, musePeerStagedSession)
		poisonDir := filepath.Join(staged, "foreign")
		if err := os.MkdirAll(poisonDir, 0700); err != nil {
			os.Exit(6)
		}
		poisonSeq := 0
		poisonLine := func(kind string, payload any) string {
			poisonSeq++
			record := museFixtureRecord(kind)
			record["id"] = museTestUUID(fmt.Sprintf("peer-poison-%d-%d", os.Getpid(), poisonSeq))
			record["sequence"] = poisonSeq
			record["stream"].(map[string]any)["id"] = id
			record["payload"] = payload
			b, _ := json.Marshal(record)
			return string(b)
		}
		poisonOpened := museFixturePayload("session.opened.observed")
		poisonOpened["record"].(map[string]any)["session_id"] = id
		poisonRoute := museFixturePayload("runtime.session.route_facts")
		poisonRoute["record"].(map[string]any)["pid"] = os.Getpid()
		first := poisonLine("session.opened.observed", poisonOpened)
		second := poisonLine("runtime.session.route_facts", poisonRoute)
		poisonSeq++
		poisonFuture := map[string]any{"schema_version": 1, "id": museTestUUID(fmt.Sprintf("peer-poison-%d-%d", os.Getpid(), poisonSeq)), "stream": map[string]any{"kind": "session", "id": id}, "sequence": poisonSeq, "recorded_at": 1790878873999999, "record_type": "event", "durability": "durable", "causation_id": nil, "payload_type": "runtime.future", "payload_schema_version": 1, "payload": map[string]any{"kind": "future"}}
		poisonFutureLine, _ := json.Marshal(poisonFuture)
		if err := os.WriteFile(filepath.Join(poisonDir, "session.jsonl"), []byte(first+"\n"+second+"\n"+string(poisonFutureLine)+"\n"), 0600); err != nil {
			os.Exit(6)
		}
		// Both trees live under the test's temp HOME, so the rename
		// stays on one filesystem and is atomic. Only after it does
		// the peer append the primary route records below.
		if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
			os.Exit(6)
		}
		if err := os.Rename(staged, dir); err != nil {
			os.Exit(6)
		}
	}
	logPath := filepath.Join(dir, "session.jsonl")
	freshLog := true
	if st, e := os.Stat(logPath); e == nil && st.Size() > 0 {
		freshLog = false
	}
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(4)
	}
	// Envelope stamps (r12): the fold requires unique ids and
	// sequences, so the peer stamps every emission. Sequences
	// continue past the log's small-seq high water mark (a resume
	// peer shares the log with its predecessor); test appends live
	// at 1e9 and up and stay invisible to the scan. The tail
	// identity below is captured before the first emission: a
	// resumed record must cite the prior epoch.
	peerSeq := musePeerLogMaxSeq(logPath)
	resumeID, resumeSeq := musePeerLogTail(logPath)
	emit := func(kind string, payload any) (string, int) {
		key := kind
		p := payload.(map[string]any)
		if kind == "runtime.session" {
			key = p["event"].(map[string]any)["kind"].(string)
		}
		record := museFixtureRecord(key)
		peerSeq++
		record["id"] = museTestUUID(fmt.Sprintf("peer-%d-%d", os.Getpid(), peerSeq))
		record["sequence"] = peerSeq
		record["stream"].(map[string]any)["id"] = id
		record["payload"] = payload
		b, _ := json.Marshal(record)
		if string(mode) == "partial-log" && key == "assistant_message_committed" {
			// Deterministic partial append (tb-R198): write the first
			// half, rendezvous with the test so a RunOwner poll
			// provably observes the partial tail, wait for the
			// test's ack (sent after counted fold polls), then write
			// the rest. No sleeps: both steps are pipe handshakes.
			_, _ = log.Write(b[:len(b)/2])
			_ = log.Sync()
			musePeerRendezvous(home, musePeerPartialHalf, musePeerPartialAck)
			_, _ = log.Write(append(b[len(b)/2:], '\n'))
		} else {
			_, _ = log.Write(append(b, '\n'))
		}
		return record["id"].(string), peerSeq
	}
	opened := museFixturePayload("session.opened.observed")
	if raw, err := os.ReadFile(filepath.Join(home, "peer-opened-payload")); err == nil {
		opened = nil
		if err := json.Unmarshal(raw, &opened); err != nil {
			os.Exit(5)
		}
	}
	opened["record"].(map[string]any)["session_id"] = id
	emit("session.opened.observed", opened)
	if resuming && freshLog {
		// A resume reopens a prior epoch. On a fresh log (the
		// synthetic contract-mutant log only) the stub prior
		// epoch ends here so the resumed below follows a
		// legitimate reopen; an orphan resumed is unknown state
		// since r11. A same-log resume already carries its end.
		// The resumed below cites this end: the reference must
		// resolve to the prior epoch since r12.
		end := museFixturePayload("session.end")
		end["record"].(map[string]any)["session_id"] = id
		endID, endSeq := emit("session.end", end)
		resumeID, resumeSeq = endID, endSeq
	}
	route := museFixturePayload("runtime.session.route_facts")
	route["record"].(map[string]any)["pid"] = os.Getpid()
	emit("runtime.session.route_facts", route)
	// L3G three-record startup: the peer witnesses opened, route and one
	// informational record. Hold/unknown modes under-test the admission
	// gate instead: two records only, or an unknown third.
	switch string(mode) {
	case "hold-third":
	case "unknown-third":
		peerSeq++
		future := map[string]any{"schema_version": 1, "id": museTestUUID(fmt.Sprintf("peer-future-%d-%d", os.Getpid(), peerSeq)), "stream": map[string]any{"kind": "session", "id": id}, "sequence": peerSeq, "recorded_at": 1790878873999999, "record_type": "event", "durability": "durable", "causation_id": nil, "payload_type": "runtime.future", "payload_schema_version": 1, "payload": map[string]any{"kind": "future"}}
		if b, e := json.Marshal(future); e == nil {
			_, _ = log.Write(append(b, '\n'))
		}
	default:
		third := museFixturePayload("command.invoked")
		third["record"].(map[string]any)["session_id"] = id
		emit("command.invoked", third)
	}
	if resuming {
		if freshLog {
			reopened := museFixturePayload("session.opened.observed")
			reopened["record"].(map[string]any)["session_id"] = id
			emit("session.opened.observed", reopened)
			reroute := museFixturePayload("runtime.session.route_facts")
			reroute["record"].(map[string]any)["pid"] = os.Getpid()
			emit("runtime.session.route_facts", reroute)
		}
		resumed := museFixturePayload("session.resumed")
		resumed["record"].(map[string]any)["session_id"] = id
		resumed["record"].(map[string]any)["resumed_from_record_id"] = resumeID
		resumed["record"].(map[string]any)["resumed_from_sequence"] = resumeSeq
		emit("session.resumed", resumed)
	}
	run := func(kind, runID string) {
		p := museFixturePayload(kind)
		p["run_id"] = runID
		event := p["event"].(map[string]any)
		if kind == "terminal" {
			text := "peer reply"
			if b, e := os.ReadFile(filepath.Join(home, "peer-terminal")); e == nil {
				var override map[string]any
				if json.Unmarshal(b, &override) != nil {
					os.Exit(5)
				}
				if value, ok := override["text"].(string); ok {
					text = value
				}
				for k, v := range override {
					if k != "text" {
						event[k] = v
					}
				}
			}
			message := museFixturePayload("assistant_message_committed")
			message["run_id"] = runID
			message["event"].(map[string]any)["text"] = text
			emit("runtime.session", message)
		}
		emit("runtime.session", p)
	}
	queue := func(itemID string) {
		run("started", museTestUUID("queue-run"))
		p := museFixturePayload("inbox_item_queued")
		p["run_id"], p["source_run_record_id"] = museTestUUID("queue-run"), itemID
		emit("runtime.session", p)
		run("terminal", museTestUUID("queue-run"))
	}
	frame := func(text string, dialog bool) {
		musePeerFrame(os.Stdout, string(mode), text, dialog)
	}
	if string(mode) == "busy" {
		run("started", museTestUUID("already-running"))
	}
	if string(mode) == "queued" || string(mode) == "drained" {
		queue(museTestUUID("queued-1"))
		if string(mode) == "drained" {
			// The drained peer settles the drain target run: a
			// forward-only drain stays pending and never idle.
			target := museFixturePayload("inbox_item_drained")["run_id"].(string)
			run("started", target)
			run("terminal", target)
			p := museFixturePayload("inbox_item_drained")
			p["event"].(map[string]any)["item_id"] = museTestUUID("queued-1")
			emit("runtime.session", p)
		}
	}
	initial := ""
	if string(mode) == "nonempty" {
		initial = "human draft"
	}
	frame(initial, string(mode) == "trust" || string(mode) == "permissions")
	if string(mode) == "spinner" {
		fmt.Print("\x1b]0;⠋\a")
	}
	if string(mode) == "unknown-vt" {
		fmt.Print("\x1b[99z")
	}
	musePeerFrameHandshakeWrite(home)
	seq := 0
	input := ""
	pasted := ""
	inPaste := false
	for {
		b, e := reader.ReadByte()
		if e != nil {
			os.Exit(0)
		}
		input += string([]byte{b})
		if strings.HasSuffix(input, "\x1b[200~") {
			inPaste = true
			pasted = ""
			input = ""
			continue
		}
		if inPaste {
			pasted += string([]byte{b})
			if strings.HasSuffix(pasted, "\x1b[201~") {
				pasted = strings.TrimSuffix(pasted, "\x1b[201~")
				_ = os.WriteFile(filepath.Join(home, "peer-paste"), []byte(pasted), 0600)
				inPaste = false
				input = ""
				switch string(mode) {
				case "dialog-after-paste":
					frame(pasted, true)
				case "busy-after-paste":
					run("started", museTestUUID("racing-run"))
					frame(pasted, false)
				case "queue-after-paste":
					queue(museTestUUID("racing-item"))
					frame(pasted, false)
				case "foreign-composer":
					frame("foreign "+pasted, false)
				case "completed-before-submit":
					// This unrelated run completes before Enter and cannot acknowledge it.
					run("started", museTestUUID("pre-submit-run"))
					run("terminal", museTestUUID("pre-submit-run"))
					frame(pasted, false)
				default:
					frame(pasted, false)
				}
			}
			continue
		}

		if b == '\f' {
			frame("", false)
			_ = os.WriteFile(filepath.Join(home, "peer-redraw"), []byte("rendered"), 0600)
			if string(mode) == "settle-dialog" {
				frame("", true)
			}
			musePeerFrameHandshakeWrite(home)
			input = ""
			continue
		}
		if b == '\r' {
			seq++
			_ = os.WriteFile(filepath.Join(home, "peer-submit"), []byte(fmt.Sprintf("%d:%s", seq, pasted)), 0600)
			if string(mode) != "no-ack" && string(mode) != "completed-before-submit" {
				run("started", museTestUUID(fmt.Sprintf("submitted-%d", seq)))
				if string(mode) == "partial-log" {
					// Hold the started record complete until the
					// test's ack (sent after counted deliver
					// folds), so deliver provably observes the
					// started run before the partial half below
					// lands. Without this wait the half can land
					// before any complete fold, deliver never
					// returns until the rest arrives, and no
					// RunOwner poll ever observes the partial.
					musePeerRendezvous(home, musePeerStartedEmit, musePeerStartedAck)
				}
				run("terminal", museTestUUID(fmt.Sprintf("submitted-%d", seq)))
			}
			frame("", false)
			musePeerFrameHandshakeWrite(home)
			input = ""
		}
	}
}

// musePeerFrame renders the shared scripted peer screen. Publish the lower
// composer rule LAST: deliver waits for that geometry via composerMatches.
// The fake quiet clock cannot order real PTY reads, so no footer/dialog bytes
// may remain in flight once the matching composer becomes observable.
func musePeerFrame(w io.Writer, mode, text string, dialog bool) {
	fmt.Fprint(w, "\x1b[1;1H\x1b[J")
	if mode == "busy" || mode == "busy-screen" {
		fmt.Fprint(w, "◆ Working (2s · esc to interrupt)\r\n\r\n")
	}
	if mode == "transcript" {
		fmt.Fprint(w, "❯ permissions\r\n◆ echo: Please confirm thinking; do you trust [y/n] approve\r\n\r\n")
	}
	fmt.Fprint(w, "── Voice input (⌥ + v to start) "+strings.Repeat("─", 120-len([]rune("── Voice input (⌥ + v to start) ")))+"\r\n")
	fmt.Fprint(w, "❯ ")
	for n, line := range strings.Split(text, "\n") {
		if n > 0 {
			fmt.Fprint(w, "  ")
		}
		for len([]rune(line)) > 110 {
			r := []rune(line)
			fmt.Fprint(w, string(r[:110])+"\r\n  ")
			line = string(r[110:])
		}
		fmt.Fprint(w, line+"\r\n")
	}
	// Reserve the lower-rule row, then render the entire decision surface.
	fmt.Fprint(w, "\r\n")
	footerRows := 1
	if dialog {
		footerRows = 3
		if strings.Contains(mode, "trust") {
			fmt.Fprint(w, "Do you trust this workspace?\r\n> 1 Trust and continue\r\n  2 Quit\r\n")
		} else {
			fmt.Fprint(w, "Permissions\r\n› 1 Auto-review\r\n  2 Ask\r\n")
		}
	} else {
		fmt.Fprint(w, "  echo · ~ · Auto-review\r\n")
	}
	// Return to the reserved row using captured VT controls. The final rune
	// completes the rule and releases the product's composer-match wait; do
	// not emit even a trailing cursor control after it.
	fmt.Fprint(w, strings.Repeat("\x1b[1A", footerRows+1)+"\r"+strings.Repeat("─", 120))
}

// musePeerLogMaxSeq scans the log for the highest peer-stamped
// sequence: small non-negative integers only. Test appends live at
// 1e9 and up and stay invisible, as do undecodable lines (a partial
// tail is never a stamp source). A missing log scans as zero.
func musePeerLogMaxSeq(logPath string) int {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return 0
	}
	max := 0
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var r struct {
			Sequence int `json:"sequence"`
		}
		if json.Unmarshal([]byte(line), &r) != nil {
			continue
		}
		if r.Sequence > max && r.Sequence < 1000000000 {
			max = r.Sequence
		}
	}
	return max
}

// musePeerLogTail returns the envelope identity of the last complete
// line in the log, or ("", 0) when the log has none. The peer calls
// it before its first emission so a resumed record can cite the
// prior epoch.
func musePeerLogTail(logPath string) (string, int) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return "", 0
	}
	var id string
	var seq int
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var r struct {
			ID       string `json:"id"`
			Sequence int    `json:"sequence"`
		}
		if json.Unmarshal([]byte(line), &r) != nil || r.ID == "" {
			continue
		}
		id, seq = r.ID, r.Sequence
	}
	return id, seq
}

func musePeerPlan(t *testing.T, mode string, resume string) (*MusePTYLaunchPlan, string) {
	t.Helper()
	home := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(binary, filepath.Join(home, "muse")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(home, "peer-mode"), []byte(mode), 0600); err != nil {
		t.Fatal(err)
	}
	if err = syscall.Mkfifo(filepath.Join(home, musePeerFrameFIFO), 0600); err != nil {
		t.Fatal(err)
	}
	req := agentic.LaunchRequest{System: "muse", Model: agentic.Model{ID: "echo"}, WorkDir: home, Env: []string{"HOME=" + home, "PATH=" + home, "XDG_DATA_HOME=" + filepath.Join(home, "data"), "TERM=xterm-256color"}, NativeArgs: []string{"--trust-workspace"}}
	if resume != "" {
		req.NativeArgs = append(req.NativeArgs, "resume", resume)
	}
	plan, err := BuildMusePTYLaunchPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	return plan, home
}

// musePeerStartsIdle reports whether the scripted peer's startup state is
// idle (empty composer, idle log). Non-idle-start modes (dialog, busy,
// spinner, unknown VT, queued inbox, human draft) refuse the first
// delivery by construction; musePeer waits for their startup screen
// instead of for idle.
func musePeerStartsIdle(mode string) bool {
	switch mode {
	case "trust", "permissions", "busy", "busy-screen", "spinner", "unknown-vt", "queued", "nonempty":
		return false
	default:
		return true
	}
}

func musePeer(t *testing.T, mode string) (*musePTYHost, string) {
	t.Helper()
	plan, home := musePeerPlan(t, mode, "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h, err := StartMusePTYHost(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			h.mu.Lock()
			t.Logf("screen=%q", strings.Join(h.vt.lines(), "\n"))
			h.mu.Unlock()
		}
		_ = h.Stop(context.Background())
	})
	// Scripted idle evidence arrives through the frame handshake
	// plus pump notifications, and quiet through the injected test
	// clock advanced past the window: never a wall-clock window
	// (tb-R198). Production default (nil clock) keeps the wall clock.
	// Startup waits on the non-quiet predicate only, so a frozen
	// clock never hangs it; the failure surfaces at delivery.
	museInjectTestSeams(t, h)
	musePeerHandshakeRead(home, musePeerFrameFIFO)
	if musePeerStartsIdle(mode) {
		museWaitScripted(t, h, "peer startup idle", func() bool { return museScriptedReady(h, "") })
	} else {
		museWaitScripted(t, h, "peer startup screen", func() bool { return !museScriptedReady(h, "") })
	}
	museAdvanceTestClockPastQuiet(h)
	if musePeerStartsIdle(mode) {
		if museTestClockFrozen() {
			if h.idle("") {
				t.Fatal("frozen clock must hold idle false (injection armed)")
			}
			return h, home
		}
		if !h.idle("") {
			t.Fatalf("peer startup: ready but not idle after clock advance (quiet=%v)", museQuietForReport(h))
		}
	} else if h.idle("") {
		t.Fatal("peer startup: refusal mode idle after clock advance")
	}
	return h, home
}
func TestMusePTYTrustDialogNeverSubmits(t *testing.T) { museRefusal(t, "trust", ErrMusePTYDeferred) }
func TestMusePTYPermissionsModalNeverSubmits(t *testing.T) {
	museRefusal(t, "permissions", ErrMusePTYDeferred)
}
func TestMusePTYBusyNeverSubmits(t *testing.T) { museRefusal(t, "busy", ErrMusePTYDeferred) }
func TestMusePTYBusyScreenNeverSubmits(t *testing.T) {
	museRefusal(t, "busy-screen", ErrMusePTYDeferred)
}
func TestMusePTYSpinnerNeverSubmits(t *testing.T) { museRefusal(t, "spinner", ErrMusePTYDeferred) }
func TestMusePTYUnknownScreenNeverSubmits(t *testing.T) {
	museRefusal(t, "unknown-vt", ErrMusePTYDeferred)
}
func TestMusePTYQueuedInboxNeverSubmits(t *testing.T) { museRefusal(t, "queued", ErrMusePTYDeferred) }
func TestMusePTYRechecksDialogBeforeSubmit(t *testing.T) {
	museRefusal(t, "dialog-after-paste", ErrMusePTYHeld)
}
func TestMusePTYRechecksLogBeforeSubmit(t *testing.T) {
	museRefusal(t, "busy-after-paste", ErrMusePTYHeld)
}
func TestMusePTYRechecksQueueBeforeSubmit(t *testing.T) {
	museRefusal(t, "queue-after-paste", ErrMusePTYHeld)
}
func TestMusePTYForeignComposerNeverSubmits(t *testing.T) {
	museRefusal(t, "foreign-composer", ErrMusePTYHeld)
}
func TestMusePTYNonemptyComposerNeverPastes(t *testing.T) {
	museRefusal(t, "nonempty", ErrMusePTYDeferred)
}
func museRefusal(t *testing.T, mode string, want error) {
	t.Helper()
	h, home := musePeer(t, mode)
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	err := h.DeliverSessionNotice(ctx, "a child reached review")
	if err == nil {
		t.Fatal("unsafe state admitted delivery")
	}
	if want == ErrMusePTYDeferred && err != want {
		t.Fatalf("got %v want %v", err, want)
	}
	if b, e := os.ReadFile(filepath.Join(home, "peer-submit")); e == nil {
		t.Fatalf("unsafe Enter written: %s", b)
	}
}
func TestMusePTYGoalTextRunStartAckAndDetach(t *testing.T) {
	h, home := musePeer(t, "idle")
	first, _ := productionParityRevisions(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stream, err := h.AttachTerminal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err = h.DeliverGoalText(ctx, first); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(home, "peer-submit"))
	if err != nil {
		t.Fatal(err)
	}
	for _, word := range []string{first, "Budget:", "End your turn while waiting, do not poll"} {
		if !strings.Contains(string(b), word) {
			t.Fatalf("directive missing %q", word)
		}
	}
	if strings.Contains(string(b), "/goal") {
		t.Fatal("native goal command used instead of text")
	}
	if b, err = os.ReadFile(filepath.Join(home, "peer-cpr")); err != nil || string(b) != "\x1b[1;1R" {
		t.Fatalf("CPR %q %v", b, err)
	}
	if b, err = os.ReadFile(filepath.Join(home, "peer-dsr")); err != nil || string(b) != "\x1b[0n" {
		t.Fatalf("DSR %q %v", b, err)
	}
	if state, _ := h.State(ctx); state != sm.ProviderHostRunning {
		t.Fatal("detach killed host")
	}
}
func TestMusePTYNoticeAckAndOwnerStatus(t *testing.T) {
	h, home := musePeer(t, "idle")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := h.DeliverSessionNotice(ctx, "a child reached review"); err != nil {
		t.Fatal(err)
	}
	museSettleScriptedIdle(t, h, home)
	result, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "perform assignment"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != sm.ProviderRunIdle || result.Output != "peer reply" {
		t.Fatalf("result %+v", result)
	}
}
func TestMusePTYResumeRecordedNativeSession(t *testing.T) {
	h, _ := musePeer(t, "idle")
	snapshot, err := museTestSnapshot(h)
	if err != nil {
		t.Fatal(err)
	}
	id := h.NativeIdentity()
	_ = h.Stop(context.Background())
	log, err := os.OpenFile(h.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	endRecord := museOwnedRecord(h, museFixtureRecord("session.end"))
	endBytes, err := json.Marshal(endRecord)
	if err != nil {
		t.Fatal(err)
	}
	_, err = log.Write(append(endBytes, '\n'))
	_ = log.Close()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildMusePTYLaunchPlan(agentic.LaunchRequest{System: "muse", Model: agentic.Model{ID: "echo"}, WorkDir: h.command.Dir, Env: h.command.Env, NativeArgs: []string{"resume", id, "--trust-workspace"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resumed, err := ResumeMusePTYHost(ctx, plan, sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(snapshot)})
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Stop(context.Background())
	if resumed.NativeIdentity() != id {
		t.Fatal("resume changed identity")
	}
	museInjectTestSeams(t, resumed)
	museSettleScriptedIdle(t, resumed, h.command.Dir)
	if err := resumed.DeliverSessionNotice(ctx, "notice after same-log resume"); err != nil {
		t.Fatal(err)
	}
}
func TestMusePTYResumeRefusesFreshPlan(t *testing.T) {
	plan, _ := musePeerPlan(t, "idle", "")
	b, _ := json.Marshal(musePTYSnapshot{Contract: "task-board.muse-pty-host-v1", SessionID: museTestUUID("peer-session")})
	if h, err := ResumeMusePTYHost(context.Background(), plan, sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(b)}); err == nil {
		_ = h.Stop(context.Background())
		t.Fatal("fresh plan admitted as resume")
	}
}
func TestMusePTYPlannerUsesPluginPostureAndEnv(t *testing.T) {
	plan, _ := musePeerPlan(t, "idle", "")
	if envValue(plan.plan.Env, "MUSE_NO_AUTO_UPDATE") != "1" {
		t.Fatal("auto update not pinned")
	}
	req := agentic.LaunchRequest{System: "muse", Model: agentic.Model{ID: "echo"}, WorkDir: plan.plan.WorkDir, Env: plan.plan.Env, PermissionMode: agentic.PermissionModeYolo, ToolRelease: "1.4.1", NativeArgs: []string{"--trust-workspace"}}
	yolo, err := BuildMusePTYLaunchPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(yolo.plan.Argv, " "), "--yolo") {
		t.Fatal("plugin posture missing")
	}
	req.NativeArgs = nil
	if _, err = BuildMusePTYLaunchPlan(req); err == nil {
		t.Fatal("untrusted plan admitted")
	}
}
func TestMusePTYMissingOrMalformedLogNeverAdmits(t *testing.T) {
	for _, bad := range []string{"missing", "malformed", "partial"} {
		t.Run(bad, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			switch bad {
			case "missing":
				_ = os.Remove(h.snapshot.LogPath)
			case "malformed":
				_ = os.WriteFile(h.snapshot.LogPath, []byte("{broken}\n"), 0600)
			case "partial":
				f, _ := os.OpenFile(h.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
				_, _ = f.WriteString("{")
				_ = f.Close()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := h.DeliverSessionNotice(ctx, "notice"); err != ErrMusePTYDeferred {
				t.Fatalf("got %v", err)
			}
			if _, err := os.Stat(filepath.Join(home, "peer-submit")); err == nil {
				t.Fatal("submitted without durable idle proof")
			}
		})
	}
}
func TestMusePTYGoalValidationAndControlTextRefused(t *testing.T) {
	h, home := musePeer(t, "idle")
	if err := h.DeliverGoalText(context.Background(), ""); err == nil {
		t.Fatal("invalid goal admitted")
	}
	if err := h.DeliverSessionNotice(context.Background(), "notice\x1b[2J"); err == nil {
		t.Fatal("terminal control admitted")
	}
	if _, err := os.Stat(filepath.Join(home, "peer-submit")); err == nil {
		t.Fatal("submitted invalid input")
	}
	if _, err := os.Stat(filepath.Join(home, "peer-paste")); err == nil {
		t.Fatal("pasted invalid input before refusal")
	}
}
