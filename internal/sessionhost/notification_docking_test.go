package sessionhost

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const dockContract = "source-host-dock/1"
const dockFrameLimit = 8192

type dockSetup struct {
	Contract      string              `json:"contract"`
	Kind          string              `json:"kind"`
	Case          string              `json:"case"`
	Binding       NotificationBinding `json:"binding"`
	CallTimeoutMS int                 `json:"call_timeout_ms"`
}

type dockReady struct {
	Contract string              `json:"contract"`
	Kind     string              `json:"kind"`
	Binding  NotificationBinding `json:"binding"`
}

type dockResult struct {
	Contract      string              `json:"contract"`
	Kind          string              `json:"kind"`
	Case          string              `json:"case"`
	Binding       NotificationBinding `json:"binding"`
	IntentCount   int                 `json:"intent_count"`
	NoticeID      string              `json:"notice_id"`
	Sequence      string              `json:"sequence"`
	NativeStage   string              `json:"native_stage"`
	WakeCount     int                 `json:"wake_count"`
	BindingActive bool                `json:"binding_active"`
	InputFenced   bool                `json:"input_fenced"`
}

func readDockSetup(r *bufio.Reader) (dockSetup, error) {
	invalid := errors.New("dock_setup_invalid")
	if r == nil || r.Size() > dockFrameLimit {
		return dockSetup{}, invalid
	}
	frame, err := r.ReadSlice('\n')
	if err != nil || len(frame) > dockFrameLimit {
		return dockSetup{}, invalid
	}
	m, err := notificationObject(frame)
	if err != nil || !notificationKeys(m, "contract", "kind", "case", "binding", "call_timeout_ms") ||
		!notificationBindingShape(m["binding"]) {
		return dockSetup{}, invalid
	}
	var setup dockSetup
	if json.Unmarshal(frame, &setup) != nil || setup.Contract != dockContract || setup.Kind != "setup" ||
		(setup.Case != "accepted-lookup" && setup.Case != "input-not-ready") ||
		setup.CallTimeoutMS != 2000 || !setup.Binding.valid() {
		return dockSetup{}, invalid
	}
	return setup, nil
}

func writeDockFrame(w io.Writer, value any) error {
	b, err := json.Marshal(value)
	if err != nil || len(b)+1 > dockFrameLimit {
		return errors.New("dock_result_invalid")
	}
	b = append(b, '\n')
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}

func inheritedDockStream(fd uintptr) (*net.UnixConn, error) {
	f := os.NewFile(fd, "dock-fixture-endpoint")
	if f == nil {
		return nil, errors.New("dock_endpoint_invalid")
	}
	defer f.Close()
	typ, err := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TYPE)
	if err != nil || typ != syscall.SOCK_STREAM {
		return nil, errors.New("dock_endpoint_invalid")
	}
	c, err := net.FileConn(f)
	if err != nil {
		return nil, errors.New("dock_endpoint_invalid")
	}
	u, ok := c.(*net.UnixConn)
	if !ok || u.LocalAddr().Network() != "unix" {
		c.Close()
		return nil, errors.New("dock_endpoint_invalid")
	}
	return u, nil
}

// The helper exercises the private Host stream and store only. It starts no
// scheduler, child process, native driver or workflow; metadata is synthetic.
// A normal module test run skips it rather than claim external docking.
func TestSourceNotificationDockHelper(t *testing.T) {
	if os.Getenv("HOST_NOTIFICATION_DOCK_HELPER") != "1" {
		t.Skip("not a docking invocation")
	}
	deadline := time.Now().Add(20 * time.Second)
	wire, err := inheritedDockStream(3)
	if err != nil {
		t.Fatal("dock wire unavailable")
	}
	defer wire.Close()
	control, err := inheritedDockStream(4)
	if err != nil {
		t.Fatal("dock control unavailable")
	}
	defer control.Close()
	if control.SetDeadline(time.Now().Add(2*time.Second)) != nil {
		t.Fatal("dock setup deadline unavailable")
	}
	setup, err := readDockSetup(bufio.NewReaderSize(control, dockFrameLimit))
	if err != nil {
		t.Fatal("dock setup invalid")
	}
	h, _ := journalUnit(t, false)
	// Rebind fixture metadata before any service starts. No product predicate
	// or runtime identity is changed; the actual supplied binding must validate.
	epoch, err := strconv.ParseUint(setup.Binding.ControllerEpoch, 10, 64)
	if err != nil {
		t.Fatal("dock epoch invalid")
	}
	h.state.Execution, h.cfg.Execution = setup.Binding.Execution, setup.Binding.Execution
	h.state.Epoch, h.state.RunnerInstance = epoch, setup.Binding.RunnerInstance
	h.state.Protocol.View.NativeSession = setup.Binding.NativeThread
	h.driver.(*journalDriver).view = h.state.Protocol.View
	if setup.Case == "input-not-ready" {
		h.state.InputState = InputBusy
	}
	// No scheduler drains kicks. The bounded recorder retains up to 32 signals,
	// so a duplicate wake cannot hide behind the production channel's coalescing.
	h.kick = make(chan struct{}, MaxRecords)
	if h.store.write(h.state) != nil {
		t.Fatal("dock fixture persistence failed")
	}
	ctx, cancel := context.WithDeadline(h.ctx, deadline)
	h.ctx = ctx
	t.Cleanup(cancel)
	done, err := h.startNotificationStream(wire, setup.Binding, setup.CallTimeoutMS)
	if err != nil {
		t.Fatal("dock stream admission failed")
	}
	t.Cleanup(func() {
		wire.Close()
		cancel()
		h.wg.Wait()
	})
	if writeDockFrame(control, dockReady{dockContract, "ready", setup.Binding}) != nil || control.SetDeadline(deadline) != nil {
		t.Fatal("dock ready failed")
	}
	select {
	case err = <-done:
		if !errors.Is(err, ErrNotificationUnresolved) {
			t.Fatal("dock stream terminal invalid")
		}
	case <-ctx.Done():
		t.Fatal("dock overall deadline")
	}
	cancel()
	h.wg.Wait()
	s, exists, err := h.store.read()
	if err != nil || !exists || s.Notification == nil || s.Notification.Binding != setup.Binding {
		t.Fatal("dock durable state unavailable")
	}
	result := dockResult{Contract: dockContract, Kind: "result", Case: setup.Case, Binding: setup.Binding,
		IntentCount: len(s.Notification.Attempts), WakeCount: len(h.kick),
		InputFenced: s.Fence == "unknown" && s.InputState == InputUnknown}
	h.mu.Lock()
	result.BindingActive = h.notificationCurrentLocked()
	h.mu.Unlock()
	if result.BindingActive || !result.InputFenced || h.driver.(*journalDriver).prepares.Load() != 0 {
		t.Fatal("dock cleanup or inertness violated")
	}
	if setup.Case == "accepted-lookup" {
		if len(s.Records) != 1 || result.IntentCount != 1 || result.WakeCount != 1 {
			t.Fatal("dock expected one durable intent and wake")
		}
		a, ok := notificationAttempt(s.Records[0], setup.Binding)
		if !ok {
			t.Fatal("dock retained attempt invalid")
		}
		result.NoticeID, result.Sequence, result.NativeStage = a.NoticeID, a.Sequence, a.NativeStage
	} else if len(s.Records) != 0 || result.IntentCount != 0 || result.WakeCount != 0 {
		t.Fatal("dock refused input produced effect")
	}
	resultDeadline := time.Now().Add(2 * time.Second)
	if deadline.Before(resultDeadline) {
		resultDeadline = deadline
	}
	if control.SetWriteDeadline(resultDeadline) != nil || writeDockFrame(control, result) != nil {
		t.Fatal("dock result failed")
	}
}

// The setup boundary accepts the complete agreed fixture form and rejects
// malformed, ambiguous, unsupported and out-of-budget forms before Host setup.
func TestSourceNotificationDockSetup(t *testing.T) {
	setup := dockSetup{dockContract, "setup", "accepted-lookup", notificationFixture("trigger").Binding, 2000}
	var encoded bytes.Buffer
	if writeDockFrame(&encoded, setup) != nil {
		t.Fatal("valid setup encoding")
	}
	valid := encoded.Bytes()
	for _, name := range []string{"accepted-lookup", "input-not-ready"} {
		q := setup
		q.Case = name
		q.Binding.ControllerEpoch = "18446744073709551615"
		var b bytes.Buffer
		writeDockFrame(&b, q)
		if got, err := readDockSetup(bufio.NewReaderSize(&b, dockFrameLimit)); err != nil || got != q {
			t.Fatal("valid fixture setup refused")
		}
	}
	cases := map[string][]byte{
		"unknown":          bytes.Replace(valid, []byte(`{"contract":`), []byte(`{"extra":true,"contract":`), 1),
		"duplicate":        bytes.Replace(valid, []byte(`"kind":`), []byte(`"kind":"setup","kind":`), 1),
		"casefold":         bytes.Replace(valid, []byte(`"kind":`), []byte(`"Kind":`), 1),
		"nested-duplicate": bytes.Replace(valid, []byte(`"execution":`), []byte(`"execution":"other","execution":`), 1),
		"missing-binding":  bytes.Replace(valid, []byte(`"binding":`), []byte(`"absent":`), 1),
		"null":             bytes.Replace(valid, []byte(`"kind":"setup"`), []byte(`"kind":null`), 1),
		"contract":         bytes.Replace(valid, []byte(dockContract), []byte("unsupported/1"), 1),
		"kind":             bytes.Replace(valid, []byte(`"kind":"setup"`), []byte(`"kind":"ready"`), 1),
		"deferred-case":    bytes.Replace(valid, []byte("accepted-lookup"), []byte("lost-reply"), 1),
		"timeout-zero":     bytes.Replace(valid, []byte(`:2000`), []byte(`:0`), 1),
		"timeout-fraction": bytes.Replace(valid, []byte(`:2000`), []byte(`:2000.0`), 1),
		"timeout-string":   bytes.Replace(valid, []byte(`:2000`), []byte(`:"2000"`), 1),
		"timeout-other":    bytes.Replace(valid, []byte(`:2000`), []byte(`:2001`), 1),
		"epoch-zero":       bytes.Replace(valid, []byte(`"controller_epoch":"1"`), []byte(`"controller_epoch":"0"`), 1),
		"workflow-digest":  bytes.Replace(valid, []byte(strings.Repeat("a", 64)), []byte("bad"), 1),
		"no-lf":            valid[:len(valid)-1],
		"trailing-value":   append(append([]byte{}, valid[:len(valid)-1]...), []byte(" {}\n")...),
		"bom":              append([]byte{0xef, 0xbb, 0xbf}, valid...),
		"utf8":             append([]byte{'{', 0xff, '}'}, '\n'),
		"trailing-space":   append(append([]byte{}, valid[:len(valid)-1]...), ' ', '\n'),
		"oversize":         []byte(strings.Repeat("x", dockFrameLimit) + "\n"),
	}
	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := readDockSetup(bufio.NewReaderSize(bytes.NewReader(frame), dockFrameLimit)); err == nil || err.Error() != "dock_setup_invalid" {
				t.Fatal("invalid fixture setup admitted")
			}
		})
	}
}

// The recorder detects a second actual journal wake even when the production
// one-slot kick channel would coalesce it. It proves no native scheduling claim.
func TestSourceNotificationDockWakeCounter(t *testing.T) {
	h, q := journalUnit(t, true)
	h.kick = make(chan struct{}, MaxRecords)
	journalCall(t, h, q, "accepted", "")
	if len(h.kick) != 1 {
		t.Fatal("first acceptance did not wake once")
	}
	for _, method := range []string{"lookup_attempt", "trigger"} {
		q.Method, q.RequestID = method, method
		journalCall(t, h, q, "existing-attempt", "")
		if len(h.kick) != 1 {
			t.Fatal("same attempt emitted an additional wake")
		}
	}
}

type dockCapture struct{ bytes.Buffer }

func (w *dockCapture) Write(p []byte) (int, error) {
	n := len(p)
	if remaining := dockFrameLimit - w.Len(); remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		w.Buffer.Write(p)
	}
	return n, nil
}

// This is Host-only self-conformance with the actual wire codec, not execution
// of the independently owned Source Adapter or a completed integrated review.
func TestSourceNotificationDockHelperProtocol(t *testing.T) {
	for _, name := range []string{"accepted-lookup", "input-not-ready"} {
		t.Run(name, func(t *testing.T) {
			wireChild, wire := notificationSocketpair(t, syscall.SOCK_STREAM)
			controlChild, control := notificationSocketpair(t, syscall.SOCK_STREAM)
			wf, err := wireChild.File()
			if err != nil {
				t.Fatal("wire fixture descriptor")
			}
			defer wf.Close()
			cf, err := controlChild.File()
			if err != nil {
				t.Fatal("control fixture descriptor")
			}
			defer cf.Close()
			wireChild.Close()
			controlChild.Close()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal("fixture executable unavailable")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestSourceNotificationDockHelper$", "-test.count=1", "-test.timeout=20s")
			cmd.Env = []string{"HOST_NOTIFICATION_DOCK_HELPER=1"}
			cmd.ExtraFiles = []*os.File{wf, cf}
			var stdout, stderr dockCapture
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if cmd.Start() != nil {
				t.Fatal("fixture child start failed")
			}
			t.Logf("owned fixture child PID=%d", cmd.Process.Pid)
			wf.Close()
			cf.Close()
			done := make(chan struct{})
			var waitErr error
			go func() { waitErr = cmd.Wait(); close(done) }()
			t.Cleanup(func() {
				wire.Close()
				control.Close()
				cancel()
				<-done
			})
			control.SetDeadline(time.Now().Add(2 * time.Second))
			q := notificationFixture("trigger")
			// Non-default identities must survive the private fixture setup.
			q.Binding.Execution, q.Binding.ControllerEpoch = "dock-execution", "7"
			q.Binding.RunnerInstance, q.Binding.NativeThread = "dock-runner", "dock-thread"
			if writeDockFrame(control, dockSetup{dockContract, "setup", name, q.Binding, 2000}) != nil {
				t.Fatal("fixture setup send failed")
			}
			reader := bufio.NewReaderSize(control, dockFrameLimit)
			frame, err := reader.ReadBytes('\n')
			var ready dockReady
			if err != nil || json.Unmarshal(frame, &ready) != nil || ready != (dockReady{dockContract, "ready", q.Binding}) {
				t.Fatal("fixture ready invalid")
			}
			wireReader := bufio.NewReader(wire)
			observe := q
			observe.Method, observe.NoticeID = "observe_binding", ""
			r := notificationExchange(t, wire, wireReader, observe)
			if r.Result != "observed" || r.View.Binding != q.Binding {
				t.Fatal("actual Host binding not observed")
			}
			r = notificationExchange(t, wire, wireReader, q)
			if name == "input-not-ready" {
				if r.Result != "refused" || r.Reason != "input-not-ready" {
					t.Fatal("actual Host refusal missing")
				}
			} else {
				if r.Result != "accepted" || r.Attempt.Sequence != "1" {
					t.Fatal("actual Host acceptance missing")
				}
				for _, method := range []string{"lookup_attempt", "trigger"} {
					q.RequestID, q.Method = method, method
					got := notificationExchange(t, wire, wireReader, q)
					if got.Result != "existing-attempt" || *got.Attempt != *r.Attempt {
						t.Fatal("same attempt changed or retriggered")
					}
				}
			}
			wire.Close()
			control.SetDeadline(time.Now().Add(2 * time.Second))
			frame, err = reader.ReadBytes('\n')
			var result dockResult
			if err != nil || json.Unmarshal(frame, &result) != nil || result.Contract != dockContract || result.Kind != "result" ||
				result.Case != name || result.Binding != q.Binding || result.BindingActive || !result.InputFenced {
				t.Fatal("fixture result invalid")
			}
			if name == "accepted-lookup" {
				if result.IntentCount != 1 || result.WakeCount != 1 || result.NoticeID != q.NoticeID || result.Sequence != "1" || result.NativeStage != "not-evidenced" {
					t.Fatal("accepted fixture evidence invalid")
				}
			} else if result.IntentCount != 0 || result.WakeCount != 0 || result.NoticeID != "" || result.Sequence != "" || result.NativeStage != "" {
				t.Fatal("refusal fixture produced evidence")
			}
			select {
			case <-done:
				if waitErr != nil {
					t.Fatal("fixture child did not exit normally")
				}
				t.Logf("owned fixture child PID=%d reaped exit=0", cmd.Process.Pid)
			case <-ctx.Done():
				t.Fatal("fixture child not reaped within budget")
			case <-time.After(2 * time.Second):
				t.Fatal("fixture final reap deadline")
			}
		})
	}
}
