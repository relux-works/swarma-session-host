package sessionhost

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

func notificationSocketpair(t *testing.T, kind int) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, kind, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ends [2]*net.UnixConn
	for i, fd := range fds {
		file := os.NewFile(uintptr(fd), "notification-test-socket")
		conn, err := net.FileConn(file)
		_ = file.Close()
		if err != nil {
			t.Fatal(err)
		}
		ends[i] = conn.(*net.UnixConn)
		t.Cleanup(func() { _ = conn.Close() })
	}
	return ends[0], ends[1]
}

func notificationService(t *testing.T, h *Host, q NotificationRequest, timeoutMS int) (*net.UnixConn, <-chan error) {
	t.Helper()
	server, client := notificationSocketpair(t, syscall.SOCK_STREAM)
	done, err := h.startNotificationStream(server, q.Binding, timeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		h.cancel()
		h.wg.Wait()
	})
	return client, done
}

func notificationServiceDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("terminal service returned success")
		}
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("owned service did not terminate")
		return nil
	}
}

func notificationExchange(t *testing.T, client *net.UnixConn, reader *bufio.Reader, q NotificationRequest) NotificationResponse {
	t.Helper()
	if err := client.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame, err := EncodeNotificationRequest(q)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := client.Write(frame); err != nil || n != len(frame) {
		t.Fatalf("request write: %d %v", n, err)
	}
	r, err := ReadNotificationResponse(reader, q)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Actual owned Unix streams carry valid observation/acceptance/reconciliation;
// retries retain one exact record. The synthetic metadata proves no enrollment
// or native execution, and loss never authorizes another attachment.
func TestNotificationStreamExchangeAndSingleton(t *testing.T) {
	h, q := journalUnit(t, false)
	client, done := notificationService(t, h, q, 1000)
	reader := bufio.NewReader(client)
	observe := q
	observe.Method, observe.NoticeID = "observe_binding", ""
	if r := notificationExchange(t, client, reader, observe); r.Result != "observed" || r.View.Gate != "open" {
		t.Fatalf("observation: %+v", r)
	}
	// The transferred handle cannot be consumed twice, even by an invalid
	// repeated start. Reject without closing or resetting the original endpoint.
	for _, timeoutMS := range []int{1000, 0} {
		if second, err := h.startNotificationStream(h.notificationStream.conn, q.Binding, timeoutMS); second != nil || !errors.Is(err, ErrNotificationUnresolved) {
			t.Fatal("owned endpoint reused")
		}
		if r := notificationExchange(t, client, reader, observe); r.Result != "observed" {
			t.Fatal("repeated start disturbed the owned endpoint")
		}
	}
	if r := notificationExchange(t, client, reader, q); r.Result != "accepted" || r.Attempt.Sequence != "1" {
		t.Fatalf("acceptance: %+v", r)
	}
	for _, method := range []string{"trigger", "lookup_attempt"} {
		q.Method, q.RequestID = method, method
		if r := notificationExchange(t, client, reader, q); r.Result != "existing-attempt" || r.Attempt.NoticeID != q.NoticeID || r.Attempt.Sequence != "1" {
			t.Fatalf("reconciliation: %+v", r)
		}
	}
	if got := h.Snapshot(); len(got.Records) != 1 || len(got.Notification.Attempts) != 1 {
		t.Fatal("duplicate intent")
	}
	for _, afterLoss := range []bool{false, true} {
		if afterLoss {
			_ = client.Close()
			notificationServiceDone(t, done)
		}
		server2, client2 := notificationSocketpair(t, syscall.SOCK_STREAM)
		second, err := h.startNotificationStream(server2, q.Binding, 1000)
		if !errors.Is(err, ErrNotificationUnresolved) || second != nil {
			t.Fatal("second service admitted")
		}
		_ = client2.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := client2.Read(make([]byte, 1)); err == nil {
			t.Fatal("rejected endpoint remains open")
		}
		if !afterLoss {
			// Rejected attachment must not close or consume the first reader.
			if r := notificationExchange(t, client, reader, observe); r.Result != "observed" {
				t.Fatal("first service was disturbed")
			}
		}
	}
	got := h.Snapshot()
	if got.Fence != "unknown" || got.InputState != InputUnknown || len(got.Records) != 1 {
		t.Fatal("loss discarded intent or failed to fence")
	}
}

// Idle time does not consume the explicit full-call budget. A subsequently
// fragmented request is valid; a partial active request expires without intent.
func TestNotificationStreamIdleAndPartialDeadline(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			h, q := journalUnit(t, false)
			client, done := notificationService(t, h, q, 100)
			if partial {
				if _, err := client.Write([]byte("{")); err != nil {
					t.Fatal(err)
				}
				notificationServiceDone(t, done)
				if len(h.Snapshot().Records) != 0 || h.Snapshot().Fence != "unknown" {
					t.Fatal("partial request dispatched or failed to fence")
				}
				return
			}
			select {
			case <-done:
				t.Fatal("idle treated as expired call")
			case <-time.After(200 * time.Millisecond):
			}
			q.Method, q.NoticeID = "observe_binding", ""
			frame, _ := EncodeNotificationRequest(q)
			_ = client.SetDeadline(time.Now().Add(3 * time.Second))
			if _, err := client.Write(frame[:1]); err != nil {
				t.Fatal(err)
			}
			if _, err := client.Write(frame[1:]); err != nil {
				t.Fatal(err)
			}
			if r, err := ReadNotificationResponse(bufio.NewReader(client), q); err != nil || r.Result != "observed" {
				t.Fatalf("valid fragmented call: %+v %v", r, err)
			}
			_ = client.Close()
			notificationServiceDone(t, done)
		})
	}
}

// Framing violations close instead of scanning for a later valid frame. The
// same well-formed trigger passes above; no malformed case creates an intent.
func TestNotificationStreamMalformedCloses(t *testing.T) {
	q := notificationFixture("trigger")
	valid, _ := EncodeNotificationRequest(q)
	cases := map[string][]byte{
		"duplicate": bytes.Replace(valid, []byte(`"contract":`), []byte(`"contract":"curator-host-notification/1","contract":`), 1),
		"unknown":   append([]byte(`{"unknown":true,`), valid[1:]...),
		"utf8":      append([]byte{'{', 0xff, '}'}, '\n'),
		"depth":     []byte(`{"a":{"b":{"c":{"d":{"e":{"f":{"g":1}}}}}}}` + "\n"),
		"oversize":  []byte(strings.Repeat("x", MaxNotificationFrame) + "\n"),
		"malformed": []byte("{broken}\n"),
	}
	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			h, q := journalUnit(t, false)
			client, done := notificationService(t, h, q, 500)
			_ = client.SetDeadline(time.Now().Add(3 * time.Second))
			_, _ = client.Write(append(append([]byte{}, frame...), valid...))
			if err := notificationServiceDone(t, done); !errors.Is(err, ErrNotificationFrame) {
				t.Fatalf("wrong framing failure: %v", err)
			}
			if s := h.Snapshot(); len(s.Records) != 0 || s.Fence != "unknown" {
				t.Fatal("malformed frame dispatched or lifecycle stayed open")
			}
		})
	}
}

// After acceptance a lost response leaves the exact durable obligation. A later
// malformed frame likewise cannot erase acceptance. No native driver runs here.
func TestNotificationStreamLossRetainsIntent(t *testing.T) {
	for _, lostResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost-response=%v", lostResponse), func(t *testing.T) {
			h, q := journalUnit(t, false)
			client, done := notificationService(t, h, q, 500)
			if lostResponse {
				frame, _ := EncodeNotificationRequest(q)
				if _, err := client.Write(frame); err != nil {
					t.Fatal(err)
				}
				select {
				case <-h.kick: // Actual durable acceptance, without reading a response.
				case <-time.After(3 * time.Second):
					t.Fatal("acceptance not persisted")
				}
				_ = client.Close()
			} else {
				if r := notificationExchange(t, client, bufio.NewReader(client), q); r.Result != "accepted" {
					t.Fatal("positive acceptance missing")
				}
				_, _ = client.Write([]byte("{bad}\n"))
			}
			notificationServiceDone(t, done)
			s, exists, err := h.store.read()
			if err != nil || !exists || len(s.Records) != 1 || len(s.Notification.Attempts) != 1 ||
				s.Records[0].Notice.ID != q.NoticeID || s.Notification.Binding != q.Binding || s.Fence != "unknown" {
				t.Fatalf("original intent lost: %+v %v", s, err)
			}
			journalCall(t, h, q, "unresolved", "observation-unavailable")
			if err := h.installNotificationBinding(q.Binding); !errors.Is(err, ErrNotificationUnresolved) {
				t.Fatal("lost binding rearmed")
			}
		})
	}
}

// Host cancellation terminates an idle owned reader. Invalid timeouts and a
// datagram endpoint never install correlation; the valid stream controls above
// prove that these rejection checks do not reject every endpoint.
func TestNotificationStreamCancellationAndAdmission(t *testing.T) {
	for _, timeoutMS := range []int{-1, 0, 60001} {
		t.Run(fmt.Sprintf("timeout=%d", timeoutMS), func(t *testing.T) {
			h, q := journalUnit(t, false)
			server, _ := notificationSocketpair(t, syscall.SOCK_STREAM)
			if done, err := h.startNotificationStream(server, q.Binding, timeoutMS); done != nil || !errors.Is(err, ErrNotificationUnresolved) || h.Snapshot().Notification != nil {
				t.Fatal("invalid timeout installed binding")
			}
		})
	}
	h, q := journalUnit(t, false)
	server, _ := notificationSocketpair(t, syscall.SOCK_DGRAM)
	if done, err := h.startNotificationStream(server, q.Binding, 1000); done != nil || !errors.Is(err, ErrNotificationUnresolved) || h.Snapshot().Notification != nil {
		t.Fatal("datagram endpoint admitted")
	}
	_, done := notificationService(t, h, q, 60000)
	h.cancel()
	notificationServiceDone(t, done)
	if len(h.Snapshot().Records) != 0 {
		t.Fatal("cancel created intent")
	}
	journalCall(t, h, q, "unresolved", "observation-unavailable")
	if err := h.installNotificationBinding(q.Binding); !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("canceled service reactivated")
	}
}

// A deliberately saturated owned socket forces the response write to block.
// Expiry still closes it and retains the already accepted original intent;
// these inert prefilled bytes are only a backpressure fixture, not wire output.
func TestNotificationStreamResponseDeadline(t *testing.T) {
	h, q := journalUnit(t, false)
	server, client := notificationSocketpair(t, syscall.SOCK_STREAM)
	if err := server.SetWriteBuffer(1024); err != nil {
		t.Fatal(err)
	}
	_ = server.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	n, err := server.Write(bytes.Repeat([]byte("x"), 1<<20))
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() || n == 0 || n == 1<<20 {
		t.Fatalf("fixture did not create bounded backpressure: %d %v", n, err)
	}
	done, err := h.startNotificationStream(server, q.Binding, 100)
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := EncodeNotificationRequest(q)
	_ = client.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := client.Write(frame); err != nil {
		t.Fatal(err)
	}
	if err := notificationServiceDone(t, done); !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal(err)
	}
	s, exists, err := h.store.read()
	if err != nil || !exists || len(s.Records) != 1 || s.Records[0].Notice.ID != q.NoticeID || s.Fence != "unknown" {
		t.Fatalf("response expiry lost accepted intent: %+v %v", s, err)
	}
}

// Actual Host.Close joins the idle stream before releasing its store/client.
// The runner owns only an inert Go child, whose PID and cleanup are recorded;
// this establishes no native or Source enrollment/use qualification.
func TestNotificationStreamHostClose(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "notification-stream-close-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "r.sock")
	s, err := runner.Start(runner.Config{Socket: socket, Execution: "execution-1", IOTimeout: time.Second,
		Plan: runner.Plan{Binary: binary, Args: []string{"-test.run=^TestNotificationJournalChild$"}, Cwd: dir,
			Env: []string{"SESSION_HOST_NOTIFICATION_CHILD=1", "GORACE=atexit_sleep_ms=0"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("owned child-start pid=%d", s.ChildPID())
	t.Cleanup(func() { _ = s.Close(); t.Logf("owned child-end pid=%d", s.ChildPID()) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := runner.Dial(ctx, socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	h, q := journalUnit(t, false)
	h.client, h.cfg.IOTimeout = client, time.Second
	_, done := notificationService(t, h, q, 60000)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotificationUnresolved) {
			t.Fatal("wrong closed lifecycle result", err)
		}
	default:
		t.Fatal("Host.Close returned before the owned stream terminated")
	}
	server2, _ := notificationSocketpair(t, syscall.SOCK_STREAM)
	if done, err := h.startNotificationStream(server2, q.Binding, 1000); done != nil || !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("closed Host admitted another service")
	}
}

// Expiry closes the endpoint before waiting for the Host state lock. This
// models a busy persistence critical section without claiming cancellation of
// a real fsync or any already issued native effect.
func TestNotificationStreamExpiryClosesBeforeStateLock(t *testing.T) {
	h, q := journalUnit(t, false)
	client, done := notificationService(t, h, q, 100)
	h.mu.Lock()
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	_, writeErr := client.Write([]byte("{"))
	_, readErr := client.Read(make([]byte, 1))
	h.mu.Unlock()
	if writeErr != nil || !errors.Is(readErr, io.EOF) {
		t.Fatalf("endpoint did not close while state lock was held: write=%v read=%v", writeErr, readErr)
	}
	notificationServiceDone(t, done)
	if s := h.Snapshot(); s.Fence != "unknown" || len(s.Records) != 0 {
		t.Fatal("partial expired request mutated intent or failed to fence")
	}
}

// Observe only this inert test process's goroutine metadata. Retain no stack
// payload: the witness must show the decoded stream handler waiting for mu
// at either the custody observation or request admission (both follow decode),
// rather than merely bytes buffered in a socket or an incomplete request.
func notificationQueuedHandler(t *testing.T) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	buf := make([]byte, 256<<10)
	for {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			t.Fatal("queue witness unavailable: bounded goroutine snapshot truncated")
		}
		for _, block := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(block, "[sync.Mutex.Lock") &&
				strings.Contains(block, "(*Host).notificationStreamExchange") {
				t.Log("queue witness: full decoded stream request waiting on Host admission mutex")
				return
			}
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("queue witness unavailable: no decoded stream handler observed waiting on mutex")
		}
	}
}

// A full valid trigger queued before the admission lock must not start new
// persistence after its call expires. Normal admission persists one intent,
// wakes once and permits one synthetic Prepare. The expired control permits
// none. No scheduler/native child runs here, and this does not cancel a store
// operation or native effect that already began inside admission before expiry.
func TestNotificationStreamFullRequestQueuedAcrossExpiry(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%v", queued), func(t *testing.T) {
			h, q := journalUnit(t, false)
			client, done := notificationService(t, h, q, 2000)
			if queued {
				func() {
					h.mu.Lock()
					defer h.mu.Unlock()
					_ = client.SetDeadline(time.Now().Add(4 * time.Second))
					frame, err := EncodeNotificationRequest(q)
					if err != nil {
						t.Fatal(err)
					}
					if n, err := client.Write(frame); err != nil || n != len(frame) {
						t.Fatalf("valid request not supplied: %d %v", n, err)
					}
					notificationQueuedHandler(t)
					// EOF witnesses endpoint closure while mu is still held;
					// the full decoded handler cannot yet enter admission.
					if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) || h.ctx.Err() != nil {
						t.Fatalf("expected expiry before releasing admission lock: %v", err)
					}
				}()
				notificationServiceDone(t, done)
			} else if r := notificationExchange(t, client, bufio.NewReader(client), q); r.Result != "accepted" {
				t.Fatalf("normal trigger not accepted: %+v", r)
			}
			state, exists, err := h.store.read()
			if err != nil || !exists {
				t.Fatal("durable state unavailable", err)
			}
			woke := false
			select {
			case <-h.kick:
				woke = true
			default:
			}
			prepared := h.prepareProtocol()
			prepares := h.driver.(*journalDriver).prepares.Load()
			t.Logf("queued=%v durable_intents=%d correlations=%d wake=%v prepared=%v prepare_calls=%d fence=%s",
				queued, len(state.Records), len(state.Notification.Attempts), woke, prepared, prepares, state.Fence)
			if queued {
				if len(state.Records) != 0 || len(state.Notification.Attempts) != 0 || woke || prepared || prepares != 0 {
					t.Fatal("expired queued trigger began a new durable intent or scheduler action")
				}
				if state.Fence != "unknown" {
					t.Fatal("expired lifecycle not fenced")
				}
			} else {
				if len(state.Records) != 1 || len(state.Notification.Attempts) != 1 || state.Records[0].Notice.ID != q.NoticeID || !woke || !prepared || prepares != 1 {
					t.Fatal("normal positive did not retain/prepare the original intent exactly once")
				}
				_ = client.Close()
				notificationServiceDone(t, done)
			}
		})
	}
}
