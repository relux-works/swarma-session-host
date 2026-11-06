package sessionhost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// This owner is an inert local model, never production manager admission.
type managerFixture struct {
	ctx      context.Context
	deadline time.Time
	deny     atomic.Bool
}

func (m *managerFixture) lifetime() (context.Context, time.Time) { return m.ctx, m.deadline }
func (m *managerFixture) checkNotificationOwner(*Host, notificationSelection) error {
	if m.deny.Load() {
		return ErrNotificationUnresolved
	}
	return nil
}

type enrollmentFixture struct {
	binding    NotificationBinding
	attachment string
	ctx        context.Context
	expires    time.Time
	check      func() error
}

func (e enrollmentFixture) enrollment() (NotificationBinding, string, context.Context, time.Time) {
	return e.binding, e.attachment, e.ctx, e.expires
}
func (e enrollmentFixture) checkEnrollment() error {
	if e.check != nil {
		return e.check()
	}
	return nil
}

func managerUnit(t *testing.T) (*Host, NotificationRequest, *managerFixture, notificationSelection) {
	t.Helper()
	h, q := journalUnit(t, false)
	m := &managerFixture{ctx: context.Background(), deadline: time.Now().Add(5 * time.Second)}
	b := q.Binding
	s := notificationSelection{Member: b.MemberRef, Connection: b.ConnectionRef, Configuration: b.ConfigurationRef,
		ProfileLock: b.ProfileLockRef, WorkflowReference: b.WorkflowReference, WorkflowSHA256: b.WorkflowSHA256}
	t.Cleanup(func() {
		if h.notificationManager != nil {
			_ = h.notificationManager.close()
		}
	})
	return h, q, m, s
}

func prepareManager(t *testing.T, h *Host, m *managerFixture, s notificationSelection) *managedNotification {
	t.Helper()
	a, err := prepareManagedNotification(h, m, s, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func managerEnrollment(q NotificationRequest, handoff sourceBootstrapHandoff) enrollmentFixture {
	return enrollmentFixture{binding: q.Binding, attachment: handoff.AttachmentRef,
		ctx: context.Background(), expires: time.Now().Add(5 * time.Second)}
}

func managerDone(t *testing.T, a *managedNotification) {
	t.Helper()
	select {
	case <-a.done:
	case <-time.After(2 * time.Second):
		t.Fatal("owned lifecycle did not terminate")
	}
}

// A nearby valid owner/binding enables the real local stream, one durable
// intent and same-attempt recovery. Preparing leaks no binding; duplicate
// preparation/transfer/completion cannot replace or disturb the first stream.
// These model checks establish no real Source receipt or native execution.
func TestManagedNotificationLifecycle(t *testing.T) {
	h, q, m, s := managerUnit(t)
	a := prepareManager(t, h, m, s)
	p, err := a.projection()
	if err != nil || p.State != "preparing" || p.Binding != nil || p.WorkflowReference != s.WorkflowReference {
		t.Fatalf("preparing: %+v %v", p, err)
	}
	data, _ := json.Marshal(p)
	if strings.Contains(string(data), "binding") || strings.Contains(string(data), "native_thread") {
		t.Fatal("preparing exposed partial binding")
	}
	if other, err := prepareManagedNotification(h, m, s, 1000); other != nil || !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("second preparation accepted")
	}
	handoff, err := a.sourceBootstrap()
	if err != nil || handoff.Binding.ConsumerGeneration != "" || handoff.Binding.EnrollmentRef != "" {
		t.Fatal("incomplete handoff invented enrollment", err)
	}
	t.Cleanup(func() { _ = handoff.Endpoint.Close() })
	if other, err := a.sourceBootstrap(); other.Endpoint != nil || !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("second endpoint transfer accepted")
	}
	if h.Snapshot().Notification != nil {
		t.Fatal("method binding installed before completion")
	}
	receipt := managerEnrollment(q, handoff)
	p, err = a.completeEnrollment(receipt)
	if err != nil || p.State != "ready" || p.Binding == nil || *p.Binding != q.Binding || p.WorkflowReference != "" {
		t.Fatalf("ready: %+v %v", p, err)
	}
	for _, duplicate := range []verifiedSourceEnrollment{receipt, nil} {
		if _, err := a.completeEnrollment(duplicate); !errors.Is(err, ErrNotificationUnresolved) {
			t.Fatal("duplicate completion accepted")
		}
	}
	reader := bufio.NewReader(handoff.Endpoint)
	if r := notificationExchange(t, handoff.Endpoint, reader, q); r.Result != "accepted" {
		t.Fatalf("valid activation cannot trigger: %+v", r)
	}
	q.Method = "lookup_attempt"
	if r := notificationExchange(t, handoff.Endpoint, reader, q); r.Result != "existing-attempt" {
		t.Fatal("same-attempt recovery missing")
	}
	_ = handoff.Endpoint.Close()
	managerDone(t, a)
	p, _ = a.projection()
	if p.State != "unresolved" || p.Reason != "attempt-unresolved" || p.Binding != nil || len(h.Snapshot().Records) != 1 {
		t.Fatal("loss erased obligation or leaked ready", p)
	}
	if other, err := prepareManagedNotification(h, m, s, 1000); other != nil || !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("terminal lifecycle rearmed")
	}
}

// Every binding field is checked against its independently fixed owner input;
// an otherwise valid substituted field cannot install a service or journal.
// Missing generation/enrollment additionally fail the existing wire predicate.
func TestManagedNotificationBinding(t *testing.T) {
	for field := 0; field < reflect.TypeOf(NotificationBinding{}).NumField(); field++ {
		name := reflect.TypeOf(NotificationBinding{}).Field(field).Name
		t.Run(name, func(t *testing.T) {
			h, q, m, s := managerUnit(t)
			a := prepareManager(t, h, m, s)
			handoff, err := a.sourceBootstrap()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = handoff.Endpoint.Close() })
			e := managerEnrollment(q, handoff)
			v := reflect.ValueOf(&e.binding).Elem().Field(field)
			switch name {
			case "ConsumerGeneration", "EnrollmentRef":
				v.SetString("") // The actual nonempty broker values are its own inputs.
			case "ControllerEpoch":
				v.SetString("2")
			case "WorkflowSHA256":
				v.SetString(strings.Repeat("b", 64))
			default:
				v.SetString("other")
			}
			if _, err := a.completeEnrollment(e); !errors.Is(err, ErrNotificationUnresolved) {
				t.Fatal("substituted binding admitted")
			}
			managerDone(t, a)
			if h.notificationStream != nil || h.Snapshot().Notification != nil || len(h.Snapshot().Records) != 0 {
				t.Fatal("rejected binding installed effects")
			}
		})
	}
}

// Invalid owner/selection/deadline/timeout rejects before the lifecycle claim
// or socket allocation. Boundary timeout values remain legitimate controls.
func TestManagedNotificationPreparation(t *testing.T) {
	for _, timeout := range []int{0, -1, 60001, 1, 60000} {
		t.Run(fmt.Sprintf("timeout=%d", timeout), func(t *testing.T) {
			h, _, m, s := managerUnit(t)
			a, err := prepareManagedNotification(h, m, s, timeout)
			valid := timeout == 1 || timeout == 60000
			if valid != (err == nil) || valid != (a != nil) || (!valid && h.notificationManager != nil) {
				t.Fatalf("timeout %d: %v", timeout, err)
			}
		})
	}
	for _, name := range []string{"owner", "deadline", "selection", "canceled"} {
		t.Run(name, func(t *testing.T) {
			h, _, m, s := managerUnit(t)
			switch name {
			case "owner":
				m.deny.Store(true)
			case "deadline":
				m.deadline = time.Now().Add(-time.Second)
			case "selection":
				s.WorkflowReference = ""
			case "canceled":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				m.ctx = ctx
			}
			if a, err := prepareManagedNotification(h, m, s, 1000); a != nil || !errors.Is(err, ErrNotificationUnresolved) || h.notificationManager != nil {
				t.Fatal("invalid preparation claimed resources")
			}
		})
	}
}

// A blocked live-enrollment check deterministically races the actual terminal
// transitions. Completion resumes only after terminal loss has won, and cannot
// publish late ready, start a method service, or create a durable intent.
func TestManagedNotificationLateCompletion(t *testing.T) {
	for _, loss := range []string{"close", "host-cancel", "owner-cancel", "epoch", "thread", "storage"} {
		t.Run(loss, func(t *testing.T) {
			h, q, m, s := managerUnit(t)
			ownerCtx, ownerCancel := context.WithCancel(context.Background())
			defer ownerCancel()
			m.ctx = ownerCtx
			a := prepareManager(t, h, m, s)
			handoff, err := a.sourceBootstrap()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = handoff.Endpoint.Close() })
			entered, resume := make(chan struct{}), make(chan struct{})
			e := managerEnrollment(q, handoff)
			e.check = func() error { close(entered); <-resume; return nil }
			result := make(chan error, 1)
			go func() { _, err := a.completeEnrollment(e); result <- err }()
			<-entered
			switch loss {
			case "close":
				_ = a.close()
			case "host-cancel":
				h.cancel()
			case "owner-cancel":
				ownerCancel()
			default:
				h.mu.Lock()
				if loss == "storage" {
					h.err = errors.New("fixture storage failure")
					h.invalidateManagedNotificationLocked()
				} else {
					next := cloneState(h.state)
					if loss == "epoch" {
						next.Epoch++
					} else {
						next.Protocol.View.NativeSession = "replacement"
					}
					if err := h.persistLocked(next); err != nil {
						t.Fatal(err)
					}
				}
				h.mu.Unlock()
			}
			managerDone(t, a)
			close(resume)
			select {
			case err := <-result:
				if !errors.Is(err, ErrNotificationUnresolved) {
					t.Fatal("late completion admitted", err)
				}
			case <-time.After(time.Second):
				t.Fatal("completion did not return")
			}
			p, _ := a.projection()
			if p.State != "unresolved" || h.notificationStream != nil || h.Snapshot().Notification != nil {
				t.Fatal("terminal transition lost", p)
			}
		})
	}
}

// Finite preparation and broker lease expiry each terminate without a caller
// poll. Cancellation after valid activation closes the idle stream and joins;
// simultaneous preparations produce one claim and one transferable pair.
func TestManagedNotificationExpiryAndConcurrency(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(map[bool]string{false: "preparation", true: "source-lease"}[ready], func(t *testing.T) {
			h, q, m, s := managerUnit(t)
			if !ready {
				m.deadline = time.Now().Add(100 * time.Millisecond)
			}
			a := prepareManager(t, h, m, s)
			if ready {
				handoff, err := a.sourceBootstrap()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = handoff.Endpoint.Close() })
				e := managerEnrollment(q, handoff)
				e.expires = time.Now().Add(100 * time.Millisecond)
				if _, err := a.completeEnrollment(e); err != nil {
					t.Fatal(err)
				}
			}
			managerDone(t, a)
			p, _ := a.projection()
			if p.State != "unresolved" || len(h.Snapshot().Records) != 0 {
				t.Fatal("expiry created effects or remained ready")
			}
		})
	}
	h, _, m, s := managerUnit(t)
	var wg sync.WaitGroup
	var successes atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := prepareManagedNotification(h, m, s, 1000); err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatal("concurrent preparations did not produce one owner", successes.Load())
	}
}

// Broker admission is distinct from syntactic binding equality: rejected,
// canceled or expired live proof cannot install anything. An admitted busy
// Host can retain its attachment while Trigger refuses without an intent.
func TestManagedNotificationEnrollmentGates(t *testing.T) {
	for _, name := range []string{"denied", "attachment", "expired", "canceled", "owner-revoked", "busy"} {
		t.Run(name, func(t *testing.T) {
			h, q, m, s := managerUnit(t)
			a := prepareManager(t, h, m, s)
			handoff, err := a.sourceBootstrap()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = handoff.Endpoint.Close() })
			e := managerEnrollment(q, handoff)
			switch name {
			case "denied":
				e.check = func() error { return ErrNotificationUnresolved }
			case "attachment":
				e.attachment = "other"
			case "expired":
				e.expires = time.Now().Add(-time.Second)
			case "canceled":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				e.ctx = ctx
			case "owner-revoked":
				m.deny.Store(true)
			case "busy":
				h.mu.Lock()
				h.state.InputState = InputBusy
				h.mu.Unlock()
			}
			p, err := a.completeEnrollment(e)
			if name == "busy" {
				if err != nil || p.State != "ready" {
					t.Fatal("busy observation treated as missing enrollment", p, err)
				}
				r := notificationExchange(t, handoff.Endpoint, bufio.NewReader(handoff.Endpoint), q)
				if r.Result != "refused" || r.Reason != "input-not-ready" || len(h.Snapshot().Records) != 0 {
					t.Fatal("attachment granted busy input", r)
				}
			} else {
				if !errors.Is(err, ErrNotificationUnresolved) {
					t.Fatal("invalid admission accepted")
				}
				managerDone(t, a)
				if h.notificationStream != nil || h.Snapshot().Notification != nil {
					t.Fatal("rejected admission installed effects")
				}
			}
		})
	}
}

// Incomplete native observation remains preparing without transferring a pair;
// only a later actual Host observation can freeze the private handoff. The
// pair's actual descriptors carry CLOEXEC; this is metadata verification, not
// production native/tool-child inheritance qualification.
func TestManagedNotificationObservationAndDescriptors(t *testing.T) {
	h, _, m, s := managerUnit(t)
	h.mu.Lock()
	h.state.Protocol.View.NativeSession = ""
	h.mu.Unlock()
	a := prepareManager(t, h, m, s)
	if handoff, err := a.sourceBootstrap(); handoff.Endpoint != nil || !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("missing native observation transferred an endpoint")
	}
	p, _ := a.projection()
	if p.State != "preparing" || a.frozen != nil || a.transferred {
		t.Fatal("incomplete observation fabricated terminal/frozen identity")
	}
	for _, conn := range []*net.UnixConn{a.hostEnd, a.sourceEnd} {
		raw, err := conn.SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var flags uintptr
		var errno syscall.Errno
		if err := raw.Control(func(fd uintptr) { flags, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFD, 0) }); err != nil || errno != 0 || flags&syscall.FD_CLOEXEC == 0 {
			t.Fatal("owned endpoint is inheritable", err, errno, flags)
		}
	}
	h.mu.Lock()
	h.state.Protocol.View.NativeSession = "thread-1"
	h.mu.Unlock()
	handoff, err := a.sourceBootstrap()
	if err != nil || handoff.Binding.NativeThread != "thread-1" {
		t.Fatal("actual complete observation could not freeze handoff", err)
	}
	_ = handoff.Endpoint.Close()
}
