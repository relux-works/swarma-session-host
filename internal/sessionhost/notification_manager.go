package sessionhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"strconv"
	"syscall"
	"time"
)

// These private interfaces are implemented by the admitted manager/broker
// boundary, not decoded from a request. There is intentionally no public proof
// constructor or production default verifier. Local fixtures model that boundary
// but cannot establish the real manager's or broker's admission.
type notificationManagerCustody interface {
	lifetime() (context.Context, time.Time)
	checkNotificationOwner(*Host, notificationSelection) error
}

type notificationSelection struct {
	Member, Connection, Configuration, ProfileLock string
	WorkflowReference, WorkflowSHA256              string
}

func (s notificationSelection) valid() bool {
	return notificationRef(s.Member, 512) && notificationRef(s.Connection, 128) &&
		notificationRef(s.Configuration, 512) && notificationRef(s.ProfileLock, 512) &&
		scalar.MatchString(s.WorkflowReference) && notificationSHA256(s.WorkflowSHA256)
}

type verifiedSourceEnrollment interface {
	// The broker-owned implementation checks its actual live callback/receipt,
	// not just equality with the binding returned by this projection.
	enrollment() (NotificationBinding, string, context.Context, time.Time)
	checkEnrollment() error
}

type sourceBootstrapHandoff struct {
	Endpoint      *net.UnixConn
	AttachmentRef string
	Binding       NotificationBinding // Generation/enrollment are not yet known.
	CallTimeoutMS int
}

type notificationProjection struct {
	Contract          string               `json:"contract"`
	State             string               `json:"state"`
	AttachmentRef     string               `json:"attachment_ref,omitempty"`
	WorkflowReference string               `json:"workflow_reference,omitempty"`
	WorkflowSHA256    string               `json:"workflow_sha256,omitempty"`
	Binding           *NotificationBinding `json:"binding,omitempty"`
	Reason            string               `json:"reason,omitempty"`
}

// All fields except done are protected by the owning Host.mu. The lifecycle
// claim is never reset; even a failed bootstrap cannot allocate a second pair.
type managedNotification struct {
	host          *Host
	custody       notificationManagerCustody
	ownerContext  context.Context
	selection     notificationSelection
	deadline      time.Time
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	enrolled      chan struct{}
	attachment    string
	callTimeoutMS int
	hostEnd       *net.UnixConn
	sourceEnd     *net.UnixConn
	transferred   bool
	frozen        *NotificationBinding
	binding       *NotificationBinding
	sourceContext context.Context
	leaseExpires  time.Time
	streamDone    <-chan error
	state         string
}

func prepareManagedNotification(h *Host, custody notificationManagerCustody, selection notificationSelection, timeoutMS int) (*managedNotification, error) {
	if h == nil || custody == nil || !selection.valid() || timeoutMS < 1 || timeoutMS > 60000 {
		return nil, ErrNotificationUnresolved
	}
	ownerContext, deadline := custody.lifetime()
	if ownerContext == nil || ownerContext.Err() != nil || !time.Now().Before(deadline) || custody.checkNotificationOwner(h, selection) != nil {
		return nil, ErrNotificationUnresolved
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil || h.err != nil || h.state.StopRequested || h.notificationClosed || h.notificationStream != nil || h.notificationManager != nil || ownerContext.Err() != nil || !time.Now().Before(deadline) {
		return nil, ErrNotificationUnresolved
	}
	ctx, cancel := context.WithCancel(h.ctx)
	a := &managedNotification{host: h, custody: custody, ownerContext: ownerContext, selection: selection,
		deadline: deadline, ctx: ctx, cancel: cancel, done: make(chan struct{}), enrolled: make(chan struct{}), callTimeoutMS: timeoutMS, state: "preparing"}
	h.notificationManager = a // Claim before allocating, including allocation failure.
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		a.failLocked()
		close(a.done)
		return nil, ErrNotificationUnresolved
	}
	a.attachment = "host-attachment-" + hex.EncodeToString(id[:])
	var err error
	a.hostEnd, a.sourceEnd, err = notificationPair()
	if err != nil {
		a.failLocked()
		close(a.done)
		return nil, ErrNotificationUnresolved
	}
	h.wg.Add(1) // Same lock/barrier used by Host.Close and stream admission.
	go a.watch()
	return a, nil
}

// ForkLock covers socket creation and CLOEXEC marking. net.FileConn duplicates
// into Go's non-inheritable poller; the temporary originals are always closed.
func notificationPair() (*net.UnixConn, *net.UnixConn, error) {
	syscall.ForkLock.RLock()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err == nil {
		for _, fd := range fds {
			syscall.CloseOnExec(fd)
		}
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	var ends [2]*net.UnixConn
	for i, fd := range fds {
		file := os.NewFile(uintptr(fd), "host-notification")
		conn, convertErr := net.FileConn(file)
		_ = file.Close()
		if convertErr != nil {
			for _, end := range ends {
				if end != nil {
					_ = end.Close()
				}
			}
			for _, remaining := range fds[i+1:] {
				_ = syscall.Close(remaining)
			}
			return nil, nil, convertErr
		}
		ends[i] = conn.(*net.UnixConn)
	}
	return ends[0], ends[1], nil
}

func (a *managedNotification) currentLocked() bool {
	return a.state != "unresolved" && a.ctx.Err() == nil && a.ownerContext.Err() == nil &&
		time.Now().Before(a.deadline) && (a.sourceContext == nil || (a.sourceContext.Err() == nil && time.Now().Before(a.leaseExpires)))
}

func (a *managedNotification) sourceBootstrap() (sourceBootstrapHandoff, error) {
	if a.custody.checkNotificationOwner(a.host, a.selection) != nil {
		_ = a.close()
		return sourceBootstrapHandoff{}, ErrNotificationUnresolved
	}
	h := a.host
	h.mu.Lock()
	defer h.mu.Unlock()
	if !a.currentLocked() || a.transferred || h.err != nil || h.state.StopRequested {
		return sourceBootstrapHandoff{}, ErrNotificationUnresolved
	}
	s := a.selection
	b := NotificationBinding{Execution: h.state.Execution, ControllerEpoch: strconv.FormatUint(h.state.Epoch, 10),
		RunnerInstance: h.state.RunnerInstance, MemberRef: s.Member, ConnectionRef: s.Connection,
		ConfigurationRef: s.Configuration, ProfileLockRef: s.ProfileLock, WorkflowReference: s.WorkflowReference, WorkflowSHA256: s.WorkflowSHA256}
	if h.state.Protocol != nil {
		b.NativeThread = h.state.Protocol.View.NativeSession
	}
	// This is only the Host-owned identity predicate. Full binding validity
	// remains mandatory when the broker supplies its actual enrollment.
	if !h.notificationHostIdentityMatchesLocked(b) {
		return sourceBootstrapHandoff{}, ErrNotificationUnresolved // Still preparing.
	}
	a.frozen = &b
	a.transferred = true
	end := a.sourceEnd
	a.sourceEnd = nil // Ownership passes once, even if the broker later refuses.
	return sourceBootstrapHandoff{Endpoint: end, AttachmentRef: a.attachment, Binding: b, CallTimeoutMS: a.callTimeoutMS}, nil
}

func (a *managedNotification) completeEnrollment(receipt verifiedSourceEnrollment) (notificationProjection, error) {
	return a.completeEnrollmentChecked(receipt, func() error {
		return a.custody.checkNotificationOwner(a.host, a.selection)
	})
}

// An adapter that freshly rechecks the provider within Bootstrap can supply the
// manager-only custody check here without recursively entering its operation.
// The actual receipt is still checked below.
func (a *managedNotification) completeEnrollmentChecked(receipt verifiedSourceEnrollment, checkOwner func() error) (notificationProjection, error) {
	a.host.mu.Lock()
	preparing := a.state == "preparing"
	a.host.mu.Unlock()
	if !preparing {
		return notificationProjection{}, ErrNotificationUnresolved
	}
	if receipt == nil || checkOwner == nil || checkOwner() != nil || receipt.checkEnrollment() != nil {
		a.host.mu.Lock()
		if a.state == "preparing" {
			a.failLocked()
		}
		a.host.mu.Unlock()
		return notificationProjection{}, ErrNotificationUnresolved
	}
	b, attachment, sourceContext, expiry := receipt.enrollment()
	h := a.host
	h.mu.Lock()
	defer h.mu.Unlock()
	// A repeated callback cannot consume or close a live service. A terminal
	// lifecycle stays terminal; its first claim and durable journal survive.
	if a.state != "preparing" {
		return notificationProjection{}, ErrNotificationUnresolved
	}
	if !a.currentLocked() || !a.transferred || a.frozen == nil || sourceContext == nil || sourceContext.Err() != nil || !time.Now().Before(expiry) || attachment != a.attachment || !b.valid() {
		a.failLocked()
		return notificationProjection{}, ErrNotificationUnresolved
	}
	expected := *a.frozen
	expected.ConsumerGeneration, expected.EnrollmentRef = b.ConsumerGeneration, b.EnrollmentRef
	if b != expected || !h.notificationHostMatchesLocked(b) || h.err != nil || h.state.StopRequested {
		a.failLocked()
		return notificationProjection{}, ErrNotificationUnresolved
	}
	// Admission and service installation share mu with terminal transitions.
	// Source's callback must not await the installed ACK or method service here.
	a.sourceContext, a.leaseExpires = sourceContext, expiry
	done, err := h.startNotificationStreamLocked(a.hostEnd, b, a.callTimeoutMS)
	if err != nil {
		a.hostEnd = nil // The stream call consumed this endpoint, even on refusal.
		a.failLocked()
		return notificationProjection{}, ErrNotificationUnresolved
	}
	a.streamDone = done
	if !a.currentLocked() || a.state != "preparing" {
		a.failLocked()
		return notificationProjection{}, ErrNotificationUnresolved
	}
	a.binding, a.state = &b, "ready"
	close(a.enrolled)
	return a.projectionLocked(), nil
}

func (a *managedNotification) failLocked() {
	a.state = "unresolved"
	a.cancel()
	// Closing the endpoint does not join the service or acquire Host.mu. Close
	// before persistence can block; the watcher joins outside this lock.
	if a.hostEnd != nil {
		_ = a.hostEnd.Close()
	}
	if a.sourceEnd != nil {
		_ = a.sourceEnd.Close()
		a.sourceEnd = nil
	}
}

func (h *Host) invalidateManagedNotificationLocked() {
	a := h.notificationManager
	if a == nil || a.state == "unresolved" {
		return
	}
	if h.err != nil || h.state.StopRequested || !a.currentLocked() {
		a.failLocked()
		return
	}
	if a.frozen != nil {
		b := *a.frozen
		if !h.notificationHostIdentityMatchesLocked(b) {
			a.failLocked()
		}
	}
}

func (a *managedNotification) watch() {
	defer a.host.wg.Done()
	defer close(a.done)
	timer := time.NewTimer(time.Until(a.deadline))
	defer timer.Stop()
	select {
	case <-a.ctx.Done():
	case <-a.ownerContext.Done():
	case <-timer.C:
	case <-a.enrolled:
		a.host.mu.Lock()
		sourceContext, expires, streamDone := a.sourceContext, a.leaseExpires, a.streamDone
		a.host.mu.Unlock()
		lease := time.NewTimer(time.Until(expires))
		defer lease.Stop()
		select {
		case <-a.ctx.Done():
		case <-a.ownerContext.Done():
		case <-sourceContext.Done():
		case <-timer.C:
		case <-lease.C:
		case <-streamDone:
		}
	}
	a.host.mu.Lock()
	a.failLocked()
	done := a.streamDone
	a.host.mu.Unlock()
	if done != nil {
		<-done // Only outside mu; the service itself needs mu to fence loss.
	}
}

func (a *managedNotification) projectionLocked() notificationProjection {
	p := notificationProjection{Contract: NotificationContract, State: a.state, AttachmentRef: a.attachment}
	if a.state == "ready" {
		b := *a.binding
		p.Binding = &b
	} else {
		p.WorkflowReference, p.WorkflowSHA256 = a.selection.WorkflowReference, a.selection.WorkflowSHA256
		if a.state == "unresolved" {
			p.Reason = "observation-unavailable"
			if a.host.state.Notification != nil && len(a.host.state.Notification.Attempts) > 0 {
				p.Reason = "attempt-unresolved"
			}
		}
	}
	return p
}

func (a *managedNotification) projection() (notificationProjection, error) {
	if a.custody.checkNotificationOwner(a.host, a.selection) != nil {
		_ = a.close()
	}
	a.host.mu.Lock()
	defer a.host.mu.Unlock()
	a.host.invalidateManagedNotificationLocked()
	return a.projectionLocked(), nil
}

func (a *managedNotification) close() error {
	a.host.mu.Lock()
	a.failLocked()
	a.host.mu.Unlock()
	<-a.done
	return nil
}
