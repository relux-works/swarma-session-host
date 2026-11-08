package providerprotocol

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"syscall"
	"time"
)

// Callbacks must honor ctx, retain their own admitted objects and return without
// awaiting ACK, a method call, this client's methods or Done. Enrollment is only
// wire metadata; Enroll must perform actual live verification and installation
// in the owned Host composition. Only an internal inert docking is supplied;
// no real origin verification or production composition is provided.
type Callbacks struct {
	RecheckHost func(context.Context) error
	Enroll      func(context.Context, Enrollment) error
}

// Client owns one already transferred Unix STREAM and no other resource.
// Operations are serialized. A terminal error closes the stream permanently;
// neither retries, fallback, reconnection nor re-enrollment are implemented.
type Client struct {
	conn                       *net.UnixConn
	mu                         sync.Mutex
	gate                       chan struct{}
	events                     chan Frame
	done, readDone             chan struct{}
	terminal                   bool
	err                        error
	expected                   string
	expectedID                 uint32
	sequence                   uint32
	phase                      string // mutated only while holding gate
	refs                       References
	prepare                    PrepareRequest
	custodyExpiry, leaseExpiry time.Time
	timer                      *time.Timer
	callbackDone               <-chan struct{}
}

// NewClient consumes conn even on refusal. It negotiates version before sending
// any effectful request. ctx owns this handshake, not the later lifetime.
// custodyExpiry is the finite independently retained owner lifetime.
func NewClient(ctx context.Context, conn *net.UnixConn, custodyExpiry time.Time) (*Client, error) {
	if conn == nil {
		return nil, ErrState
	}
	if ctx == nil || !time.Now().Before(custodyExpiry) || !stream(conn) {
		_ = conn.Close()
		return nil, ErrState
	}
	c := &Client{conn: conn, gate: make(chan struct{}, 1), events: make(chan Frame, 1), done: make(chan struct{}), readDone: make(chan struct{}), custodyExpiry: custodyExpiry, phase: "new"}
	c.mu.Lock()
	c.timer = time.AfterFunc(time.Until(custodyExpiry), func() { c.terminate(ErrUnresolved) })
	c.mu.Unlock()
	go c.readLoop()
	if err := c.operation(ctx, MaxOperation, func(ctx context.Context, id uint32) error {
		_, err := c.exchange(id, "hello", "hello_ok", nil)
		if err == nil {
			c.phase = "negotiated"
		}
		return err
	}); err != nil {
		c.terminate(err)
		_ = c.join(time.Now().Add(CleanupBound))
		return nil, err
	}
	return c, nil
}

func stream(conn *net.UnixConn) bool {
	if conn.LocalAddr().Network() != "unix" {
		return false
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return false
	}
	var kind int
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		kind, socketErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TYPE)
	})
	return err == nil && socketErr == nil && kind == syscall.SOCK_STREAM
}

func (c *Client) Done() <-chan struct{} { return c.done }
func (c *Client) Err() error            { c.mu.Lock(); defer c.mu.Unlock(); return c.err }

func (c *Client) terminate(err error) {
	c.mu.Lock()
	if c.terminal {
		c.mu.Unlock()
		return
	}
	c.terminal, c.err = true, err
	if c.timer != nil {
		c.timer.Stop()
	}
	close(c.done)
	c.mu.Unlock()
	_ = c.conn.Close()
}

func (c *Client) readLoop() {
	defer close(c.readDone)
	for {
		f, err := ReadFrame(c.conn)
		if err != nil {
			c.terminate(err)
			return
		}
		c.mu.Lock()
		if c.terminal {
			c.mu.Unlock()
			return
		}
		if c.expected == "" || f.ID != c.expectedID || (f.Type != c.expected && f.Type != "refused") {
			c.mu.Unlock()
			c.terminate(ErrState)
			return
		}
		c.expected = ""
		if f.Type == "refused" {
			var r Refusal
			_ = json.Unmarshal(f.Body, &r)
			c.mu.Unlock()
			c.terminate(RefusedError{Reason: r.Reason})
			return
		}
		select {
		case c.events <- f:
			c.mu.Unlock()
		default:
			c.mu.Unlock()
			c.terminate(ErrState)
			return
		}
		if f.Type == "closed" {
			c.terminate(nil)
			return
		}
	}
}

func earlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
func (c *Client) lifetime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.custodyExpiry
	if !c.leaseExpiry.IsZero() {
		d = earlier(d, c.leaseExpiry)
	}
	return d
}

// operation gives gate wait, the entire exchange and all callbacks one absolute
// budget. No per-frame or callback progress can renew it.
func (c *Client) operation(parent context.Context, bound time.Duration, fn func(context.Context, uint32) error) error {
	return c.operate(parent, bound, false, fn)
}

func (c *Client) operate(parent context.Context, bound time.Duration, gateOwned bool, fn func(context.Context, uint32) error) error {
	if parent == nil {
		if gateOwned {
			<-c.gate
		}
		return ErrState
	}
	d := earlier(time.Now().Add(bound), c.lifetime())
	if p, ok := parent.Deadline(); ok {
		d = earlier(d, p)
	}
	ctx, cancel := context.WithDeadline(parent, d)
	defer cancel()
	if !gateOwned {
		select {
		case c.gate <- struct{}{}:
		case <-ctx.Done():
			return ErrUnresolved
		case <-c.done:
			return c.terminalError()
		}
	}
	defer func() { <-c.gate }()
	if ctx.Err() != nil {
		return ErrUnresolved
	}
	select {
	case <-c.done:
		return c.terminalError()
	default:
	}
	if c.sequence == ^uint32(0) {
		c.terminate(ErrState)
		return ErrState
	}
	c.sequence++
	if c.conn.SetDeadline(d) != nil {
		c.terminate(ErrUnresolved)
		return ErrUnresolved
	}
	fired := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { c.terminate(ErrUnresolved); close(fired) })
	err := fn(ctx, c.sequence)
	if !stop() {
		<-fired
	}
	if ctx.Err() != nil && err == nil {
		err = ErrUnresolved
	}
	if err == nil {
		select {
		case <-c.done:
			err = c.Err()
		default:
		}
	}
	if err != nil {
		c.terminate(err)
		return err
	}
	if c.conn.SetDeadline(c.lifetime()) != nil {
		select {
		case <-c.done:
			return c.Err()
		default:
		}
		c.terminate(ErrUnresolved)
		return ErrUnresolved
	}
	return nil
}

func (c *Client) terminalError() error {
	if err := c.Err(); err != nil {
		return err
	}
	return ErrState
}

func (c *Client) send(id uint32, kind, expected string, body any) error {
	f, err := NewFrame(id, kind, body)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.terminal || c.expected != "" {
		c.mu.Unlock()
		return ErrState
	}
	c.expected, c.expectedID = expected, id
	c.mu.Unlock()
	if err := WriteFrame(c.conn, f); err != nil {
		c.terminate(err)
		return err
	}
	return nil
}

func (c *Client) receive() (Frame, error) {
	var f Frame
	select {
	case f = <-c.events:
	case <-c.done:
		if err := c.Err(); err != nil {
			return Frame{}, err
		}
		select {
		case f = <-c.events:
		default:
			return Frame{}, ErrState
		}
	}
	if err := c.Err(); err != nil {
		return Frame{}, err
	}
	if f.Type == "refused" {
		var r Refusal
		_ = json.Unmarshal(f.Body, &r)
		err := RefusedError{Reason: r.Reason}
		c.terminate(err)
		return Frame{}, err
	}
	return f, nil
}
func (c *Client) exchange(id uint32, kind, expected string, body any) (Frame, error) {
	if err := c.send(id, kind, expected, body); err != nil {
		return Frame{}, err
	}
	return c.receive()
}

func (c *Client) Prepare(ctx context.Context, p PrepareRequest) (References, error) {
	var result References
	if !p.valid() {
		return result, ErrFrame
	}
	err := c.operation(ctx, MaxOperation, func(_ context.Context, id uint32) error {
		if c.phase != "negotiated" {
			return ErrState
		}
		f, err := c.exchange(id, "prepare", "prepared", p)
		if err != nil {
			return err
		}
		_ = json.Unmarshal(f.Body, &result) // ReadFrame has validated the exact DTO.
		if result.ProfileLockRef != p.ProfileLockRef {
			return ErrState
		}
		c.refs, c.prepare, c.phase = result, p, "prepared"
		return nil
	})
	if err != nil {
		return References{}, err
	}
	return result, nil
}

func (c *Client) Recheck(ctx context.Context) error {
	return c.operation(ctx, MaxOperation, func(_ context.Context, id uint32) error {
		if c.phase != "prepared" && c.phase != "ready" {
			return ErrState
		}
		_, err := c.exchange(id, "recheck", "rechecked", nil)
		return err
	})
}

func (c *Client) callback(ctx context.Context, fn func() error) error {
	exit := make(chan struct{})
	result := make(chan error, 1)
	c.mu.Lock()
	if c.terminal {
		c.mu.Unlock()
		return c.terminalError()
	}
	c.callbackDone = exit
	c.mu.Unlock()
	go func() { defer close(exit); result <- fn() }()
	select {
	case err := <-result:
		<-exit
		if ctx.Err() != nil {
			return ErrUnresolved
		}
		select {
		case <-c.done:
			return c.terminalError()
		default:
		}
		return err
	case <-ctx.Done():
		return ErrUnresolved
	case <-c.done:
		return c.terminalError()
	}
}

func (c *Client) Bootstrap(ctx context.Context, b BootstrapRequest, cb Callbacks) error {
	if !b.valid() || cb.RecheckHost == nil || cb.Enroll == nil {
		return ErrFrame
	}
	return c.operation(ctx, MaxOperation, func(ctx context.Context, id uint32) error {
		if c.phase != "prepared" || b.Binding.references() != c.refs || b.Binding.WorkflowReference != c.prepare.WorkflowReference || b.Binding.WorkflowSHA256 != c.prepare.WorkflowSHA256 {
			return ErrState
		}
		c.phase = "bootstrapping" // A sent bootstrap is never retryable.
		if _, err := c.exchange(id, "bootstrap", "recheck_host", b); err != nil {
			return err
		}
		if err := c.callback(ctx, func() error { return cb.RecheckHost(ctx) }); err != nil {
			return ErrUnresolved
		}
		f, err := c.exchange(id, "host_checked", "enrollment", nil)
		if err != nil {
			return err
		}
		var e Enrollment
		_ = json.Unmarshal(f.Body, &e)
		expires, err := e.expiry()
		if err != nil || !time.Now().Before(expires) {
			return ErrState
		}
		// Received metadata is only a conservative lifetime bound, not
		// admission. Apply it before waiting for the live source recheck.
		c.mu.Lock()
		if c.terminal {
			c.mu.Unlock()
			return c.terminalError()
		}
		if c.leaseExpiry.IsZero() || expires.Before(c.leaseExpiry) {
			c.leaseExpiry = expires
		}
		lifetime := earlier(c.custodyExpiry, c.leaseExpiry)
		c.timer.Reset(time.Until(lifetime))
		c.mu.Unlock()
		ctx, cancel := context.WithDeadline(ctx, lifetime)
		defer cancel()
		deadline, _ := ctx.Deadline()
		if err := c.conn.SetDeadline(deadline); err != nil {
			return ErrUnresolved
		}
		// Enter the enrollment callback handler with a fresh live source recheck
		// of its retained object, before local comparisons and installation. This
		// is a nested exchange under the Bootstrap ID, not a second operation or
		// a recursive call through the composite Host recheck.
		if _, err := c.exchange(id, "recheck", "rechecked", nil); err != nil {
			return err
		}
		expected := b.Binding
		expected.ConsumerGeneration, expected.EnrollmentRef = e.Binding.ConsumerGeneration, e.Binding.EnrollmentRef
		if e.Binding != expected || e.AttachmentRef != b.AttachmentRef || !time.Now().Before(expires) {
			return ErrState
		}
		if err := c.callback(ctx, func() error { return cb.Enroll(ctx, e) }); err != nil {
			return ErrUnresolved
		}
		// installed is sent only after the callback has successfully returned.
		if _, err := c.exchange(id, "installed", "ack", nil); err != nil {
			return err
		}
		c.phase = "ready"
		return nil
	})
}

// Close requests protocol cleanup and joins the reader and any owned callback
// within five seconds, or the earlier caller deadline. Failure is never reported
// as clean. An uncooperative Go callback cannot be forcibly terminated.
func (c *Client) Close(ctx context.Context) error {
	if ctx == nil {
		c.terminate(ErrState)
		return ErrState
	}
	d := time.Now().Add(CleanupBound)
	if p, ok := ctx.Deadline(); ok {
		d = earlier(d, p)
	}
	var err error
	select {
	case c.gate <- struct{}{}:
		err = c.operate(ctx, CleanupBound, true, func(_ context.Context, id uint32) error {
			_, err := c.exchange(id, "close", "closed", nil)
			return err
		})
	default:
		// Do not spend the cleanup budget waiting behind bootstrap/callback
		// work. Stream loss cancels that operation before joining its owners.
		err = ErrUnresolved
	}
	c.terminate(err)
	if joined := c.join(d); joined != nil {
		return joined
	}
	// An already cleanly closed client has an idempotent successful Close.
	if c.Err() == nil {
		return nil
	}
	return c.Err()
}

func (c *Client) join(deadline time.Time) error {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-c.readDone:
	case <-timer.C:
		return ErrCleanup
	}
	c.mu.Lock()
	callbackDone := c.callbackDone
	c.mu.Unlock()
	if callbackDone != nil {
		select {
		case <-callbackDone:
		case <-timer.C:
			return ErrCleanup
		}
	}
	return nil
}
