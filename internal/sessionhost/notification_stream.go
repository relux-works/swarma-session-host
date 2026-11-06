package sessionhost

import (
	"bufio"
	"context"
	"net"
	"sync"
	"time"
)

type notificationAttachment struct {
	conn *net.UnixConn
}

// This service is private preparation, not an enrollment or activation API.
// Its future facade must verify actual enrollment and endpoint custody first.
// A supplied Unix stream is consumed by this call, including on rejection.
// Passing the already consumed connection again cannot disturb its owner.
// There is no listener, descriptor discovery, child inheritance or live caller.
func (h *Host) startNotificationStream(conn *net.UnixConn, binding NotificationBinding, timeoutMS int) (<-chan error, error) {
	if conn == nil {
		return nil, ErrNotificationUnresolved
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.startNotificationStreamLocked(conn, binding, timeoutMS)
}

// Caller holds mu through binding installation, so a facade completion cannot
// race a terminal lifecycle transition between its check and service admission.
func (h *Host) startNotificationStreamLocked(conn *net.UnixConn, binding NotificationBinding, timeoutMS int) (<-chan error, error) {
	if conn == nil {
		return nil, ErrNotificationUnresolved
	}
	if h.notificationStream != nil {
		owned := h.notificationStream.conn == conn
		if !owned {
			_ = conn.Close()
		}
		return nil, ErrNotificationUnresolved
	}
	if h.ctx.Err() != nil || h.notificationClosed || timeoutMS < 1 || timeoutMS > 60000 || !binding.valid() || conn.LocalAddr().Network() != "unix" {
		_ = conn.Close()
		return nil, ErrNotificationUnresolved
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, ErrNotificationUnresolved
	}
	h.notificationStream = &notificationAttachment{conn: conn} // Never reset in this lifecycle.
	h.wg.Add(1)
	if err := h.installNotificationBindingLocked(binding); err != nil {
		_ = conn.Close()
		h.wg.Done()
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		defer h.wg.Done()
		done <- h.serveNotificationStream(conn, time.Duration(timeoutMS)*time.Millisecond)
		close(done)
	}()
	return done, nil
}

// The first idle read asks for one octet, so waiting for the first request does
// not consume a call budget. After that, codec reads may fill a bounded buffer.
// Any bytes left for the next call came from the last underlying read: retain
// that timestamp instead of restarting their budget when they are consumed.
type notificationStreamReader struct {
	conn       *net.UnixConn
	first      bool
	lastReadAt time.Time
}

func (r *notificationStreamReader) Read(p []byte) (int, error) {
	if r.first && len(p) > 1 {
		p = p[:1]
	}
	n, err := r.conn.Read(p)
	if n > 0 {
		r.lastReadAt = time.Now()
	}
	return n, err
}

func (h *Host) serveNotificationStream(conn *net.UnixConn, timeout time.Duration) error {
	var loss sync.Once
	lose := func() {
		loss.Do(func() {
			// Close first: persistence may be waiting on a store operation. A
			// caller then has UNKNOWN, never evidence of non-application.
			_ = conn.Close()
			h.loseNotificationBinding()
		})
	}
	stopContext := context.AfterFunc(h.ctx, lose)
	defer stopContext()
	defer lose()
	source := &notificationStreamReader{conn: conn}
	reader := bufio.NewReader(source)
	for {
		if h.ctx.Err() != nil || conn.SetDeadline(time.Time{}) != nil {
			return ErrNotificationUnresolved
		}
		source.first = reader.Buffered() == 0
		if _, err := reader.Peek(1); err != nil {
			return ErrNotificationUnresolved
		}
		source.first = false
		deadline := source.lastReadAt.Add(timeout)
		if !time.Now().Before(deadline) || conn.SetDeadline(deadline) != nil {
			return ErrNotificationUnresolved
		}
		expired := make(chan struct{})
		timer := time.AfterFunc(time.Until(deadline), func() {
			lose()
			close(expired)
		})
		err := h.notificationStreamExchange(conn, reader, deadline)
		if !timer.Stop() {
			// Join a fired callback before touching the next call. It may
			// still be retaining an accepted intent after slow persistence.
			<-expired
			return ErrNotificationUnresolved
		}
		if err != nil {
			return err
		}
	}
}

func (h *Host) notificationStreamExchange(conn *net.UnixConn, reader *bufio.Reader, deadline time.Time) error {
	q, err := ReadNotificationRequest(reader)
	if err != nil {
		return err
	}
	if !time.Now().Before(deadline) || h.ctx.Err() != nil {
		return ErrNotificationUnresolved
	}
	r, err := h.notificationRequestUntil(q, deadline)
	if err != nil {
		return err
	}
	frame, err := EncodeNotificationResponse(r, q)
	if err != nil {
		return err
	}
	// The same absolute deadline remains in force through the response.
	// A failed/partial write cannot retract the already persisted intent.
	n, err := conn.Write(frame)
	if err != nil || n != len(frame) {
		return ErrNotificationUnresolved
	}
	return nil
}
