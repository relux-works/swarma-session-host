package providerprotocol

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// Only the nested source response changes between the withheld case and its
// valid neighbor. Received metadata bounds the wait but never admits enrollment.
func TestReceivedLeaseBoundsNestedRecheck(t *testing.T) {
	for _, mode := range []string{"withheld", "rechecked", "expired"} {
		t.Run(mode, func(t *testing.T) {
			sent := make(chan time.Time, 1)
			peerClosed := make(chan struct{})
			var rechecked, installed atomic.Bool
			a := script(t, func(conn *net.UnixConn) {
				prelude(t, conn)
				f := read(t, conn, "bootstrap")
				reply(t, conn, f.ID, "recheck_host", nil)
				read(t, conn, "host_checked")
				e := enrolled(bootstrap())
				expiry := time.Now().Add(150 * time.Millisecond)
				if mode == "rechecked" {
					expiry = time.Now().Add(time.Second)
				} else if mode == "expired" {
					expiry = time.Now().Add(-time.Second)
				}
				e.LeaseExpiresAt = expiry.UTC().Format(time.RFC3339Nano)
				reply(t, conn, f.ID, "enrollment", e)
				if mode != "expired" {
					read(t, conn, "recheck")
				}
				sent <- expiry
				if mode != "rechecked" {
					if f, err := ReadFrame(conn); err == nil {
						t.Error("frame after withheld/expired lease", f.Type)
					}
					close(peerClosed)
					return
				}
				rechecked.Store(true)
				reply(t, conn, f.ID, "rechecked", nil)
				read(t, conn, "installed")
				if !installed.Load() {
					t.Error("installed frame before enrollment callback returned")
				}
				reply(t, conn, f.ID, "ack", nil)
				f = read(t, conn, "close")
				reply(t, conn, f.ID, "closed", nil)
			})
			c := prepared(t, a)
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()
			cb := callbacks()
			original := cb.Enroll
			cb.Enroll = func(ctx context.Context, e Enrollment) error {
				if !rechecked.Load() {
					t.Error("metadata admitted without fresh source recheck")
				}
				expires, err := e.expiry()
				if err != nil {
					t.Error(err)
				}
				if d, ok := ctx.Deadline(); !ok || d.After(expires) {
					t.Error("callback context exceeds lease")
				}
				defer installed.Store(true)
				return original(ctx, e)
			}
			result := make(chan error, 1)
			go func() { result <- c.Bootstrap(ctx, bootstrap(), cb) }()
			var expiry time.Time
			select {
			case expiry = <-sent:
			case <-ctx.Done():
				t.Fatal("fake did not reach enrollment/recheck")
			}
			if mode == "rechecked" {
				if err := <-result; err != nil {
					t.Fatal("valid neighbor refused", err)
				}
				if !installed.Load() {
					t.Fatal("valid neighbor not installed")
				}
				if err := c.Close(ctx); err != nil {
					t.Fatal(err)
				}
				return
			}
			wait := 250 * time.Millisecond
			if mode == "withheld" {
				wait += time.Until(expiry)
			}
			select {
			case <-c.Done():
				if mode == "withheld" && time.Now().Before(expiry) {
					t.Error("stream closed before lease expiry")
				}
			case <-time.After(wait):
				t.Error("received expiry did not promptly close pending stream")
				cancel() // Join the original failing control without hiding it.
			}
			if err := <-result; err == nil {
				t.Error("expired/withheld bootstrap succeeded")
			}
			if installed.Load() {
				t.Error("installation after expired/withheld recheck")
			}
			select {
			case <-peerClosed:
			case <-time.After(250 * time.Millisecond):
				t.Error("peer stream did not close")
			}
			if c.Err() == nil {
				t.Error("expiry did not retain terminal failure")
			}
		})
	}
}
