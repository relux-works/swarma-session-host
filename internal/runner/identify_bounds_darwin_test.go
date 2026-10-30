//go:build darwin && cgo

package runner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// A manual accepted connection permits precise acceptance-time/credential
// fixtures. It still uses registration and the real bounded socket handler.
func identifyAcceptedFixture(t *testing.T, s *Server, accepted time.Time, peer func(*net.UnixConn) (uint32, error)) *net.UnixConn {
	t.Helper()
	l, err := localipc.Listen(filepath.Join(filepath.Dir(s.cfg.Socket), "accepted.sock"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.DialUnix("unix", nil, l.Addr().(*net.UnixAddr))
	if err != nil {
		l.Close()
		t.Fatal(err)
	}
	server, err := l.AcceptUnix()
	l.Close()
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	if !s.register(server) {
		server.Close()
		c.Close()
		t.Fatal("IDENTIFY_FIXTURE_REGISTER")
	}
	go s.handleWithPeerUID(server, accepted, peer)
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(2 * time.Second))
	return c
}

// Late classification cannot reset the accept-based budget. Wrong/unreadable
// peer UID closes before parsing/disclosure; these are declared seams, not a
// cross-UID OS test. Live same-UID timely controls appear in the adjacent suite.
func TestIdentifyAcceptedBoundsAndAuth(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  time.Duration
		peer func(*net.UnixConn) (uint32, error)
	}{
		{"late", 2 * time.Second, localipc.PeerUID},
		{"wrongUID", 0, func(*net.UnixConn) (uint32, error) { return uint32(os.Getuid() + 1), nil }},
		{"missingUID", 0, func(*net.UnixConn) (uint32, error) { return 0, errors.New("unreadable") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, "echo")
			s, _, err := startIdentifyTestRunner(t, cfg)
			if err != nil {
				t.Fatal(err)
			}
			c := identifyAcceptedFixture(t, s, time.Now().Add(-tc.age), tc.peer)
			c.Write(identifyRequestFor(s))
			data, err := bufio.NewReader(c).ReadBytes('\n')
			if err == nil || len(data) != 0 {
				t.Fatalf("IDENTIFY_ACCEPT_AUTH_%s: %q %v", tc.name, data, err)
			}
		})
	}
}

// A reply held past the original configured limit closes without late proof.
// Context cancellation closes only its new client connection; opening the gate
// releases the slot and leaves the ordinary controller usable.
func TestIdentifyDeadlineAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "expired", true: "cancelled"}[cancelled], func(t *testing.T) {
			cfg := testConfig(t, "echo")
			cfg.IOTimeout = 150 * time.Millisecond
			s, _, err := startIdentifyTestRunner(t, cfg)
			if err != nil {
				t.Fatal(err)
			}
			release, open := identifyBarrier(t)
			entered := make(chan struct{}, 1)
			setIdentifyGate(s, func() { entered <- struct{}{}; <-release })
			ctx, cancel := context.WithTimeout(context.Background(), identifyBudget)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := identifyForQualification(ctx, cfg.Socket, os.Getpid(), uint32(os.Getuid()), identifyMatchingRequest(s))
				result <- err
			}()
			awaitIdentifyEvent(t, entered, "IDENTIFY_DEADLINE_SNAPSHOT")
			if cancelled {
				cancel()
			} else {
				timer := time.NewTimer(200 * time.Millisecond)
				<-timer.C
				open()
			}
			select {
			case err := <-result:
				if !errors.Is(err, errIdentifyUnavailable) {
					t.Fatalf("IDENTIFY_DEADLINE_CANCEL: %v", err)
				}
			case <-time.After(300 * time.Millisecond):
				open()
				t.Fatal("IDENTIFY_CANCEL_OWN_CONNECTION")
			}
			open()
			setIdentifyGate(s, nil)
			limit := time.Now().Add(time.Second)
			for len(s.identifySlots) != 0 && time.Now().Before(limit) {
				time.Sleep(time.Millisecond)
			}
			if len(s.identifySlots) != 0 {
				t.Fatal("IDENTIFY_CANCEL_SLOT_RELEASE")
			}
			c := testClient(t, cfg)
			call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
		})
	}
}

// Each fake reply uses a real private same-PID socket and exactly one accepted
// connection. Wrong echoes are conflicts; malformed/old/truncated evidence is
// unavailable. No fallback offer is made. This qualifies only this client.
func TestIdentifyClientResponses(t *testing.T) {
	good := identifySuccessWire()
	for _, tc := range []struct {
		name string
		wire []byte
		want error
	}{
		{"valid", good, nil},
		{"execution", bytes.Replace(good, []byte("exec-a"), []byte("other"), 1), errIdentifyConflict},
		{"instance", bytes.Replace(good, []byte(identifyTestInstance), []byte("ffffffffffffffffffffffffffffffff"), 1), errIdentifyConflict},
		{"version", bytes.Replace(good, []byte("response-v1"), []byte("response-v0"), 1), errIdentifyUnavailable},
		{"legacy", []byte("{\"code\":\"ok\",\"protocol_version\":3}\n"), errIdentifyUnavailable},
		{"noLF", bytes.TrimSuffix(good, []byte("\n")), errIdentifyUnavailable},
		{"truncated", good[:len(good)/2], errIdentifyUnavailable},
		{"oversize", append(bytes.Repeat([]byte(" "), 1025-len(good)), good...), errIdentifyUnavailable},
		{"silence", nil, errIdentifyUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "session-id-reply-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			})
			resourceReceipt(t, "tempdir", 0, dir)
			l, err := localipc.Listen(filepath.Join(dir, "s.sock"))
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			resourceReceipt(t, "socket", 0, l.Addr().String())
			done := make(chan error, 1)
			go func() {
				c, err := l.AcceptUnix()
				if err != nil {
					done <- err
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(time.Second))
				b, err := bufio.NewReader(c).ReadBytes('\n')
				if err != nil {
					done <- err
					return
				}
				if _, err = decodeIdentifyRequest(b); err != nil {
					done <- err
					return
				}
				_, err = c.Write(tc.wire)
				done <- err
			}()
			ctx, cancel := identifyContext(t)
			defer cancel()
			_, err = identifyForQualification(ctx, l.Addr().String(), os.Getpid(), uint32(os.Getuid()), identifyRequest{Schema: identifyRequestSchema, Execution: "exec-a", ExpectedInstance: identifyTestInstance})
			if !errors.Is(err, tc.want) {
				t.Fatalf("IDENTIFY_CLIENT_%s: %v", tc.name, err)
			}
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("IDENTIFY_CLIENT_SINGLE_OFFER")
			}
		})
	}
}

// Cleanup branches are exercised repeatedly, with two subsequent successful
// slots each time. Disconnect, malformed, refusal and success cannot consume a
// permanent classified slot.
func TestIdentifySlotCleanupBranches(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, _, err := startIdentifyTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	good := identifyRequestFor(s)
	for _, b := range [][]byte{[]byte("{\"schema\":null}\n"), bytes.Replace(good, []byte(s.cfg.Execution), []byte("other"), 1), good} {
		for n := 0; n < 3; n++ {
			identifyRaw(t, cfg, b)
			limit := time.Now().Add(time.Second)
			for len(s.identifySlots) != 0 && time.Now().Before(limit) {
				time.Sleep(time.Millisecond)
			}
			if len(s.identifySlots) != 0 {
				t.Fatal("IDENTIFY_BRANCH_SLOT_RELEASE")
			}
			for i := 0; i < 2; i++ {
				r, err := identifyCall(t, s)
				if err != nil || r.Result != "verified" {
					t.Fatalf("IDENTIFY_BRANCH_FRESH_SLOT: %+v %v", r, err)
				}
			}
		}
	}
}

// The server's accepted-time cap honors shorter configured timeouts and never
// grants the configured minute to a classified identify exchange.
func TestIdentifyDeadlinePolicy(t *testing.T) {
	at := time.Unix(1800000000, 0)
	for _, d := range []time.Duration{100 * time.Millisecond, time.Second, time.Minute} {
		want := d
		if want > time.Second {
			want = time.Second
		}
		if got := identifyDeadline(at, d); !got.Equal(at.Add(want)) {
			t.Fatal("IDENTIFY_DEADLINE_POLICY")
		}
	}
}

// Unclassified connections retain the original total-eight cap, independent of
// the two classified identify slots. The ninth closes promptly without proof.
func TestIdentifyLegacyConnectionCap(t *testing.T) {
	cfg := testConfig(t, "echo")
	cfg.IOTimeout = 5 * time.Second
	s, _, err := startIdentifyTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var clients []*net.UnixConn
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()
	for i := 0; i < 8; i++ {
		c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: cfg.Socket, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
	}
	limit := time.Now().Add(time.Second)
	count := 0
	for time.Now().Before(limit) {
		s.mu.Lock()
		count = len(s.conns)
		s.mu.Unlock()
		if count == 8 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if count != 8 {
		t.Fatal("IDENTIFY_LEGACY_CONNECTION_SETUP")
	}
	ninth, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: cfg.Socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer ninth.Close()
	ninth.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var b [1]byte
	n, err := ninth.Read(b[:])
	var timeout net.Error
	if n != 0 || err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
		t.Fatal("IDENTIFY_LEGACY_CONNECTION_CAP")
	}
	if len(s.identifySlots) != 0 {
		t.Fatal("IDENTIFY_UNCLASSIFIED_SLOTS")
	}
}
