package sessionhost_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

type securityProtocol struct {
	ap.Driver
	stopped *atomic.Bool
}

func (d securityProtocol) Observe(c ap.Chunk) (ap.Transition, error) {
	tr, err := d.Driver.Observe(c)
	if err == nil && d.stopped.Load() {
		tr.View.Activity = ap.SecurityStop
	}
	return tr, err
}

// A synthetic security observation at the pure protocol boundary latches in the
// actual host journal. Later idle, explicit release and replacement cannot clear
// it or dispatch queued input. Live-output tests supply the unlatched control.
func TestProtocolSecurityStopLatch(t *testing.T) {
	cfg, _, _ := setupProtocol(t, "normal")
	factory := cfg.ProtocolFactory
	stopped := new(atomic.Bool)
	cfg.ProtocolFactory = func(cp []byte) (ap.Driver, uint64, error) {
		d, o, e := factory(cp)
		return securityProtocol{d, stopped}, o, e
	}
	h := openHost(t, cfg)
	awaitProtocol(t, h, "idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if err := h.Fence(false); err != nil {
		t.Fatal(err)
	}
	notice := sessionhost.Notice{ID: "waiting", Reference: "synthetic/waiting"}
	if _, err := h.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	stopped.Store(true)
	awaitProtocol(t, h, "security STOP", func(s sessionhost.State) bool {
		return s.SecurityStopped && s.InputState == sessionhost.InputSecurityStop
	})
	stopped.Store(false)
	awaitProtocol(t, h, "idle cannot clear STOP", func(s sessionhost.State) bool { return s.SecurityStopped && s.InputState == sessionhost.InputIdle })
	h.Close()
	next := openHost(t, cfg)
	awaitProtocol(t, next, "restored STOP", func(s sessionhost.State) bool { return s.SecurityStopped && s.InputState == sessionhost.InputIdle })
	if _, err := next.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	if _, err := next.Enqueue(sessionhost.Notice{ID: "forbidden", Reference: "synthetic/forbidden"}); err == nil || err.Error() != "security_stopped" {
		t.Fatalf("new notice: %v", err)
	}
	if err := next.Fence(true); err == nil || err.Error() != "security_stopped" {
		t.Fatalf("release: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if s := next.Snapshot(); !s.SecurityStopped || s.Records[0].State != "queued" || inspectRunner(t, cfg).Writes != 3 {
		t.Fatal("security STOP dispatched input")
	}
}

// Unsupported persisted boundary/release values refuse before claiming or
// writing, preserve the exact journal, and do not prevent the valid snapshot
// from restoring once the isolated corrupt fixture is replaced.
func TestProtocolStoredVersionAndReleaseRefusal(t *testing.T) {
	for _, field := range []string{"version", "release"} {
		t.Run(field, func(t *testing.T) {
			cfg, _, _ := setupProtocol(t, "normal")
			h := openHost(t, cfg)
			before := awaitProtocol(t, h, "idle", func(s sessionhost.State) bool {
				return s.InputState == sessionhost.InputIdle && len(s.Protocol.Pending) == 0
			})
			h.Close()
			path := filepath.Join(cfg.StateDir, "state.json")
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			bad := before
			if field == "version" {
				bad.Protocol.BoundaryVersion = ap.Version + 1
			} else {
				bad.Protocol.ObservedRelease = "other-release"
			}
			data, err := json.Marshal(bad)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			got, err := sessionhost.Open(cfg)
			if got != nil {
				got.Close()
				t.Fatal("unsupported stored metadata opened")
			}
			want := "state_corrupt"
			if field == "release" {
				want = "protocol_release_conflict"
			}
			if err == nil || err.Error() != want {
				t.Fatalf("refusal: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(data) {
				t.Fatal("refusal changed journal")
			}
			if s := inspectRunner(t, cfg); s.Writes != 3 || s.Epoch != before.Epoch || s.Exited {
				t.Fatal("refusal contacted runner")
			}
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			valid := openHost(t, cfg)
			awaitProtocol(t, valid, "valid restored control", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
			if _, err := valid.Enqueue(sessionhost.Notice{ID: "valid", Reference: "synthetic/valid"}); err != nil {
				t.Fatal(err)
			}
			await(t, valid, "valid", "applied")
		})
	}
}

// A synthetic old/incompatible claim response cannot authorize protocol writes.
// Real runner claims are the nearby positive controls in the other host tests.
func TestProtocolRunnerVersionMismatch(t *testing.T) {
	for _, kind := range []string{"protocol", "receipt"} {
		t.Run(kind, func(t *testing.T) {
			cfg, _, dir := setupProtocol(t, "normal")
			socket := filepath.Join(dir, "incompatible.sock")
			listener, err := localipc.Listen(socket)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			result := make(chan string, 1)
			go func() {
				conn, err := listener.AcceptUnix()
				if err != nil {
					result <- "accept_failed"
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(2 * time.Second))
				reader := bufio.NewReader(conn)
				var req runner.Request
				if err := localipc.Read(reader, &req); err != nil {
					result <- "read_failed"
					return
				}
				r := runner.Response{Code: "ok", Instance: strings.Repeat("a", 32), PID: 1, Epoch: req.Epoch, ProtocolVersion: runner.ProtocolVersion, WriteReceiptVersion: runner.WriteReceiptVersion}
				if kind == "protocol" {
					r.ProtocolVersion--
				} else {
					r.WriteReceiptVersion--
				}
				if err := localipc.Write(conn, r); err != nil {
					result <- "reply_failed"
					return
				}
				var forbidden runner.Request
				if localipc.Read(reader, &forbidden) == nil {
					result <- "extra_operation"
					return
				}
				result <- req.Op
			}()
			original := cfg.Socket
			cfg.Socket = socket
			h, err := sessionhost.Open(cfg)
			if h != nil {
				h.Close()
				t.Fatal("incompatible runner opened")
			}
			if err == nil || err.Error() != "runner_protocol_unsupported" {
				t.Fatalf("mismatch refusal: %v", err)
			}
			if op := <-result; op != "claim" {
				t.Fatalf("mismatch sent operation: %s", op)
			}
			cfg.Socket = original
			if s := inspectRunner(t, cfg); s.Writes != 0 || s.Epoch != 0 {
				t.Fatal("mismatch wrote to live child")
			}
		})
	}
}
