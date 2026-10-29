package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type coreProxyRunner struct {
	*hostCommitProbeRunner
	target, home, thread string
	policy               ClientToolPolicy
}

func (r *coreProxyRunner) AttachmentTarget(context.Context) (string, error) { return r.target, nil }
func (r *coreProxyRunner) ClientHome() string                               { return r.home }
func (r *coreProxyRunner) NativeIdentity() string                           { return r.thread }
func (r *coreProxyRunner) ClientToolPolicy() ClientToolPolicy               { return r.policy }
func coreProxyFixture(t *testing.T) (*SessionHost, *hostCommitProbeCodec, *coreProxyRunner, AttachRequest) {
	t.Helper()
	h, c, r, q, _ := attachmentProbe(t)
	root := os.Getenv("SESSIONHOST_TEST_SOCKET_ROOT")
	if root == "" {
		t.Fatal("short isolated socket root required")
	}
	dir, e := os.MkdirTemp(root, "proxy-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	h.options.Layout.AttachmentsDir = filepath.Join(dir, "a")
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done })
	runner := &coreProxyRunner{hostCommitProbeRunner: r, target: "ws://" + listener.Addr().String(), home: "  /fixture/client  ", thread: "native-thread", policy: ClientToolPolicy{}}
	// Use the real original host interfaces. Only process/network target supply
	// is a boundary fixture; the core, codec, proxy and filesystem are real.
	h.options.Factories = []ProviderHostFactory{coreProxyFactory{runner, q.Provider}}
	return h, c.hostCommitProbeCodec, runner, q
}

type coreProxyFactory struct {
	r        *coreProxyRunner
	provider string
}

func (f coreProxyFactory) Provider() string { return f.provider }
func (f coreProxyFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	return f.r, nil
}
func (f coreProxyFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return f.r, nil
}

// Actual Attach publishes a working session-owned transport and original client
// metadata; Detach retains it, acknowledged Stop removes it and only an empty
// directory. This is isolated transport, not provider/auth qualification.
func TestHostProxyPublicationDetachAndAcknowledgedStop(t *testing.T) {
	ctx := context.Background()
	h, _, r, q := coreProxyFixture(t)
	a, e := h.Attach(ctx, q)
	if e != nil || a.ManagerEndpoint == "" || a.ClientHome != "/fixture/client" {
		t.Fatalf("actual proxy/metadata absent: %+v %v", a, e)
	}
	// Thread identity is Codex-specific, not copied from every native provider.
	if q.Provider != "codex" && a.CodexThreadID != "" {
		t.Fatal("foreign provider thread projected as Codex")
	}
	u, e := url.Parse(a.ManagerEndpoint)
	if e != nil || u.Scheme != "unix" {
		t.Fatalf("invalid actual endpoint: %v", e)
	}
	connection, e := net.Dial("unix", u.Path)
	if e != nil {
		t.Fatal(e)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if _, e = connection.Write([]byte("echo")); e != nil {
		t.Fatal(e)
	}
	b := make([]byte, 4)
	if _, e = io.ReadFull(connection, b); e != nil || string(b) != "echo" {
		t.Fatalf("real proxy did not relay: %q %v", b, e)
	}
	if _, e = h.Detach(ctx, a.Session.SessionID, a.AttachmentID); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(u.Path); e != nil {
		t.Fatal("detach removed live proxy", e)
	}
	again, e := h.Attach(ctx, q)
	if e != nil || again.ManagerEndpoint != a.ManagerEndpoint {
		t.Fatalf("same controller replaced proxy: %+v %v", again, e)
	}
	if _, e = h.Stop(ctx, a.Session.SessionID); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Lstat(u.Path); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("stopped-final ACK left socket: %v", e)
	}
	if _, e = os.Stat(h.options.Layout.AttachmentsDir); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("last socket left empty directory: %v", e)
	}
	if r.forces != 1 {
		t.Fatal("proxy cleanup duplicated provider stop")
	}
}

// Invalid target setup publishes no client. Failed stopped-final ACK keeps
// the existing proxy until recovery; Close joins its listener without Stop.
func TestHostProxyFailureAndCloseCustody(t *testing.T) {
	for _, mode := range []string{"invalid-target", "foreign-path", "occupied-directory", "final-ack", "close"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h, c, r, q := coreProxyFixture(t)
			if mode == "invalid-target" {
				if e := os.MkdirAll(h.options.Layout.AttachmentsDir, 0700); e != nil {
					t.Fatal(e)
				}
				r.target = "ws://foreign.example:1"
				a, e := h.Attach(ctx, q)
				if e == nil || a != nil || len(h.attachments) != 0 {
					t.Fatalf("bad target published client: %+v %v", a, e)
				}
				return
			}
			a, e := h.Attach(ctx, q)
			if e != nil {
				t.Fatal(e)
			}
			u, _ := url.Parse(a.ManagerEndpoint)

			if mode == "foreign-path" {
				if e = os.Rename(u.Path, u.Path+".owned-backup"); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(u.Path, []byte("foreign fixture"), 0600); e != nil {
					t.Fatal(e)
				}
				if e = h.closeProviderProxyLocked(a.Session.SessionID); e == nil {
					t.Fatal("replaced proxy inode silently removed")
				}
				before := len(h.attachments)
				if next, e := h.Attach(ctx, q); e == nil || next != nil || len(h.attachments) != before {
					t.Fatalf("occupied path published client: %+v %v", next, e)
				}
				b, e := os.ReadFile(u.Path)
				if e != nil || string(b) != "foreign fixture" {
					t.Fatal("foreign path was changed", e)
				}
				return
			}
			if mode == "occupied-directory" {
				keep := filepath.Join(h.options.Layout.AttachmentsDir, "keep")
				if e = os.WriteFile(keep, []byte("keep"), 0600); e != nil {
					t.Fatal(e)
				}
				if _, e = h.Stop(ctx, a.Session.SessionID); e != nil {
					t.Fatal(e)
				}
				b, e := os.ReadFile(keep)
				if e != nil || string(b) != "keep" {
					t.Fatal("cleanup removed foreign directory entry", e)
				}
				return
			}
			if mode == "close" {
				h.Close()
				if r.forces != 0 {
					t.Fatal("Close stopped provider")
				}
				if _, e = os.Stat(u.Path); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("Close left owned proxy", e)
				}
				return
			}
			fault := errors.New("final record ACK unavailable")
			c.ack = func(input LegacyRecordInput) error {
				var record RunnerRecord
				if e := json.Unmarshal(input.Bytes, &record); e != nil {
					return e
				}
				if record.State == SessionStopped {
					return fault
				}
				return nil
			}
			if _, e = h.Stop(ctx, a.Session.SessionID); !errors.Is(e, fault) {
				t.Fatalf("failed final ACK became success: %v", e)
			}
			if _, e = os.Stat(u.Path); e != nil {
				t.Fatal("unacknowledged stopped candidate removed proxy", e)
			}
		})
	}
}

// A fresh real preparation has no client/proxy. Its acknowledged Commit opens
// the actual endpoint before publishing the first attachment. The discard ACK
// supplier here is the existing isolated codec fixture, not default qualification.
func TestHostProxyStagedPublication(t *testing.T) {
	ctx := context.Background()
	h, _, _, q := coreProxyFixture(t)
	p, e := h.PrepareAttach(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = os.Lstat(h.options.Layout.AttachmentsDir); !errors.Is(e, os.ErrNotExist) || len(h.attachments) != 0 {
		t.Fatalf("prepare published transport/client: %v", e)
	}
	a, e := h.CommitAttach(ctx, attachmentCommit(p))
	if e != nil || a.ManagerEndpoint == "" || a.ClientHome != "/fixture/client" {
		t.Fatalf("committed actual staged endpoint absent: %+v %v", a, e)
	}
}
