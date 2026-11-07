package providerprotocol

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func pair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ends [2]*net.UnixConn
	for i, fd := range fds {
		f := os.NewFile(uintptr(fd), "fixture-stream")
		conn, err := net.FileConn(f)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		ends[i] = conn.(*net.UnixConn)
		t.Cleanup(func() { _ = conn.Close() })
	}
	return ends[0], ends[1]
}

func selection() PrepareRequest {
	return PrepareRequest{"selected-connection", "profile-lock", "workflow", strings.Repeat("a", 64)}
}
func references() References {
	return References{"member", "opaque-connection", "configuration", "profile-lock"}
}
func bootstrap() BootstrapRequest {
	return BootstrapRequest{Binding: Binding{
		Execution: "execution", ControllerEpoch: "1", RunnerInstance: "runner", NativeThread: "native",
		MemberRef: "member", ConnectionRef: "opaque-connection", ConfigurationRef: "configuration", ProfileLockRef: "profile-lock",
		WorkflowReference: "workflow", WorkflowSHA256: strings.Repeat("a", 64),
	}, AttachmentRef: "attachment", CallTimeoutMS: 1000}
}
func enrolled(b BootstrapRequest) Enrollment {
	x := b.Binding
	x.ConsumerGeneration = "issued-generation"
	x.EnrollmentRef = "persisted-enrollment"
	return Enrollment{x, b.AttachmentRef, "source-session", "consumer", "lease-holder", "lease-host", 123, time.Now().Add(5 * time.Second).UTC().Format(time.RFC3339Nano), "process-birth"}
}
func callbacks() Callbacks {
	return Callbacks{RecheckHost: func(context.Context) error { return nil }, Enroll: func(_ context.Context, e Enrollment) error {
		want := enrolled(bootstrap())
		if e.Binding != want.Binding || e.SourceSession != want.SourceSession || e.ConsumerRef != want.ConsumerRef || e.LeaseHolder != want.LeaseHolder || e.LeaseHost != want.LeaseHost || e.LeasePID != want.LeasePID || e.ProcessBirth != want.ProcessBirth {
			return ErrState
		}
		return nil
	}}
}
func read(t *testing.T, c *net.UnixConn, kind string) Frame {
	t.Helper()
	f, err := ReadFrame(c)
	if err != nil {
		t.Fatal("fake read", kind, err)
	}
	if f.Type != kind {
		t.Fatal("unexpected fake request", f.Type, kind)
	}
	return f
}
func reply(t *testing.T, c *net.UnixConn, id uint32, kind string, body any) {
	t.Helper()
	f, err := NewFrame(id, kind, body)
	if err != nil {
		t.Fatal("fake frame", err)
	}
	if err := WriteFrame(c, f); err != nil {
		t.Fatal("fake write", err)
	}
}
func prelude(t *testing.T, c *net.UnixConn) {
	t.Helper()
	f := read(t, c, "hello")
	if f.ID != 1 {
		t.Fatal("first sequence")
	}
	reply(t, c, f.ID, "hello_ok", nil)
	f = read(t, c, "prepare")
	reply(t, c, f.ID, "prepared", references())
}
func bootstrapPrelude(t *testing.T, c *net.UnixConn, e *Enrollment) uint32 {
	t.Helper()
	f := read(t, c, "bootstrap")
	var b BootstrapRequest
	if err := json.Unmarshal(f.Body, &b); err != nil {
		t.Fatal(err)
	}
	reply(t, c, f.ID, "recheck_host", nil)
	g := read(t, c, "host_checked")
	if g.ID != f.ID {
		t.Fatal("callback sequence")
	}
	if e == nil {
		v := enrolled(b)
		e = &v
	}
	reply(t, c, f.ID, "enrollment", *e)
	g = read(t, c, "recheck")
	if g.ID != f.ID {
		t.Fatal("nested source recheck sequence")
	}
	reply(t, c, f.ID, "rechecked", nil)
	return f.ID
}
func script(t *testing.T, fn func(*net.UnixConn)) *net.UnixConn {
	t.Helper()
	a, b := pair(t)
	if err := b.SetDeadline(time.Now().Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); defer b.Close(); fn(b) }()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("fake did not join")
		}
	})
	return a
}
func prepared(t *testing.T, c *net.UnixConn) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := NewClient(ctx, c, time.Now().Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = client.Close(ctx)
	})
	refs, err := client.Prepare(ctx, selection())
	if err != nil {
		t.Fatal(err)
	}
	if refs != references() {
		t.Fatal("wrong retained references")
	}
	return client
}

// The child is this test executable, not a2a. Its sole ExtraFiles entry is fd 3.
// stdin/stdout/stderr are diagnostics, never protocol transports.
func TestProviderChild(t *testing.T) {
	if os.Getenv("PROVIDER_FIXTURE") != "valid" {
		return
	}
	f := os.NewFile(3, "inherited-provider-stream")
	conn, err := net.FileConn(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	c, ok := conn.(*net.UnixConn)
	if !ok {
		t.Fatal("not Unix stream")
	}
	defer c.Close()
	if !stream(c) {
		t.Fatal("not STREAM")
	}
	if err := c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	prelude(t, c)
	r := read(t, c, "recheck")
	reply(t, c, r.ID, "rechecked", nil)
	id := bootstrapPrelude(t, c, nil)
	r = read(t, c, "installed")
	if r.ID != id {
		t.Fatal("installed sequence")
	}
	reply(t, c, id, "ack", nil)
	r = read(t, c, "recheck")
	reply(t, c, r.ID, "rechecked", nil)
	r = read(t, c, "close")
	reply(t, c, r.ID, "closed", nil)
}

func TestValidInheritedProcessProvider(t *testing.T) {
	a, b := pair(t)
	f, err := b.File()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProviderChild$", "-test.v")
	cmd.Env = append(os.Environ(), "PROVIDER_FIXTURE=valid")
	cmd.ExtraFiles = []*os.File{f}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	_ = b.Close()
	wait := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(wait) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-wait:
		default:
			_ = cmd.Process.Kill()
			select {
			case <-wait:
			case <-time.After(time.Second):
				t.Error("fake process did not reap")
			}
		}
	})
	c := prepared(t, a)
	if err := c.Recheck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Bootstrap(ctx, bootstrap(), callbacks()); err != nil {
		t.Fatal(err)
	}
	if err := c.Recheck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal("idempotent close", err)
	}
	<-wait
	if waitErr != nil {
		t.Fatal("fake process exit", waitErr)
	}
}

func TestFirstVersionMismatchBeforeEffects(t *testing.T) {
	a := script(t, func(c *net.UnixConn) {
		read(t, c, "hello")
		if _, err := c.Write(wire(`{"version":2,"id":1,"type":"hello_ok"}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFrame(c); err == nil {
			t.Error("request after version mismatch")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := NewClient(ctx, a, time.Now().Add(2*time.Second))
	if c != nil || !errors.Is(err, ErrVersion) {
		t.Fatal("version mismatch", err)
	}
}

func TestCallbackReturnsBeforeACK(t *testing.T) {
	var hostReturned, enrollReturned atomic.Bool
	a := script(t, func(c *net.UnixConn) {
		prelude(t, c)
		f := read(t, c, "bootstrap")
		reply(t, c, f.ID, "recheck_host", nil)
		read(t, c, "host_checked")
		if !hostReturned.Load() {
			t.Error("Host callback still running")
		}
		reply(t, c, f.ID, "enrollment", enrolled(bootstrap()))
		read(t, c, "recheck")
		reply(t, c, f.ID, "rechecked", nil)
		read(t, c, "installed")
		if !enrollReturned.Load() {
			t.Error("enrollment callback still running")
		}
		reply(t, c, f.ID, "ack", nil)
		f = read(t, c, "close")
		reply(t, c, f.ID, "closed", nil)
	})
	c := prepared(t, a)
	cb := callbacks()
	cb.RecheckHost = func(context.Context) error { defer hostReturned.Store(true); return nil }
	original := cb.Enroll
	cb.Enroll = func(ctx context.Context, e Enrollment) error {
		defer enrollReturned.Store(true)
		return original(ctx, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Bootstrap(ctx, bootstrap(), cb); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestACKFailures(t *testing.T) {
	for _, mode := range []string{"missing", "duplicate", "truncated", "early", "wrong-id"} {
		t.Run(mode, func(t *testing.T) {
			a := script(t, func(c *net.UnixConn) {
				prelude(t, c)
				if mode == "early" {
					f := read(t, c, "bootstrap")
					reply(t, c, f.ID, "ack", nil)
					return
				}
				id := bootstrapPrelude(t, c, nil)
				read(t, c, "installed")
				switch mode {
				case "missing":
					_, _ = ReadFrame(c)
				case "duplicate":
					reply(t, c, id, "ack", nil)
					reply(t, c, id, "ack", nil)
					_, _ = ReadFrame(c)
				case "truncated":
					_, _ = c.Write([]byte{0, 0, 0, 100, '{'})
					_ = c.CloseWrite()
				case "wrong-id":
					reply(t, c, id+1, "ack", nil)
				}
			})
			c := prepared(t, a)
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			err := c.Bootstrap(ctx, bootstrap(), callbacks())
			if mode != "duplicate" && err == nil {
				t.Fatal("bootstrap reported ready")
			}
			select {
			case <-c.Done():
			case <-time.After(time.Second):
				t.Fatal("failure did not fence lifetime")
			}
			if c.Err() == nil {
				t.Fatal("failure lost")
			}
			if mode == "duplicate" && !errors.Is(c.Err(), ErrState) {
				t.Fatal(c.Err())
			}
			if err := c.Recheck(context.Background()); err == nil {
				t.Fatal("terminal client reused")
			}
		})
	}
}

func TestAllBindingSubstitutionsAndAttachment(t *testing.T) {
	for field := 0; field < 13; field++ {
		name := "attachment"
		if field < 12 {
			name = reflect.TypeOf(Binding{}).Field(field).Name
		}
		t.Run(name, func(t *testing.T) {
			e := enrolled(bootstrap())
			if field < 12 {
				v := reflect.ValueOf(&e.Binding).Elem().Field(field)
				switch field {
				case 1:
					v.SetString("2")
				case 11:
					v.SetString(strings.Repeat("b", 64))
				default:
					v.SetString(v.String() + "-other")
				}
			} else {
				e.AttachmentRef = "other-attachment"
			}
			a := script(t, func(c *net.UnixConn) {
				prelude(t, c)
				bootstrapPrelude(t, c, &e)
				if f, err := ReadFrame(c); err == nil && f.Type == "installed" {
					t.Error("installation after substitution")
				}
			})
			c := prepared(t, a)
			if err := c.Bootstrap(context.Background(), bootstrap(), callbacks()); err == nil {
				t.Fatal("substitution admitted")
			}
		})
	}
}

func TestRefusalRetainsClosedReason(t *testing.T) {
	a := script(t, func(c *net.UnixConn) {
		prelude(t, c)
		f := read(t, c, "recheck")
		reply(t, c, f.ID, "refused", Refusal{"origin-unavailable"})
	})
	c := prepared(t, a)
	err := c.Recheck(context.Background())
	var r RefusedError
	if !errors.Is(err, ErrRefused) || !errors.As(err, &r) || r.Reason != "origin-unavailable" {
		t.Fatal(err)
	}
	if err := c.Bootstrap(context.Background(), bootstrap(), callbacks()); err == nil {
		t.Fatal("fallback after refusal")
	}
}

func TestCallbackCancellationAndUnjoinedCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	a := script(t, func(c *net.UnixConn) {
		prelude(t, c)
		bootstrapPrelude(t, c, nil)
		if f, err := ReadFrame(c); err == nil {
			t.Error("frame after canceled callback", f.Type)
		}
	})
	c := prepared(t, a)
	cb := callbacks()
	cb.Enroll = func(context.Context, Enrollment) error { close(entered); <-release; return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Bootstrap(ctx, bootstrap(), cb) }()
	<-entered
	if err := <-result; err == nil {
		t.Fatal("canceled bootstrap succeeded")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := c.Close(closeCtx); !errors.Is(err, ErrCleanup) {
		t.Fatal("unjoined callback reported clean", err)
	}
	closeCancel()
	close(release)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := c.Close(ctx2); errors.Is(err, ErrCleanup) || err == nil {
		t.Fatal("terminal failure/cleanup lost", err)
	}
}

func TestExpiryFencesReadyClient(t *testing.T) {
	e := enrolled(bootstrap())
	e.LeaseExpiresAt = time.Now().Add(250 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	a := script(t, func(c *net.UnixConn) {
		prelude(t, c)
		id := bootstrapPrelude(t, c, &e)
		read(t, c, "installed")
		reply(t, c, id, "ack", nil)
		_, _ = ReadFrame(c)
	})
	c := prepared(t, a)
	if err := c.Bootstrap(context.Background(), bootstrap(), callbacks()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("lease expiry did not fence")
	}
	if c.Err() == nil {
		t.Fatal("expiry reported clean")
	}
}

func TestNoEffectfulCallBeforePrepare(t *testing.T) {
	a := script(t, func(c *net.UnixConn) {
		f := read(t, c, "hello")
		reply(t, c, f.ID, "hello_ok", nil)
		if _, err := ReadFrame(c); err == nil {
			t.Error("bootstrap sent before prepare")
		}
	})
	c, err := NewClient(context.Background(), a, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Bootstrap(context.Background(), bootstrap(), callbacks()); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Close(ctx)
}

func TestNestedSourceRecheckRefusalBeforeInstallation(t *testing.T) {
	var called atomic.Bool
	a := script(t, func(c *net.UnixConn) {
		prelude(t, c)
		f := read(t, c, "bootstrap")
		reply(t, c, f.ID, "recheck_host", nil)
		read(t, c, "host_checked")
		reply(t, c, f.ID, "enrollment", enrolled(bootstrap()))
		read(t, c, "recheck")
		reply(t, c, f.ID, "refused", Refusal{"stale-binding"})
		if _, err := ReadFrame(c); err == nil {
			t.Error("frame after source recheck refusal")
		}
	})
	c := prepared(t, a)
	cb := callbacks()
	cb.Enroll = func(context.Context, Enrollment) error { called.Store(true); return nil }
	if err := c.Bootstrap(context.Background(), bootstrap(), cb); !errors.Is(err, ErrRefused) {
		t.Fatal(err)
	}
	if called.Load() {
		t.Fatal("installation before successful live recheck")
	}
}

func TestCompletedCallerDoesNotRevokeLifetime(t *testing.T) {
	a := script(t, func(c *net.UnixConn) {
		prelude(t, c)
		id := bootstrapPrelude(t, c, nil)
		read(t, c, "installed")
		reply(t, c, id, "ack", nil)
		f := read(t, c, "recheck")
		reply(t, c, f.ID, "rechecked", nil)
		f = read(t, c, "close")
		reply(t, c, f.ID, "closed", nil)
	})
	c := prepared(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := c.Bootstrap(ctx, bootstrap(), callbacks()); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := c.Recheck(ctx2); err != nil {
		t.Fatal("completed caller revoked lifetime", err)
	}
	if err := c.Close(ctx2); err != nil {
		t.Fatal(err)
	}
}

func TestCloseCancelsAndJoinsActiveCallback(t *testing.T) {
	entered, returned := make(chan struct{}), make(chan struct{})
	a := script(t, func(c *net.UnixConn) {
		prelude(t, c)
		bootstrapPrelude(t, c, nil)
		if _, err := ReadFrame(c); err == nil {
			t.Error("frame after active close")
		}
	})
	c := prepared(t, a)
	cb := callbacks()
	cb.Enroll = func(ctx context.Context, _ Enrollment) error {
		defer close(returned)
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	result := make(chan error, 1)
	go func() { result <- c.Bootstrap(context.Background(), bootstrap(), cb) }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Close(ctx); err == nil || errors.Is(err, ErrCleanup) {
		t.Fatal("active close failed to cancel/join", err)
	}
	select {
	case <-returned:
	default:
		t.Fatal("Close returned before callback joined")
	}
	if err := <-result; err == nil {
		t.Fatal("bootstrap succeeded after close")
	}
}
