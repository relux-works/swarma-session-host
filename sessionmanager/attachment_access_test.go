package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// These controls exercise actual host admission/sole-writer state with a
// counted transport double. Native execution is qualified in sessionadapter.
func TestHostAttachmentAccessAdmission(t *testing.T) {
	h, _, runner, q, _ := attachmentProbe(t)
	ctx := context.Background()
	a, err := h.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	port, ok := any(h).(AttachmentAccessHost)
	if !ok {
		t.Fatal("SessionHost lacks frozen attachment access runtime")
	}
	lease, _, err := h.Input(ctx, a.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	access, err := port.AcquireAttachment(ctx, a.Session.SessionID, a.AttachmentID)
	if err != nil || access == nil || access.Lease() != lease {
		t.Fatalf("valid pair did not capture actual controller: %v %v", access, err)
	}
	if err := access.Release(); err != nil {
		t.Fatal(err)
	}
	if err := access.Release(); err != nil {
		t.Fatal(err)
	}
	otherRequest := q
	otherRequest.Owner = "another-real-owner"
	other, err := h.Attach(ctx, otherRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"", a.AttachmentID}, {a.Session.SessionID, ""}, {"foreign", a.AttachmentID}, {a.Session.SessionID, "foreign"}, {a.Session.SessionID, other.AttachmentID}, {other.Session.SessionID, a.AttachmentID}} {
		got, err := port.AcquireAttachment(ctx, pair[0], pair[1])
		if got != nil || !errors.Is(err, ErrProviderHostNotFound) {
			t.Fatalf("foreign pair admitted: %q %v %v", pair, got, err)
		}
	}
	if runner.writeCount() != 0 {
		t.Fatal("admission touched provider input")
	}
	if _, err := h.Detach(ctx, a.Session.SessionID, a.AttachmentID); err != nil {
		t.Fatal(err)
	}
	if got, err := port.AcquireAttachment(ctx, a.Session.SessionID, a.AttachmentID); got != nil || !errors.Is(err, ErrProviderHostNotFound) {
		t.Fatalf("detach-first admitted: %v %v", got, err)
	}
}

// Each invalid state refuses acquisition, stream opening and resize before
// touching the transport. These are host fences, not native protocol tests.
func TestHostAttachmentAccessRefusals(t *testing.T) {
	for _, kind := range []string{"uncertain", "pending", "conditional", "retiring", "stopped", "epoch", "closed"} {
		t.Run(kind, func(t *testing.T) {
			h, _, _, q, _ := attachmentProbe(t)
			ctx := context.Background()
			a, err := h.Attach(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			id := a.Session.SessionID
			native := &attachmentTerminalProbe{RunnerHost: h.runners[id], stream: &attachmentStream{}}
			h.runners[id] = native
			want := ErrLegacyCommitUncertain
			switch kind {
			case "uncertain":
				h.records.uncertain[id] = true
			case "pending":
				h.pendingInitial[id] = pendingInitialRunner{}
			case "conditional":
				r := h.sessions[id]
				r.ConditionalStop = &ConditionalStopRecord{}
				h.sessions[id] = r
			case "retiring":
				r := h.sessions[id]
				r.GracefulRetirement = true
				h.sessions[id] = r
				want = ErrHostCapabilityUnavailable
			case "stopped":
				r := h.sessions[id]
				r.State = SessionStopped
				h.sessions[id] = r
				want = ErrHostCapabilityUnavailable
			case "epoch":
				epoch, err := h.epochs.reserveContext(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				input, err := h.installInput(ctx, id, epoch, &inputTestBackend{})
				if err != nil {
					t.Fatal(err)
				}
				h.inputs[id] = input
				want = ErrStaleInputController
			case "closed":
				h.Close()
				want = ErrHostClosed
			}
			got, err := h.AcquireAttachment(ctx, id, a.AttachmentID)
			if got != nil || !errors.Is(err, want) {
				t.Fatalf("%s acquisition = %v / %v, want %v", kind, got, err, want)
			}
			stream, err := h.Terminal(ctx, id, a.AttachmentID)
			if stream != nil || !errors.Is(err, want) {
				t.Fatalf("%s terminal = %v / %v", kind, stream, err)
			}
			if err := h.ResizeTerminal(ctx, id, a.AttachmentID, 24, 80); !errors.Is(err, want) {
				t.Fatalf("%s resize = %v", kind, err)
			}
			if native.opens != 0 || native.resizes != 0 {
				t.Fatal("invalid state reached terminal provider")
			}
		})
	}
}

type attachmentStream struct{ closed bool }

func (s *attachmentStream) Read([]byte) (int, error)    { return 0, io.EOF }
func (s *attachmentStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *attachmentStream) Close() error                { s.closed = true; return nil }

type attachmentTerminalProbe struct {
	RunnerHost
	stream           *attachmentStream
	opens, resizes   int
	rows, cols       uint16
	entered, release chan struct{}
}

func (p *attachmentTerminalProbe) AttachTerminal(ctx context.Context) (io.ReadWriteCloser, error) {
	p.opens++
	if p.entered != nil {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.stream, nil
}
func (p *attachmentTerminalProbe) ResizeTerminal(rows, cols uint16) error {
	p.resizes++
	p.rows = rows
	p.cols = cols
	return nil
}

// Forward exactly the native stream and geometry; Detach does not close an
// already admitted stream. The provider here is a counted port fixture.
func TestHostAttachmentTerminalForwarding(t *testing.T) {
	h, _, _, q, _ := attachmentProbe(t)
	ctx := context.Background()
	a, err := h.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	id := a.Session.SessionID
	native := &attachmentTerminalProbe{RunnerHost: h.runners[id], stream: &attachmentStream{}}
	h.runners[id] = native
	for _, pair := range [][2]string{{"foreign", a.AttachmentID}, {id, "foreign"}, {id, ""}} {
		if s, err := h.Terminal(ctx, pair[0], pair[1]); s != nil || !errors.Is(err, ErrProviderHostNotFound) {
			t.Fatalf("foreign terminal: %v %v", s, err)
		}
		if err := h.ResizeTerminal(ctx, pair[0], pair[1], 1, 2); !errors.Is(err, ErrProviderHostNotFound) {
			t.Fatalf("foreign resize: %v", err)
		}
	}
	if native.opens != 0 || native.resizes != 0 {
		t.Fatal("foreign pair touched physical terminal")
	}
	stream, err := h.Terminal(ctx, id, a.AttachmentID)
	if err != nil || stream != native.stream {
		t.Fatalf("native stream identity lost: %v %v", stream, err)
	}
	if err := h.ResizeTerminal(ctx, id, a.AttachmentID, 37, 119); err != nil || native.rows != 37 || native.cols != 119 || native.opens != 1 || native.resizes != 1 {
		t.Fatalf("native geometry/call count lost: %+v %v", native, err)
	}
	if _, err := h.Detach(ctx, id, a.AttachmentID); err != nil {
		t.Fatal(err)
	}
	if native.stream.closed {
		t.Fatal("detach retroactively closed admitted stream")
	}
	if _, err := stream.Write([]byte("admitted")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Terminal(ctx, id, a.AttachmentID); !errors.Is(err, ErrProviderHostNotFound) {
		t.Fatalf("detached stream reopened: %v", err)
	}
	if err := h.ResizeTerminal(ctx, id, a.AttachmentID, 2, 3); !errors.Is(err, ErrProviderHostNotFound) {
		t.Fatalf("detached resize admitted: %v", err)
	}
	if native.opens != 1 || native.resizes != 1 {
		t.Fatal("detach reached physical terminal")
	}
	if err := stream.Close(); err != nil || !native.stream.closed {
		t.Fatal("caller lost stream ownership")
	}
	if len(h.attachmentAccesses) != 0 {
		t.Fatal("terminal access leaked")
	}
}

// A barrier orders native opening after admission but before Detach. Only the
// admitted call finishes; independent metadata and Detach can acquire host mu.
func TestHostAttachmentAccessDetachOrdering(t *testing.T) {
	h, _, _, q, _ := attachmentProbe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, err := h.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	id := a.Session.SessionID
	native := &attachmentTerminalProbe{RunnerHost: h.runners[id], stream: &attachmentStream{}, entered: make(chan struct{}), release: make(chan struct{})}
	h.runners[id] = native
	var once sync.Once
	release := func() { once.Do(func() { close(native.release) }) }
	defer release()
	result := make(chan error, 1)
	go func() { _, err := h.Terminal(ctx, id, a.AttachmentID); result <- err }()
	select {
	case <-native.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := h.List(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := h.AcquireAttachment(ctx, id, a.AttachmentID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.AcquireAttachment(ctx, id, a.AttachmentID)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	_, retained := h.attachmentAccesses[second.(*attachmentAccess)]
	h.mu.Unlock()
	if !retained {
		t.Fatal("one release removed another acquisition")
	}
	if _, err := h.Detach(ctx, id, a.AttachmentID); err != nil {
		t.Fatal(err)
	}
	if got, err := h.AcquireAttachment(ctx, id, a.AttachmentID); got != nil || !errors.Is(err, ErrProviderHostNotFound) {
		t.Fatalf("detach-first admitted: %v %v", got, err)
	}
	release()
	if err := inputValue(t, ctx, result); err != nil {
		t.Fatalf("acquire-first retroactively refused: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
	if len(h.attachmentAccesses) != 0 {
		t.Fatal("release leaked acquisition")
	}
}

// Both real allocation sites encounter a forced detached-ID collision, skip
// the issued ID and publish a fresh client. Persistent collision fails closed.
func TestHostAttachmentIDNeverReused(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprint(staged), func(t *testing.T) {
			h, _, _, q, s := attachmentProbe(t)
			ctx := context.Background()
			old, err := h.Attach(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			id := old.Session.SessionID
			if _, err := h.Detach(ctx, id, old.AttachmentID); err != nil {
				t.Fatal(err)
			}
			calls := 0
			h.newAttachmentID = func() (string, error) {
				calls++
				if calls == 1 {
					return old.AttachmentID, nil
				}
				return "ATT-fresh-collision-control", nil
			}
			allocate := func() (*Attachment, error) {
				if !staged {
					return h.Attach(ctx, q)
				}
				p, err := s.PrepareAttach(ctx, q)
				if err != nil {
					return nil, err
				}
				return s.CommitAttach(ctx, attachmentCommit(p))
			}
			fresh, err := allocate()
			if err != nil || fresh == nil || fresh.AttachmentID == old.AttachmentID || calls != 2 {
				t.Fatalf("detached identity reissued (staged=%t): %+v %v calls=%d", staged, fresh, err, calls)
			}
			if got, err := h.AcquireAttachment(ctx, id, old.AttachmentID); got != nil || !errors.Is(err, ErrProviderHostNotFound) {
				t.Fatalf("old ID revived: %v %v", got, err)
			}
			access, err := h.AcquireAttachment(ctx, id, fresh.AttachmentID)
			if err != nil {
				t.Fatal(err)
			}
			access.Release()
			h.newAttachmentID = func() (string, error) { return old.AttachmentID, nil }
			before := len(h.attachments)
			got, err := allocate()
			if got != nil || !errors.Is(err, ErrHostCapabilityUnavailable) || len(h.attachments) != before {
				t.Fatalf("persistent collision published client: %+v %v", got, err)
			}
		})
	}
}

// A valid provider without terminal capability refuses both operations after
// admission; access cleanup does not depend on a native success response.
func TestHostAttachmentTerminalUnsupported(t *testing.T) {
	h, _, _, q, _ := attachmentProbe(t)
	ctx := context.Background()
	a, err := h.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if stream, err := h.Terminal(ctx, a.Session.SessionID, a.AttachmentID); stream != nil || !errors.Is(err, ErrTerminalUnsupported) {
		t.Fatalf("unsupported terminal: %v %v", stream, err)
	}
	if err := h.ResizeTerminal(ctx, a.Session.SessionID, a.AttachmentID, 1, 2); !errors.Is(err, ErrTerminalUnsupported) {
		t.Fatalf("unsupported resize: %v", err)
	}
	if len(h.attachmentAccesses) != 0 {
		t.Fatal("unsupported path leaked access")
	}
}

// Real host close/reopen never imports old membership. Old captures/writers
// cannot drive the higher controller; releasing an old pin cannot cross hosts.
func TestHostAttachmentAccessHostLifetime(t *testing.T) {
	h, _, _, q, _ := attachmentProbe(t)
	ctx := context.Background()
	a, err := h.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	id := a.Session.SessionID
	old, err := h.AcquireAttachment(ctx, id, a.AttachmentID)
	if err != nil {
		t.Fatal(err)
	}
	lease, writer, err := h.Input(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	if got, err := h.AcquireAttachment(ctx, id, a.AttachmentID); got != nil || !errors.Is(err, ErrHostClosed) {
		t.Fatalf("closed host admitted: %v %v", got, err)
	}
	next, err := NewHost(h.options)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if err := next.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := next.AcquireAttachment(ctx, id, a.AttachmentID); got != nil || !errors.Is(err, ErrProviderHostNotFound) {
		t.Fatalf("restart imported old membership: %v %v", got, err)
	}
	fresh, err := next.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	access, err := next.AcquireAttachment(ctx, id, fresh.AttachmentID)
	if err != nil {
		t.Fatal(err)
	}
	defer access.Release()
	if access.Lease().Epoch <= old.Lease().Epoch {
		t.Fatal("restart reused captured controller")
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	if len(next.attachmentAccesses) != 1 {
		t.Fatal("old release crossed host lifetime")
	}
	if _, err := writer.Submit(ctx, textInput(lease, "closed-writer", "forbidden")); !errors.Is(err, ErrStaleInputController) {
		t.Fatalf("closed writer drove replacement: %v", err)
	}
}
