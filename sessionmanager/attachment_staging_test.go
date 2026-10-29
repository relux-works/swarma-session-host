package sessionmanager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type attachmentProbeCodec struct {
	*hostCommitProbeCodec
	discard  func(LegacyRecordDiscard) error
	discards int
}

func (c *attachmentProbeCodec) AcknowledgeDiscarded(ctx context.Context, d LegacyRecordDiscard) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.discards++
	if c.discard != nil {
		return c.discard(d)
	}
	return nil
}
func attachmentProbe(t *testing.T) (*SessionHost, *attachmentProbeCodec, *hostCommitProbeRunner, AttachRequest, StagedAttachmentHost) {
	t.Helper()
	_, c, r, _ := hostCommitProbe(t)
	codec := &attachmentProbeCodec{hostCommitProbeCodec: c}
	runner := &hostCommitProbeRunner{inputTestBackend: &inputTestBackend{}, snapshot: r.ProviderHost.Snapshot}
	dir := t.TempDir()
	h, err := NewHost(HostOptions{Layout: Layout{Namespace: "staged-probe", StateDir: dir, SessionsDir: filepath.Join(dir, "sessions"), LockFile: filepath.Join(dir, "manager.lock")}, Codec: codec, Config: hostCommitProbeConfig{}, Notices: hostCommitProbeNotices{}, Factories: []ProviderHostFactory{hostCommitProbeFactory{runner: runner}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	staged, ok := any(h).(StagedAttachmentHost)
	if !ok {
		t.Fatal("host lacks admitted attachment staging")
	}
	return h, codec, runner, AttachRequest{Owner: r.Owner, Provider: r.Provider}, staged
}
func attachmentAbort(p AttachmentPreparation) AttachmentAbort {
	return AttachmentAbort{Token: p.Token, Lease: p.Lease, BaseSHA256: p.BaseSHA256}
}
func attachmentCommit(p AttachmentPreparation) AttachmentCommit {
	return AttachmentCommit{Token: p.Token, Lease: p.Lease, BaseSHA256: p.BaseSHA256}
}

// The real sole writer acknowledges initial bytes without a client. Fresh
// abort observes disposal before ack; cancellation cannot free its reservation.
// Transport/process effects are counted doubles here, qualified natively later.
func TestHostAttachmentStagingFresh(t *testing.T) {
	h, c, r, q, s := attachmentProbe(t)
	ctx := context.Background()
	p, err := s.PrepareAttach(ctx, q)
	if err != nil || p.Token == "" || p.Writer == nil || p.Lease.SessionID != p.Session.SessionID || p.BaseSHA256 == "" || p.Session.AttachedClients != 0 || len(h.attachments) != 0 || c.acks != 1 {
		t.Fatalf("preparation did not acknowledge unpublished own state: %+v %v", p, err)
	}
	if _, _, err := h.Input(ctx, p.Session.SessionID); !errors.Is(err, ErrLegacyCommitUncertain) {
		t.Fatalf("fresh provisional authority escaped: %v", err)
	}
	if _, err := h.Attach(ctx, q); !errors.Is(err, ErrLegacyCommitUncertain) {
		t.Fatalf("staged owner launched/published a duplicate: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if a, err := s.CommitAttach(cancelled, attachmentCommit(p)); a != nil || !errors.Is(err, context.Canceled) || len(h.attachments) != 0 {
		t.Fatalf("cancelled commit published: %+v %v", a, err)
	}
	if _, err := h.Attach(ctx, q); !errors.Is(err, ErrLegacyCommitUncertain) {
		t.Fatalf("cancelled commit lost reservation: %v", err)
	}
	path, _ := h.records.path(p.Session.SessionID)
	c.discard = func(d LegacyRecordDiscard) error {
		if d.NamespaceFingerprint != "staged-probe" || d.SessionID != p.Session.SessionID || d.DiscardedSHA256 == "" {
			t.Error("discard acknowledgement lost identity")
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("disposal ack preceded actual absence: %v", err)
		}
		if _, err := os.Lstat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("disposal ack preceded actual absence: directory: %v", err)
		}
		return nil
	}
	a, err := s.AbortAttach(ctx, attachmentAbort(p))
	if err != nil || a.Disposition != AttachmentAbortDiscardedFresh || a.DiscardedSHA256 == "" || a.CommittedSHA256 != "" || a.Token != p.Token || a.Lease != p.Lease || a.BaseSHA256 != p.BaseSHA256 || r.forces != 1 || c.discards != 1 || len(h.attachments) != 0 {
		t.Fatalf("fresh failure did not stop/discard exact owned state: %+v %v stops=%d", a, err, r.forces)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh record retained: %v", err)
	}
	if _, err := p.Writer.Submit(ctx, textInput(p.Lease, "late", "forbidden")); err == nil || r.writeCount() != 0 {
		t.Fatalf("aborted provisional writer remained live: %v", err)
	}
}

// A reused session keeps its actual runner and old legitimate client while
// abort commits recovering. A later successful preparation publishes once.
func TestHostAttachmentStagingReused(t *testing.T) {
	h, c, r, q, s := attachmentProbe(t)
	ctx := context.Background()
	a, err := h.Attach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.PrepareAttach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if p.Session.SessionID != a.Session.SessionID || p.Session.AttachedClients != 1 || len(h.attachments) != 1 {
		t.Fatal("reuse changed native/session identity or published")
	}
	abort, err := s.AbortAttach(ctx, attachmentAbort(p))
	if err != nil || abort.Disposition != AttachmentAbortPreservedRecovering || abort.CommittedSHA256 == "" || abort.DiscardedSHA256 != "" || h.sessions[p.Session.SessionID].State != SessionRecovering || r.forces != 0 || c.discards != 0 || len(h.attachments) != 1 {
		t.Fatalf("reused failure stopped/discarded or omitted recovering commit: %+v %v", abort, err)
	}
	p, err = s.PrepareAttach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.CommitAttach(ctx, attachmentCommit(p))
	if err != nil || out == nil || out.Session.AttachedClients != 2 {
		t.Fatalf("valid staged commit failed: %+v %v", out, err)
	}
	if again, err := s.CommitAttach(ctx, attachmentCommit(p)); again != nil || err == nil || len(h.attachments) != 2 {
		t.Fatalf("consumed token published twice: %+v %v", again, err)
	}
	if _, err := p.Writer.Submit(ctx, textInput(p.Lease, "after-commit", "forbidden")); err == nil {
		t.Fatal("provisional capability outlived commit")
	}
}

// Token/lease/base are conjunctive, host-owned authority. Each refused variant
// has a valid commit neighbor and no client or native-write side effect.
func TestHostAttachmentStagingAuthority(t *testing.T) {
	for _, mode := range []string{"token", "epoch", "session", "base", "disk", "recovery"} {
		t.Run(mode, func(t *testing.T) {
			h, _, r, q, s := attachmentProbe(t)
			ctx := context.Background()
			p, err := s.PrepareAttach(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			commit := attachmentCommit(p)
			switch mode {
			case "token":
				commit.Token += "foreign"
			case "epoch":
				commit.Lease.Epoch++
			case "session":
				commit.Lease.SessionID += "foreign"
			case "base":
				commit.BaseSHA256 = "wrong"
			case "disk":
				path, _ := h.records.path(p.Session.SessionID)
				if err := os.WriteFile(path, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
			case "recovery":
				if err := h.Recover(ctx); err != nil {
					t.Fatal(err)
				}
			}
			out, err := s.CommitAttach(ctx, commit)
			if err == nil || out != nil || len(h.attachments) != 0 || r.writeCount() != 0 {
				t.Fatalf("invalid staged authority published: %s %+v %v", mode, out, err)
			}
			if mode != "disk" && mode != "recovery" {
				out, err = s.CommitAttach(ctx, attachmentCommit(p))
				if err != nil || out == nil || len(h.attachments) != 1 {
					t.Fatalf("valid neighbor refused: %+v %v", out, err)
				}
			}
		})
	}
}

// A failed acknowledgement after real deletion is uncertainty, not success or
// permission for duplicate launch. Retry can acknowledge only its retained hash.
func TestHostAttachmentStagingUncertainDisposal(t *testing.T) {
	h, c, r, q, s := attachmentProbe(t)
	ctx := context.Background()
	p, err := s.PrepareAttach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("disposal acknowledgement unavailable")
	c.discard = func(LegacyRecordDiscard) error { return fault }
	if a, err := s.AbortAttach(ctx, attachmentAbort(p)); a.Token != "" || !errors.Is(err, fault) {
		t.Fatalf("failed cleanup claimed receipt: %+v %v", a, err)
	}
	if _, err := h.Attach(ctx, q); !errors.Is(err, ErrLegacyCommitUncertain) {
		t.Fatalf("uncertain disposal lost owner reservation: %v", err)
	}
	c.discard = nil
	a, err := s.AbortAttach(ctx, attachmentAbort(p))
	if err != nil || a.Disposition != AttachmentAbortDiscardedFresh || r.forces != 1 {
		t.Fatalf("owned disposal retry failed or stopped twice: %+v %v stops=%d", a, err, r.forces)
	}
}

// An actual independently opened journal replaces the controller, rather than
// merely changing a request field. Neither commit nor abort may borrow the old
// token for the new epoch; recovery invalidates it and admits a new preparation.
func TestHostAttachmentStagingExternalEpochReplacement(t *testing.T) {
	h, _, r, q, s := attachmentProbe(t)
	ctx := context.Background()
	p, err := s.PrepareAttach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	other, err := newControllerEpochJournal(h.options.Layout.StateDir, string(h.options.Layout.Namespace))
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := other.reserveContext(ctx, p.Session.SessionID)
	if err != nil || epoch <= p.Lease.Epoch {
		t.Fatalf("external replacement failed: %d %v", epoch, err)
	}
	if out, err := s.CommitAttach(ctx, attachmentCommit(p)); out != nil || !errors.Is(err, ErrStaleInputController) || len(h.attachments) != 0 {
		t.Fatalf("replaced controller published old token: %+v %v", out, err)
	}
	if out, err := s.AbortAttach(ctx, attachmentAbort(p)); out.Token != "" || !errors.Is(err, ErrStaleInputController) || r.forces != 0 || len(h.preparations) != 1 {
		t.Fatalf("old token cleaned successor or lost reservation: %+v %v stops=%d", out, err, r.forces)
	}
	if err := h.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if out, err := s.CommitAttach(ctx, attachmentCommit(p)); out != nil || err == nil {
		t.Fatalf("recovery revived old preparation: %+v %v", out, err)
	}
	next, err := s.PrepareAttach(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if next.Lease.Epoch <= epoch {
		t.Fatal("recovery reused replaced epoch")
	}
	if out, err := s.CommitAttach(ctx, attachmentCommit(next)); err != nil || out == nil {
		t.Fatalf("valid recovered neighbor refused: %+v %v", out, err)
	}
}
