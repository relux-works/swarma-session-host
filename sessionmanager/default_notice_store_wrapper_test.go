package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func storedNoticeFixture() SessionNotice {
	return SessionNotice{SessionID: "SES-a", RunID: "RUN-a", ElementID: "ELEMENT-a", Role: "dev", Runtime: "codex", State: "completed", Summary: "result", NextCommand: "inspect", CreatedAt: time.Unix(1, 0).UTC()}
}

func newStoredNoticeFixture(t *testing.T) (*SessionNoticeStore, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "notices")
	s, err := NewSessionNoticeStore(Layout{NoticesDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	return s, dir
}

// The wrapper keeps the original persisted envelope and whole-burst rendering;
// the receipt identifies a readable queue file, never the business run.
func TestSessionNoticeStorePreservesLegacyRecordsAndReceipts(t *testing.T) {
	s, dir := newStoredNoticeFixture(t)
	ctx := context.Background()
	want := storedNoticeFixture()
	if err := s.Enqueue(ctx, want.SessionID, Notice{Payload: &want}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Pending(ctx, want.SessionID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("pending = %+v, %v", rows, err)
	}
	want.Contract = SessionNoticeContract
	if !reflect.DeepEqual(rows[0].Payload, &want) || rows[0].Text != "" || rows[0].ID == want.RunID || rows[0].SessionID != want.SessionID || !rows[0].CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("envelope = %+v", rows[0])
	}
	name := filepath.Join(dir, want.SessionID, rows[0].ID)
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var disk SessionNotice
	if err := json.Unmarshal(data, &disk); err != nil || !reflect.DeepEqual(disk, want) {
		t.Fatalf("legacy disk = %+v, %v", disk, err)
	}
	for _, p := range []string{dir, filepath.Dir(name), name} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		mode := os.FileMode(0600)
		if info.IsDir() {
			mode = 0700
		}
		if info.Mode().Perm() != mode {
			t.Fatalf("mode %s = %o, want %o", p, info.Mode().Perm(), mode)
		}
	}
	second := want
	second.RunID = "RUN-b"
	second.Summary = "second"
	second.CreatedAt = second.CreatedAt.Add(time.Second)
	if err := s.Enqueue(ctx, second.SessionID, Notice{Payload: &second}); err != nil {
		t.Fatal(err)
	}
	rows, err = s.Pending(ctx, want.SessionID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("burst = %+v, %v", rows, err)
	}
	if got := FormatSessionNoticeBatch([]SessionNotice{*rows[0].Payload, *rows[1].Payload}); got != FormatSessionNoticeBatch([]SessionNotice{want, second}) {
		t.Fatalf("burst changed: %q", got)
	}
	if err := s.Acknowledge(ctx, want.SessionID, rows[0].ID); err != nil {
		t.Fatal(err)
	}
	remaining, err := s.Pending(ctx, want.SessionID)
	if err != nil || len(remaining) != 1 || remaining[0].Payload.RunID != second.RunID {
		t.Fatalf("ack removed another receipt: %+v, %v", remaining, err)
	}
}

// A healthy receipt can be acknowledged despite a corrupt neighbor; changing
// its own retained bytes after Pending refuses deletion and preserves evidence.
func TestSessionNoticeStoreKeepsCorruptEntriesOnAcknowledgement(t *testing.T) {
	s, dir := newStoredNoticeFixture(t)
	ctx := context.Background()
	notice := storedNoticeFixture()
	if err := s.Enqueue(ctx, notice.SessionID, Notice{Payload: &notice}); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(dir, notice.SessionID, "00000000000000000002-ffffff.json")
	if err := os.WriteFile(bad, []byte("{invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Pending(ctx, notice.SessionID)
	if err == nil || len(rows) != 1 {
		t.Fatalf("corrupt neighbor = %+v, %v", rows, err)
	}
	if err := s.Acknowledge(ctx, notice.SessionID, rows[0].ID); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(bad); err != nil || string(b) != "{invalid" {
		t.Fatalf("corrupt neighbor changed: %s, %v", b, err)
	}
	if err := s.Enqueue(ctx, notice.SessionID, Notice{Payload: &notice}); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.Pending(ctx, notice.SessionID)
	if len(rows) != 1 {
		t.Fatalf("healthy row missing: %+v", rows)
	}
	changed := filepath.Join(dir, notice.SessionID, rows[0].ID)
	if err := os.WriteFile(changed, []byte("{changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Acknowledge(ctx, notice.SessionID, rows[0].ID); !errors.Is(err, ErrNoticeReceiptChanged) {
		t.Fatalf("changed receipt = %v", err)
	}
	if b, err := os.ReadFile(changed); err != nil || string(b) != "{changed" {
		t.Fatalf("changed receipt deleted: %s, %v", b, err)
	}
}

// Structural and address refusals precede queue writes, and an unissued handle
// cannot remove even a real readable file. A nearby valid enqueue still works.
func TestSessionNoticeStoreRefusesAddressAndReceiptForgeryWithoutWrites(t *testing.T) {
	s, dir := newStoredNoticeFixture(t)
	ctx := context.Background()
	payload := storedNoticeFixture()
	foreign := payload
	foreign.SessionID = "SES-other"
	for _, row := range []Notice{{Text: "pre-rendered"}, {Payload: &foreign}, {Payload: &payload, ID: "caller-receipt"}, {Payload: &payload, Text: "surrogate"}, {Payload: &payload, SessionID: "SES-other"}} {
		if err := s.Enqueue(ctx, payload.SessionID, row); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid envelope = %v", err)
		}
	}
	for _, missing := range []string{"run", "state"} {
		n := payload
		want := "session notice requires a run id"
		if missing == "run" {
			n.RunID = ""
		} else {
			n.State = ""
			want = "session notice requires a terminal state"
		}
		if err := s.Enqueue(ctx, payload.SessionID, Notice{Payload: &n}); err == nil || err.Error() != want {
			t.Fatalf("legacy %s refusal = %v", missing, err)
		}
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused inputs wrote state: %v", err)
	}
	if err := s.Enqueue(ctx, payload.SessionID, Notice{Payload: &payload}); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(filepath.Join(dir, payload.SessionID))
	if err != nil || len(files) != 1 {
		t.Fatalf("valid control = %v, %v", files, err)
	}
	for _, id := range []string{files[0].Name(), "../escape", "/absolute.json"} {
		if err := s.Acknowledge(ctx, payload.SessionID, id); !errors.Is(err, ErrNoticeReceiptUnknown) {
			t.Fatalf("unissued receipt %q = %v", id, err)
		}
	}
	if err := s.Enqueue(ctx, "../escape", Notice{Payload: &payload}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("session escape = %v", err)
	}
	rows, err := s.Pending(ctx, payload.SessionID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("unknown ack removed file: %+v, %v", rows, err)
	}
	if err := s.Acknowledge(ctx, "SES-other", rows[0].ID); !errors.Is(err, ErrNoticeReceiptUnknown) {
		t.Fatalf("foreign session receipt = %v", err)
	}
	if err := s.Acknowledge(ctx, payload.SessionID, rows[0].ID); err != nil {
		t.Fatal(err)
	}
	foreign.Contract = SessionNoticeContract
	b, err := json.Marshal(foreign)
	if err != nil {
		t.Fatal(err)
	}
	forged := filepath.Join(dir, payload.SessionID, "00000000000000000002-ffffff.json")
	if err := os.WriteFile(forged, b, 0600); err != nil {
		t.Fatal(err)
	}
	rows, err = s.Pending(ctx, payload.SessionID)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "foreign addressed notice retained") || len(rows) != 0 {
		t.Fatalf("foreign disk envelope = %+v, %v", rows, err)
	}
	if got, err := os.ReadFile(forged); err != nil || string(got) != string(b) {
		t.Fatalf("foreign bytes changed: %s, %v", got, err)
	}
}

// Original files survive store recreation, returned payloads are caller-owned,
// and concurrent producers retain separate notices. No provider is launched.
func TestSessionNoticeStoreRestartAndOwnedValues(t *testing.T) {
	s, dir := newStoredNoticeFixture(t)
	ctx := context.Background()
	payload := storedNoticeFixture()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); n := payload; errs <- s.Enqueue(ctx, n.SessionID, Notice{Payload: &n}) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	restored, err := NewSessionNoticeStore(Layout{NoticesDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := restored.Pending(ctx, payload.SessionID)
	if err != nil || len(rows) != 8 {
		t.Fatalf("restart = %d, %v", len(rows), err)
	}
	rows[0].Payload.Summary = "caller edit"
	fresh, err := restored.Pending(ctx, payload.SessionID)
	if err != nil || len(fresh) != 8 || fresh[0].Payload.Summary != payload.Summary {
		t.Fatalf("caller changed retained value: %+v, %v", fresh, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := restored.Acknowledge(cancelled, payload.SessionID, fresh[0].ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ack = %v", err)
	}
	if err := restored.Acknowledge(ctx, payload.SessionID, fresh[0].ID); err != nil {
		t.Fatal(err)
	}
}
