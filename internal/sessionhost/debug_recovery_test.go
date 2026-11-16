package sessionhost

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// debugReplace closes the current private custody and opens a replacement
// host incarnation on the same store, as Open does after a crash. It reads the
// durable snapshot back instead of trusting in-memory state.
func debugReplace(t *testing.T, h *Host) (*Host, error) {
	t.Helper()
	h.debug.close()
	state, exists, err := h.store.read()
	if err != nil || !exists {
		t.Fatalf("snapshot unreadable: %v", err)
	}
	state.Epoch++
	state.Fence = "open"
	ctx, cancel := context.WithCancel(t.Context())
	next := &Host{store: h.store, ctx: ctx, cancel: cancel, kick: make(chan struct{}, 1), debugRegistry: h.debugRegistry, debugEffects: h.debugEffects, debugNotify: h.debugNotify, state: state}
	if err := next.openDebug(true); err != nil {
		cancel()
		return nil, err
	}
	next.cfg.IOTimeout = 10 * time.Second
	t.Cleanup(func() { cancel(); next.debug.close() })
	return next, nil
}

// JournalPrivacyAndCrash (recovery): a complete committed frame/receipt
// transaction reconstructs sequence, reservation and stages on replacement and
// accepts fresh input again; a frame accepted before the snapshot commit is
// retained as an unknown effect, an issued marker without a receipt becomes
// unknown, and an unframed tail holds new effects while stop stays reachable.
// A reopen with inputs merely flagged failed would not pass the positive leg.
func TestJournalPrivacyAndCrashRecovery(t *testing.T) {
	h, e := debugEffectFixture(t)
	token, revision := debugWriter(t, h, "view", "writer_control", "text")
	record, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "reply-1", Text: "private-body-canary", ExpectedRevision: revision})
	debugWant(t, err, "")
	if strings.Contains(string(debugDisk(t, h)), "private-body-canary") {
		t.Fatal("body entered the ordinary snapshot")
	}
	clean, err := debugReplace(t, h)
	debugWant(t, err, "")
	j := clean.debug.journal
	if j.torn || j.failed || j.sequence != 3 || j.reserved != debugReceiptReserve || clean.state.Debug.Inputs[0].Stage != "write_known" || clean.state.Debug.Writer != "held" {
		t.Fatalf("clean reconstruction: torn=%v failed=%v seq=%d reserved=%d %+v", j.torn, j.failed, j.sequence, j.reserved, clean.state.Debug)
	}
	fresh, current := debugWriter(t, clean, "view", "writer_control", "text")
	// The exact retry repeats the original whole intent, including the writer
	// revision it was bound to; the receipt lookup precedes the stale check.
	retry, err := clean.debugInput(fresh, debugInputIntent{Kind: "follow_up", ID: "reply-1", Text: "private-body-canary", ExpectedRevision: revision})
	debugWant(t, err, "")
	if retry.Stage != record.Stage || retry.Digest != record.Digest || e.count() != 1 {
		t.Fatal("retained receipt lost across replacement or replayed")
	}
	next, err := clean.debugInput(fresh, debugInputIntent{Kind: "follow_up", ID: "reply-2", Text: "after recovery", ExpectedRevision: current})
	debugWant(t, err, "")
	if next.Stage != "write_known" || next.Sequence != 5 || e.count() != 2 {
		t.Fatalf("recovered journal refused valid input: %+v", next)
	}
	for _, crash := range []string{"before-snapshot", "after-issue", "torn-tail"} {
		t.Run(crash, func(t *testing.T) {
			h, e := debugEffectFixture(t)
			token, revision := debugWriter(t, h, "view", "writer_control", "text")
			g := h.debug.grants[token]
			switch crash {
			case "before-snapshot":
				// The accepted frame exists; the host died before the snapshot commit.
				debugWant(t, h.debugAppendLocked(g, "follow_up", "orphan", []byte(`{"kind":"follow_up","id":"orphan","text":"orphan-body-canary"}`), false), "")
			case "after-issue":
				_, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "issued", Text: "x", ExpectedRevision: revision})
				debugWant(t, err, "")
				h.state.Debug.Inputs[0].Stage = "issued"
				debugWant(t, h.persistLocked(h.state), "")
			case "torn-tail":
				if _, err := h.debug.journal.file.Write([]byte(`{"contract":"host-debug-attach/1","sequence":9,"kind":"follow_up","input_id":"torn","body":"dG9ybi1ib2R5LWNhbmFyeQ=="`)); err != nil {
					t.Fatal(err)
				}
			}
			r, err := debugReplace(t, h)
			debugWant(t, err, "")
			fresh, revision := debugWriter(t, r, "view", "writer_control", "text", "stop")
			_, err = r.debugInput(fresh, debugInputIntent{Kind: "follow_up", ID: "new", Text: "y", ExpectedRevision: revision})
			debugWant(t, err, "input_fenced")
			debugWant(t, r.debugControl(fresh, "resume_automation", revision), "input_fenced")
			disk := string(debugDisk(t, r))
			if strings.Contains(disk, "orphan-body-canary") || strings.Contains(disk, "torn-body") {
				t.Fatal("recovery copied a body into the snapshot")
			}
			switch crash {
			case "before-snapshot":
				in := r.state.Debug.Inputs[0]
				if len(r.state.Debug.Inputs) != 1 || in.ID != "orphan" || in.Stage != "effect_unknown" || in.Actor != "manager" || r.debug.journal.torn {
					t.Fatalf("orphan frame not retained as unknown: %+v", r.state.Debug.Inputs)
				}
			case "after-issue":
				if r.state.Debug.Inputs[0].Stage != "effect_unknown" || e.count() != 1 {
					t.Fatal("issued marker without receipt was not held unknown or was replayed")
				}
			case "torn-tail":
				if !r.debug.journal.torn || len(r.state.Debug.Inputs) != 0 {
					t.Fatal("unframed tail not reported torn")
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			debugWant(t, r.debugStop(ctx, fresh), "")
			if !r.state.StopRequested {
				t.Fatal("stop unreachable during recovery hold")
			}
			if e.count() > 1 {
				t.Fatal("recovery replayed an effect")
			}
		})
	}
}

// JournalPrivacyAndCrash (object checks at reopen): a replaced journal object,
// an extra hard link, a wrong directory mode or a foreign-owned-looking
// directory refuse V17 with the host unopened, while the intact private file
// reopens. These are same-UID fixture checks, not OS isolation.
func TestJournalPrivacyReopenObjects(t *testing.T) {
	for _, mode := range []string{"intact", "symlink", "hardlink", "directory-mode", "fifo"} {
		t.Run(mode, func(t *testing.T) {
			h := debugFixture(t)
			token := debugView(t, h, "view", "writer_control")
			debugWant(t, h.debugControl(token, "claim", 0), "")
			path := filepath.Join(h.debug.journal.dir, "input.journal")
			h.debug.close()
			switch mode {
			case "symlink":
				target := filepath.Join(h.store.dir, "elsewhere")
				if err := os.Rename(path, target); err != nil || os.Symlink(target, path) != nil {
					t.Fatal("symlink fixture")
				}
			case "hardlink":
				if err := os.Link(path, filepath.Join(h.store.dir, "second-link")); err != nil {
					t.Fatal(err)
				}
			case "directory-mode":
				if err := os.Chmod(h.debug.journal.dir, 0750); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := os.Remove(path); err != nil || syscall.Mkfifo(path, 0600) != nil {
					t.Fatal("fifo fixture")
				}
			}
			_, err := openDebugJournal(h.store.dir, true)
			if mode == "intact" {
				debugWant(t, err, "")
				return
			}
			debugWant(t, err, "input_storage_unavailable")
			if mode == "directory-mode" {
				if err := os.Chmod(h.debug.journal.dir, 0700); err != nil {
					t.Fatal(err)
				}
				_, err := openDebugJournal(h.store.dir, true)
				debugWant(t, err, "")
			}
		})
	}
}

// RetirementPurgeProof (close and sync failures): an injected failure of the
// journal close, the private directory sync or close, the parent directory sync
// or close, or the snapshot commit sync leaves retirement incomplete with the
// exact storage class, no retirement or abandonment recorded, the neighbouring
// state file untouched and conservative retry evidence; after the injection is
// removed the same retirement succeeds (or, for the snapshot commit, the
// pending-transaction marker holds replacement until reconciliation).
func TestRetirementPurgeProofCloseAndSyncFailures(t *testing.T) {
	for _, mode := range []string{"journal-close", "directory-sync", "directory-close", "parent-sync", "parent-close", "state-sync"} {
		t.Run(mode, func(t *testing.T) {
			h, e := debugEffectFixture(t)
			e.err = errors.New("reply_lost")
			token, revision := debugWriter(t, h, "view", "writer_control", "text", "stop")
			_, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "lost", Text: "close-sync-body-canary", ExpectedRevision: revision})
			debugWant(t, err, "input_effect_unknown")
			sibling := filepath.Join(h.store.dir, "neighbor.json")
			if err := os.WriteFile(sibling, []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			debugWant(t, h.debugStop(ctx, token), "")
			j := h.debug.journal
			journalPath := filepath.Join(j.dir, "input.journal")
			injected := errors.New("injected storage failure")
			isDir := func(f *os.File) bool { info, err := f.Stat(); return err == nil && info.IsDir() }
			want := "input_storage_unavailable"
			switch mode {
			case "journal-close":
				j.closeFile = func(f *os.File) error { f.Close(); return injected }
			case "directory-sync":
				j.sync = func(f *os.File) error {
					if isDir(f) && f.Name() == j.dir {
						return injected
					}
					return f.Sync()
				}
			case "directory-close":
				j.closeFile = func(f *os.File) error {
					if isDir(f) && f.Name() == j.dir {
						f.Close()
						return injected
					}
					return f.Close()
				}
			case "parent-sync":
				j.sync = func(f *os.File) error {
					if isDir(f) && f.Name() == filepath.Dir(j.dir) {
						return injected
					}
					return f.Sync()
				}
			case "parent-close":
				j.closeFile = func(f *os.File) error {
					if isDir(f) && f.Name() == filepath.Dir(j.dir) {
						f.Close()
						return injected
					}
					return f.Close()
				}
			case "state-sync":
				h.store.sync = func(*os.File) error { return injected }
				want = "host_storage_unavailable"
			}
			debugWant(t, h.debugRetire(true), want)
			if h.state.Debug.Retired || h.state.Debug.Inputs[0].Abandoned {
				t.Fatal("failed close/sync recorded retirement or abandonment")
			}
			if restored, exists, readErr := h.store.read(); mode != "state-sync" && (readErr != nil || !exists || restored.Debug.Retired) {
				t.Fatal("durable snapshot claims retirement after a failed purge")
			}
			if data, err := os.ReadFile(sibling); err != nil || string(data) != "{}" {
				t.Fatal("failed purge touched a neighbouring file")
			}
			switch mode {
			case "journal-close":
				if _, err := os.Stat(journalPath); err != nil {
					t.Fatal("failed close already unlinked the journal")
				}
			case "state-sync":
				// The interrupted snapshot transaction is retained as unknown and
				// holds replacement until explicit reconciliation; it is never read
				// as a completed retirement.
				if _, err := os.Lstat(filepath.Join(h.store.dir, "state.pending")); err != nil {
					t.Fatal("failed commit left no pending marker")
				}
				if _, _, err := h.store.read(); err == nil || err.Error() != "state_update_unknown" {
					t.Fatalf("pending transaction read as settled: %v", err)
				}
				return
			}
			j.sync, j.closeFile = nil, nil
			debugWant(t, h.debugRetire(true), "")
			if !h.state.Debug.Retired || !h.state.Debug.Inputs[0].Abandoned {
				t.Fatal("retry after the removed injection did not retire")
			}
			if _, err := os.Lstat(j.dir); !os.IsNotExist(err) {
				t.Fatal("private directory survived the successful retry")
			}
			if _, err := os.Stat(sibling); err != nil {
				t.Fatal("retry deleted outside the owned private root")
			}
		})
	}
}

// RetirementPurgeProof (failures and tombstones): an unresolved effect blocks
// retirement until explicitly abandoned; an injected unlink failure leaves
// retirement incomplete and the abandonment unrecorded for retry; the later
// success removes only the owned private artifacts, keeps a sibling file, keeps
// the content-free unknown tombstone and leaves no later-accessible custody.
func TestRetirementPurgeProofFailures(t *testing.T) {
	h, e := debugEffectFixture(t)
	e.err = errors.New("reply_lost")
	token, revision := debugWriter(t, h, "view", "writer_control", "text", "stop")
	_, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "lost", Text: "tombstone-body-canary", ExpectedRevision: revision})
	debugWant(t, err, "input_effect_unknown")
	sibling := filepath.Join(h.store.dir, "neighbor.json")
	if err := os.WriteFile(sibling, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	debugWant(t, h.debugStop(ctx, token), "")
	debugWant(t, h.debugRetire(false), "input_effect_unknown")
	if err := os.Chmod(h.debug.journal.dir, 0500); err != nil {
		t.Fatal(err)
	}
	debugWant(t, h.debugRetire(true), "input_storage_unavailable")
	if h.state.Debug.Retired || h.state.Debug.Inputs[0].Abandoned {
		t.Fatal("failed purge recorded retirement or abandonment")
	}
	if err := os.Chmod(h.debug.journal.dir, 0700); err != nil {
		t.Fatal(err)
	}
	debugWant(t, h.debugRetire(true), "")
	in := h.state.Debug.Inputs[0]
	if !h.state.Debug.Retired || in.Stage != "effect_unknown" || !in.Abandoned || len(h.debug.grants) != 0 || !h.debug.tail.closed {
		t.Fatalf("retirement state: %+v", h.state.Debug)
	}
	if _, err := os.Lstat(h.debug.journal.dir); !os.IsNotExist(err) {
		t.Fatal("private directory survived retirement")
	}
	if _, err := os.Stat(sibling); err != nil {
		t.Fatal("purge deleted outside the owned private root")
	}
	if bytes.Contains(debugDisk(t, h), []byte("tombstone-body-canary")) {
		t.Fatal("tombstone carried the body")
	}
	_, err = h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
	_, err = h.debugIssueGrant(debugAuthority{"manager", "manager", 1}, []string{"view"})
	debugWant(t, err, "input_stopped")
	debugWant(t, h.debugRetire(true), "")
}
