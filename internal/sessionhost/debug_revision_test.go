package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
)

// R4-F01: exercise the production driver hook, not a separate redactor.
func TestPrediscardPayloadViewAllowlistSkeleton(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []struct{ name, frame, want string }{
		{"unknown", `{"method":"future/notification","params":{"environment":{"CHILD_ENV_CANARY":"env-secret-canary"},"accessToken":"token-secret-canary"}}`, "unqualified_payload"},
		{"rpc-result", `{"id":77,"result":{"environment":{"CHILD_ENV_CANARY":"env-secret-canary"},"accessToken":"token-secret-canary"}}`, "unqualified_payload"},
		{"message", `{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","environment":{"CHILD_ENV_CANARY":"env-secret-canary"},"item":{"id":"message","type":"agentMessage","text":"message-control-canary","accessToken":"token-secret-canary"}}}`, "message-control-canary"},
		{"tool", `{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"id":"tool","type":"commandExecution","command":"fixture command","aggregatedOutput":"tool-control-canary","environment":{"CHILD_ENV_CANARY":"env-secret-canary"},"accessToken":"token-secret-canary"}}}`, "tool-control-canary"},
		{"question", `{"id":7,"method":"item/tool/requestUserInput","params":{"threadId":"thread","turnId":"turn","itemId":"tool","accessToken":"token-secret-canary","questions":[{"id":"q","header":"Choice","question":"question-control-canary","environment":{"CHILD_ENV_CANARY":"env-secret-canary"},"options":[{"label":"yes","description":"continue","accessToken":"token-secret-canary"}]}]}}`, "question-control-canary"},
		{"message-delta", `{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"i","delta":"delta-control-canary","accessToken":"token-secret-canary"}}`, "delta-control-canary"},
		{"tool-delta", `{"method":"item/commandExecution/outputDelta","params":{"threadId":"thread","turnId":"turn","itemId":"i","delta":"tool-delta-control-canary","environment":{"CHILD_ENV_CANARY":"env-secret-canary"}}}`, "tool-delta-control-canary"},
		{"patch-delta", `{"method":"item/fileChange/outputDelta","params":{"threadId":"thread","turnId":"turn","itemId":"i","delta":"patch-delta-control-canary","accessToken":"token-secret-canary"}}`, "patch-delta-control-canary"},
		{"patch", `{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"id":"i","type":"fileChange","changes":[{"path":"fixture.txt","diff":"patch-control-canary","accessToken":"token-secret-canary"}],"environment":{"CHILD_ENV_CANARY":"env-secret-canary"}}}}`, "patch-control-canary"},
		{"unknown-request", `{"id":7,"method":"future/approval","params":{"threadId":"thread","turnId":"turn","itemId":"i","question":"token-secret-canary"}}`, "unqualified_payload"},
		{"unknown-item", `{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"id":"i","type":"future","text":"token-secret-canary"}}}`, "unqualified_payload"},
		{"unqualified-known", `{"method":"item/completed","params":{"item":{"id":"i","type":"agentMessage","text":"token-secret-canary"}}}`, "unqualified_payload"},
		{"wrong-shape", `{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"i","delta":{"accessToken":"token-secret-canary"}}}`, "unqualified_payload"},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			d, _, err := codexadapter.OpenDriver(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: "provider", Model: "model", Effort: "effort"}, Workspace: workspace}, nil)
			if err != nil {
				t.Fatal(err)
			}
			tail := &debugTail{now: time.Now, epoch: 1, execution: "fixture"}
			d.SetDebugObserver(tail.observe)
			if _, err := d.Begin(); err != nil {
				t.Fatal(err)
			}
			seq := tail.next
			tr, err := d.Observe(ap.Chunk{Offset: 0, Bytes: []byte(fixture.frame + "\n"), AtTail: true})
			if err != nil && fixture.name != "rpc-result" {
				t.Fatal(err)
			}
			event := tail.read(seq).Event
			if !bytes.Contains(event, []byte(fixture.want)) {
				t.Fatalf("missing valid projection %q: %s", fixture.want, event)
			}
			for _, sink := range [][]byte{event, debugRender(event), tr.Checkpoint, d.Snapshot().Checkpoint} {
				for _, secret := range []string{"env-secret-canary", "token-secret-canary", "CHILD_ENV_CANARY", "accessToken", "environment"} {
					if bytes.Contains(sink, []byte(secret)) {
						t.Fatalf("unqualified field %q retained/rendered", secret)
					}
				}
			}
		})
	}
}

// R4-F02: repeated eligible controls cannot consume stop/revoke obligations.
func TestJournalCapAndReserveRoutineControlsSkeleton(t *testing.T) {
	for _, op := range []string{"claim", "renew", "yield", "resume_automation", "ack", "disposition", "detach"} {
		t.Run(op, func(t *testing.T) {
			h := debugFixture(t)
			token := debugView(t, h, "view", "writer_control", "stop")
			if op != "claim" {
				debugWant(t, h.debugControl(token, "claim", 0), "")
			}
			h.state.Debug.Results = []debugResult{{ID: "result", Thread: "thread", Turn: "turn", Outcome: "completed", Revision: 1, NotificationPending: true, Permitted: true}}
			j := h.debug.journal
			j.used = debugJournalCap - debugControlReserve - j.reserved
			before := cloneState(h.state)
			disk := debugDisk(t, h)
			seq := j.sequence
			lease := h.debug.leaseUntil
			if op == "detach" {
				debugWant(t, h.debugControl(token, op, before.Debug.WriterRevision), "input_capacity_exceeded")
				if h.debug.viewer != "" || h.state.Debug.Writer != "held" || h.state.StopRequested || !reflect.DeepEqual(h.state.Debug.Results, before.Debug.Results) || j.sequence != seq {
					t.Fatal("capacity detach failed own cleanup or spent journal budget")
				}
				if _, exists := h.debug.grants[token]; exists {
					t.Fatal("detached grant remains")
				}
				token = debugView(t, h, "view", "stop")
			} else {
				for range 100 {
					var err error
					switch op {
					case "ack":
						err = h.debugDisposition(token, "result", 1, true, false)
					case "disposition":
						err = h.debugDisposition(token, "result", 1, false, false)
					default:
						err = h.debugControl(token, op, h.state.Debug.WriterRevision)
					}
					debugWant(t, err, "input_capacity_exceeded")
				}
				if !reflect.DeepEqual(before, h.state) || !bytes.Equal(disk, debugDisk(t, h)) || seq != j.sequence || !lease.Equal(h.debug.leaseUntil) {
					t.Fatal("routine capacity refusal changed state or journal")
				}
			}
			g := h.debug.grants[token]
			g.rights = []string{"view", "stop"}
			h.debug.grants[token] = g
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			debugWant(t, h.debugStop(ctx, token), "")
			debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", h.state.Epoch}, token), "")
			if !h.state.StopRequested || h.debug.viewer != "" || j.used+j.reserved > debugJournalCap {
				t.Fatal("protected stop/revoke failed")
			}
		})
	}
	// A valid ordinary-budget neighbour still renews; missing rights cannot spend reserve.
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	debugWant(t, h.debugControl(token, "renew", h.state.Debug.WriterRevision), "")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	debugWant(t, h.debugStop(ctx, token), "input_right_required")
}

// R4-F03: the abandoned client makes no call after advancing the clock.
func TestLeaseYieldTimeoutDetachAbandonedGrantSkeleton(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "view-only", true: "claimed"}[claimed], func(t *testing.T) {
			h := debugFixture(t)
			now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
			h.debug.now = func() time.Time { return now }
			token := debugView(t, h, "view", "writer_control")
			if claimed {
				debugWant(t, h.debugControl(token, "claim", 0), "")
			}
			h.state.Debug.Results = []debugResult{{ID: "result", Thread: "thread", Turn: "turn", Outcome: "completed", Revision: 1, NotificationPending: true}}
			other := debugToken(t, h, "view")
			debugWant(t, h.debugAttachView(other), "attach_in_use")
			now = now.Add(5 * time.Minute)
			h.mu.Lock()
			h.debugExpireLeaseLocked()
			h.mu.Unlock()
			want := "automatic"
			if claimed {
				want = "held"
			}
			if h.debug.viewer != "" || len(h.debug.grants) != 0 || h.state.Debug.Writer != want || h.state.Debug.Results[0].Permitted {
				t.Fatal("maintenance retained expired attachment or changed terminal block")
			}
			fresh := debugToken(t, h, "view")
			debugWant(t, h.debugAttachView(fresh), "")
			_, err := h.debugRead(token, 0)
			debugWant(t, err, "attach_grant_revoked")
		})
	}
}
func TestGrantAllowlistAndRevocationExpiryAllocationSkeleton(t *testing.T) {
	h := debugFixture(t)
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	h.debug.now = func() time.Time { return now }
	for range 4 {
		for range 16 {
			debugToken(t, h, "view")
		}
		_, err := h.debugIssueGrant(debugAuthority{"manager", "manager", 1}, []string{"view"})
		debugWant(t, err, "input_invalid")
		now = now.Add(5 * time.Minute)
		// Allocation itself reaps; there is no old-client read or explicit maintenance.
	}
	token := debugToken(t, h, "view")
	debugWant(t, h.debugAttachView(token), "")
	now = now.Add(5 * time.Minute)
	// Attach allocation must also reap an abandoned slot, independently of issuance.
	fresh := debugToken(t, h, "view")
	debugWant(t, h.debugAttachView(fresh), "")
}

func debugEntries(t *testing.T, h *Host) []debugJournalEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(h.debug.journal.dir, "input.journal"))
	if err != nil {
		t.Fatal(err)
	}
	var out []debugJournalEntry
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var e debugJournalEntry
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}
func debugIntent(t *testing.T, e debugJournalEntry) map[string]any {
	t.Helper()
	var v map[string]any
	if len(e.Body) == 0 || json.Unmarshal(e.Body, &v) != nil || e.Digest != wireDigest(e.Body) {
		t.Fatalf("missing immutable intent: %+v", e)
	}
	return v
}

// R4-F04: inspect durable accepted intent, including failed subsequent commits.
func TestJournalSenderTimeBeforeEffectControlIntentSkeleton(t *testing.T) {
	digests := map[string]bool{}
	for _, choice := range []struct {
		revision uint64
		permit   bool
	}{{1, false}, {1, true}, {2, true}} {
		h := debugFixture(t)
		token := debugView(t, h, "view", "writer_control")
		h.state.Debug.Results = []debugResult{{ID: "result", Thread: "thread", Turn: "turn", Outcome: "completed", Revision: choice.revision, NotificationPending: true}}
		debugWant(t, h.debugDisposition(token, "result", choice.revision, false, choice.permit), "")
		e := debugEntries(t, h)[0]
		v := debugIntent(t, e)
		decision := "keep_held"
		if choice.permit {
			decision = "permit_automatic"
		}
		if v["target"] != "result" || v["expected_revision"] != float64(choice.revision) || v["decision"] != decision || v["operation"] != "disposition_terminal" || e.Actor != "manager" || e.GrantRevision != 1 {
			t.Fatalf("incomplete decision: %s", e.Body)
		}
		if digests[e.Digest] {
			t.Fatal("different intent has same digest")
		}
		digests[e.Digest] = true
		if bytes.Contains(e.Body, []byte(token)) {
			t.Fatal("bearer token journaled")
		}
		seq := h.debug.journal.sequence
		debugWant(t, h.debugDisposition(token, "result", choice.revision, false, choice.permit), "input_observation_stale")
		if seq != h.debug.journal.sequence {
			t.Fatal("stale decision accepted")
		}
	}
	h := debugFixture(t)
	a := debugAuthority{"owner", "owner", 1}
	first, second := debugToken(t, h, "view"), debugToken(t, h, "view")
	g := h.debug.grants[second]
	g.revision = 2
	h.debug.grants[second] = g
	debugWant(t, h.debugRevoke(a, first), "")
	debugWant(t, h.debugRevoke(a, second), "")
	entries := debugEntries(t, h)
	one, two := debugIntent(t, entries[0]), debugIntent(t, entries[1])
	if one["target"] == two["target"] || one["target"] == "" || entries[0].Digest == entries[1].Digest || one["expected_revision"] != float64(1) || two["expected_revision"] != float64(2) {
		t.Fatal("revocation targets indistinguishable")
	}
	for _, e := range entries {
		if strings.Contains(string(e.Body), first) || strings.Contains(string(e.Body), second) {
			t.Fatal("bearer token in revoke journal")
		}
	}
}
func TestJournalSenderTimeBeforeEffectControlFailuresSkeleton(t *testing.T) {
	for _, op := range []string{"disposition", "revoke"} {
		for _, failure := range []string{"sync", "commit"} {
			t.Run(op+"/"+failure, func(t *testing.T) {
				h := debugFixture(t)
				token := debugView(t, h, "view", "writer_control")
				debugWant(t, h.debugControl(token, "claim", 0), "")
				h.state.Debug.Results = []debugResult{{ID: "result", Thread: "thread", Turn: "turn", Outcome: "completed", Revision: 1, NotificationPending: true}}
				before := cloneState(h.state)
				seq := h.debug.journal.sequence
				if failure == "sync" {
					// A pipe accepts the small write but refuses fsync. Exercise
					// the actual post-write Sync failure, rather than a closed-FD
					// write error. No native input or reader is involved.
					reader, writer, pipeErr := os.Pipe()
					if pipeErr != nil {
						t.Fatal(pipeErr)
					}
					t.Cleanup(func() { reader.Close(); writer.Close() })
					if _, err := writer.Write(nil); err != nil {
						t.Fatal(err)
					}
					if writer.Sync() == nil {
						t.Fatal("sync-failure fixture accepted fsync")
					}
					h.debug.journal.file.Close()
					h.debug.journal.file = writer
				} else {
					if err := os.WriteFile(filepath.Join(h.store.dir, "state.pending"), []byte("fixture"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				if op == "disposition" {
					err = h.debugDisposition(token, "result", 1, false, true)
				} else {
					err = h.debugRevoke(debugAuthority{"owner", "owner", 1}, token)
				}
				if err == nil {
					t.Fatal("failure acknowledged control")
				}
				if h.state.Debug.Writer != before.Debug.Writer || !reflect.DeepEqual(h.state.Debug.Results, before.Debug.Results) || h.debug.viewer != token {
					t.Fatal("failed control mutated target")
				}
				if _, exists := h.debug.grants[token]; !exists {
					t.Fatal("failed revoke removed target")
				}
				want := seq
				if failure == "commit" {
					want++
					e := debugEntries(t, h)
					debugIntent(t, e[len(e)-1])
				}
				if h.debug.journal.sequence != want {
					t.Fatal("intent not durable before host commit")
				}
			})
		}
	}
}

func TestJournalCapAndReserveProtectedObligationsSkeleton(t *testing.T) {
	h := debugFixture(t)
	// Long admitted identity forms exercise the worst-case protected encodings.
	h.state.Execution = strings.Repeat("e", 128)
	h.state.Epoch = ^uint64(0)
	h.state.Debug.WriterRevision = ^uint64(0) - 1
	a := debugAuthority{strings.Repeat("m", 128), "manager", h.state.Epoch}
	var tokens []string
	for range 16 {
		token, err := h.debugIssueGrant(a, []string{"view", "stop"})
		debugWant(t, err, "")
		g := h.debug.grants[token]
		g.revision = ^uint64(0)
		h.debug.grants[token] = g
		tokens = append(tokens, token)
	}
	debugWant(t, h.debugAttachView(tokens[0]), "")
	j := h.debug.journal
	j.sequence = ^uint64(0) - 100
	j.used = debugJournalCap - debugControlReserve - j.reserved
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	debugWant(t, h.debugStop(ctx, tokens[0]), "")
	for _, token := range tokens {
		debugWant(t, h.debugRevoke(a, token), "")
	}
	_, err := h.debugIssueGrant(a, []string{"view"})
	debugWant(t, err, "input_capacity_exceeded")
	for i := range MaxRecords {
		next := cloneState(h.state)
		debugWant(t, h.debugTerminalLocked(&next, "thread", "turn", "completed"), "")
		debugWant(t, h.persistLocked(next), "")
		if len(h.state.Debug.Results) != i+1 {
			t.Fatal("terminal fact lost")
		}
	}
	if j.used+j.reserved > debugJournalCap {
		t.Fatal("protected obligations exceed total budget")
	}
	before := j.sequence
	next := cloneState(h.state)
	debugWant(t, h.debugTerminalLocked(&next, "thread", "turn", "completed"), "input_capacity_exceeded")
	if j.sequence != before {
		t.Fatal("terminal overflow accepted")
	}
	// Even a direct routine caller marking itself control cannot borrow reserve.
	debugWant(t, j.append(debugJournalEntry{Kind: "renew"}, true, time.Now()), "input_capacity_exceeded")
}

func TestGrantAllowlistAndRevocationMaintenanceTimerSkeleton(t *testing.T) {
	h := debugFixture(t)
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	// The clock is fixed before launching maintenance; no old-client call occurs.
	h.debug.now = func() time.Time { return now }
	token := debugView(t, h, "view")
	now = now.Add(5 * time.Minute)
	h.wg.Add(1)
	go h.debugExpiryTimerLoop()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		h.mu.Lock()
		reaped := h.debug.viewer == "" && len(h.debug.grants) == 0
		h.mu.Unlock()
		if reaped {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("maintenance did not reclaim expired grant")
		case <-ticker.C:
		}
	}
	if h.state.Debug.Writer != "automatic" {
		t.Fatal("view-only expiry claimed a hold")
	}
	fresh := debugToken(t, h, "view")
	debugWant(t, h.debugAttachView(fresh), "")
	h.cancel()
	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("maintenance did not stop")
	}
	if fresh == token {
		t.Fatal("grant reused")
	}
}

func TestJournalCapAndReserveRenewEncodedBoundarySkeleton(t *testing.T) {
	h := debugFixture(t)
	now := time.Date(2026, 11, 9, 0, 0, 0, 123456000, time.UTC)
	h.debug.now = func() time.Time { return now }
	h.debug.journal.started = now
	token := debugView(t, h, "view", "writer_control", "stop")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	debugWant(t, h.debugControl(token, "renew", h.state.Debug.WriterRevision), "")
	entries := debugEntries(t, h)
	probe := entries[len(entries)-1]
	probe.Sequence++
	encoded, err := json.Marshal(probe)
	if err != nil {
		t.Fatal(err)
	}
	cost := int64(len(encoded) + 1 + debugReceiptReserve)
	j := h.debug.journal
	j.used = debugJournalCap - debugControlReserve - j.reserved - cost
	debugWant(t, h.debugControl(token, "renew", h.state.Debug.WriterRevision), "")
	if j.used+j.reserved != debugJournalCap-debugControlReserve {
		t.Fatal("real encoded renewal did not meet frontier")
	}
	for range 100 {
		debugWant(t, h.debugControl(token, "renew", h.state.Debug.WriterRevision), "input_capacity_exceeded")
	}
	g := h.debug.grants[token]
	g.rights = []string{"view", "stop"}
	h.debug.grants[token] = g
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	debugWant(t, h.debugStop(ctx, token), "")
	debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", 1}, token), "")
}
