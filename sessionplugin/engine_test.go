package sessionplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type testHost struct {
	mu               sync.Mutex
	starts           int
	sessions         map[string]HostSession
	failure          error
	unreadable       bool
	entered, release chan struct{}
}

func (h *testHost) Launch(ctx context.Context, q Request) (HostSession, error) {
	if h.entered != nil {
		close(h.entered)
		select {
		case <-h.release:
		case <-ctx.Done():
			return HostSession{}, Uncertain{}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.starts++
	if h.failure != nil {
		return HostSession{}, h.failure
	}
	id := fmt.Sprint(h.starts)
	r := HostSession{ID: id, State: "unknown"}
	h.sessions[id] = r
	return r, nil
}
func (h *testHost) Inspect(_ context.Context, id string) (HostSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unreadable {
		return HostSession{}, errors.New("unreadable")
	}
	r, ok := h.sessions[id]
	if !ok {
		return HostSession{}, errors.New("missing")
	}
	return r, nil
}

var testClock = time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)

func testEngine(t *testing.T) (Engine, *testHost) {
	t.Helper()
	h := &testHost{sessions: map[string]HostSession{}}
	return Engine{Dir: filepath.Join(t.TempDir(), "plugin"), Host: h, Now: func() time.Time { return testClock }}, h
}
func request(n int) map[string]any {
	return map[string]any{
		"operation_id": fmt.Sprintf("%08x-1111-4111-8111-111111111111", n), "submitted_at": testClock.Format(time.RFC3339), "admission_key": nil, "runtime": "codex_cli", "profile": map[string]any{"name": "sample", "lock_sha256": strings.Repeat("1", 64), "fragment_digest": "sha256:" + strings.Repeat("2", 64)}, "cwd": "/workspace", "model": nil, "effort": nil, "service_tier": nil, "weights_id": nil, "permissions": map[string]any{"mode": "native", "source": "flag"}, "system_prompt": nil, "sandbox": map[string]any{"mode": "none", "writable_roots": []string{}}, "harness_memory": false, "session_name": "example", "labels": map[string]string{}, "child_env": map[string]string{}, "board_goal": "none", "doorbell": nil, "credential_source": nil, "native_tail": []string{}, "detach": true,
	}
}
func call(t *testing.T, e Engine, op string, q any) any {
	t.Helper()
	b, _ := json.Marshal(q)
	name, v, err := e.Handle(context.Background(), op, b)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if _, err = Reply(name, v); err != nil {
		t.Fatalf("invalid %s response: %v: %#v", name, err, v)
	}
	return v
}
func rejected(t *testing.T, e Engine, op string, q any, code string) {
	t.Helper()
	b, _ := json.Marshal(q)
	_, _, err := e.Handle(context.Background(), op, b)
	var refusal Refusal
	if !errors.As(err, &refusal) || refusal.Code != code {
		t.Fatalf("want %s; got %v", code, err)
	}
}
func decode[T any](t *testing.T, v any) T {
	t.Helper()
	b, _ := json.Marshal(v)
	var result T
	if json.Unmarshal(b, &result) != nil {
		t.Fatal("decode test response")
	}
	return result
}

// A semantic replay returns the same public handle without a second provider
// start; a changed payload cannot overwrite the retained operation result.
func TestReplayAndConflict(t *testing.T) {
	e, h := testEngine(t)
	q := request(1)
	first := call(t, e, "launch", q)
	b, _ := json.MarshalIndent(q, "", "  ")
	name, replay, err := e.Handle(context.Background(), "launch", b)
	if err != nil || name != "session" || !bytes.Equal(marshal(first), marshal(replay)) {
		t.Fatalf("semantic replay changed result: %v", err)
	}
	if h.starts != 1 {
		t.Fatalf("replay started %d providers", h.starts)
	}
	q["session_name"] = "changed"
	rejected(t, e, "launch", q, "session_operation_conflict")
	if h.starts != 1 {
		t.Fatal("conflict started a provider")
	}
	q["session_name"] = "example"
	call(t, e, "launch", q)
}
func marshal(v any) []byte { b, _ := json.Marshal(v); return b }

// Unseen submissions older than seven days cannot start; the exact boundary
// admits, and recorded results remain replayable after the retention window.
func TestExpiryAndRetention(t *testing.T) {
	e, h := testEngine(t)
	q := request(1)
	q["submitted_at"] = testClock.Add(-7*24*time.Hour - time.Second).Format(time.RFC3339)
	rejected(t, e, "launch", q, "session_operation_expired")
	if h.starts != 0 {
		t.Fatal("expired request started provider")
	}
	q["submitted_at"] = testClock.Add(-7 * 24 * time.Hour).Format(time.RFC3339)
	call(t, e, "launch", q)
	e.Now = func() time.Time { return testClock.Add(14 * 24 * time.Hour) }
	call(t, e, "launch", q)
	if h.starts != 1 {
		t.Fatal("retained replay started provider")
	}
}

// A durable cancellation tombstone fences late delivery;
// cancelling a started operation reports its outcome without stopping it.
func TestCancelFence(t *testing.T) {
	e, h := testEngine(t)
	q := request(1)
	v := decode[map[string]any](t, call(t, e, "cancel", map[string]any{"operation_id": q["operation_id"]}))
	if v["outcome"] != "cancelled" {
		t.Fatal("missing tombstone")
	}
	rejected(t, e, "launch", q, "session_host_unavailable")
	if h.starts != 0 {
		t.Fatal("cancelled operation started provider")
	}
	q = request(2)
	call(t, e, "launch", q)
	v = decode[map[string]any](t, call(t, e, "cancel", map[string]any{"operation_id": q["operation_id"]}))
	if v["outcome"] != "started" || h.starts != 1 {
		t.Fatal("cancel changed admitted operation")
	}
}

// Reservation excludes a competing key while the provider start is in flight,
// status can observe it, and cancelling admitted evidence changes nothing.
func TestAdmissionReservation(t *testing.T) {
	e, h := testEngine(t)
	h.entered = make(chan struct{})
	h.release = make(chan struct{})
	q := request(1)
	q["admission_key"] = "werkstatt:example"
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := e.Handle(ctx, "launch", marshal(q)); done <- err }()
	select {
	case <-h.entered:
	case <-ctx.Done():
		t.Fatal("provider start not entered")
	}
	other := request(2)
	other["admission_key"] = q["admission_key"]
	rejected(t, e, "launch", other, "werkstatt_already_up")
	v := decode[map[string]any](t, call(t, e, "cancel", map[string]any{"operation_id": q["operation_id"]}))
	if v["outcome"] != "admitted" {
		t.Fatal("cancel changed reservation")
	}
	close(h.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("provider launch did not complete")
	}
	if h.starts != 1 {
		t.Fatal("competing reservation started provider")
	}
}

// Unknown starts hold their key after restart and refuse replay. Distinct null
// keys remain independent rather than becoming one implicit global owner.
func TestUnknownAndNullAdmission(t *testing.T) {
	e, h := testEngine(t)
	h.failure = Uncertain{}
	q := request(1)
	q["admission_key"] = "werkstatt:example"
	rejected(t, e, "launch", q, "session_liveness_unknown")
	h.failure = nil
	other := request(2)
	other["admission_key"] = q["admission_key"]
	rejected(t, e, "launch", other, "werkstatt_already_up")
	rejected(t, e, "launch", q, "session_liveness_unknown")
	call(t, e, "launch", request(3))
	call(t, e, "launch", request(4))
	if h.starts != 3 {
		t.Fatalf("null admission starts=%d", h.starts)
	}
}

// A typed failure remains a successful historical status response on
// a later ledger read; replay retains its code without another provider start.
func TestFailedOutcome(t *testing.T) {
	e, h := testEngine(t)
	h.failure = Fail("policy_refused")
	q := request(1)
	rejected(t, e, "launch", q, "policy_refused")
	h.failure = nil
	v := decode[map[string]any](t, call(t, e, "status", map[string]any{"operation_id": q["operation_id"]}))
	if v["outcome"] != "failed" || v["code"] != "policy_refused" {
		t.Fatal("failed evidence lost")
	}
	rejected(t, e, "launch", q, "policy_refused")
	if h.starts != 1 {
		t.Fatal("failed replay started provider")
	}
}

// Pages freeze values and membership; label matching is exact, and a cursor
// cannot be transferred to another filter or page size.
func TestPaginationSnapshot(t *testing.T) {
	e, h := testEngine(t)
	q := request(1)
	q["labels"] = map[string]string{"selector": "value"}
	call(t, e, "launch", q)
	q = request(2)
	q["labels"] = map[string]string{"selector": "value"}
	call(t, e, "launch", q)
	filter := map[string]any{"labels": map[string]string{"selector": "value"}, "limit": 1}
	first := decode[struct {
		Sessions []Record
		Next     *string `json:"next_cursor"`
	}](t, call(t, e, "list", filter))
	if len(first.Sessions) != 1 || first.Next == nil {
		t.Fatal("missing snapshot positive control")
	}
	h.mu.Lock()
	for id, s := range h.sessions {
		s.State = "down"
		h.sessions[id] = s
	}
	h.mu.Unlock()
	call(t, e, "launch", request(3))
	filter["cursor"] = *first.Next
	second := decode[struct {
		Sessions []Record
		Next     *string `json:"next_cursor"`
	}](t, call(t, e, "list", filter))
	if len(second.Sessions) != 1 || second.Next != nil || second.Sessions[0].Session == first.Sessions[0].Session || second.Sessions[0].State != "unknown" {
		t.Fatal("continuation changed snapshot")
	}
	filter["limit"] = 2
	rejected(t, e, "list", filter, "launch_plan_invalid")
	page := decode[struct{ Sessions []Record }](t, call(t, e, "list", map[string]any{"labels": map[string]string{"selector": "val"}}))
	if len(page.Sessions) != 0 {
		t.Fatal("label prefix matched")
	}
}

// Missing host evidence projects only indexed identity and labels, and can
// never support a historical started outcome. Corrupt ledger bytes fail closed.
func TestUnreadableEvidence(t *testing.T) {
	e, h := testEngine(t)
	q := request(1)
	r := decode[Record](t, call(t, e, "launch", q))
	h.unreadable = true
	row := decode[Record](t, call(t, e, "status", map[string]any{"session": r.Session}))
	if row.Readable || row.Generation != nil || row.Runtime != nil || row.State != "unknown" || row.Session != r.Session {
		t.Fatal("unreadable row invented lifecycle")
	}
	v := decode[map[string]any](t, call(t, e, "status", map[string]any{"operation_id": q["operation_id"]}))
	if v["outcome"] != "unknown" || v["session"] != nil {
		t.Fatal("unreadable evidence claimed started")
	}
	if err := os.WriteFile(filepath.Join(e.Dir, "ledger.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	rejected(t, e, "list", map[string]any{}, "session_host_unavailable")
	if h.starts != 1 {
		t.Fatal("corruption started provider")
	}
}

// Malformed, duplicate-key, oversized, secret and reserved requests refuse
// before host execution, next to a harmless child environment positive control.
func TestWireRejections(t *testing.T) {
	e, h := testEngine(t)
	q := request(1)
	q["child_env"] = map[string]string{"SAFE_OPTION": "safe"}
	call(t, e, "launch", q)
	for _, bad := range []map[string]string{{"SAFE_OPTION": "sk-sentinel"}, {"MY_API_KEY": "safe"}, {"SAFE_OPTION": strings.Repeat("a", 4097)}} {
		q["child_env"] = bad
		rejected(t, e, "launch", q, "secret_policy_violation")
	}
	q["child_env"] = map[string]string{"CODEX_HOME": "safe"}
	rejected(t, e, "launch", q, "secret_policy_violation")
	if _, err := Reply("refusal", Refusal{Protocol: Protocol, Code: "credential_reserved_name", Details: map[string]string{"name": "CODEX_HOME"}}); err == nil {
		t.Fatal("launcher-local reserved-name code escaped the plugin schema")
	}
	for _, b := range [][]byte{[]byte(`{"x":1,"x":1}`), []byte(`{} {}`), []byte(`{"extra":true}`)} {
		if _, _, err := e.Handle(context.Background(), "describe", b); err == nil {
			t.Fatal("malformed request accepted")
		}
	}
	if _, err := Read(strings.NewReader(strings.Repeat(" ", Limit+1))); err == nil {
		t.Fatal("oversize request accepted")
	}
	if h.starts != 1 {
		t.Fatal("invalid input started a provider")
	}
}

// The canonical digest normalizes object order, escapes and safe integral
// exponent spellings; it preserves materially different Unicode strings.
func TestCanonicalDigest(t *testing.T) {
	a, err := Digest([]byte(`{"z":1e0,"a":"\u0061"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Digest([]byte(`{"a":"a","z":1}`))
	if err != nil || a != b {
		t.Fatal("equivalent JSON has different digest")
	}
	c, _ := Digest([]byte(`{"a":"a\u2028","z":1}`))
	if c == b {
		t.Fatal("different Unicode has equal digest")
	}
}

// Existing ledger loss and a symlink substitution never reset operation
// authority; the external target is unchanged and no provider starts.
func TestLedgerAuthority(t *testing.T) {
	for _, kind := range []string{"missing", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			e, h := testEngine(t)
			call(t, e, "launch", request(1))
			path := filepath.Join(e.Dir, "ledger.json")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if kind == "symlink" {
				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
				defer func() {
					b, _ := os.ReadFile(target)
					if string(b) != "preserve" {
						t.Error("external file modified")
					}
				}()
			}
			rejected(t, e, "launch", request(2), "session_host_unavailable")
			if h.starts != 1 {
				t.Fatal("lost authority started provider")
			}
		})
	}
}

// Recovery preserves a pre-start reservation, but start intent becomes unknown;
// both retain the admission key and neither may execute a duplicate provider.
func TestRecoveryStartIntent(t *testing.T) {
	for _, intent := range []bool{false, true} {
		t.Run(fmt.Sprint(intent), func(t *testing.T) {
			e, h := testEngine(t)
			q := request(1)
			q["admission_key"] = "werkstatt:crash"
			digest, _ := Digest(marshal(q))
			key := q["admission_key"].(string)
			if err := e.transaction(func(s *ledger) (bool, error) {
				s.Operations[q["operation_id"].(string)] = &operation{Digest: digest, Key: &key, State: "admitted", Generation: 1, StartIntent: intent}
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := e.Recover(); err != nil {
				t.Fatal(err)
			}
			want := "admitted"
			if intent {
				want = "unknown"
			}
			v := call(t, e, "status", map[string]any{"operation_id": q["operation_id"]}).(map[string]any)
			if v["outcome"] != want {
				t.Fatalf("intent=%v: want %s, got %v", intent, want, v)
			}
			q2 := request(2)
			q2["admission_key"] = key
			rejected(t, e, "launch", q2, "werkstatt_already_up")
			if h.starts != 0 {
				t.Fatal("recovery started provider")
			}
		})
	}
}

// Constructor hooks are inert by default, including with hostile environment
// values. Only a directly installed hook can interrupt the durable boundary.
func TestCheckpointDefaultInert(t *testing.T) {
	t.Setenv("TASK_BOARD_PLUGIN_FAULT", "crash-after-reservation")
	e, h := testEngine(t)
	call(t, e, "launch", request(1))
	if h.starts != 1 {
		t.Fatal("ambient environment enabled a hook")
	}
	e.Checkpoint = func(phase string) error {
		if phase == "prepared" {
			return unavailable()
		}
		return nil
	}
	q := request(2)
	rejected(t, e, "launch", q, "session_host_unavailable")
	v := call(t, e, "status", map[string]any{"operation_id": q["operation_id"]}).(map[string]any)
	if v["outcome"] != "prepared" || h.starts != 1 {
		t.Fatal("explicit hook missed boundary")
	}
	call(t, e, "cancel", map[string]any{"operation_id": q["operation_id"]})
	e.Checkpoint = nil
	rejected(t, e, "launch", q, "session_host_unavailable")
}

// A maximum-size label set cannot turn a valid multirow list into oversized
// wire output; continuation preserves every row while respecting the limit.
func TestLargePageBound(t *testing.T) {
	e, _ := testEngine(t)
	labels := map[string]string{}
	for i := 0; i < 16; i++ {
		labels[fmt.Sprintf("label%d", i)] = strings.Repeat("a", 128)
	}
	for i := 1; i <= 30; i++ {
		q := request(i)
		q["labels"] = labels
		call(t, e, "launch", q)
	}
	total := 0
	cursor := ""
	for {
		q := map[string]any{"labels": labels, "limit": 100}
		if cursor != "" {
			q["cursor"] = cursor
		}
		v := decode[struct {
			Sessions []Record `json:"sessions"`
			Next     *string  `json:"next_cursor"`
		}](t, call(t, e, "list", q))
		if len(v.Sessions) == 0 {
			t.Fatal("empty continuation")
		}
		total += len(v.Sessions)
		if v.Next == nil {
			break
		}
		cursor = *v.Next
		if total >= 30 {
			t.Fatal("continuation failed to terminate")
		}
	}
	if total != 30 {
		t.Fatal("large page lost rows")
	}
}
