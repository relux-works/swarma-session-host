package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
)

// Own transport cleanup is independent of ordinary input admission. There is
// no native input to accept, and cleanup may not borrow emergency capacity.
func TestLeaseYieldTimeoutDetachCleanupSkeleton(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		for _, storage := range []string{"room", "capacity", "write-failure", "sync-failure", "commit-failure"} {
			name := "view-only/"
			if claimed {
				name = "claimed/"
			}
			t.Run(name+storage, func(t *testing.T) {
				h := debugFixture(t)
				rights := []string{"view"}
				if claimed {
					rights = append(rights, "writer_control")
				}
				token := debugView(t, h, rights...)
				if claimed {
					debugWant(t, h.debugControl(token, "claim", 0), "")
				}
				h.state.Debug.Results = []debugResult{{ID: "result", Thread: "thread", Turn: "turn", Outcome: "completed", Revision: 1, NotificationPending: true}}
				beforeFacts := append([]debugResult(nil), h.state.Debug.Results...)
				beforeRevision := h.state.Debug.WriterRevision
				j := h.debug.journal
				beforeProtected := [3]int{j.stops, j.revokes, j.terminals}
				want := ""
				switch storage {
				case "capacity":
					j.used = debugJournalCap - debugControlReserve - j.reserved
					want = "input_capacity_exceeded"
				case "write-failure":
					j.file.Close()
					want = "input_storage_unavailable"
				case "sync-failure":
					reader, writer, err := os.Pipe()
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { reader.Close(); writer.Close() })
					if writer.Sync() == nil {
						t.Fatal("sync failure fixture accepted fsync")
					}
					j.file.Close()
					j.file = writer
					want = "input_storage_unavailable"
				case "commit-failure":
					if err := os.WriteFile(filepath.Join(h.store.dir, "state.pending"), []byte("fixture"), 0600); err != nil {
						t.Fatal(err)
					}
					if claimed {
						want = "host_storage_unavailable"
					}
				}
				beforeSequence, beforeUsed, beforeReserved := j.sequence, j.used, j.reserved
				// A diagnostic error reports journal/hold durability, never an occupied slot.
				err := h.debugControl(token, "detach", beforeRevision)
				if h.debug.viewer != "" {
					t.Fatal("detach retained own viewer")
				}
				if _, ok := h.debug.grants[token]; ok {
					t.Fatal("detach retained own grant")
				}
				writer := "automatic"
				revision := beforeRevision
				if claimed {
					writer = "held"
					revision++
				}
				if h.state.Debug.Writer != writer || h.state.Debug.WriterRevision != revision || h.state.StopRequested || !reflect.DeepEqual(h.state.Debug.Results, beforeFacts) {
					t.Fatal("detach stopped/resumed or changed terminal ownership")
				}
				debugWant(t, err, want)
				if beforeProtected != [3]int{j.stops, j.revokes, j.terminals} {
					t.Fatal("cleanup spent protected capacity")
				}
				if storage == "capacity" && (j.sequence != beforeSequence || j.used != beforeUsed || j.reserved != beforeReserved) {
					t.Fatal("capacity cleanup appended or reserved bytes")
				}
				if storage == "commit-failure" && claimed && h.state.Fence != "unknown" {
					t.Fatal("failed hold commit did not retain unknown fence")
				}
				_, err = h.debugRead(token, 0)
				debugWant(t, err, "attach_grant_revoked")
				fresh := debugView(t, h, "view", "stop")
				if storage == "room" || storage == "capacity" {
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					debugWant(t, h.debugStop(ctx, fresh), "")
					debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", h.state.Epoch}, fresh), "")
				}
			})
		}
	}
}

func TestLeaseYieldTimeoutDetachForeignSlotSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	other := debugToken(t, h, "view")
	before := cloneState(h.state)
	sequence := h.debug.journal.sequence
	debugWant(t, h.debugControl(other, "detach", h.state.Debug.WriterRevision), "attach_unauthorized")
	if h.debug.viewer != token || len(h.debug.grants) != 2 || !reflect.DeepEqual(before, h.state) || h.debug.journal.sequence != sequence {
		t.Fatal("foreign detach released another attachment")
	}
	debugWant(t, h.debugControl(token, "detach", h.state.Debug.WriterRevision), "")
	debugWant(t, h.debugAttachView(other), "")
}

// Exercise the actual pre-discard hook and the canonical parser on the same
// frame. The rejected body must not reach either retained or rendered events.
func TestUnknownEventAndTerminalEscapeEnvelopeSkeleton(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const params = `{"threadId":"thread","turnId":"turn","itemId":"item","delta":"unqualified-body-canary"}`
	const method = `"method":"item/agentMessage/delta"`
	const question = `{"threadId":"thread","turnId":"turn","itemId":"item","questions":[{"id":"q","header":"Choice","question":"unqualified-body-canary","options":[{"label":"yes","description":"continue"}]}]}`
	fixtures := []struct {
		name, frame string
		valid       bool
		noEvent     bool
	}{
		{"valid-notification", `{` + method + `,"params":` + params + `}`, true, false},
		{"valid-extra-fields", `{` + method + `,"params":` + params + `,"extra":{"accessToken":"extra-field-canary"}}`, true, false},
		{"valid-whitespace", `  {` + method + `,"params":` + params + `} `, true, false},
		{"valid-request", `{"id":7,"method":"item/tool/requestUserInput","params":` + question + `}`, true, false},
		{"duplicate-method", `{` + method + `,` + method + `,"params":` + params + `}`, false, false},
		{"escaped-duplicate-method", `{` + method + `,"\u006dethod":"item/agentMessage/delta","params":` + params + `}`, false, false},
		{"duplicate-params", `{` + method + `,"params":` + params + `,"params":` + params + `}`, false, false},
		{"nested-duplicate", `{` + method + `,"params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"first","delta":"unqualified-body-canary"}}`, false, false},
		{"duplicate-unknown-field", `{` + method + `,"params":` + params + `,"extra":{"key":1,"key":2}}`, false, false},
		{"method-result", `{` + method + `,"params":` + params + `,"result":{}}`, false, false},
		{"method-null-result", `{` + method + `,"params":` + params + `,"result":null}`, false, false},
		{"method-error", `{` + method + `,"params":` + params + `,"error":{}}`, false, false},
		{"method-null-error", `{` + method + `,"params":` + params + `,"error":null}`, false, false},
		{"request-result", `{"id":7,"method":"item/tool/requestUserInput","params":` + question + `,"result":{}}`, false, false},
		{"request-error", `{"id":7,"method":"item/tool/requestUserInput","params":` + question + `,"error":{}}`, false, false},
		{"duplicate-request-id", `{"id":6,"id":7,"method":"item/tool/requestUserInput","params":` + question + `}`, false, false},
		{"array", `[{` + method + `,"params":` + params + `}]`, false, false},
		{"multiple-objects", `{` + method + `,"params":` + params + `}{}`, false, true},
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
			transition, err := d.Observe(ap.Chunk{Offset: 0, Bytes: []byte(fixture.frame + "\n"), AtTail: true})
			event := tail.read(0).Event
			if fixture.valid {
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(event, []byte("unqualified-body-canary")) {
					t.Fatal("valid payload lost")
				}
				if !bytes.Contains(debugRender(event), []byte("unqualified-body-canary")) {
					t.Fatal("valid rendered control lost")
				}
			} else {
				if err == nil || transition.View.Activity != ap.Unknown || len(transition.Actions) != 0 || len(transition.Facts) != 0 {
					t.Fatal("canonical parser did not fence rejected envelope")
				}
				if fixture.noEvent {
					if len(event) != 0 {
						t.Fatal("malformed frame retained")
					}
				} else {
					var envelope debugEnvelope
					if json.Unmarshal(event, &envelope) != nil || envelope.Kind != "diagnostic" || string(envelope.Payload) != `{"diagnostic":"unqualified_payload"}` || envelope.Thread != "" || envelope.Turn != "" {
						t.Fatalf("unqualified envelope projected: %s", event)
					}
				}
				for _, sink := range [][]byte{event, debugRender(event)} {
					if bytes.Contains(sink, []byte("unqualified-body-canary")) {
						t.Fatal("rejected body retained/rendered")
					}
				}
			}
			for _, sink := range [][]byte{event, debugRender(event), transition.Checkpoint, d.Snapshot().Checkpoint} {
				if bytes.Contains(sink, []byte("extra-field-canary")) {
					t.Fatal("extra field bypassed projection")
				}
			}
			for _, sink := range [][]byte{transition.Checkpoint, d.Snapshot().Checkpoint} {
				if bytes.Contains(sink, []byte("unqualified-body-canary")) {
					t.Fatal("payload leaked to checkpoint")
				}
			}
		})
	}
}
