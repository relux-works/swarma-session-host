package sessionmanager

import (
	"context"
	"errors"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// Real read-only socket responses qualify the activity boundary without a
// provider process or signal. The idle positive neighbors foreign identity,
// unknown/busy status, contradictory in-progress work and a later busy event.
func TestCodexConditionalFreshActivityRefusals(t *testing.T) {
	for _, name := range []string{"idle", "foreign-thread", "active", "unknown", "idle-with-turn", "later-busy-event"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				client, err := websocket.Accept(w, req, nil)
				if err != nil {
					return
				}
				defer client.Close(websocket.StatusNormalClosure, "")
				for {
					var frame codexRPCRequest
					if err := wsjson.Read(ctx, client, &frame); err != nil {
						return
					}
					if frame.Method == "initialized" {
						continue
					}
					result := any(map[string]any{})
					if frame.Method == "thread/read" {
						id, status := "conditional-thread", "idle"
						turns := []any{}
						switch name {
						case "foreign-thread":
							id = "foreign"
						case "active":
							status = "active"
						case "unknown":
							status = "unknown"
						case "idle-with-turn":
							turns = append(turns, map[string]any{"id": "active-turn", "status": "inProgress"})
						}
						result = map[string]any{"thread": map[string]any{"id": id, "status": map[string]any{"type": status}, "turns": turns}}
						if name == "later-busy-event" {
							if err := wsjson.Write(ctx, client, map[string]any{"method": "turn/started", "params": map[string]any{"threadId": id, "turn": map[string]any{"id": "event-turn", "status": "inProgress"}}}); err != nil {
								return
							}
						}
					} else if frame.Method != "initialize" {
						t.Errorf("read-only acquisition wrote native method %s", frame.Method)
						return
					}
					if err := wsjson.Write(ctx, client, map[string]any{"id": frame.ID, "result": result}); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			r, err := newCodexRunner(CodexRunnerSnapshot{Contract: codexHostSnapshotContract, PID: os.Getpid(), URL: "ws" + strings.TrimPrefix(server.URL, "http"), ThreadID: "conditional-thread", ProjectDir: t.TempDir(), CodexHome: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer r.closeObservation()
			port, ok := any(r).(ProviderConditionalStop)
			if !ok {
				t.Fatal("native runner lacks typed conditional Stop")
			}
			lease, err := port.AcquireIdleStop(ctx)
			if name == "idle" {
				if err != nil || lease.ID == "" {
					t.Fatalf("fresh idle positive refused: %#v / %v", lease, err)
				}
				if err := port.ReleaseIdleStop(ctx, lease); err != nil {
					t.Fatal(err)
				}
			} else {
				expected := errCodexTurnFenced
				if name == "foreign-thread" {
					expected = ErrInputAcknowledgementMismatch
				}
				if lease.ID != "" || !errors.Is(err, expected) {
					t.Fatalf("fresh activity accepted forbidden %s: %#v / %v", name, lease, err)
				}
			}

		})
	}
}
