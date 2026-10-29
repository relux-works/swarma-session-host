package sessionhost_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// The real Codex driver emits initialized + thread/start in one transition.
// A synthetic runner commits the first action then reports output termination
// in its reply: the host must persist its fence before another RPC and retain
// the second action without sending it. A live reply initializes normally.
func TestProtocolTerminalWriteReplyFencesOutbox(t *testing.T) {
	for _, ended := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "ended"}[ended], func(t *testing.T) {
			cfg, _, dir := setupProtocol(t, "normal")
			listener, err := localipc.Listen(filepath.Join(dir, "reply.sock"))
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			cfg.Socket = listener.Addr().String()
			var writes atomic.Int32
			var durableFence atomic.Bool
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.AcceptUnix()
				if err != nil {
					return
				}
				defer conn.Close()
				reader := bufio.NewReader(conn)
				var output []byte
				var epoch uint64
				for {
					var req runner.Request
					if localipc.Read(reader, &req) != nil {
						return
					}
					r := runner.Response{Code: "ok", Instance: strings.Repeat("b", 32), PID: 1, Epoch: epoch, ProtocolVersion: runner.ProtocolVersion, WriteReceiptVersion: runner.WriteReceiptVersion}
					if ended && writes.Load() >= 2 {
						var s sessionhost.State
						data, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
						if err == nil && json.Unmarshal(data, &s) == nil && s.Fence == "unknown" {
							durableFence.Store(true)
						}
					}
					switch req.Op {
					case "claim":
						epoch = req.Epoch
						r.Epoch = epoch
					case "write":
						writes.Add(1)
						r.Code = "written"
						var rpc struct {
							ID     uint64         `json:"id"`
							Method string         `json:"method"`
							Params map[string]any `json:"params"`
						}
						if json.Unmarshal(req.Data, &rpc) != nil {
							return
						}
						var result any
						switch rpc.Method {
						case "initialize":
							result = map[string]any{"id": rpc.ID, "result": map[string]any{"userAgent": "synthetic", "platformFamily": "unix", "platformOs": "macos"}}
						case "thread/start":
							result = map[string]any{"id": rpc.ID, "result": map[string]any{"thread": map[string]any{"id": "synthetic-thread"}, "model": rpc.Params["model"], "modelProvider": rpc.Params["modelProvider"]}}
						}
						if result != nil {
							b, _ := json.Marshal(result)
							output = append(output, append(b, '\n')...)
						}
					case "read":
						if req.Offset >= 0 && req.Offset <= len(output) {
							r.Data = append([]byte(nil), output[req.Offset:]...)
						} else {
							return
						}
					default:
						if req.Op != "status" {
							return
						}
					}
					r.Writes = int(writes.Load())
					r.Offset = len(output)
					r.OutputEnded = ended && r.Writes >= 2
					conn.SetWriteDeadline(time.Now().Add(time.Second))
					if localipc.Write(conn, r) != nil {
						return
					}
				}
			}()
			h := openHost(t, cfg)
			if ended {
				s := awaitProtocol(t, h, "terminal reply fence", func(s sessionhost.State) bool {
					return s.Fence == "unknown" && s.InputState == sessionhost.InputUnknown
				})
				if s.Protocol.WireSequence != 3 || len(s.Protocol.Pending) != 1 || s.Protocol.Pending[0].Sequence != 3 {
					t.Fatal("terminal reply lost valid receipt or remaining intent")
				}
				deadline := time.Now().Add(time.Second)
				for !durableFence.Load() && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if !durableFence.Load() {
					t.Fatal("terminal reply fence not durable before next RPC")
				}
			} else {
				awaitProtocol(t, h, "live two-action initialization", func(s sessionhost.State) bool {
					return s.InputState == sessionhost.InputIdle && len(s.Protocol.Pending) == 0
				})
			}
			h.Close()
			<-done
			want := int32(3)
			if ended {
				want = 2
			}
			if writes.Load() != want {
				t.Fatalf("post-EOF outbox write: got %d want %d", writes.Load(), want)
			}
		})
	}
}
