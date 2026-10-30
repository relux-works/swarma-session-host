package codexadapter

import (
	"bytes"
	"encoding/json"
	"testing"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/testsupport/adaptercorpus"
)

const offlineSentinel = "fabricated-offline-output-only-canary"

// Metadata membership and values come from the fixture contract; the checker
// does not call production checkpoint validators to obtain its expected verdict.
func offlineMetadata(t *testing.T, saved []byte) {
	t.Helper()
	cfg := driverConfig()
	values := adaptercorpus.MetadataValues{Model: cfg.Selection.Model, Provider: cfg.Selection.Provider, Effort: cfg.Selection.Effort, Workspace: cfg.Workspace, Thread: "thread-a", Turns: []string{"turn-a"}, Inputs: []string{"offline-next"}}
	if err := adaptercorpus.CheckpointMetadata(saved, values, 0); err != nil {
		t.Fatal(err)
	}
}

func offlineRequest() ap.Input {
	return ap.Input{ID: "offline-next", Reference: "synthetic.offline", Mode: ap.NextTurn}
}

// These 50 direct cases cover selected factor pairs only. Complete valid replay
// admits one next-turn; malformed offsets/bounds/EOF and corrupt checkpoints
// remain closed. Earlier refusals are recorded, never counted as later injections.
func TestOfflineAdapterCorpus(t *testing.T) {
	for _, tc := range adaptercorpus.Generate() {
		if tc.Array != "adapter" {
			continue
		}
		t.Run(tc.ID, func(t *testing.T) {
			status := map[string]string{"C": "not-reached", "O": "not-reached", "V": "not-reached", "E": "not-reached", "K": "not-reached"}
			defer func() {
				b, _ := json.Marshal(struct {
					Case   adaptercorpus.Case
					Status map[string]string
				}{tc, status})
				t.Log("OFFLINE_CASE " + string(b))
			}()
			d, offset := idleDriver(t)
			full := []byte("{\"method\":\"item/agentMessage/delta\",\"params\":{\"delta\":\"" + offlineSentinel + "\"}}\n")
			prefix := full[:len(full)-3]
			at := offset
			for _, p := range adaptercorpus.Parts(prefix, tc.C) {
				x, err := d.Observe(ap.Chunk{Offset: at, Bytes: p})
				at += uint64(len(p))
				if err != nil || len(x.Actions) != 0 || len(x.Facts) != 0 {
					t.Fatal("OFFLINE_PREFIX", err)
				}
			}
			status["C"] = "reached-prefix-partition"
			saved := d.Snapshot().Checkpoint
			offlineMetadata(t, saved)
			mutated, err := adaptercorpus.Corrupt(saved, tc.K)
			if err != nil {
				t.Fatal("OFFLINE_CHECKPOINT_SETUP", err)
			}
			r, from, err := OpenDriver(driverConfig(), mutated)
			status["K"] = "reached"
			if tc.K != 0 {
				want := "checkpoint_refused"
				if tc.K == 1 {
					want = "driver_checkpoint_refused"
				}
				if err == nil || r != nil || err.Error() != want {
					t.Fatalf("OFFLINE_CHECKPOINT_REFUSAL: want=%s got=%v", want, err)
				}
				for _, k := range []string{"O", "V", "E"} {
					status[k] = "not-reached-checkpoint-refusal"
				}
				return
			}
			if err != nil || from != offset {
				t.Fatalf("OFFLINE_REPLAY_OFFSET: from=%d expected=%d err=%v", from, offset, err)
			}
			begun, err := r.Begin()
			if err != nil || len(begun.Actions) != 0 || len(begun.Facts) != 0 {
				t.Fatal("OFFLINE_NO_REPLAY: restored initialization", err)
			}
			// Probe the stream's own replay gate as well as the public driver.
			// The stream has no restored partial bytes, so this catches a removed
			// replay guard independently of the driver's additional input fence.
			if b, err := r.stream.Submit("synthetic.replay-probe"); err == nil || len(b) != 0 {
				t.Fatal("OFFLINE_INPUT_FENCE: stream admitted pre-tail input")
			}
			if x, err := r.Prepare(offlineRequest()); err == nil || len(x.Actions) != 0 {
				t.Fatal("OFFLINE_INPUT_FENCE: driver admitted pre-tail input")
			}
			var parts [][]byte
			switch tc.V {
			case 0:
				payload := full
				if tc.E != 0 {
					payload = prefix
				}
				parts = adaptercorpus.Parts(payload, tc.C)
			case 1:
				parts = [][]byte{bytes.Repeat([]byte("x"), MaxFrame+1)}
				status["C"] = "reached-prefix-partition;overflow-chunk-dominates-segmentation"
			case 2:
				// Every offered chunk is bounded; only accumulated no-LF bytes
				// cross the line bound. This is distinct from oversized chunks.
				for _, p := range [][]byte{bytes.Repeat([]byte("x"), MaxFrame/2), bytes.Repeat([]byte("x"), MaxFrame/2+1)} {
					parts = append(parts, adaptercorpus.Parts(p, tc.C)...)
				}
			}
			current := from
			var gotErr error
			var last ap.Transition
			for n, p := range parts {
				off := current
				if n == 0 && tc.O != 0 {
					if tc.O == 1 {
						off++
					} else {
						off--
					}
				}
				last, gotErr = r.Observe(ap.Chunk{Offset: off, Bytes: p, AtTail: n == len(parts)-1 && tc.E == 0})
				status["O"] = "reached"
				if tc.V != 0 {
					status["V"] = "reached"
				}
				if len(last.Actions) != 0 || len(last.Facts) != 0 {
					t.Fatal("OFFLINE_NO_REPLAY: observation emitted effect")
				}
				current += uint64(len(p))
				if gotErr != nil {
					break
				}
			}
			wantErr := ""
			switch {
			case tc.O != 0:
				wantErr = "stream_offset_or_bound"
				if tc.V != 0 {
					status["V"] = "dominated-by-offset"
				}
			case tc.V == 1:
				wantErr = "stream_offset_or_bound"
			case tc.V == 2:
				wantErr = "stream_frame_bound"
			}
			if wantErr != "" {
				marker := "OFFLINE_OFFSET"
				if tc.O == 0 {
					marker = "OFFLINE_FRAME_BOUND"
				}
				if gotErr == nil || gotErr.Error() != wantErr || last.View.Activity != ap.Unknown {
					t.Fatalf("%s: want=%s got=%v state=%s", marker, wantErr, gotErr, last.View.Activity)
				}
				if tc.E != 0 {
					status["E"] = "not-reached-earlier-refusal"
				}
			} else if gotErr != nil {
				t.Fatal("OFFLINE_VALID_REPLAY", gotErr)
			}
			if wantErr == "" && tc.E != 0 {
				r.stream.EOF()
				status["E"] = "reached-partial-frame"
				tail := full[len(prefix):]
				if _, err := r.stream.Feed(current, tail); err == nil || err.Error() != "stream_closed" {
					t.Fatalf("OFFLINE_EOF_STICKY: suffix after EOF: %v", err)
				}
				last = r.Snapshot()
			}
			offlineMetadata(t, r.Snapshot().Checkpoint)
			x, err := r.Prepare(offlineRequest())
			if wantErr != "" || tc.E != 0 {
				if err == nil || len(x.Actions) != 0 || x.View.Activity != ap.Unknown {
					t.Fatal("OFFLINE_INPUT_FENCE: failed evidence opened input", err)
				}
			} else {
				if err != nil || len(x.Actions) != 1 || x.View.Activity != ap.Busy {
					t.Fatal("OFFLINE_VALID_NEXT_TURN", err)
				}
				if len(x.Facts) != 0 {
					t.Fatal("OFFLINE_NO_REPLAY: preparation invented acceptance")
				}
			}
		})
	}
}

// The line limit accepts a complete valid MaxFrame-byte JSON object, while a
// bounded-chunk partial line one byte larger is rejected before any LF/parser.
func TestOfflineFrameBoundary(t *testing.T) {
	d, offset := idleDriver(t)
	prefix := []byte("{\"method\":\"future/notification\",\"params\":{\"padding\":\"")
	suffix := []byte("\"}}")
	line := append(append(append([]byte(nil), prefix...), bytes.Repeat([]byte("x"), MaxFrame-len(prefix)-len(suffix))...), suffix...)
	if len(line) != MaxFrame {
		t.Fatal("OFFLINE_BOUNDARY_SETUP")
	}
	x, err := d.Observe(ap.Chunk{Offset: offset, Bytes: line})
	if err != nil || len(x.Actions) != 0 {
		t.Fatal("OFFLINE_VALID_FRAME_BOUND", err)
	}
	x, err = d.Observe(ap.Chunk{Offset: offset + MaxFrame, Bytes: []byte{'\n'}, AtTail: true})
	if err != nil || x.View.Activity != ap.Idle {
		t.Fatal("OFFLINE_VALID_FRAME_BOUND", err)
	}
	s, _ := NewStream(config())
	s.Initialize()
	if _, err := s.Feed(0, bytes.Repeat([]byte("x"), MaxFrame)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Feed(MaxFrame, []byte("x")); err == nil || err.Error() != "stream_frame_bound" {
		t.Fatalf("OFFLINE_FRAME_BOUND: %v", err)
	}
}

// An actual checkpoint taken with a private partial suffix remains metadata-only;
// the fixture's nonempty sentinel prevents an always-empty privacy check.
func TestOfflineMetadataWitness(t *testing.T) {
	d, offset := idleDriver(t)
	p := []byte("{\"method\":\"future/notification\",\"params\":{\"text\":\"" + offlineSentinel)
	if _, err := d.Observe(ap.Chunk{Offset: offset, Bytes: p}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(d.stream.partial, []byte(offlineSentinel)) {
		t.Fatal("OFFLINE_METADATA_SETUP")
	}
	offlineMetadata(t, d.Snapshot().Checkpoint)
}
