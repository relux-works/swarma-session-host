package codexadapter

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

// Driver adapts the release-specific parser to the shared host-side boundary.
// It performs no I/O and currently qualifies next-turn synthetic notices only.
type Driver struct {
	mu                           sync.Mutex
	stream                       *Stream
	inputID, accepted, completed string
	begun, restored, atTail      bool
}

type driverRecord struct {
	Version   int             `json:"version"`
	Stream    json.RawMessage `json:"stream"`
	InputID   string          `json:"input_id"`
	Accepted  string          `json:"accepted"`
	Completed string          `json:"completed"`
	Begun     bool            `json:"begun"`
}

var _ ap.Driver = (*Driver)(nil)

func Descriptor() ap.Descriptor {
	return ap.Descriptor{Name: "codex", PrimaryTransport: "pipe", Capabilities: ap.Capabilities{NextTurn: true, NativeTurnIdentity: true, Activity: true, IdleFence: true}}
}
func (d *Driver) Descriptor() ap.Descriptor { return Descriptor() }

// OpenDriver restores metadata only. The returned offset is a replay request,
// never authority to trust an unobserved runner or write another turn.
func OpenDriver(cfg ap.Config, saved []byte) (*Driver, uint64, error) {
	if !Descriptor().SupportsLifecycle(cfg) || cfg.NativeSession != "" {
		return nil, 0, ap.ErrUnsupported
	}
	c := Config{Provider: cfg.Selection.Provider, Model: cfg.Selection.Model, Effort: cfg.Selection.Effort, Workspace: cfg.Workspace}
	if len(saved) == 0 {
		s, err := NewStream(c)
		if err != nil {
			return nil, 0, err
		}
		return &Driver{stream: s}, 0, nil
	}
	if len(saved) > ap.MaxCheckpoint || !json.Valid(saved) || !uniqueKeys(saved) {
		return nil, 0, errors.New("driver_checkpoint_refused")
	}
	if _, ok := requiredFields(saved, "version", "stream", "input_id", "accepted", "completed", "begun"); !ok {
		return nil, 0, errors.New("driver_checkpoint_refused")
	}
	var r driverRecord
	decoder := json.NewDecoder(bytes.NewReader(saved))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&r) != nil || r.Version != 1 {
		return nil, 0, errors.New("driver_checkpoint_refused")
	}
	for _, v := range []string{r.InputID, r.Accepted, r.Completed} {
		if v != "" && !identifier(v) {
			return nil, 0, errors.New("driver_checkpoint_refused")
		}
	}
	if (r.Accepted != "" || r.Completed != "") && r.InputID == "" {
		return nil, 0, errors.New("driver_checkpoint_refused")
	}
	if r.Completed != "" && r.Completed != r.Accepted {
		return nil, 0, errors.New("driver_checkpoint_refused")
	}
	s, offset, err := RestoreStream(c, r.Stream)
	if err != nil {
		return nil, 0, err
	}
	return &Driver{stream: s, inputID: r.InputID, accepted: r.Accepted, completed: r.Completed, begun: r.Begun, restored: true}, offset, nil
}

func (d *Driver) checkpoint() ([]byte, error) {
	b, err := d.stream.Checkpoint()
	if err != nil {
		return nil, err
	}
	return json.Marshal(driverRecord{Version: 1, Stream: b, InputID: d.inputID, Accepted: d.accepted, Completed: d.completed, Begun: d.begun})
}

func (d *Driver) snapshot() ap.Transition {
	v := d.stream.Snapshot()
	activity := ap.Activity(v.State)
	if !d.atTail {
		activity = ap.Unknown
	}
	b, _ := d.checkpoint()
	t := ap.Transition{View: ap.View{InputID: d.inputID, NativeSession: v.Thread, CurrentTurn: v.Turn, Activity: activity}, Checkpoint: b}
	if v.AuthorityRequired {
		p := d.stream.protocol
		p.mu.Lock()
		request := p.request
		p.mu.Unlock()
		t.Attention = []ap.Attention{{Kind: ap.AuthorityRequired, Key: fmt.Sprintf("codex-authority-%d", request), Class: "unclassified", OwnerOnly: true}}
	}
	return t
}
func (d *Driver) Snapshot() ap.Transition { d.mu.Lock(); defer d.mu.Unlock(); return d.snapshot() }

func action(w []byte, input string) ap.Action {
	var id struct {
		ID uint64 `json:"id"`
	}
	json.Unmarshal(w, &id)
	key := "codex-initialized"
	if id.ID != 0 {
		key = fmt.Sprintf("codex-rpc-%d", id.ID)
	}
	if input != "" {
		sum := sha256.Sum256([]byte(input))
		key = "notice-" + hex.EncodeToString(sum[:])
	}
	return ap.Action{Key: key, InputID: input, Bytes: w}
}

func (d *Driver) finish(actions []ap.Action, facts []ap.Fact) (ap.Transition, error) {
	t := d.snapshot()
	t.Actions = actions
	t.Facts = facts
	if ap.Validate(Descriptor(), t) != nil {
		d.stream.EOF()
		d.atTail = false
		return d.snapshot(), errors.New("driver_transition_refused")
	}
	return t, nil
}

func (d *Driver) Begin() (ap.Transition, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.restored || d.begun {
		return d.finish(nil, nil)
	}
	w, err := d.stream.Initialize()
	if err != nil {
		return d.snapshot(), err
	}
	d.begun = true
	return d.finish([]ap.Action{action(w, "")}, nil)
}

func (d *Driver) Prepare(in ap.Input) (ap.Transition, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !Descriptor().Capabilities.Supports(in.Mode) {
		return d.snapshot(), ap.ErrUnsupported
	}
	if !identifier(in.ID) || !identifier(in.Reference) || in.ExpectedTurn != "" {
		return d.snapshot(), errors.New("invalid_notice_framing")
	}
	if in.ID == d.inputID {
		return d.snapshot(), errors.New("duplicate_protocol_input")
	}
	if d.snapshot().View.Activity != ap.Idle {
		return d.snapshot(), errors.New("driver_input_closed")
	}
	w, err := d.stream.Submit("Notice reference: " + in.Reference + ". For this synthetic notice, reply ok without tools.")
	if err != nil {
		return d.snapshot(), err
	}
	d.inputID = in.ID
	d.accepted = ""
	d.completed = ""
	return d.finish([]ap.Action{action(w, in.ID)}, nil)
}

func (d *Driver) Observe(chunk ap.Chunk) (ap.Transition, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.atTail = false
	wires, err := d.stream.Feed(chunk.Offset, chunk.Bytes)
	if err != nil {
		return d.snapshot(), err
	}
	if chunk.EOF {
		d.stream.EOF()
		return d.finish(nil, nil)
	}
	if chunk.AtTail && d.stream.ConfirmTail(chunk.Offset+uint64(len(chunk.Bytes))) == nil {
		d.atTail = true
	}
	actions := make([]ap.Action, 0, len(wires))
	for _, w := range wires {
		actions = append(actions, action(w, ""))
	}
	var facts []ap.Fact
	v := d.stream.Snapshot()
	if d.atTail && v.State != Unknown && v.State != Approval && d.inputID != "" {
		p := d.stream.protocol
		p.mu.Lock()
		accepted := p.pending == 0 && (p.phase == "turn" || p.phase == "ready") && v.Turn != ""
		p.mu.Unlock()
		if accepted && d.accepted != v.Turn {
			d.accepted = v.Turn
			facts = append(facts, ap.Fact{Kind: ap.InputAccepted, InputID: d.inputID, NativeSession: v.Thread, NativeID: v.Turn})
		}
		if v.CompletedTurn != "" && d.completed != v.CompletedTurn {
			d.completed = v.CompletedTurn
			facts = append(facts, ap.Fact{Kind: ap.TurnCompleted, InputID: d.inputID, NativeSession: v.Thread, NativeID: v.CompletedTurn})
		}
	}
	return d.finish(actions, facts)
}
