package codexadapter

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

const maxCheckpoint = 8192

type protocolRecord struct {
	PlanDigest string   `json:"plan_digest"`
	Phase      string   `json:"phase"`
	View       Snapshot `json:"view"`
	Request    uint64   `json:"request"`
	Pending    uint64   `json:"pending"`
	Operation  string   `json:"operation"`
}
type streamRecord struct {
	Version  int            `json:"version"`
	Protocol protocolRecord `json:"protocol"`
	Offset   uint64         `json:"offset"`
	Closed   bool           `json:"closed"`
}

func planDigest(cfg Config) string {
	b, _ := json.Marshal(cfg)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Checkpoint retains only protocol metadata and the last complete-frame offset.
// Partial output is intentionally excluded: replay it from the runner, rather
// than persisting private message content in the adapter checkpoint.
func (s *Stream) Checkpoint() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.protocol
	p.mu.Lock()
	defer p.mu.Unlock()
	r := streamRecord{Version: 1, Protocol: protocolRecord{PlanDigest: planDigest(p.cfg), Phase: p.phase, View: p.view, Request: p.request, Pending: p.pending, Operation: p.operation}, Offset: s.next - uint64(len(s.partial)), Closed: s.closed}
	b, err := json.Marshal(r)
	if err != nil || len(b) > maxCheckpoint {
		return nil, errors.New("checkpoint_bound")
	}
	return b, nil
}

// RestoreStream emits no control traffic. Input stays closed until the host
// replays from the returned offset and confirms the actual retained output tail.
func RestoreStream(cfg Config, b []byte) (*Stream, uint64, error) {
	if len(b) == 0 || len(b) > maxCheckpoint || !json.Valid(b) || !uniqueKeys(b) {
		return nil, 0, errors.New("checkpoint_refused")
	}
	outer, ok := requiredFields(b, "version", "protocol", "offset", "closed")
	if !ok {
		return nil, 0, errors.New("checkpoint_refused")
	}
	inner, ok := requiredFields(outer["protocol"], "plan_digest", "phase", "view", "request", "pending", "operation")
	if !ok {
		return nil, 0, errors.New("checkpoint_refused")
	}
	if _, ok := requiredFields(inner["view"], "State", "Thread", "Turn", "CompletedTurn", "AuthorityRequired"); !ok {
		return nil, 0, errors.New("checkpoint_refused")
	}
	var r streamRecord
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&r) != nil || r.Version != 1 || r.Protocol.PlanDigest != planDigest(cfg) || !validProtocolRecord(r.Protocol) {
		return nil, 0, errors.New("checkpoint_refused")
	}
	s, err := NewStream(cfg)
	if err != nil {
		return nil, 0, err
	}
	s.next = r.Offset
	s.closed = r.Closed
	s.replaying = true
	p := s.protocol
	p.phase = r.Protocol.Phase
	p.view = r.Protocol.View
	p.request = r.Protocol.Request
	p.pending = r.Protocol.Pending
	p.operation = r.Protocol.Operation
	if s.closed {
		p.phase = "lost"
		p.view.State = Unknown
		p.view.CompletedTurn = ""
	}
	return s, r.Offset, nil
}

func requiredFields(b []byte, keys ...string) (map[string]json.RawMessage, bool) {
	var m map[string]json.RawMessage
	if json.Unmarshal(b, &m) != nil || len(m) != len(keys) {
		return nil, false
	}
	for _, k := range keys {
		v, ok := m[k]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return nil, false
		}
	}
	return m, true
}

func validProtocolRecord(r protocolRecord) bool {
	if r.View.AuthorityRequired && r.Phase != "approval" && r.Phase != "lost" {
		return false
	}
	if r.Request == ^uint64(0) || r.Pending > r.Request {
		return false
	}
	if r.Pending == 0 && r.Operation != "" {
		return false
	}
	if r.Pending != 0 && (r.Pending != r.Request || (r.Operation != "initialize" && r.Operation != "thread/start" && r.Operation != "turn/start")) {
		return false
	}
	for _, v := range []string{r.View.Thread, r.View.Turn, r.View.CompletedTurn} {
		if v != "" && !identifier(v) {
			return false
		}
	}
	if r.View.CompletedTurn != "" && (r.View.State != Idle || r.View.CompletedTurn != r.View.Turn) {
		return false
	}
	switch r.Phase {
	case "new":
		return r.View.State == Unknown && r.Request == 0 && r.Pending == 0 && r.View.Thread == ""
	case "initialize":
		return r.View.State == Unknown && r.Request == 1 && r.Pending == 1 && r.Operation == "initialize" && r.View.Thread == ""
	case "thread":
		return r.View.State == Unknown && r.Request == 2 && r.Pending == 2 && r.Operation == "thread/start" && r.View.Thread == ""
	case "ready":
		return r.View.State == Idle && identifier(r.View.Thread) && r.Request >= 2 && r.Pending == 0
	case "turn":
		return r.View.State == Busy && identifier(r.View.Thread) && r.Request >= 3 && (r.Pending == 0 || r.Operation == "turn/start")
	case "approval":
		return r.View.State == Approval && r.View.AuthorityRequired
	case "lost":
		return r.View.State == Unknown
	}
	return false
}
