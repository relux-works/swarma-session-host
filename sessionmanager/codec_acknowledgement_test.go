package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// The existing initial-retry fixture owns one fixed encoded candidate, with no
// private provider queue. It still refuses a foreign committed input/hash.
func (c *initialRetryCodec) AcknowledgeCommitted(ctx context.Context, input LegacyRecordInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if input.SessionID != c.record.SessionID || !bytes.Equal(input.Bytes, c.raw) || input.SHA256 != legacyRecordImage(input.SessionID, input.Bytes).SHA256 {
		return ErrLegacyViewStale
	}
	return nil
}

// JSON here is an isolated neutral test codec, not a production legacy/private
// state conversion. It lets the host's actual writer exercise changing records.
type hostCommitProbeCodec struct {
	encoded map[string][]byte
	ack     func(LegacyRecordInput) error
	acks    int
}

func (c *hostCommitProbeCodec) DecodeLegacy(_ context.Context, input LegacyRecordInput) (RunnerRecord, error) {
	var record RunnerRecord
	if input.SHA256 != legacyRecordImage(input.SessionID, input.Bytes).SHA256 {
		return record, ErrLegacyViewStale
	}
	if err := json.Unmarshal(input.Bytes, &record); err != nil {
		return record, err
	}
	if record.SessionID != input.SessionID {
		return RunnerRecord{}, ErrLegacyViewStale
	}
	return record, nil
}
func (c *hostCommitProbeCodec) encode(record RunnerRecord) ([]byte, error) {
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if c.encoded == nil {
		c.encoded = map[string][]byte{}
	}
	c.encoded[legacyRecordImage(record.SessionID, raw).SHA256] = append([]byte(nil), raw...)
	return raw, nil
}
func (c *hostCommitProbeCodec) EncodeInitial(_ context.Context, record RunnerRecord) ([]byte, error) {
	return c.encode(record)
}
func (c *hostCommitProbeCodec) EncodeMerge(_ context.Context, _ LegacyRecordInput, record RunnerRecord) ([]byte, error) {
	return c.encode(record)
}
func (c *hostCommitProbeCodec) AcknowledgeCommitted(ctx context.Context, input LegacyRecordInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.acks++
	if input.SHA256 != legacyRecordImage(input.SessionID, input.Bytes).SHA256 || !bytes.Equal(c.encoded[input.SHA256], input.Bytes) {
		return ErrLegacyViewStale
	}
	if c.ack != nil {
		return c.ack(input)
	}
	return nil
}

func hostCommitProbe(t *testing.T) (*codecRecords, *hostCommitProbeCodec, RunnerRecord, string) {
	t.Helper()
	store, original, path := initialRetryStore(t)
	codec := &hostCommitProbeCodec{}
	store.codec = codec
	return store, codec, original.record, path
}

// Every replacement is actually visible before its own acknowledgement. The
// codec receives detached bytes; mutating them cannot change the file or base.
func TestHostCodecCommittedAcknowledgementOrder(t *testing.T) {
	s, c, r, path := hostCommitProbe(t)
	ctx := context.Background()
	var events []string
	s.commit = func(path string, raw []byte) error {
		events = append(events, "commit")
		return writePrivateFile(path, raw)
	}
	c.ack = func(input LegacyRecordInput) error {
		events = append(events, "ack")
		actual, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(actual, input.Bytes) || input.SessionID != r.SessionID || input.SHA256 != legacyRecordImage(input.SessionID, actual).SHA256 {
			t.Errorf("acknowledgement preceded actual committed bytes: err=%v", err)
			return ErrLegacyViewStale
		}
		input.Bytes[0] = '!'
		return nil
	}
	if err := s.create(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.UpdatedAt = r.UpdatedAt.Add(time.Second)
	r.LivenessDetail = "second transaction"
	if err := s.update(ctx, r); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"commit", "ack", "commit", "ack"}) || c.acks != 2 {
		t.Fatalf("each actual commit lost its own acknowledgement: %v", events)
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, s.bases[r.SessionID].Bytes) || actual[0] == '!' {
		t.Fatalf("codec acknowledgement mutated committed base: %v", err)
	}
}

// A visible commit with a failed acknowledgement is uncertain, never silently
// promoted. Repeated writes stay fenced; exact committed-image reload permits
// a new transaction with a fresh acknowledgement, not the failed receipt.
func TestHostCodecAcknowledgementFailureFencesRecord(t *testing.T) {
	for _, initial := range []bool{true, false} {
		t.Run(map[bool]string{true: "initial", false: "replacement"}[initial], func(t *testing.T) {
			s, c, r, path := hostCommitProbe(t)
			ctx := context.Background()
			commits := 0
			s.commit = func(path string, raw []byte) error { commits++; return writePrivateFile(path, raw) }
			if !initial {
				if err := s.create(ctx, r); err != nil {
					t.Fatal(err)
				}
				r.UpdatedAt = r.UpdatedAt.Add(time.Second)
				r.LivenessDetail = "failed acknowledgement"
			}
			fault := errors.New("owned acknowledgement failure")
			c.ack = func(LegacyRecordInput) error { return fault }
			var err error
			if initial {
				err = s.create(ctx, r)
			} else {
				err = s.update(ctx, r)
			}
			if !errors.Is(err, ErrLegacyCommitUncertain) || !errors.Is(err, fault) || !s.uncertain[r.SessionID] {
				t.Fatalf("failed acknowledgement lost uncertain fence: %v", err)
			}
			actual, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			visible, err := c.DecodeLegacy(ctx, legacyRecordImage(r.SessionID, actual))
			if err != nil || !reflect.DeepEqual(visible, r) {
				t.Fatalf("failed ack hid actual visible commit: %v", err)
			}
			beforeCommits, beforeAcks := commits, c.acks
			if err = s.update(ctx, r); !errors.Is(err, ErrLegacyCommitUncertain) || commits != beforeCommits || c.acks != beforeAcks {
				t.Fatalf("failed ack allowed another transaction: %v", err)
			}
			c.ack = nil
			if initial {
				if _, err = s.recoverInitial(ctx, r); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err = s.load(ctx, r.SessionID); err != nil {
					t.Fatal(err)
				}
			}
			r.UpdatedAt = r.UpdatedAt.Add(time.Second)
			r.LivenessDetail = "fresh transaction after recovery"
			if err = s.update(ctx, r); err != nil || c.acks != beforeAcks+1 || commits != beforeCommits+1 || s.uncertain[r.SessionID] {
				t.Fatalf("recovery did not admit fresh acknowledgement: %v", err)
			}
		})
	}
}

// A writer returning nil is insufficient: replacing the candidate with foreign
// bytes must refuse before codec acknowledgement and retain the uncertainty.
func TestHostCodecCommitObservationFailureNeverAcknowledges(t *testing.T) {
	s, c, r, _ := hostCommitProbe(t)
	s.commit = func(path string, _ []byte) error { return writePrivateFile(path, []byte("foreign visible bytes")) }
	err := s.create(context.Background(), r)
	if !errors.Is(err, ErrLegacyCommitUncertain) || !errors.Is(err, ErrLegacyViewStale) || c.acks != 0 || !s.uncertain[r.SessionID] || len(s.pendingInitial) != 1 {
		t.Fatalf("unobserved candidate gained acknowledgement: %v acks=%d", err, c.acks)
	}
}

type hostCommitProbeConfig struct{}

func (hostCommitProbeConfig) Snapshot(context.Context) (HostSettings, error) {
	return HostSettings{}, nil
}

type hostCommitProbeNotices struct{}

func (hostCommitProbeNotices) Enqueue(context.Context, string, Notice) error     { return nil }
func (hostCommitProbeNotices) Pending(context.Context, string) ([]Notice, error) { return nil, nil }
func (hostCommitProbeNotices) Acknowledge(context.Context, string, string) error { return nil }

type hostCommitProbeRunner struct {
	*inputTestBackend
	snapshot       RunnerSnapshot
	states, forces int
}

func (r *hostCommitProbeRunner) State(context.Context) (ProviderHostState, error) {
	r.states++
	return ProviderHostRunning, nil
}
func (r *hostCommitProbeRunner) Snapshot() (RunnerSnapshot, error) {
	return copyRunnerSnapshot(r.snapshot), nil
}
func (r *hostCommitProbeRunner) Stop(context.Context) error { r.forces++; return nil }

type hostCommitProbeFactory struct{ runner *hostCommitProbeRunner }

func (f hostCommitProbeFactory) Provider() string { return "muse" }
func (f hostCommitProbeFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	return f.runner, nil
}
func (f hostCommitProbeFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return f.runner, nil
}
func hostCommitProbeHost(t *testing.T) (*SessionHost, *hostCommitProbeCodec, *hostCommitProbeRunner, string) {
	t.Helper()
	_, c, r, _ := hostCommitProbe(t)
	dir := t.TempDir()
	runner := &hostCommitProbeRunner{inputTestBackend: &inputTestBackend{}, snapshot: r.ProviderHost.Snapshot}
	h, err := NewHost(HostOptions{Layout: Layout{Namespace: "probe", StateDir: dir, SessionsDir: filepath.Join(dir, "sessions"), LockFile: filepath.Join(dir, "manager.lock")}, Codec: c, Config: hostCommitProbeConfig{}, Notices: hostCommitProbeNotices{}, Factories: []ProviderHostFactory{hostCommitProbeFactory{runner: runner}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	a, err := h.Attach(context.Background(), AttachRequest{Owner: r.Owner, Provider: r.Provider})
	if err != nil {
		t.Fatal(err)
	}
	return h, c, runner, a.Session.SessionID
}

// Actual NewHost/Attach/Session failure revokes a previously issued writer and
// closes all new admission before returning uncertainty. Recovery of the exact
// visible record issues a higher durable epoch, never resurrects the old one.
// The transport is an input fixture; this does not qualify native force yet.
func TestHostCodecAcknowledgementFailureRevokesInput(t *testing.T) {
	h, c, runner, id := hostCommitProbeHost(t)
	ctx := context.Background()
	lease, writer, err := h.Input(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := writer.Submit(ctx, textInput(lease, "before", "valid"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Await(ctx, receipt); err != nil || runner.writeCount() != 1 {
		t.Fatalf("positive input failed: %v", err)
	}
	fault := errors.New("private acknowledgement unavailable")
	c.ack = func(LegacyRecordInput) error { return fault }
	if _, err = h.Session(ctx, id); !errors.Is(err, ErrLegacyCommitUncertain) || !errors.Is(err, fault) {
		t.Fatalf("ack fault not returned: %v", err)
	}
	if _, err = writer.Submit(ctx, textInput(lease, "after-fault", "forbidden")); !errors.Is(err, ErrStaleInputController) || runner.writeCount() != 1 {
		t.Fatalf("ack fault retained live writer: %v", err)
	}
	if _, _, err = h.Input(ctx, id); !errors.Is(err, ErrLegacyCommitUncertain) {
		t.Fatalf("new input admitted under uncertainty: %v", err)
	}
	states := runner.states
	if _, err = h.Session(ctx, id); !errors.Is(err, ErrLegacyCommitUncertain) || runner.states != states {
		t.Fatalf("uncertainty observed provider again: %v", err)
	}
	if _, err = h.Attach(ctx, AttachRequest{Owner: h.sessions[id].Owner, Provider: "muse"}); !errors.Is(err, ErrLegacyCommitUncertain) {
		t.Fatalf("uncertainty admitted attachment: %v", err)
	}
	c.ack = nil
	if err = h.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	next, nextWriter, err := h.Input(ctx, id)
	if err != nil || next.Epoch <= lease.Epoch {
		t.Fatalf("recovery did not allocate fresh epoch: %v", err)
	}
	if _, err = writer.Submit(ctx, textInput(lease, "old-after-recover", "forbidden")); !errors.Is(err, ErrStaleInputController) {
		t.Fatalf("recovery resurrected old writer: %v", err)
	}
	receipt, err = nextWriter.Submit(ctx, textInput(next, "new-after-recover", "valid"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nextWriter.Await(ctx, receipt); err != nil || runner.writeCount() != 2 {
		t.Fatalf("new epoch did not write: %v", err)
	}
}
