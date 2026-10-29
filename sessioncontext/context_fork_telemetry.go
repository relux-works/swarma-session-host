package sessioncontext

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Per-fork telemetry is machine-local, and that is a consequence of what it is
// rather than a storage convenience.
//
// The evidence it folds is a provider transcript or an app-server stream: files
// on this machine, produced by this machine's runs, that the board never sees
// and a board-server could not be handed without shipping conversation content
// off the box. So the rows live beside the materialization catalog, at the same
// layer as the fork sources they describe, and the board holds none of it.
//
// Nothing here can fail a run. CV-6 is explicit that completeness is feedback,
// never a gate, and the code is arranged so that is structurally true: every
// entry point returns advisory data or nothing, and the capture path swallows
// its own failures into a recorded unavailable row.

const contextForkTelemetryFileName = "fork-telemetry.jsonl"

// contextForkTelemetryRetention bounds the log.
//
// It is one day longer than the CV-6 measurement window so a row is never
// dropped while it is still inside the window a reader is about to measure.
const contextForkTelemetryRetention = ContextCompletenessWindow + 24*time.Hour

type contextForkTelemetryStore struct {
	path string
}

func newContextForkTelemetryStore(stateDir string) *contextForkTelemetryStore {
	return &contextForkTelemetryStore{path: filepath.Join(stateDir, contextForkTelemetryFileName)}
}

// append adds one row and prunes anything past retention.
//
// Rewriting the whole file on every append is affordable because the file is
// bounded by an eight-day window of forks, and it keeps the pruning honest: a
// log that only ever grew would eventually make the seven-day rate a lifetime
// average without anything saying so.
func (s *contextForkTelemetryStore) append(
	row ContextForkTelemetry,
	now time.Time,
) error {
	if s == nil {
		return nil
	}
	rows, err := s.load()
	if err != nil {
		return err
	}
	rows = append(rows, row)
	cutoff := now.UTC().Add(-contextForkTelemetryRetention)
	kept := make([]ContextForkTelemetry, 0, len(rows))
	for _, existing := range rows {
		if existing.ObservedAt.UTC().Before(cutoff) {
			continue
		}
		kept = append(kept, existing)
	}
	var encoded bytes.Buffer
	for _, row := range kept {
		b, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("encoding fork telemetry: %w", err)
		}
		encoded.Write(b)
		encoded.WriteByte('\n')
	}
	return writePrivateFile(s.path, encoded.Bytes())
}

func (s *contextForkTelemetryStore) load() ([]ContextForkTelemetry, error) {
	if s == nil {
		return nil, nil
	}
	if err := rejectSymlinkAncestors(s.path); err != nil {
		return nil, err
	}
	content, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading fork telemetry log: %w", err)
	}
	rows := make([]ContextForkTelemetry, 0, 64)
	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var row ContextForkTelemetry
		if err := json.Unmarshal(line, &row); err != nil {
			// One unreadable row does not discard the log. The aggregate is
			// advisory, and losing a week of measurement because a single line was
			// truncated by a crash would be a worse outcome than measuring the rest.
			continue
		}
		rows = append(rows, row)
	}
	return rows, scanner.Err()
}

// ContextForkTelemetry returns the retained rows for one base, newest first.
func (m *Service) ContextForkTelemetry(ctxID string) ([]ContextForkTelemetry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows, err := m.forkTelemetry.load()
	if err != nil {
		return nil, err
	}
	normalized := NormalizeContextID(ctxID)
	filtered := make([]ContextForkTelemetry, 0, len(rows))
	for _, row := range rows {
		if normalized != "" && NormalizeContextID(row.CtxID) != normalized {
			continue
		}
		filtered = append(filtered, row)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].ObservedAt.After(filtered[j].ObservedAt)
	})
	return filtered, nil
}

// ContextCompleteness is the CV-6 aggregate for one base.
//
// It is a read. There is no consequence, no transition, and no error path that
// can make a base unforkable, which is what keeps "completeness is feedback"
// a property of the code rather than a promise in a comment.
func (m *Service) ContextCompleteness(ctxID string) (*ContextCompletenessFeedback, error) {
	rows, err := m.ContextForkTelemetry(ctxID)
	if err != nil {
		return nil, err
	}
	return AggregateContextCompleteness(ctxID, rows, m.now()), nil
}

// RecordForkTelemetry accepts measured provider observations only for a known
// local context/run. Missing instrumentation is explicitly unavailable.
func (m *Service) RecordForkTelemetry(row ContextForkTelemetry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.materializations[normalizeContextID(row.CtxID)] == nil || !materializationRunIDPattern.MatchString(row.RunID) || row.ObservedAt.IsZero() || row.ContractVersion != ContextForkTelemetryContract {
		return fmt.Errorf("matching context/run telemetry required")
	}
	return m.forkTelemetry.append(row, m.now())
}
func (m *Service) recordConsumerForkTelemetryLocked(_ context.Context, r *materializationRecord, run string) {
	if r.Source != MaterializationSourceForked {
		return
	}
	row := ContextForkTelemetry{ContractVersion: ContextForkTelemetryContract, CtxID: r.CtxID, RunID: run, Role: r.TargetRole, Provider: r.Provider, Status: ContextConsumerObservationUnavailable, Observable: false, ObservedAt: m.now()}
	if err := m.forkTelemetry.append(row, m.now()); err != nil {
		m.logMessagef("recording unavailable fork observation: %v", err)
	}
}
