package sessioncontext

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Realized economics is recorded here for the same reason fork telemetry is:
// the evidence is machine-local. Provider usage, transcript sizes, and payload
// transfers are facts about this box's runs, and the board holds none of them.
//
// What this file is careful about is the difference between "zero" and "we did
// not look". Every measurement is written with an explicit measured flag, and
// nothing invents a number to fill a gap — because the measurement that
// justified context sharing found prompt-cache reuse swinging from 19% to 98%
// between sibling forks of one base, so a layer that estimated any of this would
// be reporting a saving nobody paid.

const contextEconomicsFileName = "run-economics.jsonl"

// contextEconomicsRetention bounds the log. It matches the fork-telemetry
// window so the two views describe the same period rather than two.
const contextEconomicsRetention = contextForkTelemetryRetention

type contextEconomicsStore struct {
	path string
}

func newContextEconomicsStore(stateDir string) *contextEconomicsStore {
	return &contextEconomicsStore{path: filepath.Join(stateDir, contextEconomicsFileName)}
}

func (s *contextEconomicsStore) append(row ContextRunEconomics, now time.Time) error {
	if s == nil {
		return nil
	}
	rows, err := s.load()
	if err != nil {
		return err
	}
	rows = append(rows, row)
	cutoff := now.UTC().Add(-contextEconomicsRetention)
	kept := make([]ContextRunEconomics, 0, len(rows))
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
			return fmt.Errorf("encoding run economics: %w", err)
		}
		encoded.Write(b)
		encoded.WriteByte('\n')
	}
	return writePrivateFile(s.path, encoded.Bytes())
}

func (s *contextEconomicsStore) load() ([]ContextRunEconomics, error) {
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
		return nil, fmt.Errorf("reading run economics log: %w", err)
	}
	rows := make([]ContextRunEconomics, 0, 64)
	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row ContextRunEconomics
		if json.Unmarshal(line, &row) != nil {
			// A torn line is skipped rather than failing the read. The aggregate
			// counts what it could parse; a log that refused to be read at all would
			// make one bad append hide every good row behind it.
			continue
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning run economics log: %w", err)
	}
	return rows, nil
}

// ContextRunUsage is what a completing run reports about its own spend.
//
// Every field is optional and every one is paired with the fact that it was
// observed. A provider that reports nothing produces a row that says so, which
// is the input the aggregate needs to be able to say "40 runs, 12 of them we
// could not see" instead of quietly averaging over 28 and calling it 40.
type ContextRunUsage struct {
	Usage *sm.ProviderUsage `json:"usage,omitempty"`
	// WallTime is the measured turn duration. Zero means unmeasured.
	WallTime time.Duration `json:"wall_time,omitempty"`
	// ProviderCostUSD is the provider's own reported cost, with the basis it was
	// priced on. Both are required together: a dollar figure whose basis is
	// unknown cannot be reconciled against provider telemetry or a dated list
	// price, and the measurement this feature rests on found those two differing
	// by a third.
	ProviderCostUSD float64 `json:"provider_cost_usd,omitempty"`
	CostBasis       string  `json:"cost_basis,omitempty"`
	// PayloadUploadedBytes and PayloadDownloadedBytes attribute cross-machine
	// transport to this context per BR-11.
	PayloadUploadedBytes   int64 `json:"payload_uploaded_bytes,omitempty"`
	PayloadDownloadedBytes int64 `json:"payload_downloaded_bytes,omitempty"`
	PayloadStoredBytes     int64 `json:"payload_stored_bytes,omitempty"`
}

// recordRunEconomicsLocked folds one completed run into a realized row.
//
// Like fork telemetry it runs after the completion is durable and can never undo
// it: this is measurement, and a run that failed because its cost could not be
// measured would make an observation able to break the thing it observes.
func (m *Service) recordRunEconomicsLocked(
	record *materializationRecord,
	runID string,
	usage *ContextRunUsage,
	registry *ContextRecord,
) {
	if m == nil || m.economics == nil || record == nil {
		return
	}
	now := m.now().UTC()
	row := ContextRunEconomics{
		CtxID:            NormalizeContextID(record.CtxID),
		RunID:            strings.TrimSpace(runID),
		Provider:         record.Provider,
		Source:           contextEconomicsSource(record.Source),
		BindingElementID: record.BindingElementID,
		ObservedAt:       now,
	}
	if bytes, measured := m.measureContextSourceBytes(record); measured {
		row.SourceBytes = bytes
		row.SourceBytesMeasured = true
	}
	attributeContextPayloadBytes(&row, record, registry)
	if usage != nil {
		if usage.Usage != nil {
			row.Tokens = ContextTokenUsage{
				InputTokens:         usage.Usage.InputTokens,
				CacheReadTokens:     usage.Usage.CachedInputTokens,
				CacheCreationTokens: usage.Usage.CacheCreationTokens,
				OutputTokens:        usage.Usage.OutputTokens,
			}
			row.TokensMeasured = true
		}
		if usage.WallTime > 0 {
			row.WallTime = usage.WallTime
			row.WallTimeMeasured = true
		}
		if usage.ProviderCostUSD > 0 && strings.TrimSpace(usage.CostBasis) != "" {
			// A cost with no basis is dropped rather than recorded. It would look
			// like measured spend and could not be reconciled against anything, which
			// is the failure mode the economics measurement found when a wrapper's
			// cost telemetry turned out to price a model nobody was measuring.
			row.ProviderCostUSD = usage.ProviderCostUSD
			row.CostBasis = strings.TrimSpace(usage.CostBasis)
			row.CostMeasured = true
		}
		// Added rather than assigned: the manager already attributed what it could
		// observe of this context's cross-machine payload, and a caller that also
		// measured a transfer is reporting a second, real movement of bytes.
		row.PayloadUploadedBytes += usage.PayloadUploadedBytes
		row.PayloadDownloadedBytes += usage.PayloadDownloadedBytes
		if usage.PayloadStoredBytes > row.PayloadStoredBytes {
			row.PayloadStoredBytes = usage.PayloadStoredBytes
		}
	}
	row = NormalizeContextRunEconomics(row)
	if err := ValidateContextRunEconomics(row); err != nil {
		m.logMessagef("refusing an inconsistent economics row for %s: %v", record.CtxID, err)
		return
	}
	if err := m.economics.append(row, now); err != nil {
		m.logMessagef("recording run economics for %s: %v", record.CtxID, err)
	}
}

// attributeContextPayloadBytes charges this context's cross-machine payload to
// it, per BR-11.
//
// The registry record is the measurement, not an estimate: the board wrote
// SizeBytes when the blob was accepted, against a verified digest, so it is what
// the payload store actually holds for this context. Stored bytes are a level
// and are recorded on every run that used the context; a download is a flow and
// is charged only to the run that actually fetched it, because a fork of a
// locally present context moved nothing across a network.
//
// A context with no payload attributes nothing, which is the ordinary case while
// `contexts.payloads.mode` is off. Nothing here invents a byte count from a
// transcript size: local bytes are already reported as source bytes, and
// counting them again as payload transport would report cross-machine cost for a
// feature that never ran.
func attributeContextPayloadBytes(
	row *ContextRunEconomics,
	record *materializationRecord,
	registry *ContextRecord,
) {
	if row == nil || registry == nil || record == nil {
		return
	}
	if registry.Payload.State != ContextPayloadStateAvailable ||
		registry.Payload.SizeBytes <= 0 {
		return
	}
	row.PayloadStoredBytes = registry.Payload.SizeBytes
	if record.Source == MaterializationSourceFetched {
		row.PayloadDownloadedBytes = registry.Payload.SizeBytes
	}
}

func contextEconomicsSource(source MaterializationSource) ContextMaterializationSource {
	switch source {
	case MaterializationSourceBuilt:
		return ContextSourceBuilt
	case MaterializationSourceFetched:
		return ContextSourceFetched
	case MaterializationSourceForked:
		return ContextSourceForked
	default:
		return ContextSourceCold
	}
}

// measureContextSourceBytes sizes the provider payload this run's context
// occupies on disk.
//
// It measures rather than estimates, and reports "not measured" rather than zero
// when it cannot reach the files. Zero is a legitimate size; an unreachable
// provider home is not, and folding the two together would make a machine whose
// snapshot could not be decoded look like a machine storing nothing.
func (m *Service) measureContextSourceBytes(record *materializationRecord) (int64, bool) {
	path, err := materializationTranscriptPath(record.Provider, record.ProviderHost)
	if err != nil {
		return 0, false
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return 0, false
	}
	return info.Size(), true
}

// ContextEconomics folds this machine's realized rows into the operator report.
//
// It is a read with no consequence. Nothing in selection, retention, or spawn
// policy reads it, which is what keeps realized cache behaviour telemetry rather
// than a budget: the measurement found it unpredictable enough that treating it
// as one would be planning against noise.
func (m *Service) ContextEconomics(
	rates ContextPayloadCostRates,
) (ContextEconomicsReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows, err := m.economics.load()
	if err != nil {
		return ContextEconomicsReport{}, err
	}
	return DeriveContextEconomics(rows, rates, m.now()), nil
}
