package sessioncontext

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	ClaudeCompactionMarker = "compactMetadata"
	// CodexCompactionMarker is the authoritative Codex rollout record: a
	// top-level type=compacted entry whose payload carries the
	// replacement_history that replaced the dropped conversation. It is the
	// record that proves the truncation happened, so it is what the detector
	// keys off.
	CodexCompactionMarker = "compacted"
	// CodexCompactionUIMarker is the event_msg notice Codex emits alongside the
	// authoritative record for the UI. It corroborates a compaction but carries
	// no replacement history, and it is the stream more plausibly trimmed in a
	// future headless mode, so it is never preferred over the record above. It
	// still quarantines on its own: refusing is the fail-closed verdict.
	CodexCompactionUIMarker = "context_compacted"
)

// TranscriptCompaction identifies a provider compaction event without
// retaining any transcript content or private provider handle.
type TranscriptCompaction struct {
	Provider  ContextProvider `json:"provider"`
	Marker    string          `json:"marker"`
	Timestamp string          `json:"timestamp,omitempty"`
}

type transcriptCompactionEnvelope struct {
	Type            string          `json:"type"`
	CompactMetadata json.RawMessage `json:"compactMetadata"`
	Payload         struct {
		Type               string          `json:"type"`
		ReplacementHistory json.RawMessage `json:"replacement_history"`
	} `json:"payload"`
	Timestamp string `json:"timestamp"`
}

// codexCompaction classifies one decoded Codex rollout record.
//
// The authoritative record is the top-level type=compacted entry: its payload
// carries the replacement_history that stands in for everything the thread
// dropped. A record of that type without a replacement_history is still a
// compaction — a compaction that lost its own evidence is not a reason to
// trust the base — so the shape is not required, only preferred.
func codexCompaction(envelope transcriptCompactionEnvelope) (marker string, authoritative bool) {
	if envelope.Type == CodexCompactionMarker {
		return CodexCompactionMarker, true
	}
	if envelope.Payload.Type == CodexCompactionUIMarker {
		return CodexCompactionUIMarker, false
	}
	return "", false
}

// TranscriptInspection is the full result of one structural transcript scan.
//
// It carries only derived evidence: a content digest, the byte and record
// counts that were actually scanned, and the compaction marker when one is
// present. It never retains transcript content, a filesystem path, or a
// provider handle, so it is safe to persist on the board and to transport to a
// board-server that must never read the transcript itself.
type TranscriptInspection struct {
	Provider   ContextProvider       `json:"provider"`
	SHA256     string                `json:"sha256"`
	Bytes      int64                 `json:"bytes"`
	Records    int                   `json:"records"`
	Compaction *TranscriptCompaction `json:"compaction,omitempty"`
}

// DetectTranscriptCompaction scans a provider JSONL transcript and returns the
// first structural compaction marker. Marker-like text inside message content
// is ignored. A malformed transcript fails closed because its integrity cannot
// be established.
func DetectTranscriptCompaction(provider ContextProvider, transcript io.Reader) (*TranscriptCompaction, error) {
	inspection, err := InspectTranscript(provider, transcript)
	if err != nil {
		return nil, err
	}
	return inspection.Compaction, nil
}

// InspectTranscript scans a provider JSONL transcript once and reports both the
// first structural compaction marker and the derived integrity evidence.
//
// The whole stream is always consumed so the digest identifies the complete
// transcript even when scanning stops early at a marker. A malformed transcript
// fails closed because its integrity cannot be established.
func InspectTranscript(provider ContextProvider, transcript io.Reader) (TranscriptInspection, error) {
	scan, err := scanTranscript(provider, transcript)
	if err != nil {
		return TranscriptInspection{}, err
	}
	return scan.Inspection, nil
}

// transcriptScan is one pass over a transcript. Integrity and classification are
// two verdicts over the same bytes, so they are derived from one decode: a second
// pass would need the stream rewound, and a second read could see different bytes
// than the digest was computed over.
type transcriptScan struct {
	Inspection TranscriptInspection
}

func scanTranscript(provider ContextProvider, transcript io.Reader) (transcriptScan, error) {
	provider = ContextProvider(strings.ToLower(strings.TrimSpace(string(provider))))
	switch provider {
	case ContextProviderClaude, ContextProviderCodex:
	default:
		return transcriptScan{}, fmt.Errorf("unsupported transcript provider %q", provider)
	}
	if transcript == nil {
		return transcriptScan{}, errors.New("transcript reader is required")
	}

	digest := sha256.New()
	counter := &countingWriter{}
	scanned := io.TeeReader(transcript, io.MultiWriter(digest, counter))

	inspection := TranscriptInspection{Provider: provider}
	decoder := json.NewDecoder(scanned)
	for {
		var raw json.RawMessage
		var envelope transcriptCompactionEnvelope
		err := decoder.Decode(&raw)
		if err == nil {
			err = json.Unmarshal(raw, &envelope)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// A marker already seen outranks an undecodable tail. Reporting the
			// integrity failure instead would refuse this call but leave the base
			// unquarantined, so a later clean-looking transcript could still serve
			// it; the compaction is the durable verdict.
			if inspection.Compaction != nil {
				if drainErr := drainTranscript(provider, scanned); drainErr != nil {
					return transcriptScan{}, drainErr
				}
				break
			}
			return transcriptScan{}, fmt.Errorf(
				"decode %s transcript record %d: %w", provider, inspection.Records+1, err)
		}
		inspection.Records++

		var marker string
		authoritative := true
		switch provider {
		case ContextProviderClaude:
			if len(envelope.CompactMetadata) > 0 {
				marker = ClaudeCompactionMarker
			}
		case ContextProviderCodex:
			marker, authoritative = codexCompaction(envelope)
		}
		if marker == "" {
			continue
		}
		if inspection.Compaction == nil || authoritative {
			inspection.Compaction = &TranscriptCompaction{
				Provider:  provider,
				Marker:    marker,
				Timestamp: envelope.Timestamp,
			}
		}
		if !authoritative {
			// A corroborating marker is enough to refuse, but the authoritative
			// record is what the evidence should name, so keep scanning for it.
			continue
		}
		// Drain the remainder so the digest always identifies the whole
		// transcript, not just the prefix scanned before the marker.
		if err := drainTranscript(provider, scanned); err != nil {
			return transcriptScan{}, err
		}
		break
	}

	inspection.SHA256 = hex.EncodeToString(digest.Sum(nil))
	inspection.Bytes = counter.total

	scan := transcriptScan{Inspection: inspection}
	return scan, nil
}

// drainTranscript consumes the rest of the stream so the digest and byte count
// always identify the whole transcript, not just the prefix that was decoded.
func drainTranscript(provider ContextProvider, scanned io.Reader) error {
	if _, err := io.Copy(io.Discard, scanned); err != nil {
		return fmt.Errorf("reading %s transcript after marker: %w", provider, err)
	}
	return nil
}

type countingWriter struct {
	total int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.total += int64(len(p))
	return len(p), nil
}
