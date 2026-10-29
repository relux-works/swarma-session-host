package sessioncontext

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

func TestDetectTranscriptCompaction(t *testing.T) {
	tests := []struct {
		name       string
		provider   string
		transcript string
		want       *TranscriptCompaction
	}{
		{
			name:     "claude compact metadata",
			provider: string(ContextProviderClaude),
			transcript: strings.Join([]string{
				`{"type":"user","message":{"content":"before"}}`,
				`{"type":"system","subtype":"compact_boundary","timestamp":"2026-07-28T20:30:00Z","compactMetadata":{"trigger":"auto","preTokens":468493,"postTokens":14997}}`,
				`{"type":"assistant","message":{"content":"after"}}`,
			}, "\n"),
			want: &TranscriptCompaction{
				Provider:  ContextProviderClaude,
				Marker:    ClaudeCompactionMarker,
				Timestamp: "2026-07-28T20:30:00Z",
			},
		},
		{
			// The authoritative record on its own is enough: detection must not
			// depend on the UI event stream being present.
			name:     "codex authoritative compacted record without the ui event",
			provider: string(ContextProviderCodex),
			transcript: strings.Join([]string{
				`{"timestamp":"2026-07-28T20:29:59Z","type":"event_msg","payload":{"type":"token_count"}}`,
				`{"timestamp":"2026-07-28T20:30:00Z","type":"compacted","payload":{"message":"","replacement_history":[{"type":"message","role":"user","content":[]}]}}`,
				`{"timestamp":"2026-07-28T20:30:01Z","type":"event_msg","payload":{"type":"token_count"}}`,
			}, "\n"),
			want: &TranscriptCompaction{
				Provider:  ContextProviderCodex,
				Marker:    CodexCompactionMarker,
				Timestamp: "2026-07-28T20:30:00Z",
			},
		},
		{
			// A compaction that lost its own replacement history is still a
			// compaction; the base is not trustworthy because the evidence is thin.
			name:     "codex compacted record without replacement history still refuses",
			provider: string(ContextProviderCodex),
			transcript: strings.Join([]string{
				`{"timestamp":"2026-07-28T20:30:00Z","type":"compacted","payload":{"message":""}}`,
			}, "\n"),
			want: &TranscriptCompaction{
				Provider:  ContextProviderCodex,
				Marker:    CodexCompactionMarker,
				Timestamp: "2026-07-28T20:30:00Z",
			},
		},
		{
			// The UI event alone still quarantines — refusing is the fail-closed
			// verdict — but it is reported as the weaker marker it is.
			name:     "codex ui event alone is corroborating evidence",
			provider: string(ContextProviderCodex),
			transcript: strings.Join([]string{
				`{"timestamp":"2026-07-28T20:29:59Z","type":"event_msg","payload":{"type":"token_count"}}`,
				`{"timestamp":"2026-07-28T20:30:00Z","type":"event_msg","payload":{"type":"context_compacted"}}`,
				`{"timestamp":"2026-07-28T20:30:01Z","type":"event_msg","payload":{"type":"token_count"}}`,
			}, "\n"),
			want: &TranscriptCompaction{
				Provider:  ContextProviderCodex,
				Marker:    CodexCompactionUIMarker,
				Timestamp: "2026-07-28T20:30:00Z",
			},
		},
		{
			// When both are present in either order, the evidence names the record
			// that carries the replacement history.
			name:     "codex authoritative record outranks a preceding ui event",
			provider: string(ContextProviderCodex),
			transcript: strings.Join([]string{
				`{"timestamp":"2026-07-28T20:30:00Z","type":"event_msg","payload":{"type":"context_compacted"}}`,
				`{"timestamp":"2026-07-28T20:30:02Z","type":"compacted","payload":{"replacement_history":[]}}`,
			}, "\n"),
			want: &TranscriptCompaction{
				Provider:  ContextProviderCodex,
				Marker:    CodexCompactionMarker,
				Timestamp: "2026-07-28T20:30:02Z",
			},
		},
		{
			// A Claude record type must not leak into the Codex verdict, and the
			// Codex record type must not fire on a Claude transcript.
			name:     "codex record type is not a claude marker",
			provider: string(ContextProviderClaude),
			transcript: strings.Join([]string{
				`{"timestamp":"2026-07-28T20:30:00Z","type":"compacted","payload":{"replacement_history":[]}}`,
			}, "\n"),
		},
		{
			name:     "marker text in content is not structural evidence",
			provider: string(ContextProviderClaude),
			transcript: strings.Join([]string{
				`{"type":"user","message":{"content":"look for compactMetadata"}}`,
				`{"type":"assistant","message":{"content":"Codex calls it context_compacted"}}`,
			}, "\n"),
		},
		{
			name:     "other codex payload event is clean",
			provider: string(ContextProviderCodex),
			transcript: strings.Join([]string{
				`{"type":"session_meta","payload":{"id":"private"}}`,
				`{"type":"event_msg","payload":{"type":"token_count"}}`,
			}, "\n"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DetectTranscriptCompaction(ContextProvider(test.provider), strings.NewReader(test.transcript))
			if err != nil {
				t.Fatalf("DetectTranscriptCompaction() error = %v", err)
			}
			if test.want == nil {
				if got != nil {
					t.Fatalf("DetectTranscriptCompaction() = %#v, want nil", got)
				}
				return
			}
			if got == nil || *got != *test.want {
				t.Fatalf("DetectTranscriptCompaction() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestDetectTranscriptCompactionProviderFixtures(t *testing.T) {
	tests := []struct {
		name     string
		provider ContextProvider
		path     string
		marker   string
	}{
		{
			name:     "claude compacted transcript",
			provider: ContextProviderClaude,
			path:     "testdata/compaction/claude-compacted.jsonl",
			marker:   ClaudeCompactionMarker,
		},
		{
			name:     "codex compacted transcript",
			provider: ContextProviderCodex,
			path:     "testdata/compaction/codex-compacted.jsonl",
			marker:   CodexCompactionMarker,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transcript, err := os.Open(test.path)
			if err != nil {
				t.Fatalf("open fixture: %v", err)
			}
			defer transcript.Close()

			got, err := DetectTranscriptCompaction(test.provider, transcript)
			if err != nil {
				t.Fatalf("DetectTranscriptCompaction() error = %v", err)
			}
			if got == nil || got.Provider != test.provider || got.Marker != test.marker {
				t.Fatalf("DetectTranscriptCompaction() = %#v, want provider=%s marker=%s", got, test.provider, test.marker)
			}
		})
	}
}

func TestDetectTranscriptCompactionRejectsUnverifiableInput(t *testing.T) {
	tests := []struct {
		name       string
		provider   string
		transcript string
		wantError  string
	}{
		{
			name:       "unsupported provider",
			provider:   "other",
			transcript: `{}`,
			wantError:  `unsupported transcript provider "other"`,
		},
		{
			name:       "malformed transcript",
			provider:   string(ContextProviderClaude),
			transcript: "{\"type\":\"user\"}\nnot-json",
			wantError:  "decode claude transcript record 2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DetectTranscriptCompaction(ContextProvider(test.provider), strings.NewReader(test.transcript))
			if got != nil {
				t.Fatalf("DetectTranscriptCompaction() = %#v, want nil", got)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("DetectTranscriptCompaction() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

// TestDetectTranscriptCompactionOutranksAnUndecodableTail pins which verdict
// wins when a transcript is both compacted and broken. Reporting the decode
// failure would refuse the call but leave the base unquarantined, so the next
// clean-looking transcript could still serve it; the compaction is the durable
// verdict and has to survive the corrupt tail.
func TestDetectTranscriptCompactionOutranksAnUndecodableTail(t *testing.T) {
	transcript := `{"timestamp":"2026-07-28T20:30:00Z","type":"event_msg","payload":{"type":"context_compacted"}}` +
		"\n" + "not-json\n"

	got, err := DetectTranscriptCompaction(ContextProviderCodex, strings.NewReader(transcript))
	if err != nil {
		t.Fatalf("DetectTranscriptCompaction() error = %v, want the compaction verdict", err)
	}
	if got == nil || got.Marker != CodexCompactionUIMarker {
		t.Fatalf("DetectTranscriptCompaction() = %#v, want the %s marker", got, CodexCompactionUIMarker)
	}

	// Without a marker the same broken tail still fails closed on integrity.
	if _, err := DetectTranscriptCompaction(
		ContextProviderCodex,
		strings.NewReader(`{"type":"event_msg","payload":{"type":"token_count"}}`+"\n"+"not-json\n"),
	); err == nil {
		t.Fatal("DetectTranscriptCompaction(clean prefix, broken tail) = nil, want a decode failure")
	}
}

func TestDetectTranscriptCompactionRequiresReader(t *testing.T) {
	got, err := DetectTranscriptCompaction(ContextProviderClaude, nil)
	if got != nil {
		t.Fatalf("DetectTranscriptCompaction() = %#v, want nil", got)
	}
	if err == nil || err.Error() != "transcript reader is required" {
		t.Fatalf("DetectTranscriptCompaction() error = %v, want required-reader error", err)
	}
}

// TestInspectTranscriptReportsWholeStreamEvidence pins the evidence the remote
// publication flow sends in place of the transcript itself. The digest must
// identify the whole file even when scanning stops early at a marker, so the
// same transcript always produces the same digest.
func TestInspectTranscriptReportsWholeStreamEvidence(t *testing.T) {
	clean := `{"type":"user"}` + "\n" + `{"type":"assistant"}` + "\n"
	inspection, err := InspectTranscript(ContextProviderClaude, strings.NewReader(clean))
	if err != nil {
		t.Fatalf("InspectTranscript: %v", err)
	}
	if inspection.Compaction != nil {
		t.Fatalf("clean transcript compaction = %+v, want nil", inspection.Compaction)
	}
	if inspection.Records != 2 || inspection.Bytes != int64(len(clean)) {
		t.Fatalf("inspection = %d records / %d bytes, want 2 / %d",
			inspection.Records, inspection.Bytes, len(clean))
	}
	wantDigest := sha256.Sum256([]byte(clean))
	if inspection.SHA256 != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("digest = %q, want the whole-stream sha256", inspection.SHA256)
	}

	// A marker mid-stream must not truncate the digest: the trailing record is
	// still read, so the digest stays a stable identifier for the file.
	compacted := `{"type":"user"}` + "\n" + `{"compactMetadata":{}}` + "\n" + `{"type":"assistant"}` + "\n"
	marked, err := InspectTranscript(ContextProviderClaude, strings.NewReader(compacted))
	if err != nil {
		t.Fatalf("InspectTranscript(compacted): %v", err)
	}
	if marked.Compaction == nil || marked.Compaction.Marker != ClaudeCompactionMarker {
		t.Fatalf("compaction = %+v, want the claude marker", marked.Compaction)
	}
	if marked.Bytes != int64(len(compacted)) {
		t.Fatalf("compacted inspection = %d bytes, want the whole stream %d", marked.Bytes, len(compacted))
	}
	wantCompactedDigest := sha256.Sum256([]byte(compacted))
	if marked.SHA256 != hex.EncodeToString(wantCompactedDigest[:]) {
		t.Fatalf("compacted digest = %q, want the whole-stream sha256", marked.SHA256)
	}
}

func TestInspectTranscriptFailsClosedOnMalformedInput(t *testing.T) {
	if _, err := InspectTranscript(ContextProviderCodex, strings.NewReader("not-json")); err == nil {
		t.Fatal("InspectTranscript(malformed) = nil, want a decode failure")
	}
	if _, err := InspectTranscript("gemini", strings.NewReader("{}")); err == nil {
		t.Fatal("InspectTranscript(unsupported provider) = nil, want a refusal")
	}
}
