package sessionmanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const builderTraceTestCtxID = "CTX-260731-gw0001"

// gatewayRepo builds a repository whose shape exercises the boundary: an
// in-scope directory, an out-of-scope sibling holding something a builder should
// never obtain, and a nested tree for enumeration.
func gatewayRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	// The root is resolved because macOS temp dirs are symlinked through
	// /private, and a gateway anchored to the unresolved form would classify every
	// resolved path as an escape.
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	write := func(rel, content string) {
		abs := filepath.Join(resolved, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("docs/guide.md", "alpha beta alpha\n")
	write("docs/nested/deep.md", "beta only\n")
	write("pkg/service.go", "package pkg // alpha\n")
	write("secrets/key.pem", "PRIVATE KEY MATERIAL\n")
	return resolved
}

func newTestGateway(t *testing.T, root string, allowlist []string) *BuilderEvidenceGateway {
	t.Helper()
	clock := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	gateway, err := NewBuilderEvidenceGateway(BuilderEvidenceGatewayConfig{
		CtxID:          builderTraceTestCtxID,
		RunID:          "RUN-260731-abc123",
		Provider:       "claude",
		RepositoryRoot: root,
		Allowlist:      NewContextBuilderAllowlist(allowlist),
		TracePath:      filepath.Join(t.TempDir(), "trace.jsonl"),
		Now:            func() time.Time { return clock },
	})
	if err != nil {
		t.Fatalf("opening the evidence gateway: %v", err)
	}
	return gateway
}

func serve(
	t *testing.T,
	gateway *BuilderEvidenceGateway,
	request BuilderGatewayRequest,
) BuilderGatewayResponse {
	t.Helper()
	if strings.TrimSpace(request.ProviderEventID) == "" {
		request.ProviderEventID = "evt-1"
	}
	response, err := gateway.Serve(request)
	if err != nil {
		t.Fatalf("gateway refused to serve %+v: %v", request, err)
	}
	return response
}

// TestGatewayServesEachOperationAndRecordsExactlyOneEventPerCall is the
// happy-path contract: four operations, four events, and every event describing
// what was actually returned.
func TestGatewayServesEachOperationAndRecordsExactlyOneEventPerCall(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs", "pkg"})

	statResponse := serve(t, gateway, BuilderGatewayRequest{
		Operation: "stat", Path: "docs/guide.md", ProviderEventID: "evt-stat",
	})
	if !statResponse.Exists || statResponse.Kind != "file" || statResponse.Size != 17 {
		t.Fatalf("stat = %+v", statResponse)
	}

	readResponse := serve(t, gateway, BuilderGatewayRequest{
		Operation: "read", Path: "docs/guide.md", ProviderEventID: "evt-read",
	})
	if readResponse.Content != "alpha beta alpha\n" ||
		readResponse.Length != 17 || readResponse.TotalBytes != 17 {
		t.Fatalf("read = %+v", readResponse)
	}

	listResponse := serve(t, gateway, BuilderGatewayRequest{
		Operation: "list", PathSpec: "docs", ProviderEventID: "evt-list",
	})
	if strings.Join(listResponse.Entries, ",") != "docs/guide.md,docs/nested" {
		t.Fatalf("list entries = %v", listResponse.Entries)
	}

	searchResponse := serve(t, gateway, BuilderGatewayRequest{
		Operation: "search", PathSpec: "docs", Query: "alpha", ProviderEventID: "evt-search",
	})
	if strings.Join(searchResponse.Entries, ",") != "docs/guide.md" {
		t.Fatalf("search entries = %v", searchResponse.Entries)
	}
	if len(searchResponse.MatchCounts) != 1 || searchResponse.MatchCounts[0] != 2 {
		t.Fatalf("search match counts = %v", searchResponse.MatchCounts)
	}

	events, attestation := closeAndLoad(t, gateway)
	if len(events) != 4 {
		t.Fatalf("gateway recorded %d event(s) for 4 calls", len(events))
	}
	if attestation.EventCount != 4 {
		t.Fatalf("attestation binds %d event(s)", attestation.EventCount)
	}
	for index, want := range []ContextRepositoryAccessOperation{
		ContextRepositoryAccessStat,
		ContextRepositoryAccessRead,
		ContextRepositoryAccessList,
		ContextRepositoryAccessSearch,
	} {
		if events[index].Operation != want {
			t.Fatalf("event %d is %q, want %q", index+1, events[index].Operation, want)
		}
		if events[index].Outcome != ContextRepositoryAccessGranted {
			t.Fatalf("event %d outcome = %q", index+1, events[index].Outcome)
		}
		if events[index].ContentDigest == "" {
			t.Fatalf("granted event %d recorded no anchored digest", index+1)
		}
	}
	// The read event has to describe the exact window, because the coverage
	// consumer credits the base with those bytes and nothing else.
	read := events[1]
	if read.ByteOffset != 0 || read.ByteLength != 17 || read.TotalBytes != 17 || read.Truncated {
		t.Fatalf("read event range = %+v", read)
	}
	if read.ContentDigest != ContextRepositoryAccessContentDigest(
		gateway.Anchor(), []byte("alpha beta alpha\n"),
	) {
		t.Fatal("the read event digest does not describe the bytes that were returned")
	}
}

// TestGatewayRecordsPartialReadsAsPartial covers the case the coverage manifest
// most depends on: a builder that read a window of a file must not be credited
// with the whole file.
func TestGatewayRecordsPartialReadsAsPartial(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs"})

	response := serve(t, gateway, BuilderGatewayRequest{
		Operation: "read", Path: "docs/guide.md", Offset: 6, Length: 4,
	})
	if response.Content != "beta" || response.Offset != 6 || response.Length != 4 {
		t.Fatalf("partial read = %+v", response)
	}
	if response.TotalBytes != 17 {
		t.Fatalf("partial read reported total %d, want the whole file size", response.TotalBytes)
	}

	events, _ := closeAndLoad(t, gateway)
	if len(events) != 1 {
		t.Fatalf("partial read recorded %d event(s)", len(events))
	}
	event := events[0]
	if event.ByteOffset != 6 || event.ByteLength != 4 || event.TotalBytes != 17 {
		t.Fatalf("partial read event = offset %d length %d total %d",
			event.ByteOffset, event.ByteLength, event.TotalBytes)
	}
	if event.ContentDigest != ContextRepositoryAccessContentDigest(
		gateway.Anchor(), []byte("beta"),
	) {
		t.Fatal("the partial read digest does not describe the window that was returned")
	}
}

// TestGatewayRecordsMissingFileAsFailedNotDenied keeps the two outcomes apart.
// A repository full of typos must not look like a repository full of policy
// violations.
func TestGatewayRecordsMissingFileAsFailedNotDenied(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs"})

	response := serve(t, gateway, BuilderGatewayRequest{
		Operation: "read", Path: "docs/absent.md",
	})
	if response.Outcome != string(ContextRepositoryAccessFailed) ||
		response.Reason != "not_found" {
		t.Fatalf("missing file read = %+v", response)
	}
	// A stat of the same path is a granted observation with a negative answer:
	// the builder is entitled to learn that an in-scope file does not exist.
	statResponse := serve(t, gateway, BuilderGatewayRequest{
		Operation: "stat", Path: "docs/absent.md",
	})
	if statResponse.Outcome != string(ContextRepositoryAccessGranted) ||
		statResponse.Exists {
		t.Fatalf("missing file stat = %+v", statResponse)
	}

	events, _ := closeAndLoad(t, gateway)
	if len(events) != 2 {
		t.Fatalf("recorded %d event(s), want 2", len(events))
	}
	if events[0].Outcome != ContextRepositoryAccessFailed ||
		events[0].DenialReason != "" {
		t.Fatalf("missing-file read event = %+v", events[0])
	}
	if events[1].Outcome != ContextRepositoryAccessGranted {
		t.Fatalf("missing-file stat event = %+v", events[1])
	}
}

// TestGatewaySearchWalksTheTreeExactlyOnce is the streaming guarantee: the trace
// is assembled from the walk that produced the results, so recording evidence
// costs no second traversal.
func TestGatewaySearchWalksTheTreeExactlyOnce(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs"})

	serve(t, gateway, BuilderGatewayRequest{
		Operation: "search", PathSpec: "docs", Query: "beta",
	})
	// docs and docs/nested: two directories, two reads. A second walk to build
	// the event would make this four.
	if reads := gateway.DirectoryReads(); reads != 2 {
		t.Fatalf("search performed %d directory read(s) over a 2-directory tree", reads)
	}

	events, _ := closeAndLoad(t, gateway)
	if len(events) != 1 {
		t.Fatalf("search recorded %d event(s)", len(events))
	}
	if strings.Join(events[0].Entries, ",") != "docs/guide.md,docs/nested/deep.md" {
		t.Fatalf("search event enumeration = %v", events[0].Entries)
	}
}

// TestGatewayAttestationClosesTheTrace proves the terminal statement binds the
// count and the chain, and that the gateway serves nothing afterwards.
func TestGatewayAttestationClosesTheTrace(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs"})
	serve(t, gateway, BuilderGatewayRequest{Operation: "stat", Path: "docs/guide.md"})

	attestation, err := gateway.Close()
	if err != nil {
		t.Fatalf("closing the gateway: %v", err)
	}
	if attestation.ContractVersion != ContextRepositoryAccessTraceContract {
		t.Fatalf("attestation contract = %q", attestation.ContractVersion)
	}
	if attestation.Anchor != gateway.Anchor() || attestation.EventCount != 1 {
		t.Fatalf("attestation = %+v", attestation)
	}
	if _, err := gateway.Serve(BuilderGatewayRequest{
		Operation: "read", Path: "docs/guide.md", ProviderEventID: "evt-after-close",
	}); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("serving after attestation = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
	// Closing twice returns the same statement rather than minting a second one
	// that binds a different count.
	again, err := gateway.Close()
	if err != nil || again.TraceDigest != attestation.TraceDigest {
		t.Fatalf("second close = %+v err=%v", again, err)
	}
}

// TestGatewayRefusesWithoutAConfiguredAllowlist keeps the fail-closed default:
// an unbounded read scope is not a gateway, it is an open door with a logbook.
func TestGatewayRefusesWithoutAConfiguredAllowlist(t *testing.T) {
	_, err := NewBuilderEvidenceGateway(BuilderEvidenceGatewayConfig{
		CtxID:          builderTraceTestCtxID,
		Provider:       "claude",
		RepositoryRoot: gatewayRepo(t),
		Allowlist:      NewContextBuilderAllowlist(nil),
		TracePath:      filepath.Join(t.TempDir(), "trace.jsonl"),
	})
	if !builderIsContextErrorCode(err, ContextErrorBuilderAllowlistDenied) {
		t.Fatalf("gateway without an allowlist = %v, want %s",
			err, ContextErrorBuilderAllowlistDenied)
	}
}

// TestGatewayRefusesToReopenAnExistingTrace stops two runs from splicing into
// one chain whose attestation describes neither.
func TestGatewayRefusesToReopenAnExistingTrace(t *testing.T) {
	root := gatewayRepo(t)
	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
	config := BuilderEvidenceGatewayConfig{
		CtxID:          builderTraceTestCtxID,
		Provider:       "claude",
		RepositoryRoot: root,
		Allowlist:      NewContextBuilderAllowlist([]string{"docs"}),
		TracePath:      tracePath,
	}
	first, err := NewBuilderEvidenceGateway(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBuilderEvidenceGateway(config); !builderIsContextErrorCode(
		err, ContextErrorBuilderTraceUnavailable,
	) {
		t.Fatalf("reopening a written trace = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

func closeAndLoad(t *testing.T, gateway *BuilderEvidenceGateway) (
	[]ContextRepositoryAccessEvent,
	ContextRepositoryAccessAttestation,
) {
	t.Helper()
	attestation, err := gateway.Close()
	if err != nil {
		t.Fatalf("closing the gateway: %v", err)
	}
	events, loaded, err := LoadBuilderAccessTrace(gateway.trace.path)
	if err != nil {
		t.Fatalf("loading the trace back: %v", err)
	}
	if loaded.TraceDigest != attestation.TraceDigest {
		t.Fatal("the persisted attestation does not match the one Close returned")
	}
	return events, loaded
}
