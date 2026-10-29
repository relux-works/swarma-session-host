package sessionmanager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	contextpolicy "github.com/relux-works/swarma-session-host/sessionmanager/resource"
	"github.com/relux-works/swarma-session-host/sessionmanager/runner/transcript"
)

const (
	// builderEvidenceGatewayMechanism names this gateway in the evidence it
	// produces. It is versioned because a consumer that verifies a trace has to
	// know which normalization and denial rules produced it.
	builderEvidenceGatewayMechanism = "session-manager-repository-evidence-gateway-v1"

	// builderGatewayTraceDirName holds one append-only trace per context.
	builderGatewayTraceDirName = "builder-access-trace"

	// builderGatewayMaxReadBytes bounds one read response.
	//
	// The bound exists so a single call cannot pull an arbitrarily large payload
	// through a surface whose evidence is a fixed-size digest, and so the trace's
	// byte ranges stay meaningful: a builder that wants a whole large file issues
	// successive ranged reads, and the trace shows exactly which parts it took.
	builderGatewayMaxReadBytes int64 = 1 << 20

	// builderGatewayMaxEntries bounds one enumeration.
	//
	// A bounded enumeration is recorded as bounded. That matters more than the
	// number: a consumer that read a clipped listing as complete would credit the
	// base with having seen a directory it only partly saw.
	builderGatewayMaxEntries = 2000
)

// BuilderEvidenceGateway is the only repository surface a publishable
// context-builder run may reach.
//
// It exists because confinement and evidence are different guarantees. The
// Seatbelt profile shipped with the confinement work bounds *what* the builder
// process can read; it cannot say what the builder *did* read, because a kernel
// deny-by-default policy produces no record of the accesses it permitted. This
// gateway produces that record, and the tool-isolation policy beside it is what
// makes the record complete: with every native filesystem tool, shell, network
// route and uninstrumented MCP server denied, an in-scope read has nowhere to
// happen except here.
//
// The two mechanisms are deliberately not merged. The sandbox stays as the outer
// boundary precisely because this gateway is ordinary Go code with bugs in its
// future: if a normalization mistake here would otherwise serve a file outside
// the allowlist, the kernel still refuses to open it.
type BuilderEvidenceGateway struct {
	mu sync.Mutex

	ctxID    string
	runID    string
	provider ContextProvider
	// root is the resolved absolute repository root this gateway is anchored to.
	// It is resolved once, at construction, so a later change to a symlink in the
	// path cannot move the anchor under a run that is already recording against
	// it.
	root      string
	allowlist ContextBuilderAllowlist
	anchor    string

	trace *builderAccessTraceWriter
	now   func() time.Time

	sequence     int
	lastDigest   string
	closed       bool
	closeErr     error
	attestation  *ContextRepositoryAccessAttestation
	directoryOps int

	// shimStarted, shimClosed, shimCalls and shimUnknown are the stdio shim's
	// report about the provider's tool stream. See noteShimControl.
	shimStarted bool
	shimClosed  bool
	shimCalls   int
	shimUnknown int

	// unclassifiable counts calls this gateway served but could not model. The
	// event it records says an attempt happened; this says the attempt's meaning
	// was never established. They are separate numbers because a recorded denial
	// still reconciles, and a call nobody could classify must not.
	unclassifiable int
}

// BuilderEvidenceGatewayConfig is what the manager anchors a gateway to.
type BuilderEvidenceGatewayConfig struct {
	CtxID     string
	RunID     string
	Provider  ContextProvider
	ProjectID string
	// RepositoryRoot is the project directory the builder run is anchored to.
	RepositoryRoot string
	Allowlist      ContextBuilderAllowlist
	// TracePath is the append-only file this gateway streams events to.
	TracePath string
	Now       func() time.Time
}

// NewBuilderEvidenceGateway anchors a gateway and opens its trace.
//
// Every failure here is context_builder_trace_unavailable rather than a plain
// error, because all of them have the same consequence: this run cannot produce
// evidence, so whatever it builds is not a publishable project base.
func NewBuilderEvidenceGateway(
	config BuilderEvidenceGatewayConfig,
) (*BuilderEvidenceGateway, error) {
	ctxID := builderNormalizeContextID(config.CtxID)
	if err := builderValidateContextID(ctxID); err != nil {
		return nil, err
	}
	if !config.Allowlist.Configured() {
		return nil, &BuilderTraceError{
			Code: ContextErrorBuilderAllowlistDenied,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s has no configured read allowlist to serve", ctxID,
			),
		}
	}
	root := strings.TrimSpace(config.RepositoryRoot)
	if !filepath.IsAbs(root) {
		return nil, &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s must be anchored to an absolute repository root",
				ctxID,
			),
		}
	}
	resolvedRoot, err := filepath.EvalSymlinks(filepath.Clean(root))
	if err != nil {
		return nil, &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s cannot resolve its repository root", ctxID,
			),
			Cause: err,
		}
	}
	provider := ContextProvider(
		strings.ToLower(strings.TrimSpace(string(config.Provider))))
	if provider == "" {
		return nil, &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s must name the provider it records", ctxID,
			),
		}
	}
	runID := strings.ToUpper(strings.TrimSpace(config.RunID))
	trace, err := newBuilderAccessTraceWriter(config.TracePath)
	if err != nil {
		return nil, &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s cannot open its append-only trace", ctxID,
			),
			Cause: err,
		}
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &BuilderEvidenceGateway{
		ctxID:     ctxID,
		runID:     runID,
		provider:  provider,
		root:      resolvedRoot,
		allowlist: config.Allowlist,
		anchor:    ContextRepositoryAccessAnchor(ctxID, runID, provider),
		trace:     trace,
		now:       now,
	}, nil
}

// Mechanism names this gateway for the enforcement claim.
func (g *BuilderEvidenceGateway) Mechanism() string { return builderEvidenceGatewayMechanism }

// Anchor is the value every digest in this run's trace is computed under.
func (g *BuilderEvidenceGateway) Anchor() string { return g.anchor }

// BuilderGatewayRequest is one call from the confined builder.
//
// Root is present so a request that names a repository can be refused rather
// than silently reinterpreted. A gateway is anchored to exactly one root, and a
// call naming a different one is a foreign-root denial — not because the other
// root is necessarily forbidden, but because serving it would put reads from two
// repositories into one trace whose anchor claims a single origin.
type BuilderGatewayRequest struct {
	// Operation is the tool the builder invoked. An operation this gateway does
	// not model is refused as unclassifiable rather than guessed at.
	Operation string
	// ProviderEventID is the provider's identity for the tool call. It is
	// required: an access that cannot be attributed to a provider event cannot be
	// reconciled against the provider's own tool stream, and that reconciliation
	// is what detects a read that never reached the gateway.
	ProviderEventID string
	// Root, when set, must be the repository this gateway is anchored to.
	Root string
	// Path targets a stat or a read. PathSpec targets a list or a search.
	Path     string
	PathSpec string
	// Offset and Length select the byte range of a read.
	Offset int64
	Length int64
	// Query is the search pattern. It matches file content; the response carries
	// which files matched and how often, never the matching text.
	Query string
}

// BuilderGatewayResponse is what the builder gets back.
//
// It carries content only for a read, and only the exact range the trace
// records. Everything else is metadata, so there is no response field through
// which repository bytes could reach the builder without a corresponding ranged
// event.
type BuilderGatewayResponse struct {
	Operation string `json:"operation"`
	Outcome   string `json:"outcome"`
	// Denied and Failed responses carry a stable reason and nothing else, so a
	// refusal cannot be used as an oracle for the content it refused.
	Reason string `json:"reason,omitempty"`

	Path     string `json:"path,omitempty"`
	PathSpec string `json:"path_spec,omitempty"`

	Exists bool   `json:"exists,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Size   int64  `json:"size,omitempty"`

	Content    string `json:"content,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	Length     int64  `json:"length,omitempty"`
	TotalBytes int64  `json:"total_bytes,omitempty"`
	Truncated  bool   `json:"truncated,omitempty"`

	Entries          []string `json:"entries,omitempty"`
	EntriesTruncated bool     `json:"entries_truncated,omitempty"`
	MatchCounts      []int    `json:"match_counts,omitempty"`

	// Sequence is this call's position in the trace. Returning it lets the
	// provider-side shim fail loudly if the manager ever serves a call it did not
	// record, instead of the mismatch surfacing only at publication.
	Sequence int `json:"sequence"`
}

// Serve executes one gateway call and records exactly one trace event for it.
//
// The order is the contract: the access is performed, the event is sealed and
// appended, and only then is the response returned. A call whose event cannot be
// written returns an error and no content, because a read the trace does not
// describe is precisely the read this gateway exists to make impossible.
func (g *BuilderEvidenceGateway) Serve(
	request BuilderGatewayRequest,
) (BuilderGatewayResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return BuilderGatewayResponse{}, &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s was closed and attested; it serves no further reads",
				g.ctxID,
			),
		}
	}
	operation := ContextRepositoryAccessOperation(
		strings.ToLower(strings.TrimSpace(request.Operation)))
	event := ContextRepositoryAccessEvent{
		CtxID:           g.ctxID,
		RunID:           g.runID,
		Provider:        g.provider,
		ProviderEventID: request.ProviderEventID,
		Operation:       operation,
		At:              g.now(),
	}
	// An operation nobody modelled cannot be described by an event, so it is
	// refused before anything touches the filesystem — and it is still recorded,
	// under an operation the contract does model, so the attempt is not invisible.
	//
	// Recording it is not the same as accounting for it. The event says "a stat was
	// denied", which is true and is also not what happened, so the call is counted
	// as unclassifiable as well: the run reconciles only if nothing it did had to
	// be described in terms the contract cannot express.
	if !builderGatewayModelsOperation(operation) {
		g.unclassifiable++
		event.Operation = ContextRepositoryAccessStat
		event.Outcome = ContextRepositoryAccessDenied
		event.DenialReason = ContextRepositoryAccessDenialUnclassifiable
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(operation),
			Outcome:   string(ContextRepositoryAccessDenied),
			Reason:    ContextRepositoryAccessDenialUnclassifiable,
		})
	}
	if reason := g.checkRootLocked(request.Root); reason != "" {
		event.Outcome = ContextRepositoryAccessDenied
		event.DenialReason = reason
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(operation),
			Outcome:   string(ContextRepositoryAccessDenied),
			Reason:    reason,
		})
	}
	switch operation {
	case ContextRepositoryAccessStat:
		return g.serveStatLocked(request, event)
	case ContextRepositoryAccessRead:
		return g.serveReadLocked(request, event)
	case ContextRepositoryAccessList:
		return g.serveListLocked(request, event)
	case ContextRepositoryAccessSearch:
		return g.serveSearchLocked(request, event)
	}
	// Unreachable: builderGatewayModelsOperation admits exactly the four cases
	// above. Failing closed here anyway keeps a future operation from acquiring an
	// unrecorded default, and counting it keeps such an operation from publishing.
	g.unclassifiable++
	event.Operation = ContextRepositoryAccessStat
	event.Outcome = ContextRepositoryAccessDenied
	event.DenialReason = ContextRepositoryAccessDenialUnclassifiable
	return g.recordLocked(event, BuilderGatewayResponse{
		Operation: string(operation),
		Outcome:   string(ContextRepositoryAccessDenied),
		Reason:    ContextRepositoryAccessDenialUnclassifiable,
	})
}

func builderGatewayModelsOperation(op ContextRepositoryAccessOperation) bool {
	switch op {
	case ContextRepositoryAccessStat,
		ContextRepositoryAccessRead,
		ContextRepositoryAccessList,
		ContextRepositoryAccessSearch:
		return true
	}
	return false
}

// checkRootLocked refuses a call that names a repository other than the anchored
// one. An empty root means "the repository you are anchored to", which is the
// only thing a confined builder can legitimately mean.
func (g *BuilderEvidenceGateway) checkRootLocked(root string) string {
	root = strings.TrimSpace(root)
	if root == "" {
		return ""
	}
	if !filepath.IsAbs(root) {
		return ContextRepositoryAccessDenialForeignRoot
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(root))
	if err != nil {
		return ContextRepositoryAccessDenialForeignRoot
	}
	if resolved != g.root {
		return ContextRepositoryAccessDenialForeignRoot
	}
	return ""
}

func (g *BuilderEvidenceGateway) serveStatLocked(
	request BuilderGatewayRequest,
	event ContextRepositoryAccessEvent,
) (BuilderGatewayResponse, error) {
	resolved, reason := g.resolveLocked(request.Path)
	if reason != "" {
		event.Outcome = ContextRepositoryAccessDenied
		event.DenialReason = reason
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(ContextRepositoryAccessStat),
			Outcome:   string(ContextRepositoryAccessDenied),
			Reason:    reason,
		})
	}
	event.Path = resolved.rel
	info, err := os.Lstat(resolved.abs)
	// A missing path is a granted stat with a negative answer, not a failure:
	// "this file does not exist" is a legitimate observation the builder is
	// entitled to and the coverage consumer needs, and recording it as a failure
	// would make an ordinary probe look like a broken gateway.
	if err != nil && !os.IsNotExist(err) {
		event.Outcome = ContextRepositoryAccessFailed
		event.FailureReason = "stat_failed"
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(ContextRepositoryAccessStat),
			Outcome:   string(ContextRepositoryAccessFailed),
			Reason:    "stat_failed",
			Path:      resolved.rel,
		})
	}
	exists := err == nil
	kind := ""
	var size int64
	if exists {
		kind = builderGatewayNodeKind(info)
		if info.Mode().IsRegular() {
			size = info.Size()
		}
	}
	event.Outcome = ContextRepositoryAccessGranted
	event.ContentDigest = transcript.ContextRepositoryAccessStatDigest(g.anchor, exists, kind, size)
	return g.recordLocked(event, BuilderGatewayResponse{
		Operation: string(ContextRepositoryAccessStat),
		Outcome:   string(ContextRepositoryAccessGranted),
		Path:      resolved.rel,
		Exists:    exists,
		Kind:      kind,
		Size:      size,
	})
}

func (g *BuilderEvidenceGateway) serveReadLocked(
	request BuilderGatewayRequest,
	event ContextRepositoryAccessEvent,
) (BuilderGatewayResponse, error) {
	resolved, reason := g.resolveLocked(request.Path)
	if reason != "" {
		event.Outcome = ContextRepositoryAccessDenied
		event.DenialReason = reason
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(ContextRepositoryAccessRead),
			Outcome:   string(ContextRepositoryAccessDenied),
			Reason:    reason,
		})
	}
	event.Path = resolved.rel
	info, err := os.Lstat(resolved.abs)
	if err != nil {
		return g.failReadLocked(event, resolved.rel, "not_found")
	}
	// Only regular files are readable. A directory, device or socket served
	// through a byte-range surface would produce a "range" nobody can reproduce,
	// and a symlink is never reached here because resolveLocked already followed
	// it and re-checked where it landed.
	if !info.Mode().IsRegular() {
		return g.failReadLocked(event, resolved.rel, "not_a_regular_file")
	}
	total := info.Size()
	offset := request.Offset
	if offset < 0 {
		return g.failReadLocked(event, resolved.rel, "invalid_range")
	}
	if offset > total {
		return g.failReadLocked(event, resolved.rel, "invalid_range")
	}
	length := request.Length
	if length <= 0 {
		length = total - offset
	}
	if remaining := total - offset; length > remaining {
		length = remaining
	}
	truncated := false
	if length > builderGatewayMaxReadBytes {
		length = builderGatewayMaxReadBytes
		truncated = true
	}
	file, err := os.Open(resolved.abs)
	if err != nil {
		return g.failReadLocked(event, resolved.rel, "open_failed")
	}
	defer file.Close()
	payload := make([]byte, length)
	read, err := io.ReadFull(io.NewSectionReader(file, offset, length), payload)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return g.failReadLocked(event, resolved.rel, "read_failed")
	}
	payload = payload[:read]
	if int64(read) < length {
		// The file shrank under us. Record the range actually returned rather than
		// the one intended, so the trace never claims bytes the builder did not get.
		length = int64(read)
		truncated = true
	}
	event.Outcome = ContextRepositoryAccessGranted
	event.ByteOffset = offset
	event.ByteLength = length
	event.TotalBytes = total
	event.Truncated = truncated
	event.ContentDigest = transcript.ContextRepositoryAccessContentDigest(g.anchor, payload)
	return g.recordLocked(event, BuilderGatewayResponse{
		Operation:  string(ContextRepositoryAccessRead),
		Outcome:    string(ContextRepositoryAccessGranted),
		Path:       resolved.rel,
		Content:    string(payload),
		Offset:     offset,
		Length:     length,
		TotalBytes: total,
		Truncated:  truncated,
	})
}

func (g *BuilderEvidenceGateway) failReadLocked(
	event ContextRepositoryAccessEvent,
	rel string,
	reason string,
) (BuilderGatewayResponse, error) {
	event.Outcome = ContextRepositoryAccessFailed
	event.FailureReason = reason
	event.ByteOffset = 0
	event.ByteLength = 0
	event.TotalBytes = 0
	return g.recordLocked(event, BuilderGatewayResponse{
		Operation: string(ContextRepositoryAccessRead),
		Outcome:   string(ContextRepositoryAccessFailed),
		Reason:    reason,
		Path:      rel,
	})
}

func (g *BuilderEvidenceGateway) serveListLocked(
	request BuilderGatewayRequest,
	event ContextRepositoryAccessEvent,
) (BuilderGatewayResponse, error) {
	spec := request.PathSpec
	if strings.TrimSpace(spec) == "" {
		spec = request.Path
	}
	resolved, reason := g.resolveLocked(spec)
	if reason != "" {
		event.Outcome = ContextRepositoryAccessDenied
		event.DenialReason = reason
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(ContextRepositoryAccessList),
			Outcome:   string(ContextRepositoryAccessDenied),
			Reason:    reason,
		})
	}
	event.PathSpec = resolved.rel
	g.directoryOps++
	entries, err := os.ReadDir(resolved.abs)
	if err != nil {
		event.Outcome = ContextRepositoryAccessFailed
		event.FailureReason = "list_failed"
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(ContextRepositoryAccessList),
			Outcome:   string(ContextRepositoryAccessFailed),
			Reason:    "list_failed",
			PathSpec:  resolved.rel,
		})
	}
	names := make([]string, 0, len(entries))
	truncated := false
	for _, entry := range entries {
		candidate := path.Join(resolved.rel, entry.Name())
		// An entry the allowlist does not cover is simply not enumerated. Listing
		// it would disclose the existence and naming of files the project declared
		// out of scope, which is a read the allowlist exists to refuse.
		if !g.allowlist.Allows(candidate) && !g.allowlistCoversPrefix(candidate) {
			continue
		}
		if len(names) >= builderGatewayMaxEntries {
			truncated = true
			break
		}
		names = append(names, candidate)
	}
	sort.Strings(names)
	event.Outcome = ContextRepositoryAccessGranted
	event.Entries = names
	event.EntriesTruncated = truncated
	event.ContentDigest = transcript.ContextRepositoryAccessEnumerationDigest(g.anchor, names)
	return g.recordLocked(event, BuilderGatewayResponse{
		Operation:        string(ContextRepositoryAccessList),
		Outcome:          string(ContextRepositoryAccessGranted),
		PathSpec:         resolved.rel,
		Entries:          names,
		EntriesTruncated: truncated,
	})
}

// serveSearchLocked walks the requested subtree once and reports which files
// matched.
//
// It reports paths and match counts, never matching text. That is a deliberate
// limit on the surface rather than an omission: a snippet is repository content,
// and content that reached the builder without a byte range in the trace would
// be coverage nobody can account for. A builder that wants the matching text
// issues a ranged read, which is recorded as one.
func (g *BuilderEvidenceGateway) serveSearchLocked(
	request BuilderGatewayRequest,
	event ContextRepositoryAccessEvent,
) (BuilderGatewayResponse, error) {
	spec := request.PathSpec
	if strings.TrimSpace(spec) == "" {
		spec = request.Path
	}
	query := strings.TrimSpace(request.Query)
	if query == "" {
		event.Outcome = ContextRepositoryAccessDenied
		event.DenialReason = ContextRepositoryAccessDenialUnclassifiable
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(ContextRepositoryAccessSearch),
			Outcome:   string(ContextRepositoryAccessDenied),
			Reason:    ContextRepositoryAccessDenialUnclassifiable,
		})
	}
	resolved, reason := g.resolveLocked(spec)
	if reason != "" {
		event.Outcome = ContextRepositoryAccessDenied
		event.DenialReason = reason
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(ContextRepositoryAccessSearch),
			Outcome:   string(ContextRepositoryAccessDenied),
			Reason:    reason,
		})
	}
	event.PathSpec = resolved.rel
	event.Query = query
	matches, counts, truncated, err := g.searchTreeLocked(resolved, query)
	if err != nil {
		event.Outcome = ContextRepositoryAccessFailed
		event.FailureReason = "search_failed"
		event.Query = query
		return g.recordLocked(event, BuilderGatewayResponse{
			Operation: string(ContextRepositoryAccessSearch),
			Outcome:   string(ContextRepositoryAccessFailed),
			Reason:    "search_failed",
			PathSpec:  resolved.rel,
		})
	}
	event.Outcome = ContextRepositoryAccessGranted
	event.Entries = matches
	event.EntriesTruncated = truncated
	event.ContentDigest = transcript.ContextRepositoryAccessEnumerationDigest(g.anchor, matches)
	return g.recordLocked(event, BuilderGatewayResponse{
		Operation:        string(ContextRepositoryAccessSearch),
		Outcome:          string(ContextRepositoryAccessGranted),
		PathSpec:         resolved.rel,
		Entries:          matches,
		MatchCounts:      counts,
		EntriesTruncated: truncated,
	})
}

// searchTreeLocked is the single walk.
//
// The trace event is assembled from what this walk produced and nothing else, so
// recording evidence costs no second traversal of the repository. Every
// directory it descends into is counted, which is what the streaming test
// asserts against: a second walk would double the count.
func (g *BuilderEvidenceGateway) searchTreeLocked(
	resolved resolvedGatewayPath,
	query string,
) ([]string, []int, bool, error) {
	needle := []byte(query)
	matches := make([]string, 0, 16)
	counts := make([]int, 0, 16)
	truncated := false

	var walk func(abs, rel string) error
	walk = func(abs, rel string) error {
		if truncated {
			return nil
		}
		g.directoryOps++
		entries, err := os.ReadDir(abs)
		if err != nil {
			return err
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		sort.Strings(names)
		for _, name := range names {
			childRel := path.Join(rel, name)
			childAbs := filepath.Join(abs, name)
			info, err := os.Lstat(childAbs)
			if err != nil {
				continue
			}
			// Symlinks are never followed during a walk. Following one would let a
			// link inside the allowlist pull an out-of-scope file into the result
			// set under an in-scope name — the same move-then-read shape the sandbox
			// profile had to close.
			if info.Mode()&os.ModeSymlink != 0 {
				continue
			}
			if info.IsDir() {
				if !g.allowlist.Allows(childRel) && !g.allowlistCoversPrefix(childRel) {
					continue
				}
				if err := walk(childAbs, childRel); err != nil {
					return err
				}
				if truncated {
					return nil
				}
				continue
			}
			if !info.Mode().IsRegular() || !g.allowlist.Allows(childRel) {
				continue
			}
			count, err := builderGatewayCountMatches(childAbs, needle)
			if err != nil || count == 0 {
				continue
			}
			if len(matches) >= builderGatewayMaxEntries {
				truncated = true
				return nil
			}
			matches = append(matches, childRel)
			counts = append(counts, count)
		}
		return nil
	}

	info, err := os.Lstat(resolved.abs)
	if err != nil {
		return nil, nil, false, err
	}
	if info.IsDir() {
		if err := walk(resolved.abs, resolved.rel); err != nil {
			return nil, nil, false, err
		}
		return matches, counts, truncated, nil
	}
	if !info.Mode().IsRegular() || !g.allowlist.Allows(resolved.rel) {
		return matches, counts, truncated, nil
	}
	count, err := builderGatewayCountMatches(resolved.abs, needle)
	if err != nil {
		return nil, nil, false, err
	}
	if count > 0 {
		matches = append(matches, resolved.rel)
		counts = append(counts, count)
	}
	return matches, counts, truncated, nil
}

func builderGatewayCountMatches(abs string, needle []byte) (int, error) {
	payload, err := os.ReadFile(abs)
	if err != nil {
		return 0, err
	}
	return bytes.Count(payload, needle), nil
}

func builderGatewayNodeKind(info os.FileInfo) string {
	switch {
	case info.IsDir():
		return "directory"
	case info.Mode()&os.ModeSymlink != 0:
		return "symlink"
	case info.Mode().IsRegular():
		return "file"
	}
	return "other"
}

// resolvedGatewayPath is one request that survived every check.
type resolvedGatewayPath struct {
	abs string
	rel string
}

// resolveLocked turns a requested spec into a path this gateway may serve, or a
// stable denial reason.
//
// The checks run in an order that matters. Spelling is checked first, so a
// traversal or an absolute path is refused before it can be joined to anything.
// The result is then resolved through symlinks and re-anchored to the repository
// root, so a link that points outside the repository is a symlink escape rather
// than a read. Finally the *resolved* repo-relative path is re-checked against
// the allowlist — not just the requested one — because a link inside an
// allowlisted directory pointing at an out-of-scope file would otherwise pass a
// check performed on its own name.
func (g *BuilderEvidenceGateway) resolveLocked(spec string) (resolvedGatewayPath, string) {
	normalized := contextpolicy.NormalizeContextPathSpec(spec)
	if normalized == "" {
		return resolvedGatewayPath{}, ContextRepositoryAccessDenialPathEscape
	}
	if !g.allowlist.Allows(normalized) && !g.allowlistCoversPrefix(normalized) {
		return resolvedGatewayPath{}, ContextRepositoryAccessDenialOutsideAllowlist
	}
	abs := filepath.Join(g.root, filepath.FromSlash(normalized))
	// Lexical containment is re-checked after the join rather than trusted from
	// normalization alone. They agree today; a divergence between them would be a
	// path escape, and this is the cheaper place to find that out.
	if !builderGatewayWithinRoot(g.root, abs) {
		return resolvedGatewayPath{}, ContextRepositoryAccessDenialPathEscape
	}
	resolvedAbs, err := builderGatewayResolveExisting(abs)
	if err != nil {
		return resolvedGatewayPath{}, ContextRepositoryAccessDenialSymlinkEscape
	}
	if !builderGatewayWithinRoot(g.root, resolvedAbs) {
		return resolvedGatewayPath{}, ContextRepositoryAccessDenialSymlinkEscape
	}
	resolvedRel, err := filepath.Rel(g.root, resolvedAbs)
	if err != nil {
		return resolvedGatewayPath{}, ContextRepositoryAccessDenialSymlinkEscape
	}
	rel := contextpolicy.NormalizeContextPathSpec(filepath.ToSlash(resolvedRel))
	if rel == "" {
		return resolvedGatewayPath{}, ContextRepositoryAccessDenialPathEscape
	}
	if !g.allowlist.Allows(rel) && !g.allowlistCoversPrefix(rel) {
		return resolvedGatewayPath{}, ContextRepositoryAccessDenialOutsideAllowlist
	}
	return resolvedGatewayPath{abs: resolvedAbs, rel: rel}, ""
}

// allowlistCoversPrefix reports whether a path is an ancestor of something the
// allowlist permits.
//
// Without it, an allowlist of "docs/**" would make "docs" itself unlistable, so
// a builder could never discover what it is allowed to read. The permission is
// strictly navigational: it admits stat and enumeration of a containing
// directory, and every file the enumeration yields is still checked against the
// allowlist on its own.
func (g *BuilderEvidenceGateway) allowlistCoversPrefix(candidate string) bool {
	candidate = contextpolicy.NormalizeContextPathSpec(candidate)
	if candidate == "" {
		return false
	}
	prefix := candidate + "/"
	for _, allowed := range g.allowlist.Paths {
		if strings.HasPrefix(allowed, prefix) {
			return true
		}
	}
	return false
}

func builderGatewayWithinRoot(root, candidate string) bool {
	root = filepath.Clean(root)
	candidate = filepath.Clean(candidate)
	if candidate == root {
		return true
	}
	return strings.HasPrefix(candidate, root+string(filepath.Separator))
}

// builderGatewayResolveExisting resolves symlinks in the longest existing prefix
// of a path.
//
// filepath.EvalSymlinks fails outright on a path whose leaf does not exist,
// which would turn every probe for a missing file into an escape. Resolving the
// deepest existing ancestor instead answers the question that actually matters —
// does any real component of this path leave the repository — while letting a
// non-existent leaf resolve to a name under an already-verified directory.
func builderGatewayResolveExisting(abs string) (string, error) {
	current := filepath.Clean(abs)
	var tail []string
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(tail) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, tail[index])
			}
			return resolved, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		tail = append(tail, filepath.Base(current))
		current = parent
	}
}

// recordLocked seals one event, appends it, and only then releases the response.
func (g *BuilderEvidenceGateway) recordLocked(
	event ContextRepositoryAccessEvent,
	response BuilderGatewayResponse,
) (BuilderGatewayResponse, error) {
	event.Sequence = g.sequence + 1
	sealed, err := SealContextRepositoryAccessEvent(g.anchor, g.lastDigest, event)
	if err != nil {
		return BuilderGatewayResponse{}, err
	}
	if err := g.trace.append(sealed); err != nil {
		return BuilderGatewayResponse{}, &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s could not persist the trace event for a served "+
					"access, so the access is refused rather than left unrecorded",
				g.ctxID,
			),
			Cause: err,
		}
	}
	g.sequence = sealed.Sequence
	g.lastDigest = sealed.EventDigest
	response.Sequence = sealed.Sequence
	return response, nil
}

// Close writes the terminal completeness attestation and stops serving.
//
// Attesting is what converts the trace from a log into evidence, so it happens
// exactly once and is idempotent afterwards: a caller that closes twice gets the
// same attestation rather than a second one binding a different count.
func (g *BuilderEvidenceGateway) Close() (ContextRepositoryAccessAttestation, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		if g.closeErr != nil {
			return ContextRepositoryAccessAttestation{}, g.closeErr
		}
		return *g.attestation, nil
	}
	g.closed = true
	attestation, err := AttestContextRepositoryAccessTrace(
		g.anchor, g.ctxID, g.runID, g.provider, g.sequence, g.lastDigest, g.now(),
	)
	if err != nil {
		g.closeErr = err
		_ = g.trace.close()
		return ContextRepositoryAccessAttestation{}, err
	}
	if err := g.trace.attest(attestation); err != nil {
		g.closeErr = &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s could not persist its completeness attestation",
				g.ctxID,
			),
			Cause: err,
		}
		_ = g.trace.close()
		return ContextRepositoryAccessAttestation{}, g.closeErr
	}
	if err := g.trace.close(); err != nil {
		g.closeErr = &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s could not close its trace", g.ctxID,
			),
			Cause: err,
		}
		return ContextRepositoryAccessAttestation{}, g.closeErr
	}
	g.attestation = &attestation
	return attestation, nil
}

// DirectoryReads reports how many directory reads this gateway performed. It
// exists so the streaming guarantee — evidence costs no second walk — is
// testable rather than only asserted in a comment.
func (g *BuilderEvidenceGateway) DirectoryReads() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.directoryOps
}

// RecordedEvents reports how many events this gateway has appended.
//
// It exists so a test can ask whether a call produced an event at all, which is
// the question the shim's own refusal turns on: a call it declines to forward
// must leave the trace exactly as it found it, rather than adding a record
// describing an operation the builder never made.
func (g *BuilderEvidenceGateway) RecordedEvents() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sequence
}

// noteShimControl records the stdio shim's own lifecycle.
//
// The shim is manager-supplied code sitting between the provider and this
// gateway, which makes it the only vantage point that sees a tool call the
// gateway did not. "hello" is the provider having actually started the surface;
// "close" is the stream ending cleanly, with the shim's count of the calls it
// handled. Anything the gateway failed to record shows up as a disagreement
// between that count and the trace.
func (g *BuilderEvidenceGateway) noteShimControl(control string, calls, unknown int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch strings.ToLower(strings.TrimSpace(control)) {
	case "hello":
		g.shimStarted = true
	case "unknown":
		if unknown > 0 {
			g.shimUnknown += unknown
		} else {
			g.shimUnknown++
		}
	case "close":
		g.shimClosed = true
		g.shimCalls = calls
		g.shimUnknown = unknown
	default:
		// A control frame this version does not model is counted as an
		// unclassifiable shape rather than ignored, on the same reasoning as an
		// unmodelled trace record: silence about something we could not read is
		// not evidence that it was harmless.
		g.shimUnknown++
	}
}

// ToolObservation is what the shim reported about the provider's tool stream.
//
// A gateway the provider never started reports nothing, which is refused rather
// than read as "the builder chose to read nothing" — the two are
// indistinguishable from the trace alone, and only one of them is publishable.
func (g *BuilderEvidenceGateway) ToolObservation() BuilderToolObservation {
	g.mu.Lock()
	defer g.mu.Unlock()
	return BuilderToolObservation{
		Reported: g.shimStarted,
		// GatewayCalls is the shim's tally rather than the gateway's, because the
		// disagreement between the two is the signal. Using the gateway's own count
		// here would make the reconciliation compare a number with itself.
		GatewayCalls: g.shimCalls,
		// Both observers contribute unclassifiable shapes: the shim sees frames it
		// could not read, and the gateway sees calls it could not model. Either one
		// leaves a hole in the record, so they are summed rather than ranked.
		UnknownShapes: g.shimUnknown + g.unclassifiable,
		Disconnected:  g.shimStarted && !g.shimClosed,
	}
}

// builderAccessTraceWriter is the append-only sink.
//
// The file is opened O_APPEND and each record is flushed to disk before the
// gateway answers, so a run that dies mid-build leaves a trace of everything it
// had already read. A buffered writer would be faster and would lose exactly the
// events an investigator would want.
type builderAccessTraceWriter struct {
	path string
	file *os.File
}

func newBuilderAccessTraceWriter(tracePath string) (*builderAccessTraceWriter, error) {
	tracePath = strings.TrimSpace(tracePath)
	if tracePath == "" {
		return nil, fmt.Errorf("repository-access trace path is required")
	}
	if err := os.MkdirAll(filepath.Dir(tracePath), 0o700); err != nil {
		return nil, err
	}
	// O_EXCL: a trace is written once. Appending to an existing file would splice
	// two runs into one chain whose attestation describes neither.
	file, err := os.OpenFile(tracePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &builderAccessTraceWriter{path: tracePath, file: file}, nil
}

type builderAccessTraceRecord struct {
	Record      string                              `json:"record"`
	Event       *ContextRepositoryAccessEvent       `json:"event,omitempty"`
	Attestation *ContextRepositoryAccessAttestation `json:"attestation,omitempty"`
}

func (w *builderAccessTraceWriter) append(
	event ContextRepositoryAccessEvent,
) error {
	return w.write(builderAccessTraceRecord{Record: "event", Event: &event})
}

func (w *builderAccessTraceWriter) attest(
	attestation ContextRepositoryAccessAttestation,
) error {
	return w.write(builderAccessTraceRecord{Record: "attestation", Attestation: &attestation})
}

func (w *builderAccessTraceWriter) write(record builderAccessTraceRecord) error {
	if w == nil || w.file == nil {
		return fmt.Errorf("repository-access trace is not open")
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := w.file.Write(append(line, '\n')); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *builderAccessTraceWriter) close() error {
	if w == nil || w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// LoadBuilderAccessTrace reads one trace back and verifies it end to end.
//
// It is the consumer's entry point, and it refuses everything a partial reader
// would accept: a trace with no attestation, an attestation that does not match
// the events, a record shape this version does not model. A future coverage
// manifest can build on what this returns, accepting only traces that verify
// here.
func LoadBuilderAccessTrace(tracePath string) (
	[]ContextRepositoryAccessEvent,
	ContextRepositoryAccessAttestation,
	error,
) {
	raw, err := os.ReadFile(tracePath)
	if err != nil {
		return nil, ContextRepositoryAccessAttestation{}, &BuilderTraceError{
			Code:    ContextErrorBuilderTraceUnavailable,
			Message: "the repository-access trace for this context-builder run is not readable",
			Cause:   err,
		}
	}
	var events []ContextRepositoryAccessEvent
	var attestation *ContextRepositoryAccessAttestation
	for index, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record builderAccessTraceRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, ContextRepositoryAccessAttestation{}, &BuilderTraceError{
				Code: ContextErrorBuilderTraceUnavailable,
				Message: fmt.Sprintf(
					"repository-access trace record %d is not decodable", index+1,
				),
				Cause: err,
			}
		}
		switch record.Record {
		case "event":
			if attestation != nil {
				return nil, ContextRepositoryAccessAttestation{}, &BuilderTraceError{
					Code: ContextErrorBuilderTraceUnavailable,
					Message: "the repository-access trace records an access after its completeness " +
						"attestation, so the attestation does not close it",
				}
			}
			if record.Event == nil {
				return nil, ContextRepositoryAccessAttestation{}, &BuilderTraceError{
					Code:    ContextErrorBuilderTraceUnavailable,
					Message: fmt.Sprintf("repository-access trace event %d carries no event", index+1),
				}
			}
			events = append(events, *record.Event)
		case "attestation":
			if record.Attestation == nil {
				return nil, ContextRepositoryAccessAttestation{}, &BuilderTraceError{
					Code:    ContextErrorBuilderTraceUnavailable,
					Message: "the repository-access trace declares an attestation record with no attestation",
				}
			}
			attestation = record.Attestation
		default:
			// An unmodelled record kind is refused rather than skipped. Skipping it
			// would let a future writer add events this reader silently ignores,
			// which is the same hole as a missing event.
			return nil, ContextRepositoryAccessAttestation{}, &BuilderTraceError{
				Code: ContextErrorBuilderTraceUnavailable,
				Message: fmt.Sprintf(
					"repository-access trace record %d has kind %q, which this reader does not model",
					index+1, record.Record,
				),
			}
		}
	}
	if attestation == nil {
		return nil, ContextRepositoryAccessAttestation{}, &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: "the repository-access trace carries no completeness attestation, so nothing " +
				"establishes that it records every access the run made",
		}
	}
	if err := VerifyContextRepositoryAccessTrace(events, *attestation); err != nil {
		return nil, ContextRepositoryAccessAttestation{}, err
	}
	return events, *attestation, nil
}

// BuilderAccessTracePath is where one context's trace lives.
func BuilderAccessTracePath(layout Layout, ctxID string) string {
	return filepath.Join(
		layout.StateDir,
		builderGatewayTraceDirName,
		builderNormalizeContextID(ctxID)+".jsonl",
	)
}
