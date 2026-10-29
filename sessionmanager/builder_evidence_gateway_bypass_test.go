package sessionmanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The tests in this file attack the gateway rather than describe it.
//
// The story's standing bar is that a gate needs a test which fails when the gate
// wrongly admits, reached through a real call path. Reading the resolver and
// agreeing with it is what let the earlier Seatbelt profile ship with a
// move-then-read hole, so every case here issues an actual gateway call and
// asserts on both halves of the fail-closed contract: the content did not come
// back, and the attempt was recorded without leaking what it was refused.

// secretPayload is what an out-of-scope file holds. No assertion may ever find
// it in a response or in the trace.
const secretPayload = "PRIVATE KEY MATERIAL"

func assertDenied(
	t *testing.T,
	response BuilderGatewayResponse,
	wantReason string,
) {
	t.Helper()
	if response.Outcome != string(ContextRepositoryAccessDenied) {
		t.Fatalf("outcome = %q, want denied (response %+v)", response.Outcome, response)
	}
	if response.Reason != wantReason {
		t.Fatalf("denial reason = %q, want %q", response.Reason, wantReason)
	}
	if strings.Contains(response.Content, secretPayload) {
		t.Fatal("a denied call returned the content it was supposed to refuse")
	}
	if response.Size != 0 || response.TotalBytes != 0 || len(response.Entries) != 0 {
		t.Fatalf("a denied call disclosed metadata about its target: %+v", response)
	}
}

// TestGatewayRefusesEveryEscapeShape drives one denial per escape route the
// contract names. Each case is a real Serve call, and each asserts the file was
// not served.
func TestGatewayRefusesEveryEscapeShape(t *testing.T) {
	root := gatewayRepo(t)
	outsideDir := t.TempDir()
	outsidePath := filepath.Join(mustResolveDir(t, outsideDir), "outside.txt")
	if err := os.WriteFile(outsidePath, []byte(secretPayload+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A link inside the allowlist pointing outside the repository entirely.
	if err := os.Symlink(outsidePath, filepath.Join(root, "docs", "escape.md")); err != nil {
		t.Fatal(err)
	}
	// A link inside the allowlist pointing at an in-repository file the allowlist
	// does not cover. This is the move-then-read shape in link form: the requested
	// name is in scope and the bytes behind it are not.
	if err := os.Symlink(
		filepath.Join(root, "secrets", "key.pem"),
		filepath.Join(root, "docs", "sneak.md"),
	); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		request    BuilderGatewayRequest
		wantReason string
	}{
		{
			name:       "absolute path",
			request:    BuilderGatewayRequest{Operation: "read", Path: outsidePath},
			wantReason: ContextRepositoryAccessDenialPathEscape,
		},
		{
			name:       "traversal segment",
			request:    BuilderGatewayRequest{Operation: "read", Path: "docs/../secrets/key.pem"},
			wantReason: ContextRepositoryAccessDenialPathEscape,
		},
		{
			name:       "traversal above the root",
			request:    BuilderGatewayRequest{Operation: "read", Path: "../../etc/passwd"},
			wantReason: ContextRepositoryAccessDenialPathEscape,
		},
		{
			name:       "in-repository path outside the allowlist",
			request:    BuilderGatewayRequest{Operation: "read", Path: "secrets/key.pem"},
			wantReason: ContextRepositoryAccessDenialOutsideAllowlist,
		},
		{
			name:       "symlink out of the repository",
			request:    BuilderGatewayRequest{Operation: "read", Path: "docs/escape.md"},
			wantReason: ContextRepositoryAccessDenialSymlinkEscape,
		},
		{
			name:       "symlink out of the allowlist",
			request:    BuilderGatewayRequest{Operation: "read", Path: "docs/sneak.md"},
			wantReason: ContextRepositoryAccessDenialOutsideAllowlist,
		},
		{
			name: "foreign repository root",
			request: BuilderGatewayRequest{
				Operation: "read", Root: outsideDir, Path: "outside.txt",
			},
			wantReason: ContextRepositoryAccessDenialForeignRoot,
		},
		{
			name:       "unmodelled operation",
			request:    BuilderGatewayRequest{Operation: "exec", Path: "docs/guide.md"},
			wantReason: ContextRepositoryAccessDenialUnclassifiable,
		},
		{
			name:       "stat outside the allowlist",
			request:    BuilderGatewayRequest{Operation: "stat", Path: "secrets/key.pem"},
			wantReason: ContextRepositoryAccessDenialOutsideAllowlist,
		},
		{
			name:       "list outside the allowlist",
			request:    BuilderGatewayRequest{Operation: "list", PathSpec: "secrets"},
			wantReason: ContextRepositoryAccessDenialOutsideAllowlist,
		},
		{
			name: "search outside the allowlist",
			request: BuilderGatewayRequest{
				Operation: "search", PathSpec: "secrets", Query: "KEY",
			},
			wantReason: ContextRepositoryAccessDenialOutsideAllowlist,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gateway := newTestGateway(t, root, []string{"docs", "pkg"})
			response := serve(t, gateway, test.request)
			assertDenied(t, response, test.wantReason)

			events, _ := closeAndLoad(t, gateway)
			if len(events) != 1 {
				t.Fatalf("a refused call recorded %d event(s), want exactly 1", len(events))
			}
			event := events[0]
			if event.Outcome != ContextRepositoryAccessDenied {
				t.Fatalf("recorded outcome = %q", event.Outcome)
			}
			if event.DenialReason != test.wantReason {
				t.Fatalf("recorded denial reason = %q, want %q", event.DenialReason, test.wantReason)
			}
			// A denial must record the attempt without recording anything derived
			// from what it refused. A digest or a size here would be an oracle: it
			// would confirm a guess about a file the gateway just declined to serve.
			if event.ContentDigest != "" || len(event.Entries) != 0 ||
				event.TotalBytes != 0 || event.ByteLength != 0 {
				t.Fatalf("a denial event carried evidence about its target: %+v", event)
			}
		})
	}
}

// TestGatewayEnumerationsOmitOutOfScopeEntries closes the disclosure channel
// that a listing would otherwise open. Refusing to read secrets/key.pem while
// happily reporting that it exists still tells the builder what the project
// declared out of scope.
func TestGatewayEnumerationsOmitOutOfScopeEntries(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs"})

	// Listing the repository root is permitted only as far as navigation goes:
	// docs is an ancestor of something allowed, secrets is not.
	listResponse := serve(t, gateway, BuilderGatewayRequest{
		Operation: "list", PathSpec: "docs",
	})
	for _, entry := range listResponse.Entries {
		if strings.HasPrefix(entry, "secrets") {
			t.Fatalf("an enumeration disclosed an out-of-scope path: %v", listResponse.Entries)
		}
	}

	// A search that would match the out-of-scope file must not name it either.
	searchResponse := serve(t, gateway, BuilderGatewayRequest{
		Operation: "search", PathSpec: "docs", Query: "PRIVATE",
	})
	if len(searchResponse.Entries) != 0 {
		t.Fatalf("search reached outside its scope: %v", searchResponse.Entries)
	}

	events, _ := closeAndLoad(t, gateway)
	for _, event := range events {
		for _, entry := range event.Entries {
			if strings.HasPrefix(entry, "secrets") {
				t.Fatalf("the trace recorded an out-of-scope enumeration entry: %v", event.Entries)
			}
		}
	}
}

// TestGatewaySearchDoesNotFollowSymlinksIntoScope is the walk-time half of the
// link defence. resolveLocked guards a named request; this guards the tree walk,
// where a link would otherwise pull an out-of-scope file into the result set
// under an in-scope name.
func TestGatewaySearchDoesNotFollowSymlinksIntoScope(t *testing.T) {
	root := gatewayRepo(t)
	if err := os.Symlink(
		filepath.Join(root, "secrets"),
		filepath.Join(root, "docs", "linked"),
	); err != nil {
		t.Fatal(err)
	}
	gateway := newTestGateway(t, root, []string{"docs"})

	response := serve(t, gateway, BuilderGatewayRequest{
		Operation: "search", PathSpec: "docs", Query: "PRIVATE",
	})
	if len(response.Entries) != 0 {
		t.Fatalf("the walk followed a symlink out of scope: %v", response.Entries)
	}
	if strings.Contains(strings.Join(response.Entries, ","), "linked") {
		t.Fatal("the walk enumerated through a symlink")
	}
}

// TestGatewayTraceDetectsTampering proves the chain is load-bearing rather than
// decorative: an edited, removed or reordered event fails verification.
func TestGatewayTraceDetectsTampering(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs"})
	serve(t, gateway, BuilderGatewayRequest{
		Operation: "read", Path: "docs/guide.md", ProviderEventID: "evt-1",
	})
	serve(t, gateway, BuilderGatewayRequest{
		Operation: "read", Path: "docs/nested/deep.md", ProviderEventID: "evt-2",
	})
	events, attestation := closeAndLoad(t, gateway)

	t.Run("edited event", func(t *testing.T) {
		tampered := append([]ContextRepositoryAccessEvent(nil), events...)
		// Widening a recorded read is the attack the coverage manifest is exposed
		// to: it would credit the base with bytes nobody can show it saw.
		tampered[0].ByteLength = 1
		if err := VerifyContextRepositoryAccessTrace(
			tampered, attestation,
		); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
			t.Fatalf("verifying an edited trace = %v, want a refusal", err)
		}
	})

	t.Run("removed event", func(t *testing.T) {
		if err := VerifyContextRepositoryAccessTrace(
			events[:1], attestation,
		); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
			t.Fatalf("verifying a truncated trace = %v, want a refusal", err)
		}
	})

	t.Run("reordered events", func(t *testing.T) {
		swapped := []ContextRepositoryAccessEvent{events[1], events[0]}
		if err := VerifyContextRepositoryAccessTrace(
			swapped, attestation,
		); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
			t.Fatalf("verifying a reordered trace = %v, want a refusal", err)
		}
	})

	t.Run("replayed under another anchor", func(t *testing.T) {
		// Evidence minted for one context must not verify for another, or a base
		// with good coverage could lend its trace to a base with none.
		foreign := attestation
		foreign.CtxID = "CTX-260731-gw0002"
		foreign.Anchor = ContextRepositoryAccessAnchor(
			foreign.CtxID, foreign.RunID, foreign.Provider,
		)
		if err := VerifyContextRepositoryAccessTrace(
			events, foreign,
		); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
			t.Fatalf("verifying a replayed trace = %v, want a refusal", err)
		}
	})
}

// TestLoadBuilderAccessTraceRefusesAnUnclosedTrace covers the crash case: a run
// that died leaves events with no attestation, and those events are not evidence
// of complete coverage however genuine each one is.
func TestLoadBuilderAccessTraceRefusesAnUnclosedTrace(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs"})
	serve(t, gateway, BuilderGatewayRequest{Operation: "read", Path: "docs/guide.md"})
	tracePath := gateway.trace.path
	// Drop the writer without attesting, exactly as a killed manager would.
	if err := gateway.trace.close(); err != nil {
		t.Fatal(err)
	}

	_, _, err := LoadBuilderAccessTrace(tracePath)
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("loading an unattested trace = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestLoadBuilderAccessTraceRefusesEventsAfterTheAttestation stops the simplest
// forgery: appending reads after the statement that said there were none.
func TestLoadBuilderAccessTraceRefusesEventsAfterTheAttestation(t *testing.T) {
	root := gatewayRepo(t)
	gateway := newTestGateway(t, root, []string{"docs"})
	serve(t, gateway, BuilderGatewayRequest{Operation: "read", Path: "docs/guide.md"})
	tracePath := gateway.trace.path
	if _, err := gateway.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	appended := strings.Join(append(lines, lines[0]), "\n") + "\n"
	if err := os.WriteFile(tracePath, []byte(appended), 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err = LoadBuilderAccessTrace(tracePath)
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("loading a post-attestation append = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

func mustResolveDir(t *testing.T, dir string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
