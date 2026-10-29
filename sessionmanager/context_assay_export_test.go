package sessionmanager

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// Test-only exposure of the ORIGINAL safe in-process source. It is absent from
// production builds and changes neither the factory API nor isolation policy.
// The receiving tests use actual provider RPC/policy, not a replacement adapter.
func ContextAssayLoopbackForTest(t *testing.T, policy CodexContextPolicy, thread, turn, activeVersion string) (*CodexHostFactory, string, func() (int, int)) {
	t.Helper()
	var forks, total atomic.Int32
	server := newCodexContextRPCServer(t, func(r codexRPCRequest) (any, []map[string]any, error) {
		total.Add(1)
		switch r.Method {
		case "initialize":
			return map[string]any{}, nil, nil
		case "thread/read":
			v := testCodexThread(thread, "", turn, "completed", nil)
			v["cliVersion"] = activeVersion
			return map[string]any{"thread": v}, nil, nil
		case "thread/fork":
			forks.Add(1)
			return nil, nil, errors.New("method not found")
		default:
			return nil, nil, errors.New("unexpected method " + r.Method)
		}
	})
	t.Cleanup(server.Close)
	factory := testCodexAssayFactory()
	factory.contextPolicy = policy
	return factory, server.URL, func() (int, int) { return int(forks.Load()), int(total.Load()) }
}
func ContextAssayCodexSourceForTest(t *testing.T, url, home, thread, turn, version string) ProviderHostRecord {
	return testCodexContextHostRecord(t, url, home, thread, turn, version)
}
func ContextAssayWriteCodexRolloutForTest(t *testing.T, home, thread, body string) {
	writeCodexContextRollout(t, home, thread, body)
}
func ContextAssayClaudeSourceForTest(t *testing.T) (*ClaudeHostFactory, ProviderHostRecord, string, string) {
	t.Helper()
	fixture := newClaudeForkFixture(t)
	marker := filepath.Join(t.TempDir(), "claude-not-invoked.txt")
	stubClaudeBinary(t, marker, "")
	return NewClaudeHostFactory(fixture.layout), fixture.assaySource(t), fixture.basePath, marker
}
func ContextAssayClaudeSchemaForTest() []byte { return claudeCV2Schema("f1") }

// Keep context imported in the same fixture source type assertion so its actual
// optional method remains tied to the production contract at compile time.
var _ interface {
	RunContextAssay(context.Context, ProviderHostRecord, CodexContextAssayRequest) (*CodexContextAssayResult, error)
} = (*CodexHostFactory)(nil)
