package sessionmanager

import "context"

// D18 is an optional ordinary graceful retirement entry, independent of the
// not-busy lease. The host writes and acknowledges intent before RequestExit;
// a bounded wait can return a retiring view, and never falls back to force.
// ProviderGracefulExit alone grants no authority to publish that intent.
type GracefulSessionHost interface {
	StopGraceful(context.Context, string) (SessionView, error)
}

// A workspace counterpart preserves the required manager contract and adds
// exactly the same optional graceful operation. Existing implementations of
// WorkspaceSessionManager need not implement D18 and are not widened.
type GracefulWorkspaceSessionManager interface {
	WorkspaceSessionManager
	GracefulSessionHost
}
