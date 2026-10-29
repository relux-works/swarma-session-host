// Package musepty hosts the Muse PTY provider: the structural TUI screen
// model (1.4.1 captures plus the version-independent L3G banner), the
// session-log fold, the required/additive wire grammar with its typed
// startup-prefix witness, and the PTY host itself
// (BuildMusePTYLaunchPlan, StartMusePTYHost, ResumeMusePTYHost), together
// with the behavioral corpus that pins them.
//
// It lives in its own package so the corpus gets its own go test timeout
// budget instead of sharing internal/sessionmanager's (regression).
// Shared provider seams (ProviderHostRecord, the ProviderHost interfaces,
// ValidateSessionNoticeText) stay in the parent package and are referenced
// from here; nothing in the parent imports this package.
package musepty
