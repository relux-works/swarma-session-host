// Package probeprocess owns the OS adapter for first-probe processes. Discovery
// has no dependency on process creation; musebuilds' verified runner uses it.
package probeprocess

import (
	"context"
	"errors"
	"io"
	"os"
)

// Exit is the reaped direct child's actual status, independent of pipe EOF.
type Exit struct {
	Code   int
	Signal string
	Err    error
}

// Process owns two cancellable pipes and one reaping notification. Close on
// either reader must interrupt a pending Read. Kill targets the entire owned
// process group; GroupGone confirms its absence, not merely a sent signal.
type Process interface {
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Done() <-chan Exit
	Kill() error
	GroupGone() bool
}

// Launcher starts only the supplied verified, read-only held executable. It
// must honor ctx and must not retain a writable descriptor. Before exec the
// production adapter checks the execution path against this held identity.
// The caller owns the descriptor through teardown. A Start error means no
// child remains. Implementations are trusted host adapters, not user input.
type Launcher interface {
	Start(ctx context.Context, executable *os.File, env []string) (Process, error)
}

// OSLauncher is the production owned-group adapter. It accepts no argv: first
// probes always invoke --version and inherit only the supplied environment.
type OSLauncher struct{}

// ErrNotAttempted distinguishes a canceled admission from an actual fork/exec
// attempt. It cannot authorize recovery to an older build.
var ErrNotAttempted = errors.New("probe process was not attempted")

// ErrIdentityChanged is a pre-exec integrity refusal, never a start failure.
var ErrIdentityChanged = errors.New("probe execution path no longer names the held binary")
