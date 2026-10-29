package sessionmanager

import (
	"context"
	"errors"
)

// Contract delta D5: one bounded native pending-request pass, fenced by the host.
type MusePendingResolver interface {
	ResolveMusePending(context.Context, ControllerLease) error
}

// Only an explicit relist class permits the fork to schedule another bounded
// pass. Abort and stale-block are distinct outcomes; neither authorizes retry.
// Preserve the native cause by wrapping or joining it with exactly one class.
// Context/lease failures and unknown or conflicting classes fail closed.
var (
	ErrMusePendingAbort      = errors.New("muse pending resolution: abort")
	ErrMusePendingRelist     = errors.New("muse pending resolution: relist")
	ErrMusePendingStaleBlock = errors.New("muse pending resolution: stale block")
)
