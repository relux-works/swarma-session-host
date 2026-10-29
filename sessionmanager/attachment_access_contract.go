package sessionmanager

import (
	"context"
	"io"
)

// D17 is optional; membership remains solely host-owned. Every operation
// admits an exact live SID/attachment pair and current controller under the
// host lock before exposing provider access. Detached IDs are never reissued.
type AttachmentAccessHost interface {
	AcquireAttachment(context.Context, string, string) (AttachmentAccess, error)
	Terminal(context.Context, string, string) (io.ReadWriteCloser, error)
	ResizeTerminal(context.Context, string, string, uint16, uint16) error
}

// AcquireAttachment linearizes run admission against Detach. The returned
// capability pins that controller, not a reusable membership verdict. A run
// admitted first may finish after detach; detach first refuses acquisition.
// The composition checks Lease against its actual input controller, retains
// access for the whole operation and always releases it. Release is idempotent
// and cannot release another acquisition or revive a detached attachment.
type AttachmentAccess interface {
	Lease() ControllerLease
	Release() error
}
