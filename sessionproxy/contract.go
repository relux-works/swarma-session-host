// Package sessionproxy carries the original manager-owned attachment transport.
// The caller obtains target and admission gate from the actual runner; neither
// an endpoint string nor constructing a proxy grants session authority.
package sessionproxy

import (
	"context"
	"errors"
)

// ErrTurnFenced is the original relay's distinguished admission refusal. The
// runner adapter maps its existing sentinel while sharing the same gate.
var ErrTurnFenced = errors.New("Codex session is fenced against new turns")

type RelayGate interface {
	Client() RelayClient
}

// RelayClient is the actual provider_relay.go call surface. Write encloses the
// transport write in the runner's original admission lock. Unknown is the binary
// frame transition; Lost must preserve uncertain pending writes.
type RelayClient interface {
	Observe([]byte)
	Unknown()
	Write(context.Context, []byte, bool, func() error) error
	Lost()
}
