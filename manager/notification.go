// Package manager exposes the optional, a2a-independent notification composition
// seam of the continuing session manager. Configure one trusted broker in the
// composition; absence never activates a default provider. Options and returned
// projections are data, not member, profile, runtime or provider authority.
package manager

import (
	"context"
	host "github.com/relux-works/swarma-session-host/internal/sessionhost"
)

type Options = host.ManagerOptions
type Manager = host.NotificationManager
type Attachment = host.ManagerAttachment
type Broker = host.NotificationBroker
type Origin = host.ManagerOrigin
type PreparedBroker = host.PreparedNotificationBroker
type References = host.NotificationReferences
type Completion = host.NotificationCompletion
type Enrollment = host.NotificationEnrollment
type Bootstrap = host.NotificationBootstrap
type Binding = host.NotificationBinding
type Projection = host.ManagerNotificationProjection
type Selection = host.Selection

// Open retains an actual Host and configured record source for an explicit
// finite lifetime. It does not launch a native runtime or enroll a provider.
func Open(ctx context.Context, options Options) (*Manager, error) {
	return host.OpenNotificationManager(ctx, options)
}
