package sessionmanager

import "context"

var _ ProviderObservationCloser = (*codexRunner)(nil)

// CloseObservation cancels admission, joins native RPC/monitor work and closes
// only observation connections. The detached process and owned Wait survive.
// Once cancellation begins, joining is synchronous: no success can precede it.
func (r *codexRunner) CloseObservation(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.closeObservation()
	return nil
}
