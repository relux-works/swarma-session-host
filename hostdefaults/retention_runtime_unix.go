//go:build unix

package hostdefaults

import (
	"context"

	"github.com/relux-works/swarma-session-host/sessioncontext"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// ConfiguredContextRetentionSweep is the actual command-side sequence: resolve
// the current validated file, then pass the WHOLE settings value to the real
// service. It does not rewrite explicit HTTP requests or merge policy fields.
func (s *Standalone) ConfiguredContextRetentionSweep(ctx context.Context, request sessioncontext.ContextRetentionSweepRequest) (sessioncontext.ContextRetentionSweepReport, error) {
	if ctx == nil {
		return sessioncontext.ContextRetentionSweepReport{}, core.ErrInvalidInput
	}
	if s == nil || s.Contexts == nil || s.life.Err() != nil {
		return sessioncontext.ContextRetentionSweepReport{}, core.ErrHostClosed
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.life, cancel)
	defer cancel()
	defer stop()
	settings, err := s.Contexts.ContextRetentionSettings(call)
	if err != nil {
		return sessioncontext.ContextRetentionSweepReport{}, err
	}
	request.Settings = settings
	return s.Contexts.SweepContextRetention(call, request)
}
