package sessionmanager

import "fmt"

// StartupCancelledError is a daemon told to die before it ever listened.
type StartupCancelledError struct {
	// Phase is the startup step that was running when the cancellation landed.
	Phase string
	// Signal is nil when the context was cancelled by its parent rather than by
	// a signal. Naming a signal that was never observed would be inventing the
	// evidence this error exists to carry.
	Signal *ObservedShutdownSignal
	Err    error
}

func (e *StartupCancelledError) Error() string {
	if e == nil {
		return ""
	}
	phase := e.Phase
	if phase == "" {
		phase = "startup"
	}
	if e.Signal == nil {
		return fmt.Sprintf(
			"session manager startup was cancelled during %s by its parent context: %v",
			phase,
			e.Err,
		)
	}
	return fmt.Sprintf(
		"session manager startup was cancelled during %s by signal %s (parent pid at signal %d, %s): %v",
		phase,
		e.Signal.Signal,
		e.Signal.ParentPID,
		e.Signal.senderSentence(),
		e.Err,
	)
}

func (e *StartupCancelledError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
