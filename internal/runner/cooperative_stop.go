package runner

import (
	"syscall"
	"time"
)

func waitStopPhase(done <-chan struct{}, budget time.Duration) bool {
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// Even "stopped" proves only the original direct child's completion. The
// caller must separately qualify its original wrapper/provider/native chain.
// Each of the cooperative, forced-reap and output-drain waits uses at most one
// explicitly configured IOTimeout; no retry resets these phase budgets.
func (s *Server) stopCooperatively(deliver childSignaler) string {
	s.stdin.Close()
	_, signalErr := s.signalOriginalChild(syscall.SIGTERM, deliver)
	uncertain := signalErr != nil
	if !waitStopPhase(s.done, s.cfg.IOTimeout) {
		uncertain = true
		// Only the still-original unreaped child can receive this cutoff.
		s.signalOriginalChild(syscall.SIGKILL, deliver)
		if !waitStopPhase(s.done, s.cfg.IOTimeout) {
			return "stop_unknown"
		}
	}
	if !waitStopPhase(s.outputDone, s.cfg.IOTimeout) {
		uncertain = true
		s.stdout.Close()
		s.mu.Lock()
		s.blocked, s.overflow = true, true
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if uncertain || !s.exited || s.waitUnknown {
		return "stop_unknown"
	}
	return "stopped"
}
