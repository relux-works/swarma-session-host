package runner

import "slices"

// RetainsController observes this actual server's live controller generation.
// It is local resource evidence, never execution-policy or member admission.
func (s *Server) RetainsController(execution, instance string, epoch uint64, plan Plan) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ownsChildLocked() && !s.isClosedLocked() && !s.exited && !s.waitUnknown && !s.outputEnded && !s.overflow && !s.blocked && s.owner != nil && s.cfg.Execution == execution && s.instance == instance && s.epoch == epoch && epoch != 0 &&
		s.cfg.Plan.Binary == plan.Binary && s.cfg.Plan.Cwd == plan.Cwd &&
		slices.Equal(s.cfg.Plan.Args, plan.Args) && slices.Equal(s.cfg.Plan.Env, plan.Env)
}
