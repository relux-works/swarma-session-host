package sessiongoal

import core "github.com/relux-works/swarma-session-host/sessionmanager"

// RenderProviderGoal exposes the existing provider renderer to an actual
// revision delivery factory, including the exact Codex directive for a combined
// owner turn. Rendering supplies no authority or acknowledgement. PTY Text
// reflection is provider-owned and does not select the native Muse goal branch.
func RenderProviderGoal(r Revision, provider, phase string, clear bool, current *core.NativeGoalObservation) (core.RenderedInput, core.AckExpectation, error) {
	return render(r, provider, phase, clear, current)
}
