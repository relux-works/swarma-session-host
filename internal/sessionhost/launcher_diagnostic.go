package sessionhost

import "github.com/relux-works/swarma-session-host/internal/runner"

const launcherEnvironmentDiagnostic = runner.LauncherEnvironmentDiagnostic

// launcherDiagnostic is a bounded discard sink. It retains no raw stderr,
// including native diagnostics after exec. Only the exact fixed F1 label is
// recognized; all other bytes collapse to a fixed redaction label. This sink
// is also the fixed classifier used by the runner's separate stderr collector.
type launcherDiagnostic struct{ *runner.LauncherDiagnostic }

func newLauncherDiagnostic() *launcherDiagnostic {
	return &launcherDiagnostic{runner.NewLauncherDiagnostic()}
}

type launcherExitObservation = runner.LauncherExitObservation

// A native process can emit the same code or exit status. This observation
// never distinguishes pre-exec from post-exec and cannot establish admission.
func (d *launcherDiagnostic) observe(status int) launcherExitObservation {
	return d.Observe(status)
}
