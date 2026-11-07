package runner

import (
	"io"
	"os"
	"sync"
)

// ProtocolChildDiagnostics selects a fixed, bounded stderr classifier. It does
// not select a runtime, authorize execution or identify the exec phase.
const ProtocolChildDiagnostics = "protocol-child-v1"
const LauncherEnvironmentDiagnostic = "protocol_child_environment_refused\n"

// LauncherDiagnostic discards every raw byte, retaining only the state needed
// to recognize one agreed constant line. Zero values safely redact all input.
type LauncherDiagnostic struct {
	mu    sync.Mutex
	seen  int
	match bool
}

func NewLauncherDiagnostic() *LauncherDiagnostic { return &LauncherDiagnostic{match: true} }

func (d *LauncherDiagnostic) Write(data []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, b := range data {
		if d.seen >= len(LauncherEnvironmentDiagnostic) || b != LauncherEnvironmentDiagnostic[d.seen] {
			d.match = false
		}
		if d.seen <= len(LauncherEnvironmentDiagnostic) {
			d.seen++
		}
		if d.seen > len(LauncherEnvironmentDiagnostic) {
			break
		}
	}
	return len(data), nil
}

type LauncherExitObservation struct {
	Status     int
	Diagnostic string
	Phase      string
}

func (d *LauncherDiagnostic) Observe(status int) LauncherExitObservation {
	d.mu.Lock()
	defer d.mu.Unlock()
	r := LauncherExitObservation{Status: status, Phase: "unverified"}
	if d.seen != 0 {
		r.Diagnostic = "protocol_child_diagnostic_redacted"
		if d.match && d.seen == len(LauncherEnvironmentDiagnostic) {
			r.Diagnostic = "protocol_child_environment_refused"
		}
	}
	return r
}

// LauncherProcessObservation describes this Server's actual direct child. Wait
// completion and stderr EOF are independent. Neither status nor the fixed label
// proves that Launcher admission or native initialization succeeded.
type LauncherProcessObservation struct {
	PID                int
	Exited             bool
	WaitUnknown        bool
	DiagnosticEnded    bool
	DiagnosticComplete bool
	Exit               LauncherExitObservation
}

func (s *Server) LauncherDiagnostics() (LauncherProcessObservation, bool) {
	if s == nil || s.diagnostic == nil {
		return LauncherProcessObservation{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	status := -1
	if s.exited && !s.waitUnknown {
		status = s.exitCode
	}
	return LauncherProcessObservation{PID: s.pid, Exited: s.exited, WaitUnknown: s.waitUnknown,
		DiagnosticEnded: s.diagnosticEnded, DiagnosticComplete: s.diagnosticComplete,
		Exit: s.diagnostic.Observe(status)}, true
}

func (s *Server) collectLauncherDiagnostics(r *os.File) {
	defer s.wg.Done()
	defer close(s.diagnosticDone)
	defer r.Close()
	buffer := make([]byte, OutputChunk)
	for {
		n, err := r.Read(buffer)
		if n > 0 {
			_, _ = s.diagnostic.Write(buffer[:n])
		}
		if err != nil {
			s.mu.Lock()
			s.diagnosticEnded, s.diagnosticComplete = true, err == io.EOF
			s.mu.Unlock()
			return
		}
	}
}
