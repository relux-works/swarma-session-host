package musepty

import (
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

// The runtime projects its existing snapshot into the typed host record.
// Values are copied so callers cannot mutate live recovery/provenance state.
func museSnapshotToCore(s musePTYSnapshot) sm.MusePTYSnapshot {
	out := sm.MusePTYSnapshot{Contract: s.Contract, SessionID: s.SessionID, LogPath: s.LogPath, PID: s.PID, VersionFallback: s.VersionFallback, VersionFallbackFrom: s.VersionFallbackFrom}
	if w := s.VersionFallbackEvent; w != nil {
		out.VersionFallbackEvent = &sm.MusePTYFallbackWarning{Code: w.Code, State: w.State, Newest: sm.MusePTYFrozenTuple(w.Newest), FailureAttempt: w.FailureAttempt, FailureStage: w.FailureStage, FailureCode: w.FailureCode, LastGood: sm.MusePTYFrozenTuple(w.LastGood), LastGoodSequence: w.LastGoodSequence, Mode: w.Mode, Consent: w.Consent}
	}
	if w := s.VersionDeferralEvent; w != nil {
		out.VersionDeferralEvent = &sm.MusePTYDeferralWarning{Code: w.Code, State: w.State, Target: sm.MusePTYFrozenTuple(w.Target), Newest: sm.MusePTYFrozenTuple(w.Newest), Attempt: w.Attempt, Mode: w.Mode, Consent: w.Consent}
	}
	return out
}
func ptrMuseCoreSnapshot(s musePTYSnapshot) *sm.MusePTYSnapshot {
	v := museSnapshotToCore(s)
	return &v
}
func museSnapshotFromCore(s sm.MusePTYSnapshot) musePTYSnapshot {
	out := musePTYSnapshot{Contract: s.Contract, SessionID: s.SessionID, LogPath: s.LogPath, PID: s.PID, VersionFallback: s.VersionFallback, VersionFallbackFrom: s.VersionFallbackFrom}
	if w := s.VersionFallbackEvent; w != nil {
		out.VersionFallbackEvent = &musePTYFallbackWarning{Code: w.Code, State: w.State, Newest: musePTYFrozenTupleJSON(w.Newest), FailureAttempt: w.FailureAttempt, FailureStage: w.FailureStage, FailureCode: w.FailureCode, LastGood: musePTYFrozenTupleJSON(w.LastGood), LastGoodSequence: w.LastGoodSequence, Mode: w.Mode, Consent: w.Consent}
	}
	if w := s.VersionDeferralEvent; w != nil {
		out.VersionDeferralEvent = &musePTYDeferralWarning{Code: w.Code, State: w.State, Target: musePTYFrozenTupleJSON(w.Target), Newest: musePTYFrozenTupleJSON(w.Newest), Attempt: w.Attempt, Mode: w.Mode, Consent: w.Consent}
	}
	return out
}
func museRecordSnapshot(record sm.ProviderHostRecord) (musePTYSnapshot, error) {
	s := record.Snapshot
	if record.Provider != "muse" || s.Claude != nil || s.Codex != nil || s.MuseMSP != nil {
		return musePTYSnapshot{}, fmt.Errorf("invalid Muse provider snapshot")
	}
	switch s.Kind {
	case "muse-pty":
		if s.MusePTY != nil && s.MusePTYProvenance == nil {
			return museSnapshotFromCore(*s.MusePTY), nil
		}
	case "muse-pty-provenance":
		if s.MusePTYProvenance != nil && s.MusePTY == nil {
			return museSnapshotFromCore(s.MusePTYProvenance.Base), nil
		}
	}
	return musePTYSnapshot{}, fmt.Errorf("invalid Muse provider snapshot kind or payload")
}
func museTypedResumeSessionID(record sm.ProviderHostRecord) (string, error) {
	prior, err := museRecordSnapshot(record)
	if err != nil {
		return "", err
	}
	if prior.Contract != musePTYHostSnapshotContract || strings.TrimSpace(prior.SessionID) == "" {
		return "", fmt.Errorf("Muse provider snapshot carries no resumable native session")
	}
	return strings.TrimSpace(prior.SessionID), nil
}
func cloneMuseRunnerSnapshot(s sm.RunnerSnapshot) (sm.RunnerSnapshot, error) {
	prior, err := museRecordSnapshot(sm.ProviderHostRecord{Provider: "muse", Snapshot: s})
	if err != nil {
		return sm.RunnerSnapshot{}, err
	}
	if s.Kind == "muse-pty" {
		return sm.RunnerSnapshot{Kind: s.Kind, MusePTY: ptrMuseCoreSnapshot(prior)}, nil
	}
	provenance := *s.MusePTYProvenance
	provenance.Base = museSnapshotToCore(prior)
	return sm.RunnerSnapshot{Kind: s.Kind, MusePTYProvenance: &provenance}, nil
}

func museHasSnapshot(s sm.RunnerSnapshot) bool {
	return s.Kind != "" || s.Claude != nil || s.Codex != nil || s.MuseMSP != nil || s.MusePTY != nil || s.MusePTYProvenance != nil
}
