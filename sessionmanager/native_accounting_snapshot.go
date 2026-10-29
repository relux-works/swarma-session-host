package sessionmanager

// Physical providers do not own canonical accounting. Retain the actual last
// committed field group when a physical snapshot has not itself supplied it;
// raw usage/turn samples are consumed by the real renderer fold, never credited
// here or used to reset a baseline. Native identity cannot silently change.
func snapshotWithCommittedAccounting(record RunnerRecord, physical RunnerSnapshot) (RunnerSnapshot, error) {
	out := copyRunnerSnapshot(physical)
	previous := record.ProviderHost.Snapshot.MuseMSP
	if previous == nil || previous.NativeAccounting == nil || out.MuseMSP == nil {
		return out, nil
	}
	if previous.SessionID != out.MuseMSP.SessionID {
		return RunnerSnapshot{}, ErrLegacyViewStale
	}
	if out.MuseMSP.NativeAccounting == nil {
		group := copyLegacyMuseNativeAccounting(*previous.NativeAccounting)
		out.MuseMSP.NativeAccounting = &group
		out.MuseMSP.TokenUsageTotal = group.TokenUsageTotal
		out.MuseMSP.ExternalPause = group.ExternalPause
		out.MuseMSP.ActiveTurnID = group.ActiveTurnID
	}
	return out, nil
}
