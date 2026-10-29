package sessionmanager

import "context"

// CapturedOwnerRun is the actual attachment admission and committed physical
// cut. It is supplied only by SessionHost, never decoded from a control body.
// Input retains per-physical-write epoch admission; no fence spans native Await.
// Committed and Record are owned copies, not a second record writer.
type CapturedOwnerRun struct {
	Lease     ControllerLease
	Input     HostInputWriter
	Record    RunnerRecord
	Committed LegacyRecordInput
	Native    RunnerHost
}

// CapturedOwnerRunner selects the real provider renderer for precisely Lease.
// Implementations validate the committed/native identity and pending generation
// before effects and use Input for every submission. Calling a legacy provider
// RunOwner directly is not an implementation of this captured contract.
type CapturedOwnerRunner interface {
	RunCapturedOwner(context.Context, CapturedOwnerRun, ProviderRunRequest) (*ProviderRunResult, error)
}
