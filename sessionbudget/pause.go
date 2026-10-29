package sessionbudget

import (
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"strconv"
)

func makePauseInput(lease core.ControllerLease, serial, sequence uint64) core.InputSubmission {
	id := "budget-pause/" + strconv.FormatUint(uint64(lease.Epoch), 10) + "/" + strconv.FormatUint(serial, 10)
	return core.InputSubmission{Lease: lease, ID: id, Expected: core.AckExpectation{Kind: "provider-ack", Correlation: id}, Payload: core.RenderedInput{MuseGoal: &core.MuseGoalCommand{CommandID: id, Pause: &core.MuseGoalPause{Sequence: sequence}}}}
}
