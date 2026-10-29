package sessionmanager

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

// The fork prepares submission identity from exact canonical authority before
// crossing the host writer port. Native CommandID and payload stay untouched.
// Stable revision/phase identity admits retries of that same operation; a host
// receipt from another authority cannot be borrowed via Submit's duplicate path.
func goalInputAuthority(envelope *GoalEnvelope, phase string) string {
	if envelope == nil || envelope.Owner == "" || (phase != "apply" && phase != "rollback") {
		return ""
	}
	id, revision := "", 0
	if envelope.Ref != nil {
		id, revision = envelope.Ref.ID, envelope.Ref.Revision
		if id == "" || revision < 1 {
			return ""
		}
	} else if !envelope.Clear {
		return ""
	}
	identity := fmt.Sprintf("goal-input-v1:%q:%q:%d:%t:%q", envelope.Owner, id, revision, envelope.Clear, phase)
	digest := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("goal/%x/", digest)
}
func prepareGoalInput(envelope *GoalEnvelope, phase string, input InputSubmission) (InputSubmission, error) {
	authority := goalInputAuthority(envelope, phase)
	if authority == "" || input.ID == "" {
		return InputSubmission{}, ErrInvalidInput
	}
	if !strings.HasPrefix(input.ID, authority) {
		input.ID = authority + input.ID
	}
	return input, nil
}

// PrepareGoalInput preserves native command identity while binding a submission
// to the caller's admitted revision and transaction phase.
func PrepareGoalInput(envelope *GoalEnvelope, phase string, input InputSubmission) (InputSubmission, error) {
	return prepareGoalInput(envelope, phase, input)
}
