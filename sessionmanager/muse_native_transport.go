package sessionmanager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// museInputTransport owns native MSP operations only. Canonical accounting
// and turn reservations belong to the fork renderer before HostInputWriter.
// This private transport does not retain a legacy host/config/snapshot.
type museInputTransport struct {
	client            *MSPClient
	sessionID         string
	nativeGoals       bool
	requestedApproval string
	requestedModel    string
	ackTimeout        time.Duration
	lifeCtx           context.Context
	ackDeadlineHook   func(context.Context) (context.Context, context.CancelFunc)
}

func (t *museInputTransport) snapshotSessionID() string { return t.sessionID }
func (t *museInputTransport) goalAckContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if t.ackDeadlineHook != nil {
		return t.ackDeadlineHook(ctx)
	}
	return context.WithTimeout(ctx, t.ackTimeout)
}
func (t *museInputTransport) readResumeGoal(ctx context.Context, preference MSPHistoryPreference) (MSPSessionResumeResult, museGoalRead, uint64, error) {
	reader := &museGoalReader{client: t.client, sessionID: t.sessionID}
	return reader.readResumeGoal(ctx, preference)
}
func (t *museInputTransport) ensureRunning() error {
	if t.lifeCtx != nil && t.lifeCtx.Err() != nil {
		return &MuseHostError{Code: "stopped", Cause: ErrMuseHostStopped}
	}
	if err := t.client.Err(); err != nil {
		return &MuseHostError{Code: "server_crashed", Cause: errors.Join(ErrMuseServerCrashed, err)}
	}
	return nil
}
func (t *museInputTransport) wrapOperationError(operation string, err error) error {
	if t.lifeCtx != nil && t.lifeCtx.Err() != nil && !errors.Is(err, ErrMSPServerExited) {
		return &MuseHostError{Code: "stopped", Cause: errors.Join(ErrMuseHostStopped, err)}
	}
	if errors.Is(err, ErrMSPServerExited) {
		return &MuseHostError{Code: "server_crashed", Cause: fmt.Errorf("%s: %w", operation, errors.Join(ErrMuseServerCrashed, err))}
	}
	var serverError *MSPServerError
	if strings.HasPrefix(operation, "goal/") && errors.As(err, &serverError) {
		return &MuseHostError{Code: "goal_command_rejected", Cause: errors.Join(ErrMuseGoalCommandRejected, fmt.Errorf("%s: %w", operation, err))}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
func (t *museInputTransport) wrapGoalWriteError(operation string, err error) error {
	var serverError *MSPServerError
	if errors.As(err, &serverError) && serverError.Code == -32030 && strings.Contains(strings.ToLower(serverError.Message), "command_id_conflict") {
		return &MuseHostError{Code: "command_id_conflict", Cause: errors.Join(ErrMuseCommandIDConflict, err)}
	}
	return t.wrapOperationError(operation, err)
}
func (t *museInputTransport) sendTurn(ctx context.Context, prompt string, ifBusy MSPIfBusy) (MSPTurnStartResult, error) {
	if strings.TrimSpace(prompt) == "" {
		return MSPTurnStartResult{}, errors.New("Muse session turn text is required")
	}
	if err := t.ensureRunning(); err != nil {
		return MSPTurnStartResult{}, err
	}
	commandID := museUUIDv7()
	result, err := t.client.TurnStart(ctx, MSPTurnStartParams{CommandID: commandID, SessionID: t.sessionID, Input: []MSPTurnInputPart{{Type: "text", Text: prompt}}, IfBusy: ifBusy})
	if err != nil {
		return MSPTurnStartResult{}, t.wrapOperationError("turn/start", err)
	}
	return validateMuseTurnStartAck(result, commandID)
}

func museUUIDv7() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	millis := uint64(time.Now().UnixMilli())
	value[0] = byte(millis >> 40)
	value[1] = byte(millis >> 32)
	value[2] = byte(millis >> 24)
	value[3] = byte(millis >> 16)
	value[4] = byte(millis >> 8)
	value[5] = byte(millis)
	value[6] = (value[6] & 0x0f) | 0x70
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func museGoalOperationRetryable(err error, client *MSPClient) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var transportError *MSPTransportError
	if errors.As(err, &transportError) && client.Err() == nil {
		return true
	}
	return false
}

func validateMuseTurnStartAck(result MSPTurnStartResult, commandID string) (MSPTurnStartResult, error) {
	if result.CommandID != commandID || result.Status != MSPCommandAccepted || strings.TrimSpace(result.TurnID) == "" {
		return MSPTurnStartResult{}, &MuseHostError{Code: "turn_ack", Cause: errors.Join(ErrMuseTurnAckRejected, errors.New("turn/start did not acknowledge a turn identity"))}
	}
	// The pinned TurnStartDisposition enum is schema-open, so schema
	// validation alone admits future values; the host closes the admission
	// to the three pinned dispositions on both transports. The real
	// 1.4.1-R4503.1 server sends only these (captured turn/start results).
	if result.Disposition != MSPTurnStarted && result.Disposition != MSPTurnQueued && result.Disposition != MSPTurnSteered {
		return MSPTurnStartResult{}, &MuseHostError{Code: "turn_ack", Cause: errors.Join(ErrMuseTurnAckRejected, errors.New("turn/start returned an unknown disposition"))}
	}
	// startedNewTurn is true iff the turn started (r36 S5, M35-6): the
	// pinned schema makes disposition authoritative, so an ack whose flag
	// contradicts its disposition is malformed. It is refused before any
	// caller can act on either member: the steer rebind keys on
	// !StartedNewTurn, so admitting {steered, startedNewTurn:true} would
	// silently skip the rebind and strand the old goal's charge.
	if result.StartedNewTurn != (result.Disposition == MSPTurnStarted) {
		return MSPTurnStartResult{}, &MuseHostError{Code: "turn_ack", Cause: errors.Join(ErrMuseTurnAckRejected, errors.New("turn/start contradicted its disposition"))}
	}
	return result, nil
}
