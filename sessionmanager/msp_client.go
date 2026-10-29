package sessionmanager

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"
)

const mspJSONRPCVersion = "2.0"

// A host-owned input operation joins its physical stream writes before it
// releases a controller fence, even if a call stopped waiting on its context.
// This private transport capability travels only on host-created contexts.
type mspWriteDrain struct{ writes sync.WaitGroup }
type mspWriteDrainKey struct{}

func (d *mspWriteDrain) bind(ctx context.Context) context.Context {
	return context.WithValue(ctx, mspWriteDrainKey{}, d)
}

func (d *mspWriteDrain) wait() { d.writes.Wait() }

const (
	mspBackpressureErrorCode = -32031
	mspAdmissionCapacity     = 4
	mspBackpressureRetries   = 8
)

var (
	// ErrMSPClientClosed is returned when a request is attempted after Close.
	ErrMSPClientClosed = errors.New("MSP client is closed")
	// ErrMSPServerExited distinguishes a server process/stream ending while a
	// request is outstanding from an ordinary JSON-RPC error response.
	ErrMSPServerExited = errors.New("MSP server exited")
)

// MSPServerError is a JSON-RPC error returned by the Muse server.
type MSPServerError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *MSPServerError) Error() string {
	if e == nil {
		return "MSP server error"
	}
	return fmt.Sprintf("MSP server error %d: %s", e.Code, e.Message)
}

// MSPServerExitedError reports EOF or another output-stream failure while the
// MSP host was serving requests.
type MSPServerExitedError struct {
	Cause error
}

func (e *MSPServerExitedError) Error() string {
	if e == nil || e.Cause == nil {
		return ErrMSPServerExited.Error()
	}
	return fmt.Sprintf("%s: %v", ErrMSPServerExited, e.Cause)
}

func (e *MSPServerExitedError) Is(target error) bool {
	return target == ErrMSPServerExited
}

func (e *MSPServerExitedError) Unwrap() error { return e.Cause }

// MSPProtocolError reports a malformed or unsupported frame on the MSP
// connection.
type MSPProtocolError struct {
	Reason string
	Cause  error
}

func (e *MSPProtocolError) Error() string {
	if e == nil {
		return "invalid MSP protocol frame"
	}
	if e.Cause != nil {
		return fmt.Sprintf("invalid MSP protocol frame (%s): %v", e.Reason, e.Cause)
	}
	return fmt.Sprintf("invalid MSP protocol frame: %s", e.Reason)
}

func (e *MSPProtocolError) Unwrap() error { return e.Cause }

// MSPTransportError reports a local stdio write or read failure that was not
// an orderly client close or a server exit.
type MSPTransportError struct {
	Cause error
}

func (e *MSPTransportError) Error() string {
	if e == nil || e.Cause == nil {
		return "MSP transport error"
	}
	return fmt.Sprintf("MSP transport error: %v", e.Cause)
}

func (e *MSPTransportError) Unwrap() error { return e.Cause }

// mspPendingObserver is a nil-by-default test seam for the one pending-map
// site. Acquisition is protected by MSPClient.mu; callbacks run after unlock.
// Fixtures detach under that mutex and join inFlight outside it. A callback
// must not detach itself. This is not evidence of physical writer quiescence.
type mspPendingObserver struct {
	notify   func(uint64, bool)
	inFlight sync.WaitGroup
}

func (o *mspPendingObserver) publish(id uint64, present bool) {
	if o == nil {
		return
	}
	defer o.inFlight.Done()
	o.notify(id, present)
}

type mspCallResult struct {
	result   json.RawMessage
	err      error
	sequence uint64
}

type mspRequestFrame struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type mspNotificationFrame struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type mspInboundFrame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   *MSPServerError `json:"error"`
}

// MSPServerRequest is a server-initiated request. The client sends the
// presentation receipt before publishing it to Notifications so host-side
// resolution cannot hold up the protocol reader.
type MSPServerRequest struct {
	ID     json.RawMessage
	Name   string
	Params json.RawMessage
}

func (r MSPServerRequest) MSPMethod() string { return r.Name }

// MSPClient owns one MSP stdio connection. It permits concurrent in-flight
// calls on that single connection and correlates responses by their JSON-RPC
// id. Consumers must drain Notifications; a full buffer applies backpressure
// instead of silently dropping protocol events.
type MSPClient struct {
	reader  io.ReadCloser
	writer  io.WriteCloser
	writeMu sync.Mutex
	mu      sync.Mutex

	nextID             uint64
	pending            map[uint64]chan mspCallResult
	pendingObserver    *mspPendingObserver
	observationResumes map[uint64]*museObservationRing
	observations       *museObservationRing
	notifications      chan MSPNotification
	stop               chan struct{}
	done               chan struct{}
	dispatchSeq        uint64
	goalEventSeq       uint64
	completedTurns     map[mspTurnKey]struct{}

	terminal error
	closed   bool
	stopped  bool
	strict   bool
}

// NewMSPClient starts the response and notification reader for one stdio
// connection. The client takes ownership of reader and writer; Close closes
// both streams so the host observes stdin EOF.
func NewMSPClient(reader io.ReadCloser, writer io.WriteCloser) *MSPClient {
	return newMSPClient(reader, writer, false)
}

// NewMSPClientStrict starts the same stdio connection with strict pinned-
// schema validation of every typed notification and every consumed method
// result. A notification the strict client cannot fully validate
// terminates the transport with a typed protocol error instead of
// delivering zero values. Item and gap families still arrive raw; their
// host consumers validate them. A result that fails validation is a typed
// protocol error on its call, never success.
func NewMSPClientStrict(reader io.ReadCloser, writer io.WriteCloser) *MSPClient {
	return newMSPClient(reader, writer, true)
}

func newMSPClient(reader io.ReadCloser, writer io.WriteCloser, strict bool) *MSPClient {
	client := &MSPClient{
		reader:        reader,
		writer:        writer,
		pending:       make(map[uint64]chan mspCallResult),
		notifications: make(chan MSPNotification, 128),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
		strict:        strict,
	}
	go client.readLoop()
	return client
}

// Notifications returns the ordered stream of typed server notifications.
// Unknown server notifications are delivered as MSPRawNotification values.
func (c *MSPClient) Notifications() <-chan MSPNotification { return c.notifications }

// Done closes when the stdio reader has stopped.
func (c *MSPClient) Done() <-chan struct{} { return c.done }

// Err reports why the connection stopped, or nil while it is active.
func (c *MSPClient) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal != nil {
		return c.terminal
	}
	if c.closed {
		return ErrMSPClientClosed
	}
	return nil
}

// LastGoalChangedSequence returns the last inbound frame sequence carrying a
// session/goalChanged notification. It lets a read-after-write host detect a
// goal event that arrived after a resume snapshot response.
func (c *MSPClient) LastGoalChangedSequence() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.goalEventSeq
}

// Close closes both stdio streams, unblocks the reader, and fails any pending
// calls with ErrMSPClientClosed. It is safe to call more than once.
func (c *MSPClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	if !c.stopped {
		c.stopped = true
		close(c.stop)
	}
	waiters := c.pending
	c.pending = make(map[uint64]chan mspCallResult)
	c.observationResumes = nil
	ring := c.observations
	c.mu.Unlock()
	if ring != nil {
		ring.close(ErrMSPClientClosed)
	}

	var closeErr error
	if c.writer != nil {
		closeErr = errors.Join(closeErr, c.writer.Close())
	}
	if c.reader != nil {
		closeErr = errors.Join(closeErr, c.reader.Close())
	}
	for _, waiter := range waiters {
		waiter <- mspCallResult{err: ErrMSPClientClosed}
	}
	return closeErr
}

// Shutdown closes stdin and waits for the host to close stdout. If ctx expires,
// it closes the reader to unblock the local transport and returns ctx.Err().
// Use Close for an immediate local close without waiting for host shutdown.
func (c *MSPClient) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		terminal := c.terminal
		c.mu.Unlock()
		if terminal != nil {
			return terminal
		}
		select {
		case <-c.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	terminal := c.terminal
	c.closed = true
	if !c.stopped {
		c.stopped = true
		close(c.stop)
	}
	waiters := c.pending
	c.pending = make(map[uint64]chan mspCallResult)
	c.observationResumes = nil
	ring := c.observations
	c.mu.Unlock()
	if ring != nil {
		ring.close(ErrMSPClientClosed)
	}
	var closeErr error
	if c.writer != nil {
		closeErr = c.writer.Close()
	}
	for _, waiter := range waiters {
		waiter <- mspCallResult{err: ErrMSPClientClosed}
	}
	if terminal != nil {
		if c.reader != nil {
			closeErr = errors.Join(closeErr, c.reader.Close())
		}
		return errors.Join(terminal, closeErr)
	}
	select {
	case <-c.done:
		if c.reader != nil {
			closeErr = errors.Join(closeErr, c.reader.Close())
		}
		return closeErr
	case <-ctx.Done():
		if c.reader != nil {
			closeErr = errors.Join(closeErr, c.reader.Close())
		}
		<-c.done
		return errors.Join(closeErr, ctx.Err())
	}
}

// Initialize performs the MSP handshake and sends the required client-side
// "initialized" notification after the server accepts initialize.
func (c *MSPClient) Initialize(ctx context.Context, params MSPInitializeParams) (MSPInitializeResult, error) {
	var result MSPInitializeResult
	if err := c.call(ctx, "initialize", params, &result); err != nil {
		return result, err
	}
	if err := c.notify(ctx, "initialized", nil); err != nil {
		return result, err
	}
	return result, nil
}

func (c *MSPClient) SessionStart(ctx context.Context, params MSPSessionStartParams) (MSPSessionStartResult, error) {
	var result MSPSessionStartResult
	err := c.call(ctx, "session/start", params, &result)
	return result, err
}

func (c *MSPClient) SessionResume(ctx context.Context, params MSPSessionResumeParams) (MSPSessionResumeResult, error) {
	var result MSPSessionResumeResult
	err := c.call(ctx, "session/resume", params, &result)
	return result, err
}

func (c *MSPClient) SessionResumeWithSequence(ctx context.Context, params MSPSessionResumeParams) (MSPSessionResumeResult, uint64, error) {
	var result MSPSessionResumeResult
	sequence, err := c.callSequenced(ctx, "session/resume", params, &result)
	return result, sequence, err
}

func (c *MSPClient) GoalSet(ctx context.Context, params MSPGoalSetParams) (MSPGoalCommandResult, error) {
	var result MSPGoalCommandResult
	err := c.call(ctx, "goal/set", params, &result)
	return result, err
}

func (c *MSPClient) GoalEdit(ctx context.Context, params MSPGoalEditParams) (MSPGoalCommandResult, error) {
	var result MSPGoalCommandResult
	err := c.call(ctx, "goal/edit", params, &result)
	return result, err
}

func (c *MSPClient) GoalPause(ctx context.Context, params MSPGoalPauseParams) (MSPGoalCommandResult, error) {
	var result MSPGoalCommandResult
	err := c.call(ctx, "goal/pause", params, &result)
	return result, err
}

func (c *MSPClient) GoalResume(ctx context.Context, params MSPGoalResumeParams) (MSPGoalCommandResult, error) {
	var result MSPGoalCommandResult
	err := c.call(ctx, "goal/resume", params, &result)
	return result, err
}

func (c *MSPClient) GoalClear(ctx context.Context, params MSPGoalClearParams) (MSPGoalCommandResult, error) {
	var result MSPGoalCommandResult
	err := c.call(ctx, "goal/clear", params, &result)
	return result, err
}

func (c *MSPClient) TurnStart(ctx context.Context, params MSPTurnStartParams) (MSPTurnStartResult, error) {
	var result MSPTurnStartResult
	err := c.call(ctx, "turn/start", params, &result)
	return result, err
}

func (c *MSPClient) TurnSteer(ctx context.Context, params MSPTurnSteerParams) (MSPTurnSteerResult, error) {
	var result MSPTurnSteerResult
	err := c.call(ctx, "turn/steer", params, &result)
	return result, err
}

func (c *MSPClient) TurnInterrupt(ctx context.Context, params MSPTurnInterruptParams) (MSPTurnInterruptResult, error) {
	var result MSPTurnInterruptResult
	err := c.call(ctx, "turn/interrupt", params, &result)
	return result, err
}

func (c *MSPClient) call(ctx context.Context, method string, params any, result any) error {
	_, err := c.callSequenced(ctx, method, params, result)
	return err
}

func (c *MSPClient) callSequenced(ctx context.Context, method string, params any, result any) (uint64, error) {
	for attempt := 0; ; attempt++ {
		sequence, err := c.callOnce(ctx, method, params, result)
		if err == nil {
			return sequence, nil
		}
		if attempt >= mspBackpressureRetries || !isMSPBackpressured(err) {
			return 0, err
		}
		timer := time.NewTimer(mspBackpressureDelay(err, attempt))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *MSPClient) callOnce(ctx context.Context, method string, params any, result any) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	c.mu.Lock()
	if err := c.stateErrorLocked(); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	c.nextID++
	if c.nextID == 0 {
		c.nextID++
	}
	id := c.nextID
	c.mu.Unlock()

	frame := mspRequestFrame{JSONRPC: mspJSONRPCVersion, ID: id, Method: method, Params: params}
	encoded, err := json.Marshal(frame)
	if err != nil {
		return 0, fmt.Errorf("encode MSP %s request: %w", method, err)
	}
	waiter := make(chan mspCallResult, 1)
	c.mu.Lock()
	if err := c.stateErrorLocked(); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	observer := c.pendingObserver
	if observer != nil {
		observer.inFlight.Add(1)
	}
	c.pending[id] = waiter
	if ring, ok := ctx.Value(museObservationResumeKey{}).(*museObservationRing); ok && method == "session/resume" {
		if c.observationResumes == nil {
			c.observationResumes = make(map[uint64]*museObservationRing)
		}
		c.observationResumes[id] = ring
	}
	c.mu.Unlock()
	observer.publish(id, true)

	var interruptTarget []MSPTurnInterruptParams
	if method == "turn/interrupt" {
		if target, ok := params.(MSPTurnInterruptParams); ok {
			interruptTarget = append(interruptTarget, target)
		}
	}
	if method == "turn/steer" {
		if target, ok := params.(MSPTurnSteerParams); ok {
			interruptTarget = append(interruptTarget, MSPTurnInterruptParams{SessionID: target.SessionID, TurnID: target.ExpectedTurnID})
		}
	}
	write := func(call context.Context) error {
		err := c.writeFrame(call, encoded, interruptTarget...)
		if err != nil {
			// Cancellation revokes the RPC waiter immediately, while the host
			// still joins this frame's physical drain before releasing admission.
			c.removePending(id)
		}
		return err
	}
	var writeErr error
	if phase, ok := ctx.Value(hostMuseInputPhaseKey{}).(*hostMuseInputPhase); ok {
		writeErr = phase.write(ctx, write)
	} else {
		writeErr = write(ctx)
	}
	if err := writeErr; err != nil {
		c.removePending(id)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if stateErr := c.Err(); stateErr != nil {
			return 0, stateErr
		}
		return 0, err
	}

	var pending <-chan hostMusePendingCall
	var observations <-chan hostMuseObservationPublication
	// This private native wait port keeps the protocol client independent of
	// the host publication implementation and its notification dispatch table.
	scope, _ := ctx.Value(hostMuseWaitScopeKey{}).(interface {
		pendingCalls() <-chan hostMusePendingCall
		nativePublications() <-chan hostMuseObservationPublication
		serve(context.Context, hostMusePendingCall)
		publish(context.Context, hostMuseObservationPublication)
	})
	if scope != nil {
		pending = scope.pendingCalls()
		observations = scope.nativePublications()
	}
	for {
		select {
		case call := <-pending:
			scope.serve(ctx, call)
		case call := <-observations:
			scope.publish(ctx, call)
		case response := <-waiter:
			if response.err != nil {
				return 0, response.err
			}
			trimmedResult := bytes.TrimSpace(response.result)
			if len(trimmedResult) == 0 || bytes.Equal(trimmedResult, []byte("null")) {
				return 0, &MSPProtocolError{Reason: "successful response has no result"}
			}
			if c.strict {
				if err := museStrictDecodeMethodResult(method, response.result); err != nil {
					return 0, err
				}
			}
			if err := json.Unmarshal(response.result, result); err != nil {
				return 0, &MSPProtocolError{Reason: "result does not match method response shape", Cause: err}
			}
			return response.sequence, nil
		case <-ctx.Done():
			c.removePending(id)
			return 0, ctx.Err()
		}
	}
}

func isMSPBackpressured(err error) bool {
	var serverError *MSPServerError
	return errors.As(err, &serverError) && serverError.Code == mspBackpressureErrorCode
}

func mspBackpressureDelay(err error, attempt int) time.Duration {
	capacity := mspAdmissionCapacity
	var serverError *MSPServerError
	if errors.As(err, &serverError) {
		var data struct {
			Capacity int `json:"capacity"`
		}
		if json.Unmarshal(serverError.Data, &data) == nil && data.Capacity > 0 && data.Capacity <= 64 {
			capacity = data.Capacity
		}
	}
	delay := time.Duration(capacity) * 10 * time.Millisecond
	for n := 0; n < attempt && delay < 500*time.Millisecond; n++ {
		delay *= 2
	}
	if delay > 500*time.Millisecond {
		return 500 * time.Millisecond
	}
	return delay
}

func (c *MSPClient) notify(ctx context.Context, method string, params any) error {
	encoded, err := json.Marshal(mspNotificationFrame{JSONRPC: mspJSONRPCVersion, Method: method, Params: params})
	if err != nil {
		return fmt.Errorf("encode MSP %s notification: %w", method, err)
	}
	return c.writeFrame(ctx, encoded)
}

// writeFrame serializes complete frames so concurrent calls cannot interleave
// bytes. The blocking Write runs in a separate goroutine because pipe writes
// have no context support. If the context expires, the caller stops waiting;
// the write remains serialized until it finishes or Close/Shutdown closes the
// stream to interrupt it.
func (c *MSPClient) writeFrame(ctx context.Context, encoded []byte, interruptTarget ...MSPTurnInterruptParams) error {
	writeDone := make(chan error, 1)
	drain, _ := ctx.Value(mspWriteDrainKey{}).(*mspWriteDrain)
	if drain != nil {
		drain.writes.Add(1)
	}
	go func() {
		if drain != nil {
			defer drain.writes.Done()
		}
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		if err := ctx.Err(); err != nil {
			writeDone <- err
			return
		}

		c.mu.Lock()
		stateErr := c.stateErrorLocked()
		c.mu.Unlock()
		if stateErr != nil {
			writeDone <- stateErr
			return
		}
		// Recheck native terminal identity after acquiring the physical stream
		// lock: completion may have arrived while this frame was queued.
		if len(interruptTarget) != 0 && c.museTurnCompleted(interruptTarget[0].SessionID, interruptTarget[0].TurnID) {
			writeDone <- ErrMuseTurnAlreadyCompleted
			return
		}
		if err := writeMSPLine(c.writer, encoded); err != nil {
			transportErr := &MSPTransportError{Cause: err}
			c.terminate(transportErr)
			if stateErr := c.Err(); stateErr != nil {
				writeDone <- stateErr
				return
			}
			writeDone <- transportErr
			return
		}
		writeDone <- nil
	}()

	select {
	case err := <-writeDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *MSPClient) readLoop() {
	defer func() {
		c.mu.Lock()
		ring := c.observations
		c.mu.Unlock()
		if ring != nil {
			ring.close(c.Err())
		}
	}()
	defer close(c.done)
	defer close(c.notifications)
	reader := bufio.NewReader(c.reader)
	for {
		line, err := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) != 0 {
			if frameErr := c.dispatch(line); frameErr != nil {
				c.terminate(frameErr)
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.terminate(&MSPServerExitedError{Cause: err})
			} else {
				c.terminate(&MSPTransportError{Cause: err})
			}
			return
		}
	}
}

func (c *MSPClient) dispatch(line []byte) error {
	var frame mspInboundFrame
	if err := json.Unmarshal(line, &frame); err != nil {
		return &MSPProtocolError{Reason: "frame is not valid JSON", Cause: err}
	}
	if frame.JSONRPC != mspJSONRPCVersion {
		return &MSPProtocolError{Reason: "jsonrpc must be \"2.0\""}
	}
	c.mu.Lock()
	c.dispatchSeq++
	sequence := c.dispatchSeq
	if frame.Method == "session/goalChanged" {
		c.goalEventSeq = sequence
	}
	c.mu.Unlock()
	if frame.Method != "" {
		if len(frame.ID) != 0 {
			if len(frame.Result) != 0 || frame.Error != nil {
				return &MSPProtocolError{Reason: "server request contains response fields"}
			}
			if !validMSPServerRequestID(frame.ID) {
				return &MSPProtocolError{Reason: "server request id must be a string or integer"}
			}
			if err := c.replyToServerRequest(frame); err != nil {
				return err
			}
			if frame.Method == "approval/request" || frame.Method == "userInput/request" {
				request := MSPServerRequest{
					ID: append(json.RawMessage(nil), frame.ID...), Name: frame.Method,
					Params: append(json.RawMessage(nil), frame.Params...),
				}
				c.publishMuseObservation(request)
				select {
				case c.notifications <- request:
					return nil
				case <-c.stop:
					return nil
				}
			}
			return &MSPProtocolError{Reason: "unsupported server-initiated request"}
		}
		if len(frame.Result) != 0 || frame.Error != nil {
			return &MSPProtocolError{Reason: "notification contains response fields"}
		}
		notification, err := c.decodeMSPNotification(frame.Method, frame.Params)
		if err != nil {
			return err
		}
		if terminal, ok := notification.(*MSPTurnCompletedNotification); ok && terminal.SessionID != "" && terminal.TurnID != "" {
			c.mu.Lock()
			if c.completedTurns == nil {
				c.completedTurns = make(map[mspTurnKey]struct{})
			}
			c.completedTurns[mspTurnKey{session: terminal.SessionID, turn: terminal.TurnID}] = struct{}{}
			c.mu.Unlock()
		}
		c.publishMuseObservation(notification)
		select {
		case c.notifications <- notification:
			return nil
		case <-c.stop:
			return nil
		}
	}
	if len(frame.ID) == 0 {
		return &MSPProtocolError{Reason: "response has no id"}
	}
	if (frame.Error == nil) == (len(frame.Result) == 0) {
		return &MSPProtocolError{Reason: "response must contain exactly one of result or error"}
	}
	id, ok := parseMSPResponseID(frame.ID)
	if !ok {
		return &MSPProtocolError{Reason: "response id must be a non-negative integer"}
	}
	response := mspCallResult{result: frame.Result, sequence: sequence}
	if frame.Error != nil {
		response.err = frame.Error
	}
	c.mu.Lock()
	waiter := c.pending[id]
	ring := c.observationResumes[id]
	delete(c.observationResumes, id)
	if waiter != nil {
		delete(c.pending, id)
	}
	c.mu.Unlock()
	if ring != nil && response.err == nil {
		if err := ring.restoreResponse(response.result, c.strict); err != nil {
			ring.close(err)
			response.err = err
		}
	}
	if waiter != nil {
		waiter <- response
	}
	return nil
}

func validMSPServerRequestID(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	switch value.(type) {
	case string, json.Number:
		return true
	default:
		return false
	}
}

func (c *MSPClient) replyToServerRequest(frame mspInboundFrame) error {
	switch frame.Method {
	case "approval/request", "userInput/request":
		// These server requests only need a receipt on the wire. The host
		// resolves them later through approval/listPending and its typed APIs.
	default:
		return &MSPProtocolError{Reason: "unsupported server-initiated request " + frame.Method}
	}
	if c.strict {
		// Validate before receipting or routing: a malformed request is a
		// protocol error, never a trigger the host acts on (r20 rule 1).
		if err := museStrictDecodeServerRequest(frame.Method, frame.Params); err != nil {
			return err
		}
	}
	response := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result,omitempty"`
	}{JSONRPC: mspJSONRPCVersion, ID: append(json.RawMessage(nil), frame.ID...), Result: json.RawMessage(`{}`)}
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("encode MSP server-request receipt: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.writeFrame(ctx, encoded)
}

func (c *MSPClient) decodeMSPNotification(method string, params json.RawMessage) (MSPNotification, error) {
	if c.strict {
		if err := museStrictDecodeClientNotification(method, params); err != nil {
			return nil, &MSPProtocolError{Reason: "notification params failed pinned-schema validation", Cause: err}
		}
	}
	return decodeMSPNotification(method, params)
}

func decodeMSPNotification(method string, params json.RawMessage) (MSPNotification, error) {
	if len(params) == 0 || bytes.Equal(bytes.TrimSpace(params), []byte("null")) {
		// IGNORE rows and truly unknown methods drop without
		// validation, well-formed or not (r31 S2, M30-4): a
		// missing params object on an unconsumed method must not
		// terminate the transport. The frame arrives raw and the
		// dispatch drop never reads its params. Every other row
		// keeps the strict rejection below.
		if !museNotificationRequiresParamsObject(method) {
			return MSPRawNotification{Name: method, Params: append(json.RawMessage(nil), params...)}, nil
		}
		return nil, &MSPProtocolError{Reason: "notification params must be an object"}
	}
	var notification MSPNotification
	switch method {
	case "session/goalChanged":
		notification = &MSPSessionGoalChangedNotification{}
	case "session/statusChanged":
		notification = &MSPSessionStatusChangedNotification{}
	case "session/tokenUsage":
		notification = &MSPSessionTokenUsageNotification{}
	case "turn/started":
		notification = &MSPTurnStartedNotification{}
	case "turn/completed":
		notification = &MSPTurnCompletedNotification{}
	// turn/retracted and turn/retryScheduled are deliberately untyped
	// (r24 S2): they are IGNORE rows the host never consumes, so they
	// arrive raw and drop at dispatch without validation, well-formed
	// or not, exactly like every other IGNORE row.
	case "turn/unqueued":
		notification = &MSPTurnUnqueuedNotification{}
	default:
		return MSPRawNotification{Name: method, Params: append(json.RawMessage(nil), params...)}, nil
	}
	if err := json.Unmarshal(params, notification); err != nil {
		return nil, &MSPProtocolError{Reason: "notification params do not match method shape", Cause: err}
	}
	return notification, nil
}

func parseMSPResponseID(raw json.RawMessage) (uint64, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] < '0' || trimmed[0] > '9' {
		return 0, false
	}
	id, err := strconv.ParseUint(string(trimmed), 10, 64)
	return id, err == nil
}

func writeMSPLine(writer io.Writer, encoded []byte) error {
	line := append(encoded, '\n')
	for len(line) > 0 {
		written, err := writer.Write(line)
		if written > 0 {
			line = line[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (c *MSPClient) removePending(id uint64) {
	c.mu.Lock()
	_, existed := c.pending[id]
	observer := c.pendingObserver
	if existed && observer != nil {
		observer.inFlight.Add(1)
	}
	delete(c.pending, id)
	delete(c.observationResumes, id)
	c.mu.Unlock()
	if existed {
		observer.publish(id, false)
	}
}

func (c *MSPClient) terminate(err error) {
	c.mu.Lock()
	if c.closed || c.terminal != nil {
		c.mu.Unlock()
		return
	}
	c.terminal = err
	if !c.stopped {
		c.stopped = true
		close(c.stop)
	}
	waiters := c.pending
	c.pending = make(map[uint64]chan mspCallResult)
	c.observationResumes = nil
	ring := c.observations
	c.mu.Unlock()
	if ring != nil {
		ring.close(err)
	}
	for _, waiter := range waiters {
		waiter <- mspCallResult{err: err}
	}
}

func (c *MSPClient) stateErrorLocked() error {
	if c.terminal != nil {
		return c.terminal
	}
	if c.closed {
		return ErrMSPClientClosed
	}
	return nil
}

// Native terminal identity is recorded before publishing the ordered event.
// It contains no canonical ledger state and is shared by preflight and the
// final physical-write check. A target from another session cannot alias it.
type mspTurnKey struct{ session, turn string }

func (c *MSPClient) museTurnCompleted(sessionID, turnID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, completed := c.completedTurns[mspTurnKey{session: sessionID, turn: turnID}]
	return completed
}
