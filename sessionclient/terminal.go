package sessionclient

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const terminalStreamPreamble = "TBSM-TERMINAL-STREAM-V1\n"

// TerminalConn is one attached interactive terminal stream. Reads deliver
// provider output; writes deliver client keystrokes.
type TerminalConn struct {
	conn   net.Conn
	reader io.Reader
}

func (t *TerminalConn) Read(p []byte) (int, error)  { return t.reader.Read(p) }
func (t *TerminalConn) Write(p []byte) (int, error) { return t.conn.Write(p) }
func (t *TerminalConn) Close() error                { return t.conn.Close() }

// SetReadDeadline bounds pending and future socket reads, including reads through
// the buffered HTTP reader. A zero deadline restores unbounded interactive reads.
// Callers must serialize deadline ownership with reads; this does not close the
// terminal or stop its provider.
func (t *TerminalConn) SetReadDeadline(deadline time.Time) error {
	return t.conn.SetReadDeadline(deadline)
}

// OpenTerminal upgrades one dedicated control connection into the raw
// bidirectional terminal byte stream for an existing attachment.
func (c *Client) OpenTerminal(
	ctx context.Context,
	sessionID, attachmentID string,
	rows, cols uint16,
) (*TerminalConn, error) {
	if c == nil || strings.TrimSpace(c.SocketPath) == "" {
		return nil, ErrManagerNotRunning
	}
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("dialing session manager terminal socket: %w", err)
	}
	body, err := json.Marshal(map[string]any{
		"attachment_id": attachmentID,
		"rows":          rows,
		"cols":          cols,
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://session-manager/v1/sessions/"+sessionID+"/terminal",
		strings.NewReader(string(body)),
	)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.Token)
	request.Header.Set("Content-Type", "application/json")
	if err := request.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("sending terminal request: %w", err)
	}

	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("reading terminal response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		_ = response.Body.Close()
		_ = conn.Close()
		return nil, DecodeControlError(response.StatusCode, data)
	}
	// The stream preamble marks the transition from HTTP framing to
	// raw terminal bytes; everything after it is PTY output.
	preamble := make([]byte, len(terminalStreamPreamble))
	if _, err := io.ReadFull(reader, preamble); err != nil || string(preamble) != terminalStreamPreamble {
		_ = conn.Close()
		return nil, fmt.Errorf("terminal stream preamble mismatch")
	}
	return &TerminalConn{conn: conn, reader: reader}, nil
}

// ResizeTerminal reports the client terminal geometry for one attachment.
func (c *Client) ResizeTerminal(
	ctx context.Context,
	sessionID, attachmentID string,
	rows, cols uint16,
) error {
	return c.DoRequest(
		ctx,
		http.MethodPost,
		"/v1/sessions/"+sessionID+"/resize",
		map[string]any{"attachment_id": attachmentID, "rows": rows, "cols": cols},
		nil,
		true,
	)
}
