package sessionclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"strings"
	"time"
)

// Config carries transport configuration. HTTPClient can supply an existing
// standard HTTP transport; Dial constructs the authenticated Unix transport.
type Config struct {
	HTTPClient        *http.Client
	Token, SocketPath string
}
type Client Config

func New(config Config) *Client { return (*Client)(&config) }

const defaultControlCallTimeout = 10 * time.Second

func Dial(layout EndpointFiles) (*Client, error) {
	metadata, err := ReadEndpoint(layout)
	if err != nil {
		return nil, err
	}
	tokenBytes, err := ReadPrivateFile(layout.TokenFile)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrManagerNotRunning
	}
	if err != nil {
		return nil, fmt.Errorf("reading manager token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return nil, fmt.Errorf("manager token is empty")
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", metadata.SocketPath)
		},
	}
	return &Client{
		HTTPClient: &http.Client{Transport: transport},
		Token:      token,
		SocketPath: metadata.SocketPath,
	}, nil
}

func (c *Client) Close() {
	if c == nil || c.HTTPClient == nil {
		return
	}
	if transport, ok := c.HTTPClient.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

// DoRequest encodes and decodes ordinary JSON control payloads. Consumers
// with a stricter refusal protocol use RoundTrip and interpret its response.
func (c *Client) DoRequest(ctx context.Context, method, path string, requestBody, target any, applyDefaultTimeout bool) error {
	response, err := c.RoundTrip(ctx, method, path, requestBody, applyDefaultTimeout)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		if response.StatusCode == http.StatusBadRequest && isUnknownFieldRejection(data) {
			return &ProtocolUpgradeError{}
		}
		return DecodeControlError(response.StatusCode, data)
	}
	if target == nil {
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("decoding manager response: %w", err)
	}
	return nil
}
func (c *Client) RoundTrip(
	ctx context.Context,
	method, path string,
	requestBody any,
	applyDefaultTimeout bool,
) (*http.Response, error) {
	var cancelBody context.CancelFunc
	transferred := false
	if c == nil || c.HTTPClient == nil {
		return nil, ErrManagerNotRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); applyDefaultTimeout && !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultControlCallTimeout)
		defer func() {
			if !transferred {
				cancel()
			}
		}()
		cancelBody = cancel
	}
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("encoding manager request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://session-manager"+path, body)
	if err != nil {
		return nil, fmt.Errorf("creating manager request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.Token)
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("manager control request failed: %w", err)
	}
	if cancelBody != nil {
		response.Body = &cancelReadCloser{ReadCloser: response.Body, cancel: cancelBody}
		transferred = true
	}
	return response, nil
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelReadCloser) Close() error { defer b.cancel(); return b.ReadCloser.Close() }
func DecodeControlError(statusCode int, data []byte) error {
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil {
		code := strings.TrimSpace(envelope.Error.Code)
		message := strings.TrimSpace(envelope.Error.Message)
		if code != "" && message != "" {
			return &PublicError{
				Code:       code,
				Message:    message,
				StatusCode: statusCode,
			}
		}
	}
	return fmt.Errorf(
		"manager control request failed (%d): %s",
		statusCode,
		strings.TrimSpace(string(data)),
	)
}

func isUnknownFieldRejection(data []byte) bool {
	message := strings.ToLower(string(data))
	return strings.Contains(message, "unknown field") ||
		strings.Contains(message, "unknown json field")
}

var ErrManagerStartupUnhealthy = errors.New("session manager startup self-check is not healthy")

// Connect checks an existing endpoint; it never launches or replaces a host.
func Connect(ctx context.Context, files EndpointFiles, wrapperVersion string) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultControlCallTimeout)
		defer cancel()
	}
	client, err := Dial(files)
	if err != nil {
		return nil, err
	}
	healthCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	var status ProtocolStatus
	if err = client.DoRequest(healthCtx, "GET", "/v1/status", nil, &status, true); err == nil {
		err = CheckCompatibility(status, wrapperVersion)
	}
	if err == nil && !status.StartupHealthy {
		err = ErrManagerStartupUnhealthy
	}
	if err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}
