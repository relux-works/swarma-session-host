package sessionmanager

import (
	"bufio"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// builderGatewaySocketEnv and builderGatewayTokenEnv are how the stdio shim
	// learns where to connect and how to authenticate. They are passed through the
	// provider's MCP server definition rather than inherited from the manager's
	// environment, so a shim spawned by anything else gets neither.

	// builderGatewayCallTimeout bounds one shim call. A gateway call is a local
	// filesystem operation; anything slower than this is a hung mount or a wedged
	// manager, and a builder blocked forever on it produces neither context nor
	// diagnosis.
	builderGatewayCallTimeout = 2 * time.Minute

	// builderGatewayMaxRequestBytes bounds one request line.
	builderGatewayMaxRequestBytes = 1 << 20
)

// BuilderGatewayService publishes one gateway on a manager-owned Unix socket.
//
// The socket is the reason this design works identically on both providers. If
// the gateway were an MCP server the provider spawned as an ordinary child, it
// would inherit whatever filesystem policy the provider imposes on its children,
// and the same deny-everything policy that makes the trace complete would also
// stop the gateway from reading. Putting the reads in the manager and giving the
// provider a socket client removes that conflict: the child needs no filesystem
// capability at all, so both adapters can deny it everything.
type BuilderGatewayService struct {
	gateway    *BuilderEvidenceGateway
	listener   net.Listener
	socketPath string
	token      string

	mu      sync.Mutex
	stopped bool
	done    chan struct{}
}

// StartBuilderGatewayService binds the socket and begins serving.
func StartBuilderGatewayService(
	gateway *BuilderEvidenceGateway,
	socketPath string,
) (*BuilderGatewayService, error) {
	if gateway == nil {
		return nil, fmt.Errorf("a repository-evidence gateway is required")
	}
	socketPath = strings.TrimSpace(socketPath)
	if !filepath.IsAbs(socketPath) {
		return nil, fmt.Errorf(
			"context-builder gateway socket path must be absolute: %q", socketPath,
		)
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, err
	}
	// A stale socket from a crashed run would otherwise make bind fail and take
	// the whole launch down. Removing it is safe: the path is manager-private and
	// per-context.
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	token, err := newBuilderGatewayToken()
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	service := &BuilderGatewayService{
		gateway:    gateway,
		listener:   listener,
		socketPath: socketPath,
		token:      token,
		done:       make(chan struct{}),
	}
	go service.serve()
	return service, nil
}

func newBuilderGatewayToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating context-builder gateway token: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// Endpoint describes this service to a provider launch composition.
func (s *BuilderGatewayService) Endpoint(command string, args []string) BuilderGatewayEndpoint {
	return BuilderGatewayEndpoint{
		Command:    command,
		Args:       append([]string(nil), args...),
		SocketPath: s.socketPath,
		Token:      s.token,
	}
}

// SocketPath is the path a launch boundary must permit the builder to reach.
func (s *BuilderGatewayService) SocketPath() string { return s.socketPath }

func (s *BuilderGatewayService) serve() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			stopped := s.stopped
			s.mu.Unlock()
			if stopped {
				return
			}
			// A transient accept error must not silently end the gateway: a builder
			// whose gateway stopped answering would look like a builder that read
			// nothing, and that is a false coverage claim rather than a failure.
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		go s.handle(conn)
	}
}

func (s *BuilderGatewayService) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(builderGatewayCallTimeout))
	reader := bufio.NewReaderSize(conn, 64*1024)
	encoder := json.NewEncoder(conn)
	for {
		line, err := readBoundedLine(reader, builderGatewayMaxRequestBytes)
		if err != nil {
			return
		}
		if strings.TrimSpace(string(line)) == "" {
			continue
		}
		var wire builderGatewayWireRequest
		if err := json.Unmarshal(line, &wire); err != nil {
			_ = encoder.Encode(builderGatewayWireResponse{
				Error: "the repository-evidence gateway received an undecodable request",
			})
			return
		}
		// Constant-time comparison: the token is a bearer credential for writing
		// into a run's trace, and a timing oracle on it would let a local process
		// forge access evidence.
		if subtle.ConstantTimeCompare([]byte(wire.Token), []byte(s.token)) != 1 {
			_ = encoder.Encode(builderGatewayWireResponse{
				Error: "the repository-evidence gateway refused an unauthenticated caller",
			})
			return
		}
		if control := strings.TrimSpace(wire.Control); control != "" {
			s.gateway.noteShimControl(control, wire.Calls, wire.Unknown)
			if err := encoder.Encode(builderGatewayWireResponse{OK: true}); err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(builderGatewayCallTimeout))
			continue
		}
		response, err := s.gateway.Serve(BuilderGatewayRequest{
			Operation:       wire.Operation,
			ProviderEventID: wire.ProviderEventID,
			Root:            wire.Root,
			Path:            wire.Path,
			PathSpec:        wire.PathSpec,
			Offset:          wire.Offset,
			Length:          wire.Length,
			Query:           wire.Query,
		})
		if err != nil {
			_ = encoder.Encode(builderGatewayWireResponse{Error: err.Error()})
			continue
		}
		if err := encoder.Encode(builderGatewayWireResponse{
			OK:       true,
			Response: &response,
		}); err != nil {
			return
		}
		_ = conn.SetDeadline(time.Now().Add(builderGatewayCallTimeout))
	}
}

// readBoundedLine reads one newline-delimited record without letting a caller
// exhaust manager memory by never sending a newline.
func readBoundedLine(reader *bufio.Reader, limit int) ([]byte, error) {
	var buffer []byte
	for {
		chunk, isPrefix, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, chunk...)
		if len(buffer) > limit {
			return nil, fmt.Errorf("gateway request exceeds %d bytes", limit)
		}
		if !isPrefix {
			return buffer, nil
		}
	}
}

// Stop closes the socket. It does not attest the trace: closing the listener is
// a transport concern, and the attestation belongs to whoever owns the gateway's
// lifecycle.
func (s *BuilderGatewayService) Stop() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	s.mu.Unlock()
	err := s.listener.Close()
	<-s.done
	if removeErr := os.Remove(s.socketPath); removeErr != nil &&
		!errors.Is(removeErr, os.ErrNotExist) {
		err = errors.Join(err, removeErr)
	}
	return err
}

type builderGatewayWireRequest struct {
	Token string `json:"token"`
	// Control carries shim protocol evidence rather than a repository access.
	// "hello" says the configured gateway actually started; "unknown" is emitted
	// immediately for every frame the shim could not classify. Provider-native
	// lifecycle and call counts come from the adapter, not this transport.
	//
	// The pair is what makes the trace's completeness checkable from something
	// other than the trace itself. The gateway knows what it recorded; only the
	// shim knows what the provider asked for. Their disagreement is a call that
	// happened and was never captured, and their absence is a provider that never
	// reached the gateway or died before finishing.
	Control   string `json:"control,omitempty"`
	Calls     int    `json:"calls,omitempty"`
	Unknown   int    `json:"unknown,omitempty"`
	Operation string `json:"operation,omitempty"`

	ProviderEventID string `json:"provider_event_id"`
	Root            string `json:"root,omitempty"`
	Path            string `json:"path,omitempty"`
	PathSpec        string `json:"path_spec,omitempty"`
	Offset          int64  `json:"offset,omitempty"`
	Length          int64  `json:"length,omitempty"`
	Query           string `json:"query,omitempty"`
}

type builderGatewayWireResponse struct {
	OK       bool                    `json:"ok"`
	Error    string                  `json:"error,omitempty"`
	Response *BuilderGatewayResponse `json:"response,omitempty"`
}

// BuilderGatewayClient is the shim's side of the socket.
//
// It holds no policy. Every decision — normalization, allowlist, denial reason,
// trace event — happens in the manager, so a compromised or replaced shim can
// misreport what it was told but cannot obtain a read the manager would not have
// served or suppress the event for one it did.
//
// The connection is held open for the shim's whole lifetime rather than dialled
// per call, because its presence is itself evidence: an open connection with no
// closing frame is a provider that disconnected mid-run, and that is one of the
// conditions that makes the provisional context unpublishable.
type BuilderGatewayClient struct {
	socketPath string
	token      string

	mu    sync.Mutex
	conn  net.Conn
	dec   *json.Decoder
	calls int
}

// NewBuilderGatewayClientFromEnv builds a client from the environment the
// provider's MCP server definition supplied.
func NewBuilderGatewayClientFromEnv() (*BuilderGatewayClient, error) {
	socketPath := strings.TrimSpace(os.Getenv(builderGatewaySocketEnv))
	token := strings.TrimSpace(os.Getenv(builderGatewayTokenEnv))
	if socketPath == "" || token == "" {
		return nil, fmt.Errorf(
			"the repository-evidence gateway shim requires %s and %s",
			builderGatewaySocketEnv, builderGatewayTokenEnv,
		)
	}
	return &BuilderGatewayClient{socketPath: socketPath, token: token}, nil
}

// NewBuilderGatewayClient is the explicit constructor used by tests and by
// callers that already hold an endpoint.
func NewBuilderGatewayClient(socketPath, token string) *BuilderGatewayClient {
	return &BuilderGatewayClient{socketPath: socketPath, token: token}
}

// connectLocked opens the shim's single connection and announces it.
func (c *BuilderGatewayClient) connectLocked() error {
	if c.conn != nil {
		return nil
	}
	conn, err := net.DialTimeout("unix", c.socketPath, 10*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	c.dec = json.NewDecoder(conn)
	if _, err := c.exchangeLocked(builderGatewayWireRequest{
		Token:   c.token,
		Control: "hello",
	}); err != nil {
		_ = conn.Close()
		c.conn = nil
		c.dec = nil
		return err
	}
	return nil
}

func (c *BuilderGatewayClient) exchangeLocked(
	request builderGatewayWireRequest,
) (builderGatewayWireResponse, error) {
	_ = c.conn.SetDeadline(time.Now().Add(builderGatewayCallTimeout))
	payload, err := json.Marshal(request)
	if err != nil {
		return builderGatewayWireResponse{}, err
	}
	if _, err := c.conn.Write(append(payload, '\n')); err != nil {
		return builderGatewayWireResponse{}, err
	}
	var wire builderGatewayWireResponse
	if err := c.dec.Decode(&wire); err != nil {
		return builderGatewayWireResponse{}, err
	}
	if !wire.OK {
		message := strings.TrimSpace(wire.Error)
		if message == "" {
			message = "the repository-evidence gateway returned no response"
		}
		return builderGatewayWireResponse{}, errors.New(message)
	}
	return wire, nil
}

// Connect opens the connection eagerly so the manager learns the provider
// started the gateway even if the run then makes no tool calls at all.
//
// Without it, "the builder read nothing" and "the builder never had a read
// surface" would arrive at the manager as the same silence, and only one of them
// describes a base worth publishing.
func (c *BuilderGatewayClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectLocked()
}

// Call performs one gateway operation.
//
// The call is counted whether or not the manager answers it. A call that the
// gateway failed to record still happened from the provider's point of view, and
// the count is what surfaces that disagreement when the trace is reconciled.
func (c *BuilderGatewayClient) Call(
	request BuilderGatewayRequest,
) (BuilderGatewayResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connectLocked(); err != nil {
		return BuilderGatewayResponse{}, err
	}
	c.calls++
	wire, err := c.exchangeLocked(builderGatewayWireRequest{
		Token:           c.token,
		Operation:       request.Operation,
		ProviderEventID: request.ProviderEventID,
		Root:            request.Root,
		Path:            request.Path,
		PathSpec:        request.PathSpec,
		Offset:          request.Offset,
		Length:          request.Length,
		Query:           request.Query,
	})
	if err != nil {
		return BuilderGatewayResponse{}, err
	}
	if wire.Response == nil {
		return BuilderGatewayResponse{}, errors.New(
			"the repository-evidence gateway returned no response",
		)
	}
	return *wire.Response, nil
}

// ObserveUnknown streams one unclassifiable MCP frame to the manager before a
// provider disconnect can lose the shim's in-memory tally.
func (c *BuilderGatewayClient) ObserveUnknown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connectLocked(); err != nil {
		return err
	}
	_, err := c.exchangeLocked(builderGatewayWireRequest{
		Token:   c.token,
		Control: "unknown",
		Unknown: 1,
	})
	return err
}

// Disconnect closes the shim transport without claiming that the native
// provider turn ended cleanly. EOF can be either normal shutdown or a crash.
func (c *BuilderGatewayClient) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.dec = nil
	return err
}

// Close reports the shim's own tally and hangs up.
//
// Skipping it is not a clean shutdown: the manager reads a connection that ended
// without this frame as a provider that disconnected mid-run, which leaves the
// tail of the tool stream unobserved and the context unpublishable.
func (c *BuilderGatewayClient) Close(unknownShapes int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	_, err := c.exchangeLocked(builderGatewayWireRequest{
		Token:   c.token,
		Control: "close",
		Calls:   c.calls,
		Unknown: unknownShapes,
	})
	closeErr := c.conn.Close()
	c.conn = nil
	c.dec = nil
	if err != nil {
		return err
	}
	return closeErr
}
