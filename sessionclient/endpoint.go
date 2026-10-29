package sessionclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"
)

// ErrNamespaceMismatch refuses endpoint metadata for another opaque namespace.
var ErrNamespaceMismatch = errors.New("manager endpoint belongs to a different namespace")

// EndpointFiles identifies existing local authority files; it creates no state.
type EndpointFiles struct{ EndpointFile, TokenFile, Namespace string }

// Endpoint preserves the legacy metadata keys. Namespace is an opaque caller
// identity; its historical JSON key carries no authority interpretation here.
type Endpoint struct {
	Protocol        string    `json:"protocol"`
	ProtocolVersion int       `json:"protocol_version"`
	DaemonVersion   string    `json:"daemon_version"`
	Namespace       string    `json:"board_fingerprint"`
	InstanceID      string    `json:"instance_id"`
	SocketPath      string    `json:"socket_path"`
	PID             int       `json:"pid"`
	StartedAt       time.Time `json:"started_at"`
}

func ReadEndpoint(layout EndpointFiles) (Endpoint, error) {
	metadataBytes, err := ReadPrivateFile(layout.EndpointFile)
	if errors.Is(err, fs.ErrNotExist) {
		return Endpoint{}, ErrManagerNotRunning
	}
	if err != nil {
		return Endpoint{}, fmt.Errorf("reading manager endpoint metadata: %w", err)
	}
	var metadata Endpoint
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return Endpoint{}, fmt.Errorf("decoding manager endpoint metadata: %w", err)
	}
	if metadata.Namespace != layout.Namespace {
		return Endpoint{}, ErrNamespaceMismatch
	}
	if strings.TrimSpace(metadata.SocketPath) == "" {
		return Endpoint{}, fmt.Errorf("manager endpoint socket is missing")
	}
	return metadata, nil
}

func ReadPrivateFile(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("%s must be mode 0600", path)
	}
	return os.ReadFile(path)
}
