//go:build linux && !amd64

package localipc

import (
	"errors"
	"net"
)

// The raw length-visible peer adapter is qualified only for Linux/amd64.
// Other ABIs fail closed; the legacy controller PeerUID path is unchanged.
func PeerIdentity(*net.UnixConn) (uint32, int, error) {
	return 0, 0, errors.New("unsupported_platform")
}
