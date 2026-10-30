package localipc

import (
	"errors"
	"net"
)

// Linux controller UID authentication is retained. Identify peer proof has not
// been qualified here; compilation does not advertise runtime support.
func PeerIdentity(*net.UnixConn) (uint32, int, error) {
	return 0, 0, errors.New("identify_peer_unsupported")
}
