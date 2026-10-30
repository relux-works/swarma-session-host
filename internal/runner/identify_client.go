package runner

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

var errIdentifyUnavailable = errors.New("identify_unavailable")
var errIdentifyConflict = errors.New("identify_conflict")

func identifyEndpoint(path string, uid uint32) error {
	if !filepath.IsAbs(path) || len(path) > 100 || uid != uint32(os.Getuid()) {
		return errIdentifyConflict
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return errIdentifyUnavailable
	}
	socket, err := os.Lstat(path)
	if err != nil {
		return errIdentifyUnavailable
	}
	return identifyEndpointMetadata(parent, socket, uid)
}

func identifyEndpointMetadata(parent, socket os.FileInfo, uid uint32) error {
	pstat, pok := parent.Sys().(*syscall.Stat_t)
	sstat, sok := socket.Sys().(*syscall.Stat_t)
	if !pok || !sok {
		return errIdentifyUnavailable
	}
	if !parent.IsDir() || parent.Mode().Perm()&0077 != 0 || pstat.Uid != uid ||
		socket.Mode()&os.ModeSocket == 0 || socket.Mode().Perm()&0077 != 0 || sstat.Uid != uid {
		return errIdentifyConflict
	}
	return nil
}

// identifyForQualification is an internal qualification client with no command
// caller. A verified result is ONLY an ownership snapshot. It performs no
// admission, full runner-incarnation or post-reply child-live checks. Independent
// verifiers must do those within the same original caller deadline.
func identifyForQualification(ctx context.Context, path string, runnerPID int, uid uint32, req identifyRequest) (identifyResponse, error) {
	start := time.Now()
	deadline, ok := ctx.Deadline()
	if !ok {
		return identifyResponse{}, errIdentifyUnavailable
	}
	if limit := start.Add(identifyBudget); deadline.After(limit) {
		deadline = limit
	}
	if ctx.Err() != nil || !start.Before(deadline) {
		return identifyResponse{}, errIdentifyUnavailable
	}
	if runnerPID <= 0 || runnerPID > 2147483647 {
		return identifyResponse{}, errIdentifyConflict
	}
	frame, err := encodeIdentify(req)
	if err != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	if _, err := decodeIdentifyRequest(frame); err != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	if err := identifyEndpoint(path, uid); err != nil {
		return identifyResponse{}, err
	}
	if !time.Now().Before(deadline) || ctx.Err() != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	attempt, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(attempt, "unix", path)
	if err != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	defer c.Close()
	stop := context.AfterFunc(attempt, func() { c.Close() })
	defer stop()
	conn, ok := c.(*net.UnixConn)
	if !ok {
		return identifyResponse{}, errIdentifyUnavailable
	}
	if conn.SetDeadline(deadline) != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	peerUID, peerPID, err := localipc.PeerIdentity(conn)
	if err := identifyPeer(peerUID, peerPID, err, uid, runnerPID); err != nil {
		return identifyResponse{}, err
	}
	if !time.Now().Before(deadline) || attempt.Err() != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	if n, err := conn.Write(frame); err != nil || n != len(frame) {
		return identifyResponse{}, errIdentifyUnavailable
	}
	data, err := bufio.NewReaderSize(conn, identifyMaxFrame).ReadSlice('\n')
	if err != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	response, err := decodeIdentifyResponse(data)
	if err != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	if !time.Now().Before(deadline) || attempt.Err() != nil {
		return identifyResponse{}, errIdentifyUnavailable
	}
	if response.Execution != req.Execution {
		return identifyResponse{}, errIdentifyConflict
	}
	if response.Result == "verified" && (response.Instance == nil || *response.Instance != req.ExpectedInstance) {
		return identifyResponse{}, errIdentifyConflict
	}
	if response.Result == "conflict" {
		return response, errIdentifyConflict
	}
	if response.Result != "verified" {
		return response, errIdentifyUnavailable
	}
	return response, nil
}

// identifyPeer keeps unavailable evidence distinct from a positive mismatch.
func identifyPeer(peerUID uint32, peerPID int, readErr error, wantUID uint32, wantPID int) error {
	if readErr != nil || peerPID <= 0 || peerPID > 2147483647 {
		return errIdentifyUnavailable
	}
	if peerUID != wantUID || peerPID != wantPID {
		return errIdentifyConflict
	}
	return nil
}
