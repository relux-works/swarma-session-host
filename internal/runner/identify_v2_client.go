package runner

import (
	"bufio"
	"context"
	"net"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// identifyV2ForQualification performs one ownership exchange. It deliberately
// grants no native admission: full runner incarnation and executable-image
// proof, plus a fresh post-reply child observation, remain separate obligations.
func identifyV2ForQualification(ctx context.Context, path string, runnerPID int, uid uint32, req IdentifyV2Request) (IdentifyV2Response, error) {
	start := time.Now()
	deadline, ok := ctx.Deadline()
	if !ok {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	if limit := start.Add(identifyBudget); deadline.After(limit) {
		deadline = limit
	}
	if ctx.Err() != nil || !start.Before(deadline) {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	if runnerPID <= 0 || runnerPID > 2147483647 {
		return IdentifyV2Response{}, errIdentifyConflict
	}
	frame, err := encodeIdentifyV2(req)
	if err != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	if _, err := decodeIdentifyV2Request(frame); err != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	if err := identifyEndpoint(path, uid); err != nil {
		return IdentifyV2Response{}, err
	}
	if !time.Now().Before(deadline) || ctx.Err() != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	attempt, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(attempt, "unix", path)
	if err != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	defer c.Close()
	stop := context.AfterFunc(attempt, func() { c.Close() })
	defer stop()
	conn, ok := c.(*net.UnixConn)
	if !ok {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	if conn.SetDeadline(deadline) != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	peerUID, peerPID, err := localipc.PeerIdentity(conn)
	if err := identifyPeer(peerUID, peerPID, err, uid, runnerPID); err != nil {
		return IdentifyV2Response{}, err
	}
	if !time.Now().Before(deadline) || attempt.Err() != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	if n, err := conn.Write(frame); err != nil || n != len(frame) {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	data, err := bufio.NewReaderSize(conn, identifyMaxFrame).ReadSlice('\n')
	if err != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	response, err := decodeIdentifyV2Response(data)
	if err != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	if !time.Now().Before(deadline) || attempt.Err() != nil {
		return IdentifyV2Response{}, errIdentifyUnavailable
	}
	if response.Execution != req.Execution {
		return IdentifyV2Response{}, errIdentifyConflict
	}
	if response.Result == "verified" && (response.Instance == nil || *response.Instance != req.ExpectedInstance) {
		return IdentifyV2Response{}, errIdentifyConflict
	}
	if response.Result == "conflict" {
		return response, errIdentifyConflict
	}
	if response.Result != "verified" {
		return response, errIdentifyUnavailable
	}
	return response, nil
}
