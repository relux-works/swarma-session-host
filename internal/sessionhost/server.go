package sessionhost

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

type Request struct {
	Op        ControlOperation `json:"op"`
	Execution string           `json:"execution"`
	Epoch     uint64           `json:"epoch"`
	Notice    Notice           `json:"notice,omitempty"`
	Goal      *GoalControl     `json:"goal,omitempty"`
}

type Response struct {
	Code   string           `json:"code"`
	State  State            `json:"state"`
	Runner *runner.Response `json:"runner,omitempty"`
}

// Serve is a fixture control endpoint. Kernel UID checks do not isolate agents
// sharing a UID; production caller-scoped launch admission is not implemented.
func Serve(ctx context.Context, socket string, host *Host, timeout time.Duration) error {
	if timeout <= 0 || timeout > time.Minute {
		return errors.New("invalid_server_timeout")
	}
	listener, err := localipc.Listen(socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	return ServeListener(ctx, listener, host, timeout)
}

// ServeListener lets the CLI reserve its endpoint before host admission, so an
// occupied control path cannot trigger queued delivery as a startup side effect.
func ServeListener(ctx context.Context, listener *net.UnixListener, host *Host, timeout time.Duration) error {
	if timeout <= 0 || timeout > time.Minute {
		return errors.New("invalid_server_timeout")
	}
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	slots := make(chan struct{}, 8)
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case slots <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(timeout))
			uid, err := localipc.PeerUID(conn)
			if err != nil || uid != uint32(os.Getuid()) {
				return
			}
			var request Request
			if err := localipc.Read(bufio.NewReaderSize(conn, localipc.MaxFrame), &request); err != nil {
				localipc.Write(conn, Response{Code: "invalid_frame"})
				return
			}
			localipc.Write(conn, execute(ctx, timeout, host, request))
		}()
	}
}

func execute(ctx context.Context, timeout time.Duration, host *Host, request Request) Response {
	state := host.Snapshot()
	if request.Op != "status" && (request.Execution != state.Execution || request.Epoch != state.Epoch) {
		return Response{Code: "stale_session", State: state}
	}
	var err error
	var child *runner.Response
	switch request.Op {
	case "status":
	case "enqueue":
		_, err = host.Enqueue(request.Notice)
	case "hold":
		err = host.Fence(false)
	case "release":
		err = host.Fence(true)
	case "stop":
		callCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		result, callErr := host.Stop(callCtx)
		err, child = callErr, &result
		if err == nil && result.Code != "stopped" {
			err = errors.New("stop_unknown")
		}
	case GoalSet, GoalClear:
		err = errors.New("goal_operation_unsupported")
	default:
		err = errors.New("unsupported_operation")
	}
	code := "ok"
	if err != nil {
		// Errors here are closed host codes, never terminal output or input.
		code = err.Error()
		if child != nil {
			code = "stop_unknown"
		}
	}
	return Response{Code: code, State: host.Snapshot(), Runner: child}
}

func Call(ctx context.Context, socket string, request Request) (Response, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return Response{}, errors.New("deadline_required")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return Response{}, errors.New("host_unavailable")
	}
	defer conn.Close()
	uid, err := localipc.PeerUID(conn.(*net.UnixConn))
	if err != nil || uid != uint32(os.Getuid()) {
		return Response{}, errors.New("host_peer_refused")
	}
	conn.SetDeadline(deadline)
	if err := localipc.Write(conn, request); err != nil {
		return Response{}, errors.New("host_request_unknown")
	}
	var response Response
	if err := localipc.Read(bufio.NewReaderSize(conn, localipc.MaxFrame), &response); err != nil {
		return Response{}, errors.New("host_response_unknown")
	}
	return response, nil
}
