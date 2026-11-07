package sessionhost

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// serveSelectedListener belongs to the registered plugin's daemon, not the
// fixture Host.Serve. The private transport prefixes the method to the exact
// request bytes. It introduces no field to curator-session-manager/1.
func (p *registeredHostPlugin) serveSelectedListener(listener *net.UnixListener) error {
	if p == nil || p.daemon == nil || listener == nil {
		return errHostOwnerUnavailable
	}
	d := p.daemon
	stop := context.AfterFunc(d.ctx, func() { _ = listener.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	slots := make(chan struct{}, 8)
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			if d.ctx.Err() != nil {
				return nil
			}
			return errHostOwnerUnavailable
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
			_ = conn.SetDeadline(time.Now().Add(time.Minute))
			stop := context.AfterFunc(d.ctx, func() { _ = conn.Close() })
			defer stop()
			uid, err := localipc.PeerUID(conn)
			if err != nil || uid != uint32(os.Getuid()) {
				return
			}
			reader := bufio.NewReaderSize(conn, 16)
			method, err := reader.ReadSlice('\n')
			if err != nil || (string(method) != "launch\n" && string(method) != "resume\n" && string(method) != "status\n" && string(method) != "cancel\n" && string(method) != "describe\n") {
				return
			}
			kind := string(method[:len(method)-1])
			p.ServeRequest(d.ctx, selectedManagerProtocol, kind, reader, conn)
		}()
	}
}

// SelectedManagerRPC is the short executable plugin's transport. Installation
// uses the existing launcher registry; this client never creates a registry or
// daemon and never interprets failure as permission to launch locally.
func SelectedManagerRPC(ctx context.Context, protocol, kind, socket string, input io.Reader, output io.Writer) int {
	result, exit := selectedRefusal("session_host_unavailable", nil)
	if ctx == nil || protocol != selectedManagerProtocol || !filepath.IsAbs(socket) {
		return writeSelectedReply(output, result, exit)
	}
	requestSchema := kind + ".schema.json"
	budget := time.Minute
	if kind == "status" || kind == "cancel" || kind == "describe" {
		requestSchema = kind + "-request.schema.json"
		budget = 10 * time.Second
	} else if kind != "launch" && kind != "resume" {
		return writeSelectedReply(output, result, exit)
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	data, err := io.ReadAll(io.LimitReader(input, schema.Limit+1))
	if err != nil || len(data) > schema.Limit || schema.Validate(requestSchema, data) != nil {
		result, exit = selectedRefusal("launch_plan_invalid", nil)
		return writeSelectedReply(output, result, exit)
	}
	var statusRequest struct {
		Session string `json:"session"`
	}
	if kind == "status" {
		// Validation above guarantees exactly one selector. Response shape follows
		// that selector, rather than accepting either successful status envelope.
		_ = json.Unmarshal(data, &statusRequest)
	}
	parent, e1 := os.Lstat(filepath.Dir(socket))
	named, e2 := os.Lstat(socket)
	if e1 != nil || e2 != nil || !parent.IsDir() || parent.Mode().Perm() != 0700 || named.Mode()&os.ModeSocket == 0 || named.Mode().Perm() != 0600 {
		return writeSelectedReply(output, result, exit)
	}
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return writeSelectedReply(output, result, exit)
	}
	defer c.Close()
	conn, ok := c.(*net.UnixConn)
	if !ok {
		return writeSelectedReply(output, result, exit)
	}
	uid, err := localipc.PeerUID(conn)
	if err != nil || uid != uint32(os.Getuid()) {
		return writeSelectedReply(output, result, exit)
	}
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	request := append([]byte(kind+"\n"), data...)
	if n, err := conn.Write(request); err != nil || n != len(request) || conn.CloseWrite() != nil {
		return writeSelectedReply(output, result, exit)
	}
	response, err := io.ReadAll(io.LimitReader(conn, schema.Limit+1))
	if err != nil || len(response) > schema.Limit {
		return writeSelectedReply(output, result, exit)
	}
	if (kind == "launch" || kind == "resume") && validSelectedResult(response) {
		return writeSelectedReply(output, response, 0)
	}
	if kind == "status" && statusRequest.Session != "" && validSelectedSessionObservation(response, statusRequest.Session) {
		return writeSelectedReply(output, response, 0)
	}
	if (kind == "describe" && schema.Validate("describe.schema.json", response) == nil) || ((kind == "cancel" || (kind == "status" && statusRequest.Session == "")) && schema.Validate("outcome.schema.json", response) == nil) {
		return writeSelectedReply(output, response, 0)
	}
	if schema.Validate("refusal.schema.json", response) == nil {
		var refusal struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(response, &refusal) == nil {
			_, exit = selectedRefusal(refusal.Code, nil)
			return writeSelectedReply(output, response, exit)
		}
	}
	return writeSelectedReply(output, result, exit)
}

func writeSelectedReply(output io.Writer, result []byte, exit int) int {
	if len(result)+1 > schema.Limit {
		result, exit = selectedRefusal("session_host_unavailable", nil)
	}
	n, err := output.Write(append(result, '\n'))
	if err != nil || n != len(result)+1 {
		return 1
	}
	return exit
}
