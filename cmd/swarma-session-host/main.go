package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

func fail(code string, exit int) {
	fmt.Fprintln(os.Stderr, code)
	os.Exit(exit)
}

func main() {
	if len(os.Args) < 2 {
		fail("usage: swarma-session-host serve|request [flags]", 2)
	}
	switch os.Args[1] {
	case "serve":
		os.Exit(serve(os.Args[2:]))
	case "request":
		request(os.Args[2:])
	default:
		fail("unknown_command", 2)
	}
}

func serve(args []string) int {
	flags := flag.NewFlagSet("serve", flag.ExitOnError)
	var cfg sessionhost.Config
	var socket, adapter, inputState string
	flags.StringVar(&socket, "socket", "", "absolute private host socket")
	flags.StringVar(&cfg.Socket, "runner", "", "absolute runner socket")
	flags.StringVar(&cfg.StateDir, "state", "", "absolute private state directory")
	flags.StringVar(&cfg.Execution, "execution", "", "fixture execution identifier")
	flags.StringVar(&adapter, "adapter", "", "json-fixture or line-fixture")
	flags.StringVar(&inputState, "input-state", "", "explicit synthetic input state; idle permits delivery")
	flags.StringVar(&cfg.Selection.Vendor, "vendor", "", "opaque fixture selection")
	flags.StringVar(&cfg.Selection.Model, "model", "", "opaque fixture selection")
	flags.StringVar(&cfg.Selection.Reasoning, "reasoning", "", "opaque fixture selection")
	flags.DurationVar(&cfg.IOTimeout, "io-timeout", 0, "explicit bounded I/O timeout")
	flags.Parse(args)
	if flags.NArg() != 0 {
		fail("unexpected_arguments", 2)
	}
	switch adapter {
	case "json-fixture":
		cfg.Adapter = fixtureadapters.JSON{}
	case "line-fixture":
		cfg.Adapter = fixtureadapters.Lines{}
	default:
		fail("unsupported_fixture_adapter", 2)
	}
	cfg.Adapter = fixtureadapters.Gated{Adapter: cfg.Adapter, State: sessionhost.InputState(inputState)}
	listener, err := localipc.Listen(socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, "host_control_unavailable")
		return 1
	}
	defer listener.Close()
	host, err := sessionhost.Open(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}
	defer host.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := sessionhost.ServeListener(ctx, listener, host, cfg.IOTimeout); err != nil {
		fmt.Fprintln(os.Stderr, "host_control_unavailable")
		return 1
	}
	return 0
}

func request(args []string) {
	flags := flag.NewFlagSet("request", flag.ExitOnError)
	var socket string
	var timeout time.Duration
	flags.StringVar(&socket, "socket", "", "absolute host socket")
	flags.DurationVar(&timeout, "timeout", 0, "explicit request deadline")
	flags.Parse(args)
	var req sessionhost.Request
	if flags.NArg() != 0 || timeout <= 0 || timeout > time.Minute || localipc.Decode(os.Stdin, &req) != nil {
		fail("invalid_request", 2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	response, err := sessionhost.Call(ctx, socket, req)
	if err != nil {
		fail(err.Error(), 1)
	}
	json.NewEncoder(os.Stdout).Encode(response)
	if response.Code != "ok" {
		os.Exit(1)
	}
}
