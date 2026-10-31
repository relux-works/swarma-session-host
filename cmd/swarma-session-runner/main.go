package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

func main() {
	var cfg runner.Config
	flag.StringVar(&cfg.Socket, "socket", "", "absolute private runner socket")
	flag.StringVar(&cfg.Execution, "execution", "", "explicit fixture execution identifier")
	flag.DurationVar(&cfg.IOTimeout, "io-timeout", 0, "explicit bounded I/O timeout")
	flag.StringVar(&cfg.StopSignal, "stop-signal", "none", "owned-child stop signal: none or term; stopped proves only direct-child completion")
	flag.Parse()
	if flag.NArg() != 0 || localipc.Decode(os.Stdin, &cfg.Plan) != nil {
		fmt.Fprintln(os.Stderr, "invalid_fixture_plan")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	s, err := runner.Start(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fixture_runner_start_failed")
		os.Exit(1)
	}
	defer s.Close()
	// Metadata only. Child argv, environment and output are never printed.
	json.NewEncoder(os.Stdout).Encode(map[string]any{"code": "runner_ready", "runner_pid": os.Getpid(), "child_pid": s.ChildPID(), "execution": cfg.Execution})
	<-ctx.Done()
	if err := s.Close(); err != nil {
		json.NewEncoder(os.Stderr).Encode(map[string]any{"code": "runner_stop_unknown", "child_pid": s.ChildPID()})
		os.Exit(1)
	}
}
