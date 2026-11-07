//go:build t4fixture

// t4-private-fixture is a synthetic executable qualification target. It cannot
// select a production Source, caller, executable, credential or native payload.
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

	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "--protocol-fixture-child" {
		if err := sessionhost.RunPrivateProtocolFixtureChild(os.Args[2]); err != nil {
			os.Exit(3)
		}
		return
	}
	mode := flag.String("mode", "accepted", "explicit synthetic case")
	root := flag.String("root", "", "fresh private fixture directory (preserved if supplied)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected arguments")
		os.Exit(2)
	}
	if *root == "" {
		dir, err := os.MkdirTemp("/tmp", "t4-run-")
		if err != nil {
			os.Exit(2)
		}
		*root = dir
		defer os.RemoveAll(dir)
	}
	parent, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	result, err := sessionhost.RunPrivateCompositionFixture(ctx, *root, *mode)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if json.NewEncoder(os.Stdout).Encode(result) != nil {
		os.Exit(1)
	}
}
