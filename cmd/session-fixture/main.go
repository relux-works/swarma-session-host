package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/relux-works/swarma-session-host/internal/fixtureengine"
)

func main() {
	protocol := flag.String("protocol", "", "explicit synthetic protocol: json or line")
	flag.Parse()
	if flag.NArg() != 0 || fixtureengine.Run(*protocol, os.Stdin, os.Stdout) != nil {
		fmt.Fprintln(os.Stderr, "fixture_refused")
		os.Exit(2)
	}
}
