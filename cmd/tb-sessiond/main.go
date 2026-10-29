//go:build unix

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/relux-works/swarma-session-host/hostdefaults"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Explicit ldflags retain the established executable's build metadata.
var Version = "dev"
var Commit = "unknown"
var BuildDate = "unknown"

func main() {
	// The original stdio helper is a provider-owned child, not a daemon. Keep
	// ordinary process signal semantics instead of installing the daemon watch.
	if len(os.Args) > 1 && os.Args[1] == "session" {
		if err := runWithInput(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr, nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	ctx, watch, stop := core.WatchShutdownSignals(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, watch); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func defaultStateRoot() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, "Library", "Application Support", "swarma-session-host"), nil
	}
	if root := os.Getenv("XDG_STATE_HOME"); root != "" {
		if !filepath.IsAbs(root) {
			return "", fmt.Errorf("XDG_STATE_HOME must be absolute")
		}
		return filepath.Join(root, "swarma-session-host"), nil
	}
	return filepath.Join(home, ".local", "state", "swarma-session-host"), nil
}

func run(ctx context.Context, args []string, out, diagnostics io.Writer, watch *core.ShutdownSignalWatch) error {
	return runWithInput(ctx, args, os.Stdin, out, diagnostics, watch)
}

func runWithInput(ctx context.Context, args []string, input io.Reader, out, diagnostics io.Writer, watch *core.ShutdownSignalWatch) error {
	if len(args) != 0 && args[0] == "session" {
		if len(args) != 2 || args[1] != "context-gateway-shim" {
			return fmt.Errorf("unexpected session helper arguments")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		client, err := core.NewBuilderGatewayClientFromEnv()
		if err != nil {
			return err
		}
		return core.RunBuilderGatewayShim(client, input, out)
	}
	flags := flag.NewFlagSet("tb-sessiond", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	var stateRoot, namespace, configPath, socketPath string
	var showVersion bool
	flags.StringVar(&stateRoot, "state-root", "", "platform user-state root override")
	flags.StringVar(&namespace, "namespace", "default", "explicit local storage namespace; grants no caller rights")
	flags.StringVar(&configPath, "config", "", "explicit host configuration file")
	flags.StringVar(&socketPath, "socket-path", "", "absolute local control socket override")
	flags.BoolVar(&showVersion, "version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if showVersion {
		_, err := fmt.Fprintf(out, "tb-sessiond %s (commit %s, built %s)\n", Version, Commit, BuildDate)
		return err
	}
	if stateRoot == "" {
		var err error
		stateRoot, err = defaultStateRoot()
		if err != nil {
			return err
		}
	}
	layout, err := hostdefaults.LayoutAt(stateRoot, core.NamespaceKey(namespace))
	if err != nil {
		return err
	}
	if socketPath != "" {
		if !filepath.IsAbs(socketPath) {
			return fmt.Errorf("control socket path must be absolute")
		}
		layout.SocketPath = socketPath
	}
	optional := configPath == ""
	if optional {
		configPath = filepath.Join(stateRoot, "config.json")
	}
	if !filepath.IsAbs(configPath) {
		return fmt.Errorf("host configuration path must be absolute")
	}
	// This executable implements the exact original helper argv above. Select
	// that same concrete artifact here; the runtime library never discovers or
	// assumes a shim from its caller's executable.
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	shimCommand, shimArgs := core.BuilderGatewayShimUsage(executable)
	// The external raising side has not supplied an exact-session registrar,
	// holder grants, legacy revision proof or materialization authority. Nil
	// retains each genuine service's refusal; no owner/UID fallback is installed.
	host, err := hostdefaults.OpenStandalone(ctx, hostdefaults.StandaloneOptions{Layout: layout, Config: hostdefaults.FileConfig{Path: configPath, Optional: optional, Version: Version, Namespace: layout.Namespace, EnforcementKeyFile: layout.EnforcementKeyFile}, BuilderShimCommand: shimCommand, BuilderShimArgs: shimArgs, GoalDirectory: filepath.Join(stateRoot, "goals"), BudgetDirectory: filepath.Join(stateRoot, "accounting"), Signals: watch})
	if err != nil {
		return err
	}
	return host.Serve(ctx)
}
