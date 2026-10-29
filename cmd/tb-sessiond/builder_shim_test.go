//go:build unix

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

func shimCommandFixture(t *testing.T) (*core.BuilderEvidenceGateway, *core.BuilderGatewayService, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "guide.md"), []byte("controlled builder content"), 0600); err != nil {
		t.Fatal(err)
	}
	trace := filepath.Join(t.TempDir(), "trace.jsonl")
	gateway, err := core.NewBuilderEvidenceGateway(core.BuilderEvidenceGatewayConfig{CtxID: "CTX-261029-sh0001", RunID: "RUN-261029-shim01", Provider: "claude", RepositoryRoot: root, Allowlist: core.NewContextBuilderAllowlist([]string{"docs"}), TracePath: trace})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = gateway.Close() })
	short := os.Getenv("SESSIONHOST_TEST_SOCKET_ROOT")
	if short == "" {
		t.Fatal("short owned socket root required")
	}
	dir, err := os.MkdirTemp(short, "shim")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	service, err := core.StartBuilderGatewayService(gateway, filepath.Join(dir, "g.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	return gateway, service, trace
}

// The exact built executable runs the original advertised argv and real MCP
// stdio/gateway/trace pipeline. This proves local access recording only, not
// independent native completeness, confinement or canonical claim authority.
func TestBuilderShimExecutableUsesOriginalArgvAndTrace(t *testing.T) {
	binary := os.Getenv("SESSIONHOST_TEST_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("actual built binary path required")
	}
	gateway, service, trace := shimCommandFixture(t)
	command, args := core.BuilderGatewayShimUsage(binary)
	if command != binary || !reflect.DeepEqual(args, []string{"session", "context-gateway-shim"}) {
		t.Fatal("original shim argv changed")
	}
	endpoint := service.Endpoint(command, args)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, endpoint.Command, endpoint.Args...)
	// Only newly generated isolated fixture authentication is passed. Do not
	// inherit the developer/provider process environment or any live credentials.
	cmd.Env = []string{"GOMAXPROCS=2", "GOMEMLIMIT=6GiB", "TASK_BOARD_BUILDER_GATEWAY_SOCKET=" + endpoint.SocketPath, "TASK_BOARD_BUILDER_GATEWAY_TOKEN=" + endpoint.Token}
	var output, diagnostics bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &diagnostics
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid), "stat"))
	if err != nil {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatal("owned shim PID birth unavailable", err)
	}
	fields := strings.Fields(string(stat)[strings.LastIndex(string(stat), ")")+1:])
	if len(fields) < 20 {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatal("owned shim birth malformed")
	}
	t.Logf("owned shim pid=%d birth=%s", cmd.Process.Pid, fields[19])
	script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
	_, writeErr := io.WriteString(stdin, script)
	closeErr := stdin.Close()
	waitErr := cmd.Wait()
	if writeErr != nil || closeErr != nil || waitErr != nil {
		t.Fatalf("actual shim execution failed: write=%v close=%v wait=%v", writeErr, closeErr, waitErr)
	}
	if !strings.Contains(output.String(), "controlled builder content") || strings.Contains(output.String(), `"isError":true`) || gateway.RecordedEvents() != 1 {
		t.Fatal("actual shim did not record/return the allowed access")
	}
	if _, err := gateway.Close(); err != nil {
		t.Fatal(err)
	}
	events, attestation, err := core.LoadBuilderAccessTrace(trace)
	if err != nil || len(events) != 1 || attestation.EventCount != 1 || attestation.CtxID != "CTX-261029-sh0001" {
		t.Fatalf("actual terminal trace not verified: events=%d %v", len(events), err)
	}
}

// The same executable refuses a wrong isolated bearer at the actual gateway
// authentication boundary, before any trace access. The valid neighbor above
// prevents an executable which rejects every invocation from qualifying.
func TestBuilderShimExecutableRefusesUnauthenticatedSource(t *testing.T) {
	binary := os.Getenv("SESSIONHOST_TEST_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("actual built binary path required")
	}
	gateway, service, _ := shimCommandFixture(t)
	command, args := core.BuilderGatewayShimUsage(binary)
	endpoint := service.Endpoint(command, args)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = []string{"GOMAXPROCS=2", "GOMEMLIMIT=6GiB", "TASK_BOARD_BUILDER_GATEWAY_SOCKET=" + endpoint.SocketPath, "TASK_BOARD_BUILDER_GATEWAY_TOKEN=controlled-wrong-bearer"}
	var output, diagnostics bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &diagnostics
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// The child remains owned until Wait, including a refused zombie. Retain
	// its real birth before joining; no foreign process signal is involved.
	stat, e := os.ReadFile(filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid), "stat"))
	if e != nil {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatal("owned refused shim birth unavailable", e)
	}
	f := strings.Fields(string(stat)[strings.LastIndex(string(stat), ")")+1:])
	if len(f) < 20 {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatal("owned refused shim birth malformed")
	}
	t.Logf("owned refused shim pid=%d birth=%s", cmd.Process.Pid, f[19])
	_ = stdin.Close()
	err = cmd.Wait()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 1 || !strings.Contains(diagnostics.String(), "refused an unauthenticated caller") || output.Len() != 0 || gateway.RecordedEvents() != 0 {
		t.Fatalf("wrong source did not receive intended no-access refusal: exit=%v events=%d", err, gateway.RecordedEvents())
	}
}

// Missing endpoint data, malformed original helper argv and cancelled entry
// refuse before host construction or an owned gateway request.
func TestBuilderShimCommandRefusesMissingSourceAndArguments(t *testing.T) {
	t.Setenv("TASK_BOARD_BUILDER_GATEWAY_SOCKET", "")
	t.Setenv("TASK_BOARD_BUILDER_GATEWAY_TOKEN", "")
	for _, args := range [][]string{{"session", "context-gateway-shim"}, {"session", "context-gateway-shim", "extra"}, {"session", "other"}} {
		var out, diagnostics bytes.Buffer
		err := runWithInput(context.Background(), args, strings.NewReader(""), &out, &diagnostics, nil)
		if err == nil || out.Len() != 0 {
			t.Fatal("missing source/helper arguments accepted")
		}
		if len(args) == 2 && args[1] == "context-gateway-shim" && !strings.Contains(err.Error(), "requires TASK_BOARD_BUILDER_GATEWAY_SOCKET") {
			t.Fatalf("wrong missing source refusal: %v", err)
		}
		if (len(args) != 2 || args[1] != "context-gateway-shim") && !strings.Contains(err.Error(), "unexpected session helper arguments") {
			t.Fatalf("wrong helper argument refusal: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := runWithInput(ctx, []string{"session", "context-gateway-shim"}, strings.NewReader(""), &out, io.Discard, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled helper read source: %v", err)
	}
}
