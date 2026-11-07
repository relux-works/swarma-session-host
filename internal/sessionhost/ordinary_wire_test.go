package sessionhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

func ordinaryWireFixture(t *testing.T) (*ordinaryWireManager, map[string]any) {
	t.Helper()
	o, q := preChildFixture(t)
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "curator-run")
	// Inert bytes: these tests never execute a launcher or native process.
	body := []byte("inert-installed-launcher-fixture\n")
	if err := os.WriteFile(binary, body, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	cfg := ordinaryLauncherConfig{binary: binary, sha256: hex.EncodeToString(sum[:]), environment: []string{"HOME=" + u.HomeDir, "PATH=/usr/bin:/bin"}}
	m, err := openOrdinaryWireManager(context.Background(), time.Now().Add(time.Minute), o.records.path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return m, q
}

// One real retained operation derives the runner execution identity, complete
// fixed plan and built-in diagnostic mode. Config remains data: it does not
// implement operation admission and this test never starts a native process.
func TestOrdinaryWireRunnerConfig(t *testing.T) {
	m, q := ordinaryWireFixture(t)
	op, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := op.takeRunnerConfig(context.Background(), m, "/tmp/host-owned-fixture.sock", time.Second)
	if err != nil || cfg.Execution != op.claim.id || cfg.Socket != "/tmp/host-owned-fixture.sock" || cfg.IOTimeout != time.Second || cfg.StopSignal != "none" || cfg.Diagnostics != runner.ProtocolChildDiagnostics || cfg.Plan.Binary != m.launcher.path || len(cfg.Plan.Args) != 19 || cfg.Plan.Cwd != q["cwd"] {
		t.Fatal("actual retained runner config lost", cfg, err)
	}
	if _, ok := any(cfg).(managerOperationAdmission); ok {
		t.Fatal("runner configuration became policy admission")
	}
	cfg.Plan.Args[0], cfg.Plan.Env[0] = "changed", "changed"
	if op.claim.invocation.args[0] != "protocol-child-v1" || m.environment.entries[0] == "changed" {
		t.Fatal("runner config aliases retained source")
	}
	if _, err := op.takeLauncherPlan(context.Background(), m); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("config derivation did not consume original once claim", err)
	}
}

// Invalid composition resources never consume a valid operation; nearby valid
// configuration succeeds. Foreign manager queries likewise grant no authority.
// Cancellation after valid resources reaches the existing consumed boundary.
func TestOrdinaryWireRunnerConfigRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, socket string
		timeout      time.Duration
	}{
		{"relative", "r.sock", time.Second},
		{"unclean", "/tmp/../r.sock", time.Second},
		{"control", "/tmp/r\n.sock", time.Second},
		{"length", "/tmp/" + strings.Repeat("x", 100), time.Second},
		{"zero-timeout", "/tmp/r.sock", 0},
		{"long-timeout", "/tmp/r.sock", time.Minute + time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, q := ordinaryWireFixture(t)
			op, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q))
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := op.takeRunnerConfig(context.Background(), m, tc.socket, tc.timeout)
			if !errors.Is(err, errOrdinaryWireUnavailable) || !reflect.DeepEqual(cfg, runner.Config{}) {
				t.Fatal("invalid resource returned partial runner config", err)
			}
			if _, err := op.takeRunnerConfig(context.Background(), m, "/tmp/r.sock", time.Second); err != nil {
				t.Fatal("invalid resources consumed legitimate claim", err)
			}
		})
	}
	m, q := ordinaryWireFixture(t)
	other, _ := ordinaryWireFixture(t)
	op, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := op.takeRunnerConfig(context.Background(), other, "/tmp/r.sock", time.Second); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("foreign manager borrowed operation", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := op.takeRunnerConfig(ctx, m, "/tmp/r.sock", time.Second); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("canceled valid attempt admitted", err)
	}
	if _, err := op.takeRunnerConfig(context.Background(), m, "/tmp/r.sock", time.Second); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("canceled valid attempt recycled", err)
	}
}

// Concurrent runner-config requests and old plan consumers use one shared start
// claim. This proves no second configuration escapes through either API.
func TestOrdinaryWireRunnerConfigConcurrent(t *testing.T) {
	m, q := ordinaryWireFixture(t)
	op, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wins := make(chan bool, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				_, err := op.takeRunnerConfig(context.Background(), m, "/tmp/r.sock", time.Second)
				wins <- err == nil
			} else {
				_, err := op.takeLauncherPlan(context.Background(), m)
				wins <- err == nil
			}
		}()
	}
	wg.Wait()
	close(wins)
	count := 0
	for win := range wins {
		if win {
			count++
		}
	}
	if count != 1 {
		t.Fatal("multiple runner configurations or no valid control", count)
	}
}

// Complete published wire bytes reach physical member retention and one fixed
// configured plan. This is preparation only: no admission, process or broker.
func TestOrdinaryWireDispatch(t *testing.T) {
	m, q := ordinaryWireFixture(t)
	data := memberOperationBytes(t, q)
	op, err := m.dispatch(context.Background(), "launch", data)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := any(m).(managerOperationAdmission); ok {
		t.Fatal("wire preparation became runtime admission")
	}
	if _, ok := any(op).(notificationManagerCustody); ok {
		t.Fatal("plan became notification custody")
	}
	data[0] = 'x'
	if _, err := m.dispatch(context.Background(), "launch", data); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("invalid changed bytes admitted", err)
	}
	again, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil || again != op {
		t.Fatal("duplicate did not retain actual operation", err)
	}
	plan, err := op.takeLauncherPlan(context.Background(), m)
	if err != nil || plan.Binary != m.launcher.path || plan.Cwd != q["cwd"] || len(plan.Args) != 19 || plan.Args[0] != "protocol-child-v1" {
		t.Fatal("fixed plan lost", err)
	}
	if !reflect.DeepEqual(plan.Env, m.environment.entries) || strings.Contains(strings.Join(plan.Env, "\n"), "A2A_") {
		t.Fatal("baseline or paired identity placement changed")
	}
	plan.Args[0] = "changed"
	plan.Env[0] = "changed"
	if op.claim.invocation.args[0] != "protocol-child-v1" || m.environment.entries[0] == "changed" {
		t.Fatal("returned plan aliases retained inputs")
	}
	if _, err := op.takeLauncherPlan(context.Background(), m); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("second launcher plan admitted", err)
	}
}

// Serial dispatch and concurrent consumers share the pre-child once claim;
// neither another ID nor another manager can borrow that operation.
func TestOrdinaryWireConcurrentClaim(t *testing.T) {
	m, q := ordinaryWireFixture(t)
	data := memberOperationBytes(t, q)
	var wg sync.WaitGroup
	var selected *ordinaryWireOperation
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op, err := m.dispatch(context.Background(), "launch", data)
			if err != nil {
				t.Error(err)
				return
			}
			m.mu.Lock()
			if selected != nil && selected != op {
				t.Error("duplicate operation")
			}
			selected = op
			m.mu.Unlock()
		}()
	}
	wg.Wait()
	if selected == nil {
		t.Fatal("no operation")
	}
	other, _ := ordinaryWireFixture(t)
	if _, err := selected.takeLauncherPlan(context.Background(), other); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("foreign manager obtained plan", err)
	}
	q["operation_id"] = "00000000-0000-4000-8000-000000000099"
	if _, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("new ID replaced original", err)
	}
	results := make(chan bool, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := selected.takeLauncherPlan(context.Background(), m)
			results <- err == nil
		}()
	}
	wg.Wait()
	close(results)
	n := 0
	for ok := range results {
		if ok {
			n++
		}
	}
	if n != 1 {
		t.Fatal("launcher plan not consumed once", n)
	}
}

// Observed executable replacement with equal bytes, altered bytes, permissions,
// root replacement and canceled take refuse with no partial plan or retry.
func TestOrdinaryWireStartRefusals(t *testing.T) {
	for _, name := range []string{"same-bytes-replacement", "changed-bytes", "same-metadata-drift", "writable", "root-replacement", "canceled"} {
		t.Run(name, func(t *testing.T) {
			m, q := ordinaryWireFixture(t)
			op, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			path := m.launcher.path
			switch name {
			case "same-bytes-replacement":
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path, path+".held"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, b, 0700); err != nil {
					t.Fatal(err)
				}
			case "changed-bytes":
				if err := os.WriteFile(path, []byte("different\n"), 0700); err != nil {
					t.Fatal(err)
				}
			case "same-metadata-drift":
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(strings.Repeat("x", len(b))), 0700); err != nil {
					t.Fatal(err)
				}
				stamp := m.launcher.identity.ModTime()
				if err := os.Chtimes(path, stamp, stamp); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(path, 0770); err != nil {
					t.Fatal(err)
				}
			case "root-replacement":
				dir := filepath.Dir(path)
				if err := os.Rename(dir, dir+".held"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(dir + ".held") })
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			plan, err := op.takeLauncherPlan(ctx, m)
			if !errors.Is(err, errOrdinaryWireUnavailable) || plan.Binary != "" || plan.Cwd != "" || plan.Args != nil || plan.Env != nil {
				t.Fatal("invalid source returned partial plan", err)
			}
			if _, err := op.takeLauncherPlan(context.Background(), m); !errors.Is(err, errOrdinaryWireUnavailable) {
				t.Fatal("refused take recycled", err)
			}
		})
	}
}

// A failed dispatch consumes preparation. Restoring matching executable bytes
// after observed loss must not reopen it; a separate valid manager is a control.
func TestOrdinaryWireFailedDispatchAndClose(t *testing.T) {
	m, q := ordinaryWireFixture(t)
	path := m.launcher.path
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("changed"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("changed launcher admitted", err)
	}
	if err := os.WriteFile(path, b, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("failed dispatch retried", err)
	}
	good, gq := ordinaryWireFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	op, err := good.dispatch(ctx, "launch", memberOperationBytes(t, gq))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := op.takeLauncherPlan(context.Background(), good); err != nil {
		t.Fatal("short request closed continuing lifetime", err)
	}
	if err := good.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if good.launcher.file != nil || good.launcher.root != nil {
		t.Fatal("executable resources not joined")
	}
	if _, err := good.dispatch(context.Background(), "launch", memberOperationBytes(t, gq)); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("closed manager reopened", err)
	}
}

// The configured baseline is closed and copied. Optional TMPDIR is genuinely
// optional; incoming identities, native homes and secret-like variables refuse.
func TestOrdinaryWireConfiguredEnvironment(t *testing.T) {
	m, _ := ordinaryWireFixture(t)
	base := append([]string(nil), m.environment.entries...)
	for _, name := range []string{"baseline", "optional-tmpdir", "A2A_AGENT", "CODEX_HOME", "GH_TOKEN", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			env := append([]string(nil), base...)
			valid := false
			switch name {
			case "baseline":
				valid = true
			case "optional-tmpdir":
				env = append(env, "TMPDIR="+t.TempDir())
				valid = true
			case "duplicate":
				env = append(env, base[0])
			default:
				env = append(env, name+"=synthetic_fixture_value")
			}
			cfg := ordinaryLauncherConfig{binary: m.launcher.path, sha256: m.launcher.digest, environment: env}
			got, err := openOrdinaryWireManager(context.Background(), time.Now().Add(time.Minute), m.owner.records.path, cfg)
			if valid {
				if err != nil {
					t.Fatal("legitimate baseline refused", err)
				}
				defer got.close(context.Background())
				env[0] = "changed"
				if got.environment.entries[0] == "changed" {
					t.Fatal("configuration aliases caller storage")
				}
			} else if !errors.Is(err, errOrdinaryWireUnavailable) || got != nil {
				if got != nil {
					_ = got.close(context.Background())
				}
				t.Fatal("extra configured input admitted", err)
			}
		})
	}
}

// Unknown methods and request-selected executable fields have no alternate
// dispatch route. A valid unsupported create consumes its failed claim.
func TestOrdinaryWireClosedDispatch(t *testing.T) {
	m, q := ordinaryWireFixture(t)
	if _, err := m.dispatch(context.Background(), "stop", memberOperationBytes(t, q)); !errors.Is(err, errOrdinaryWireUnavailable) || m.owner.operation != nil {
		t.Fatal("unknown method allocated claim", err)
	}
	q["binary"] = "/request/override"
	if _, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errOrdinaryWireUnavailable) || m.owner.operation != nil {
		t.Fatal("request selected executable", err)
	}
	delete(q, "binary")
	q["native_tail"] = []string{"app-server"}
	if _, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errOrdinaryWireUnavailable) || m.owner.operation == nil {
		t.Fatal("unsupported semantics ignored", err)
	}
	q["native_tail"] = []string{}
	if _, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errOrdinaryWireUnavailable) {
		t.Fatal("failed unsupported operation retried", err)
	}
	good, gq := ordinaryWireFixture(t)
	if _, err := good.dispatch(context.Background(), "launch", memberOperationBytes(t, gq)); err != nil {
		t.Fatal("nearby valid dispatch refused", err)
	}
}
