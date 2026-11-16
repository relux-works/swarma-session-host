package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

func processContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func operationRequest(t *testing.T, id string) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]string{"operation_id": id})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Admission refusals are effect-free durable decisions, including after a
// restart. A formerly refused operation never retries when admission improves.
func TestNoStartAdmissionRefusalDurable(t *testing.T) {
	p, o, _, q := selectedFixture(t)
	delete(q, "notification")
	o.admitHook = func(context.Context, string, []byte) error { return ordinaryManagerRefusal{code: "policy_refused"} }
	id := q["operation_id"].(string)
	data, exit := p.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	selectedCode(t, data, exit, "policy_refused")
	if !p.daemon.provedNoStart(processContext(t), id) || o.starts.Load() != 0 {
		t.Fatal("pre-execute refusal lacks durable no-start")
	}
	directory := p.daemon.journal.store.dir
	o.admitHook = nil
	replay, replayExit := p.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	if !bytes.Equal(replay, data) || replayExit != exit || o.starts.Load() != 0 {
		t.Fatal("refused operation re-entered execution")
	}
	closeSelected(t, p)
	if p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("closed provider asserted no-start")
	}
	restarted, err := registerHostPlugin(context.Background(), time.Now().Add(20*time.Second), directory, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeSelected(t, restarted) })
	o.plugin = restarted
	if !restarted.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("restart lost committed pre-execute decision")
	}
	replay, replayExit = restarted.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	if !bytes.Equal(replay, data) || replayExit != exit || o.starts.Load() != 0 {
		t.Fatal("restart replay created execution")
	}
	if restarted.daemon.provedNoStart(processContext(t), "77777777-7777-4777-8777-777777777777") {
		t.Fatal("missing operation proved absence")
	}
}

// Cancellation wins under the same mutex as admission's reservation. Even an
// already-running admission callback cannot later enter execute.
func TestNoStartCancellationSerializesAdmission(t *testing.T) {
	p, o, _, q := selectedFixture(t)
	delete(q, "notification")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	o.admitHook = func(ctx context.Context, _ string, _ []byte) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	id := q["operation_id"].(string)
	request := memberOperationBytes(t, q)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Call(context.Background(), selectedManagerProtocol, "launch", request)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("admission did not enter")
	}
	result, exit := p.Call(processContext(t), selectedManagerProtocol, "cancel", operationRequest(t, id))
	if exit != 0 || !bytes.Contains(result, []byte(`"outcome":"cancelled"`)) || !p.daemon.provedNoStart(processContext(t), id) {
		t.Fatalf("serialized cancellation: %s exit=%d", result, exit)
	}
	unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled admission did not join")
	}
	if o.starts.Load() != 0 {
		t.Fatal("cancelled admission spawned")
	}
	before, err := os.ReadFile(filepath.Join(p.daemon.journal.store.dir, id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if !p.daemon.provedNoStart(processContext(t), id) {
			t.Fatal("durable cancellation became uncertain")
		}
		p.Call(processContext(t), selectedManagerProtocol, "status", operationRequest(t, id))
	}
	after, err := os.ReadFile(filepath.Join(p.daemon.journal.store.dir, id+".json"))
	if err != nil || !bytes.Equal(before, after) || o.starts.Load() != 0 {
		t.Fatal("observation changed cancellation or execution")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if p.daemon.provedNoStart(cancelled, id) {
		t.Fatal("failed lookup context asserted no start")
	}
}

// Every execute reservation remains unknown until a result is durably stored.
// Cancel, retry, lookup and restart cannot convert this window to safe absence.
func TestNoStartReservedExecutionRemainsUnknown(t *testing.T) {
	p, o, _, q := selectedFixture(t)
	delete(q, "notification")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	o.executeHook = func(ctx context.Context, _ string, _ []byte) (*selectedExecution, []byte, error) {
		close(entered)
		select {
		case <-release:
			return nil, nil, errors.New("lost_execution_reply")
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	id := q["operation_id"].(string)
	request := memberOperationBytes(t, q)
	done := make(chan struct{})
	go func() { defer close(done); p.Call(context.Background(), selectedManagerProtocol, "launch", request) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("execute did not enter")
	}
	result, exit := p.Call(processContext(t), selectedManagerProtocol, "cancel", operationRequest(t, id))
	if exit != 0 || !bytes.Contains(result, []byte(`"outcome":"unknown"`)) || p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("reserved operation became cancelled/no-start", string(result))
	}
	unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("execute did not join")
	}
	data, code := p.Call(processContext(t), selectedManagerProtocol, "launch", request)
	selectedCode(t, data, code, "session_liveness_unknown")
	directory := p.daemon.journal.store.dir
	closeSelected(t, p)
	restarted, err := registerHostPlugin(context.Background(), time.Now().Add(20*time.Second), directory, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeSelected(t, restarted) })
	o.plugin = restarted
	data, code = restarted.Call(processContext(t), selectedManagerProtocol, "launch", request)
	selectedCode(t, data, code, "session_liveness_unknown")
	if restarted.daemon.provedNoStart(processContext(t), id) || o.starts.Load() != 1 {
		t.Fatal("restart changed unknown or duplicated execution")
	}
	if err := os.WriteFile(filepath.Join(directory, id+".pending"), []byte("unfinished"), 0600); err != nil {
		t.Fatal(err)
	}
	if restarted.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("unfinished persistence proved no start")
	}
	data, code = restarted.Call(processContext(t), selectedManagerProtocol, "launch", request)
	selectedCode(t, data, code, "session_host_unavailable")
	if o.starts.Load() != 1 {
		t.Fatal("failed lookup duplicated execution")
	}
}

// The journal rejects an attempted unknown-to-safe rewrite and contradictory
// terminal data. The nearby new-ID tombstone demonstrates valid persistence.
func TestNoStartJournalRejectsReservationRewrite(t *testing.T) {
	p, _, _, q := selectedFixture(t)
	id := q["operation_id"].(string)
	entry := selectedOperationEntry{Digest: strings.Repeat("a", 64), Kind: "launch", Submitted: q["submitted_at"].(string)}
	if err := p.daemon.journal.write(id, entry); err != nil {
		t.Fatal(err)
	}
	for _, replacement := range []selectedOperationEntry{
		{Cancelled: true, NoStart: true},
		{NoStart: true, Digest: entry.Digest, Kind: "launch"},
	} {
		if err := p.daemon.journal.write(id, replacement); !errors.Is(err, errSelectedJournal) {
			t.Fatal("reservation was converted to no-start", err)
		}
	}
	stored, exists, err := p.daemon.journal.read(id)
	if err != nil || !exists || stored.NoStart || stored.Cancelled {
		t.Fatal("failed rewrite damaged original reservation", err)
	}
	safeID := "77777777-7777-4777-8777-777777777777"
	if err := p.daemon.journal.write(safeID, selectedOperationEntry{Cancelled: true, NoStart: true}); err != nil {
		t.Fatal("new pre-execute tombstone refused", err)
	}
	if err := p.daemon.journal.write(safeID, entry); !errors.Is(err, errSelectedJournal) {
		t.Fatal("terminal tombstone reopened", err)
	}
	if !p.daemon.provedNoStart(processContext(t), safeID) {
		t.Fatal("rejected rewrite lost valid no-start")
	}
}

// This owner is a synthetic admission oracle around a real owned /bin/cat.
// Its declared harness role/handoff does not qualify any native provider.
type selectedHeldOwner struct {
	*selectedOrdinaryFixture
	mu          sync.Mutex
	held        *runner.Server
	admission   selectedProcessAdmission
	unavailable bool
}

func (o *selectedHeldOwner) ownedHarness(ctx context.Context, e *selectedExecution) (*runner.Server, selectedProcessAdmission, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ctx.Err() != nil || o.unavailable || e != o.execution {
		return nil, selectedProcessAdmission{}, errHostOwnerUnavailable
	}
	return o.held, o.admission, nil
}

func selectedHeldFixture(t *testing.T) (*registeredHostPlugin, *selectedHeldOwner, map[string]any) {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("owned Linux pidfd fixture; Darwin qualification is a separate native gate")
	}
	p, ordinary, _, q := selectedFixture(t)
	delete(q, "notification")
	o := &selectedHeldOwner{selectedOrdinaryFixture: ordinary}
	p.daemon.owner = o
	ordinary.executeHook = func(_ context.Context, _ string, request []byte) (*selectedExecution, []byte, error) {
		// The base execute method has checked the durable reservation before
		// this actual OS spawn. No fixture harness exists during admission.
		startSelectedHeldFixture(t, o, q)
		var operation struct {
			ID string `json:"operation_id"`
		}
		if err := json.Unmarshal(request, &operation); err != nil {
			t.Fatal(err)
		}
		return o.execution, selectedSessionBytes(o.execution, operation.ID), nil
	}
	return p, o, q
}

func startSelectedHeldFixture(t *testing.T, o *selectedHeldOwner, q map[string]any) {
	t.Helper()
	dir, err := os.MkdirTemp(os.Getenv("TMPDIR"), "held-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	held, err := runner.Start(runner.Config{Socket: filepath.Join(dir, "child.sock"), Execution: "owned-fixture",
		Plan: runner.Plan{Binary: "/bin/cat", Args: []string{}, Cwd: dir, Env: []string{}}, IOTimeout: time.Second, IdentifyV2: true})
	if err != nil {
		t.Fatal("owned pidfd runner did not start", err)
	}
	t.Cleanup(func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	})
	kernel, err := held.ProcessEvidence(processContext(t))
	if err != nil {
		t.Fatal("owned live process evidence unavailable", err)
	}
	image, err := os.ReadFile("/bin/cat")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(image)
	if kernel.PID != held.ChildPID() || kernel.PID == os.Getpid() || kernel.RealUID != uint32(os.Getuid()) ||
		kernel.EffectiveUID != uint32(os.Geteuid()) || kernel.Executable != hex.EncodeToString(digest[:]) || kernel.StartTicks == "" {
		t.Fatal("kernel evidence differs from owned fixture")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	o.held = held
	o.admission = selectedProcessAdmission{Role: "harness", Handoff: strings.Repeat("d", 64), Kernel: kernel,
		Binding: selectedProcessBinding{ManagerID: "session-host", RegisteredAt: "2026-11-01T00:00:00Z", ManagerExecutable: executable,
			Operation: q["operation_id"].(string), Session: o.execution.session, Execution: kernel.Execution,
			Generation: o.execution.generation, Revision: 1, Profile: "fixture", Home: dir, Workspace: dir,
			PlanDigest: strings.Repeat("a", 64), ProfileLock: strings.Repeat("b", 64), ProfileFragment: strings.Repeat("c", 64),
			Environment: "fixture", Permissions: "fixture", Transport: "pipe", Runtime: "codex_cli",
			Vendor: "fixture", Model: "fixture", Effort: "fixture", NativeRelease: "fixture", NativeSession: "fixture",
			RunnerInstance: "fixture", RunnerPID: os.Getpid()}}
}

// An owned live child is exposed only after its started record is persisted.
// Repeated reads return copies and do not alter the journal, Host or execution.
func TestHeldProcessDurableReadOnlyCopies(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	selectedCall(t, p, "launch", q)
	e := o.execution
	id := q["operation_id"].(string)
	entry, exists, err := p.daemon.journal.read(id)
	if err != nil || !exists || entry.Started == nil || p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("started record missing or falsely no-start", err)
	}
	path := filepath.Join(p.daemon.journal.store.dir, id+".json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeDirectory, err := os.Stat(p.daemon.journal.store.dir)
	if err != nil {
		t.Fatal(err)
	}
	hostBefore := e.manager.owner.host.Snapshot()
	for i := 0; i < 4; i++ {
		got := p.daemon.observeHeldProcess(processContext(t), e.session, e.generation)
		if got == nil || *got != o.admission.Kernel {
			t.Fatal("durable held fixture omitted")
		}
		got.PID = 0
	}
	after, err := os.ReadFile(path)
	afterInfo, statErr := os.Stat(path)
	afterDirectory, directoryErr := os.Stat(p.daemon.journal.store.dir)
	if err != nil || !bytes.Equal(before, after) || o.starts.Load() != 1 || o.admissions.Load() != 1 ||
		statErr != nil || directoryErr != nil || !os.SameFile(beforeInfo, afterInfo) ||
		!beforeInfo.ModTime().Equal(afterInfo.ModTime()) || !beforeDirectory.ModTime().Equal(afterDirectory.ModTime()) ||
		!reflect.DeepEqual(hostBefore, e.manager.owner.host.Snapshot()) {
		t.Fatal("read-only proof changed state", err)
	}
	if p.daemon.observeHeldProcess(processContext(t), e.session, e.generation+1) != nil ||
		p.daemon.observeHeldProcess(processContext(t), "SES-other", e.generation) != nil {
		t.Fatal("foreign execution borrowed proof")
	}
	// The old schema/capability is unchanged in this first slice.
	description, exit := p.Call(processContext(t), selectedManagerProtocol, "describe", []byte("{}"))
	if exit != 0 || bytes.Contains(description, []byte("process-observation/1")) {
		t.Fatal("unnegotiated capability activated")
	}
}

// Each private mismatch omits evidence, despite an otherwise valid held child.
// Restoring the exact admitted evidence is the paired positive control.
func TestHeldProcessPrivatePredicates(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	selectedCall(t, p, "launch", q)
	expected := o.admission
	mutations := map[string]func(*selectedProcessAdmission){
		"executable":         func(a *selectedProcessAdmission) { a.Kernel.Executable = strings.Repeat("f", 64) },
		"role":               func(a *selectedProcessAdmission) { a.Role = "wrapper" },
		"runner-role":        func(a *selectedProcessAdmission) { a.Role = "runner" },
		"handoff":            func(a *selectedProcessAdmission) { a.Handoff = "" },
		"effective-uid":      func(a *selectedProcessAdmission) { a.Kernel.EffectiveUID++ },
		"real-uid":           func(a *selectedProcessAdmission) { a.Kernel.RealUID++ },
		"birth":              func(a *selectedProcessAdmission) { a.Kernel.StartTicks = "1" },
		"boot":               func(a *selectedProcessAdmission) { a.Kernel.BootID = "ffffffff-ffff-ffff-ffff-ffffffffffff" },
		"controller-pid":     func(a *selectedProcessAdmission) { a.Kernel.PID = os.Getpid() },
		"registration":       func(a *selectedProcessAdmission) { a.Binding.RegisteredAt = "2026-11-02T00:00:00Z" },
		"manager":            func(a *selectedProcessAdmission) { a.Binding.ManagerID = "foreign-manager" },
		"manager-executable": func(a *selectedProcessAdmission) { a.Binding.ManagerExecutable = "/different/manager" },
		"generation":         func(a *selectedProcessAdmission) { a.Binding.Generation++ },
		"execution":          func(a *selectedProcessAdmission) { a.Binding.Execution = "different" },
		"profile":            func(a *selectedProcessAdmission) { a.Binding.Profile = "different" },
		"home":               func(a *selectedProcessAdmission) { a.Binding.Home = "/different/home" },
		"workspace":          func(a *selectedProcessAdmission) { a.Binding.Workspace = "/different/workspace" },
		"plan":               func(a *selectedProcessAdmission) { a.Binding.PlanDigest = strings.Repeat("f", 64) },
		"profile-lock":       func(a *selectedProcessAdmission) { a.Binding.ProfileLock = strings.Repeat("f", 64) },
		"profile-fragment":   func(a *selectedProcessAdmission) { a.Binding.ProfileFragment = strings.Repeat("f", 64) },
		"revision":           func(a *selectedProcessAdmission) { a.Binding.Revision++ },
		"permissions":        func(a *selectedProcessAdmission) { a.Binding.Permissions = "different" },
		"native-release":     func(a *selectedProcessAdmission) { a.Binding.NativeRelease = "different" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			o.mu.Lock()
			mutate(&o.admission)
			o.mu.Unlock()
			if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil {
				t.Fatal("mismatched private proof was exposed")
			}
			o.mu.Lock()
			o.admission = expected
			o.mu.Unlock()
			if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) == nil {
				t.Fatal("valid neighboring proof was refused")
			}
		})
	}
	o.deny.Store(true)
	if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil {
		t.Fatal("lost ordinary owner exposed process")
	}
	o.deny.Store(false)
	if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) == nil {
		t.Fatal("restored owner unavailable")
	}
}

// Failure to persist the result keeps the reservation unknown. The retained
// OS handle alone cannot authorize an observation or a second launch.
func TestHeldProcessPersistenceFailureUnknown(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	id := q["operation_id"].(string)
	directory := p.daemon.journal.store.dir
	spawn := o.executeHook
	o.executeHook = func(_ context.Context, _ string, _ []byte) (*selectedExecution, []byte, error) {
		e, result, err := spawn(processContext(t), "launch", memberOperationBytes(t, q))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(directory, directory+".held"); err != nil {
			t.Fatal(err)
		}
		return e, result, nil
	}
	data, exit := p.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	selectedCode(t, data, exit, "session_liveness_unknown")
	if err := os.Rename(directory+".held", directory); err != nil {
		t.Fatal(err)
	}
	if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil ||
		p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("uncommitted process or no-start was exposed")
	}
	data, exit = p.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	selectedCode(t, data, exit, "session_host_unavailable")
	if o.starts.Load() != 1 {
		t.Fatal("failed persistence caused duplicate launch")
	}
}

// A new daemon cannot recover authority from the saved PID/birth record.
// Neither restart nor cancellation replays the completed operation.
func TestHeldProcessRestartDoesNotReacquire(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	selectedCall(t, p, "launch", q)
	directory := p.daemon.journal.store.dir
	closeSelected(t, p)
	restarted, err := registerHostPlugin(context.Background(), time.Now().Add(20*time.Second), directory, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeSelected(t, restarted) })
	o.plugin = restarted
	if restarted.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil {
		t.Fatal("disk record reconstructed process authority")
	}
	selectedCall(t, restarted, "launch", q)
	id := q["operation_id"].(string)
	if restarted.daemon.provedNoStart(processContext(t), id) || o.starts.Load() != 1 ||
		restarted.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil {
		t.Fatal("historical lookup recreated authority or execution")
	}
}

// Full private admission is checked before the first durable process proof.
// A later repaired callback cannot retroactively recapture that failed start.
func TestHeldProcessAdmissionAtStart(t *testing.T) {
	for _, name := range []string{"valid", "executable", "role", "handoff", "effective-uid", "controller-pid"} {
		t.Run(name, func(t *testing.T) {
			p, o, q := selectedHeldFixture(t)
			spawn := o.executeHook
			o.executeHook = func(ctx context.Context, kind string, request []byte) (*selectedExecution, []byte, error) {
				e, result, err := spawn(ctx, kind, request)
				switch name {
				case "executable":
					o.admission.Kernel.Executable = strings.Repeat("f", 64)
				case "role":
					o.admission.Role = "wrapper"
				case "handoff":
					o.admission.Handoff = ""
				case "effective-uid":
					o.admission.Kernel.EffectiveUID++
				case "controller-pid":
					o.admission.Kernel.PID = os.Getpid()
				}
				return e, result, err
			}
			selectedCall(t, p, "launch", q)
			got := p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation)
			if (got != nil) != (name == "valid") {
				t.Fatal("initial admission verdict differs", name, got)
			}
			entry, exists, err := p.daemon.journal.read(q["operation_id"].(string))
			if err != nil || !exists || (entry.Started != nil) != (name == "valid") {
				t.Fatal("durable proof disagrees with admission", err)
			}
			if name != "valid" {
				kernel, err := o.held.ProcessEvidence(processContext(t))
				if err != nil {
					t.Fatal(err)
				}
				o.admission.Kernel, o.admission.Role, o.admission.Handoff = kernel, "harness", strings.Repeat("d", 64)
				selectedCall(t, p, "launch", q)
				if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil || o.starts.Load() != 1 {
					t.Fatal("failed start was recaptured or replayed")
				}
			}
		})
	}
}

// A real child survives a lost execute reply. The operation remains unknown
// and its durable reservation prevents another spawn, including after restart.
func TestNoStartSpawnedChildLostReplyStaysUnknown(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	spawn := o.executeHook
	o.executeHook = func(ctx context.Context, kind string, request []byte) (*selectedExecution, []byte, error) {
		if _, _, err := spawn(ctx, kind, request); err != nil {
			return nil, nil, err
		}
		return nil, nil, errors.New("lost_execute_reply")
	}
	id := q["operation_id"].(string)
	request := memberOperationBytes(t, q)
	data, exit := p.Call(processContext(t), selectedManagerProtocol, "launch", request)
	selectedCode(t, data, exit, "session_liveness_unknown")
	if _, err := o.held.ProcessEvidence(processContext(t)); err != nil {
		t.Fatal("owned child did not survive", err)
	}
	if p.daemon.provedNoStart(processContext(t), id) ||
		p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil {
		t.Fatal("lost execute reply fabricated lifecycle evidence")
	}
	directory := p.daemon.journal.store.dir
	closeSelected(t, p)
	restarted, err := registerHostPlugin(context.Background(), time.Now().Add(20*time.Second), directory, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeSelected(t, restarted) })
	o.plugin = restarted
	data, exit = restarted.Call(processContext(t), selectedManagerProtocol, "launch", request)
	selectedCode(t, data, exit, "session_liveness_unknown")
	data, exit = restarted.Call(processContext(t), selectedManagerProtocol, "cancel", operationRequest(t, id))
	if exit != 0 || !bytes.Contains(data, []byte(`"outcome":"unknown"`)) ||
		restarted.daemon.provedNoStart(processContext(t), id) || o.starts.Load() != 1 {
		t.Fatal("restart/cancel cured uncertain spawn", string(data), exit)
	}
}

type selectedLostReplyWriter struct{}

func (selectedLostReplyWriter) Write([]byte) (int, error) { return 0, errors.New("reply_lost") }

// Losing the transport reply after persistence does not erase started evidence
// or turn it into no-start. An exact replay observes the single original start.
func TestHeldProcessPersistedBeforeLostReply(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	if exit := p.ServeRequest(processContext(t), selectedManagerProtocol, "launch",
		bytes.NewReader(memberOperationBytes(t, q)), selectedLostReplyWriter{}); exit != 1 {
		t.Fatal("lost reply not reported", exit)
	}
	id := q["operation_id"].(string)
	if p.daemon.provedNoStart(processContext(t), id) ||
		p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) == nil {
		t.Fatal("reply loss changed durable lifecycle evidence")
	}
	selectedCall(t, p, "launch", q)
	if o.starts.Load() != 1 {
		t.Fatal("reply loss duplicated start")
	}
}

// A damaged journal/failed lookup never returns previously remembered no-start.
// A valid terminal decision immediately before the damage is the positive.
func TestNoStartLookupFailureOmitsProof(t *testing.T) {
	p, o, _, q := selectedFixture(t)
	delete(q, "notification")
	o.admitHook = func(context.Context, string, []byte) error { return ordinaryManagerRefusal{code: "policy_refused"} }
	id := q["operation_id"].(string)
	p.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	if !p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("valid decision missing")
	}
	if err := os.WriteFile(filepath.Join(p.daemon.journal.store.dir, id+".pending"), []byte("unresolved"), 0600); err != nil {
		t.Fatal(err)
	}
	if p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("failed lookup reused cached no-start")
	}
	data, exit := p.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	selectedCode(t, data, exit, "session_host_unavailable")
	if o.starts.Load() != 0 {
		t.Fatal("failed lookup entered execute")
	}
}

// A retained proof cannot be transferred to another independently owned
// child, even when both children are live and use the same executable.
func TestHeldProcessRejectsSubstituteHandle(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	selectedCall(t, p, "launch", q)
	otherPlugin, other, otherRequest := selectedHeldFixture(t)
	selectedCall(t, otherPlugin, "launch", otherRequest)
	if other.held.ChildPID() == o.held.ChildPID() {
		t.Fatal("fixtures did not create distinct children")
	}
	original := o.held
	o.mu.Lock()
	o.held = other.held
	o.mu.Unlock()
	if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil {
		t.Fatal("substituted child borrowed proof")
	}
	o.mu.Lock()
	o.held = original
	o.mu.Unlock()
	if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) == nil {
		t.Fatal("original handle positive control lost")
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil {
		t.Fatal("lost hold was replaced with saved kernel data")
	}
	entry, exists, err := p.daemon.journal.read(q["operation_id"].(string))
	if err != nil || !exists || entry.Started == nil || p.daemon.provedNoStart(processContext(t), q["operation_id"].(string)) {
		t.Fatal("hold loss rewrote durable start as absence", err)
	}
}

// Old failed/cancelled records do not retroactively gain explicit no-start.
// Contradictory on-disk decisions refuse lookup instead of claiming absence.
func TestNoStartLegacyAndContradictoryRecords(t *testing.T) {
	p, _, _, q := selectedFixture(t)
	refusal, _ := selectedRefusal("policy_refused", nil)
	ids := []string{"77777777-7777-4777-8777-777777777777", "88888888-8888-4888-8888-888888888888"}
	entries := []selectedOperationEntry{
		{Cancelled: true},
		{Digest: strings.Repeat("a", 64), Kind: "launch", Submitted: q["submitted_at"].(string), Refusal: refusal},
	}
	for i, entry := range entries {
		if err := p.daemon.journal.write(ids[i], entry); err != nil {
			t.Fatal(err)
		}
		if p.daemon.provedNoStart(processContext(t), ids[i]) {
			t.Fatal("legacy terminal record upgraded")
		}
	}
	id := q["operation_id"].(string)
	entry := selectedOperationEntry{Cancelled: true, NoStart: true}
	if err := p.daemon.journal.write(id, entry); err != nil {
		t.Fatal(err)
	}
	if !p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("explicit terminal control missing")
	}
	entry.Cancelled = false
	entry.Digest, entry.Kind, entry.NoStart = strings.Repeat("a", 64), "launch", true
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.daemon.journal.store.dir, id+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("contradictory record proved no-start")
	}
	if _, _, err := p.daemon.journal.read(id); !errors.Is(err, errSelectedJournal) {
		t.Fatal("contradictory record accepted", err)
	}
}

func recoveryJournal(t *testing.T) *selectedOperationJournal {
	t.Helper()
	j, err := openSelectedOperationJournal(filepath.Join(t.TempDir(), "operations"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := j.close(); err != nil {
			t.Error(err)
		}
	})
	return j
}

// A malformed or ambiguous durable decision is unavailable, with no repair
// or lost obligation. A correctly encoded terminal decision is the positive.
func TestOperationJournalRecoveryRejectsAmbiguousDecisions(t *testing.T) {
	mutations := map[string]func([]byte) []byte{
		"duplicate-proof": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"no_start_proved":true`), []byte(`"no_start_proved":false,"no_start_proved":true`), 1)
		},
		"proof-key-case": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"no_start_proved"`), []byte(`"NO_START_PROVED"`), 1)
		},
		"unknown-member": func(b []byte) []byte { return bytes.Replace(b, []byte("{"), []byte(`{"reconciled":true,`), 1) },
		"signature-member": func(b []byte) []byte {
			return bytes.Replace(b, []byte("{"), []byte(`{"sig":"ignored-by-canonical-output",`), 1)
		},
		"null-correlation": func(b []byte) []byte { return bytes.Replace(b, []byte(`"digest":""`), []byte(`"digest":null`), 1) },
		"invalid-utf8": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"submitted_at":""`), append([]byte(`"submitted_at":"`), 0xff, '"'), 1)
		},
		"lone-surrogate": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"submitted_at":""`), []byte(`"submitted_at":"\ud800"`), 1)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			j := recoveryJournal(t)
			id := "11111111-1111-4111-8111-111111111111"
			if err := j.write(id, selectedOperationEntry{Cancelled: true, NoStart: true}); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(j.store.dir, id+".json")
			valid, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if entry, exists, err := j.read(id); err != nil || !exists || !entry.NoStart {
				t.Fatal("valid decision unavailable", err)
			}
			invalid := mutate(valid)
			if bytes.Equal(invalid, valid) {
				t.Fatal("corruption control was not applied")
			}
			if err := os.WriteFile(path, invalid, 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := j.read(id); !errors.Is(err, errSelectedJournal) {
				t.Fatal("ambiguous lifecycle decision accepted", err)
			}
			if !j.known[id] {
				t.Fatal("failed lookup discarded obligation")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, invalid) {
				t.Fatal("lookup repaired evidence", err)
			}
			if err := os.WriteFile(path, valid, 0600); err != nil {
				t.Fatal(err)
			}
			if entry, exists, err := j.read(id); err != nil || !exists || !entry.NoStart {
				t.Fatal("restored positive control refused", err)
			}
		})
	}
}

// Invalid transitions must fail before even reserving an ID or writing a
// pending marker. A valid reservation with the same ID must remain possible.
func TestOperationJournalRejectsInvalidWritesWithoutEffects(t *testing.T) {
	id := "11111111-1111-4111-8111-111111111111"
	otherID := "22222222-2222-4222-8222-222222222222"
	valid := selectedOperationEntry{Digest: strings.Repeat("a", 64), Kind: "launch", Submitted: "2026-11-01T00:00:00Z"}
	refusal, _ := selectedRefusal("policy_refused", nil)
	result := selectedSessionBytes(&selectedExecution{session: "SES-fixture", generation: 1}, id)
	mutations := map[string]func(*selectedOperationEntry){
		"bad-refusal":        func(e *selectedOperationEntry) { e.Refusal = []byte(`{"ok":true}`) },
		"cancel-and-result":  func(e *selectedOperationEntry) { e.Cancelled = true; e.Result = result },
		"cancel-and-refusal": func(e *selectedOperationEntry) { e.Cancelled = true; e.Refusal = refusal },
		"wrong-result-operation": func(e *selectedOperationEntry) {
			e.Result = selectedSessionBytes(&selectedExecution{session: "SES-fixture", generation: 1}, otherID)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			j := recoveryJournal(t)
			entry := valid
			mutate(&entry)
			if err := j.write(id, entry); !errors.Is(err, errSelectedJournal) {
				t.Fatal("invalid lifecycle snapshot persisted", err)
			}
			index, err := os.ReadFile(filepath.Join(j.store.dir, "operations.index"))
			if err != nil || len(index) != 0 || j.known[id] {
				t.Fatal("rejected write reserved operation", err)
			}
			for _, suffix := range []string{".json", ".pending"} {
				if _, err := os.Lstat(filepath.Join(j.store.dir, id+suffix)); !os.IsNotExist(err) {
					t.Fatal("rejected write left state", suffix, err)
				}
			}
			if err := j.write(id, valid); err != nil {
				t.Fatal("valid neighboring reservation refused", err)
			}
			if got, exists, err := j.read(id); err != nil || !exists || got.NoStart || len(got.Result) != 0 {
				t.Fatal("valid reservation changed", err)
			}
		})
	}
}

// Exhaustion refuses a new decision without evicting any prior obligation.
// Existing decisions remain readable and idempotent across reopening.
func TestOperationJournalCapacityRetainsObligations(t *testing.T) {
	j := recoveryJournal(t)
	entry := selectedOperationEntry{Cancelled: true, NoStart: true}
	ids := make([]string, 1024)
	for i := range ids {
		ids[i] = fmt.Sprintf("%08x-0000-4000-8000-000000000000", i+1)
		if err := j.write(ids[i], entry); err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
	}
	indexPath := filepath.Join(j.store.dir, "operations.index")
	before, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	extra := "ffffffff-0000-4000-8000-000000000000"
	if err := j.write(extra, entry); !errors.Is(err, errSelectedJournal) {
		t.Fatal("exhausted journal accepted new obligation", err)
	}
	after, err := os.ReadFile(indexPath)
	if err != nil || !bytes.Equal(before, after) || j.known[extra] {
		t.Fatal("exhaustion changed retained index", err)
	}
	for _, suffix := range []string{".json", ".pending"} {
		if _, err := os.Lstat(filepath.Join(j.store.dir, extra+suffix)); !os.IsNotExist(err) {
			t.Fatal("exhaustion left partial decision", err)
		}
	}
	directory := j.store.dir
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openSelectedOperationJournal(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if len(reopened.known) != len(ids) {
		t.Fatal("reopen lost obligations")
	}
	for _, id := range ids {
		got, exists, err := reopened.read(id)
		if err != nil || !exists || !got.NoStart || !got.Cancelled {
			t.Fatal("retained decision unavailable", id, err)
		}
	}
	if err := reopened.write(ids[0], entry); err != nil {
		t.Fatal("capacity blocked identical existing decision", err)
	}
}

type processJournalWitness struct {
	directory os.FileInfo
	files     map[string]struct {
		info os.FileInfo
		data []byte
	}
}

func witnessProcessJournal(t *testing.T, directory string) processJournalWitness {
	t.Helper()
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	w := processJournalWitness{directory: info, files: make(map[string]struct {
		info os.FileInfo
		data []byte
	})}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			t.Fatal("unexpected journal fixture member", err)
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		w.files[entry.Name()] = struct {
			info os.FileInfo
			data []byte
		}{info, data}
	}
	return w
}

func requireSameProcessJournal(t *testing.T, before, after processJournalWitness) {
	t.Helper()
	if !os.SameFile(before.directory, after.directory) || !before.directory.ModTime().Equal(after.directory.ModTime()) ||
		len(before.files) != len(after.files) {
		t.Fatal("observation changed journal directory")
	}
	for name, old := range before.files {
		current, ok := after.files[name]
		if !ok || !os.SameFile(old.info, current.info) || old.info.Mode() != current.info.Mode() ||
			!old.info.ModTime().Equal(current.info.ModTime()) || !bytes.Equal(old.data, current.data) {
			t.Fatal("observation changed journal member", name)
		}
	}
}

// Parallel observations may omit while another proof read owns the mutex.
// Every returned value is the same owned incarnation; mutation stays local,
// and no journal, input, generation, admission or launch effect is permitted.
func TestHeldProofConcurrentReadsRemainReadOnly(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	selectedCall(t, p, "launch", q)
	e := o.execution
	expected := o.admission.Kernel
	before := witnessProcessJournal(t, p.daemon.journal.store.dir)
	host := e.manager.owner.host.Snapshot()
	ctx := processContext(t)
	if p.daemon.observeHeldProcess(ctx, e.session, e.generation) == nil {
		t.Fatal("initial valid proof omitted")
	}
	var returned atomic.Int32
	errors := make(chan string, 8)
	var joined sync.WaitGroup
	for range 8 {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for range 16 {
				got := p.daemon.observeHeldProcess(ctx, e.session, e.generation)
				if got == nil {
					continue
				}
				if *got != expected {
					errors <- "concurrent proof changed"
					return
				}
				returned.Add(1)
				got.PID = 0
			}
		}()
	}
	joined.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if returned.Load() == 0 || p.daemon.observeHeldProcess(ctx, e.session, e.generation) == nil {
		t.Fatal("positive concurrent proof control unavailable")
	}
	if p.daemon.provedNoStart(ctx, q["operation_id"].(string)) || o.starts.Load() != 1 || o.admissions.Load() != 1 ||
		!reflect.DeepEqual(host, e.manager.owner.host.Snapshot()) {
		t.Fatal("observation changed lifecycle or input")
	}
	requireSameProcessJournal(t, before, witnessProcessJournal(t, p.daemon.journal.store.dir))
}

type interleavedProcessOwner struct {
	*selectedHeldOwner
	hookMu    sync.Mutex
	reads     int
	afterRead func(context.Context, int)
}

func (o *interleavedProcessOwner) ownedHarness(ctx context.Context, e *selectedExecution) (*runner.Server, selectedProcessAdmission, error) {
	held, admission, err := o.selectedHeldOwner.ownedHarness(ctx, e)
	o.hookMu.Lock()
	o.reads++
	read, hook := o.reads, o.afterRead
	o.hookMu.Unlock()
	if hook != nil {
		hook(ctx, read)
	}
	return held, admission, err
}

func (o *interleavedProcessOwner) setHook(hook func(context.Context, int)) {
	o.hookMu.Lock()
	defer o.hookMu.Unlock()
	o.reads, o.afterRead = 0, hook
}

// A correct kernel read does not authorize a reply after private admission
// changes during that read. Stable neighboring admission still returns proof.
func TestHeldProofRechecksAdmissionAfterKernelRead(t *testing.T) {
	p, base, q := selectedHeldFixture(t)
	o := &interleavedProcessOwner{selectedHeldOwner: base}
	p.daemon.owner = o
	selectedCall(t, p, "launch", q)
	expected := base.admission
	for _, name := range []string{"registration", "handoff", "effective-uid"} {
		t.Run(name, func(t *testing.T) {
			defer o.setHook(nil)
			defer func() { base.mu.Lock(); base.admission = expected; base.mu.Unlock() }()
			if p.daemon.observeHeldProcess(processContext(t), base.execution.session, base.execution.generation) == nil {
				t.Fatal("stable positive control omitted")
			}
			before := witnessProcessJournal(t, p.daemon.journal.store.dir)
			o.setHook(func(_ context.Context, read int) {
				if read != 1 {
					return
				}
				base.mu.Lock()
				defer base.mu.Unlock()
				switch name {
				case "registration":
					base.admission.Binding.RegisteredAt = "2026-11-02T00:00:00Z"
				case "handoff":
					base.admission.Handoff = strings.Repeat("e", 64)
				case "effective-uid":
					base.admission.Kernel.EffectiveUID++
				}
			})
			if p.daemon.observeHeldProcess(processContext(t), base.execution.session, base.execution.generation) != nil {
				t.Fatal("reply used admission that changed across kernel read")
			}
			requireSameProcessJournal(t, before, witnessProcessJournal(t, p.daemon.journal.store.dir))
		})
	}
}

// Caller cancellation omits the observation without discarding the original
// hold or changing its durable obligation. A fresh caller observes the same
// live child afterward, with no new admission, execution or journal effect.
func TestHeldProofCancellationPreservesOwnership(t *testing.T) {
	p, base, q := selectedHeldFixture(t)
	o := &interleavedProcessOwner{selectedHeldOwner: base}
	p.daemon.owner = o
	selectedCall(t, p, "launch", q)
	before := witnessProcessJournal(t, p.daemon.journal.store.dir)
	entered := make(chan struct{})
	o.setHook(func(ctx context.Context, read int) {
		if read == 1 {
			close(entered)
			<-ctx.Done()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan *runner.OwnedProcessEvidence, 1)
	go func() { result <- p.daemon.observeHeldProcess(ctx, base.execution.session, base.execution.generation) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("observation did not reach admission boundary")
	}
	cancel()
	select {
	case got := <-result:
		if got != nil {
			t.Fatal("cancelled caller received process proof")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled observation did not join")
	}
	o.setHook(nil)
	if got := p.daemon.observeHeldProcess(processContext(t), base.execution.session, base.execution.generation); got == nil || *got != base.admission.Kernel {
		t.Fatal("cancellation lost retained ownership")
	}
	if p.daemon.provedNoStart(processContext(t), q["operation_id"].(string)) || base.starts.Load() != 1 || base.admissions.Load() != 1 {
		t.Fatal("cancelled lookup changed durable lifecycle")
	}
	requireSameProcessJournal(t, before, witnessProcessJournal(t, p.daemon.journal.store.dir))
}

// An ambiguous retained index is unknown, even if one entry has valid proof.
// Removing the fixture ambiguity restores that original proof without a spawn.
func TestHeldProofAmbiguousExecutionIndexOmitsEvidence(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	selectedCall(t, p, "launch", q)
	e := o.execution
	ctx := processContext(t)
	if p.daemon.observeHeldProcess(ctx, e.session, e.generation) == nil {
		t.Fatal("original positive control unavailable")
	}
	other := &selectedExecution{session: e.session, generation: e.generation}
	p.daemon.mu.Lock()
	p.daemon.executions[other] = struct{}{}
	p.daemon.mu.Unlock()
	for range 8 {
		if p.daemon.observeHeldProcess(ctx, e.session, e.generation) != nil {
			t.Fatal("ambiguous index selected an owner")
		}
	}
	p.daemon.mu.Lock()
	delete(p.daemon.executions, other)
	p.daemon.mu.Unlock()
	if p.daemon.observeHeldProcess(ctx, e.session, e.generation) == nil || o.starts.Load() != 1 {
		t.Fatal("original ownership was not preserved")
	}
}

// A malformed saved proof refuses the complete operation lookup and cannot
// borrow a still-live handle. Restoring the exact fixture bytes is an explicit
// positive control; observation itself never repairs or resubmits anything.
func TestHeldProofRecoveryRejectsMalformedSnapshots(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	selectedCall(t, p, "launch", q)
	id := q["operation_id"].(string)
	path := filepath.Join(p.daemon.journal.store.dir, id+".json")
	valid, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(map[string]any){
		"proof-key-case": func(m map[string]any) {
			p := m["held_process"].(map[string]any)
			p["admissionDigest"] = p["AdmissionDigest"]
			delete(p, "AdmissionDigest")
		},
		"unknown-proof-member": func(m map[string]any) { m["held_process"].(map[string]any)["unverified"] = true },
		"missing-real-uid": func(m map[string]any) {
			delete(m["held_process"].(map[string]any)["Kernel"].(map[string]any), "RealUID")
		},
		"kernel-key-case": func(m map[string]any) {
			k := m["held_process"].(map[string]any)["Kernel"].(map[string]any)
			k["bootID"] = k["BootID"]
			delete(k, "BootID")
		},
		"unknown-kernel-member": func(m map[string]any) { m["held_process"].(map[string]any)["Kernel"].(map[string]any)["stale"] = true },
		"missing-zero-field": func(m map[string]any) {
			delete(m["held_process"].(map[string]any)["Kernel"].(map[string]any), "StartUsec")
		},
		"wrong-operation": func(m map[string]any) {
			other := "22222222-2222-4222-8222-222222222222"
			m["held_process"].(map[string]any)["Operation"] = other
			m["result"].(map[string]any)["operation_id"] = other
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if err := os.WriteFile(path, valid, 0600); err != nil {
					t.Error(err)
				}
			}()
			var object map[string]any
			if err := json.Unmarshal(valid, &object); err != nil {
				t.Fatal(err)
			}
			mutate(object)
			invalid, err := json.Marshal(object)
			if err != nil || bytes.Equal(invalid, valid) {
				t.Fatal("malformed snapshot control not applied", err)
			}
			if err := os.WriteFile(path, invalid, 0600); err != nil {
				t.Fatal(err)
			}
			before := witnessProcessJournal(t, p.daemon.journal.store.dir)
			if _, _, err := p.daemon.journal.read(id); !errors.Is(err, errSelectedJournal) {
				t.Fatal("malformed private proof accepted", err)
			}
			if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil ||
				p.daemon.provedNoStart(processContext(t), id) {
				t.Fatal("malformed persistence yielded lifecycle proof")
			}
			data, exit := p.Call(processContext(t), selectedManagerProtocol, "status", operationRequest(t, id))
			selectedCode(t, data, exit, "session_host_unavailable")
			data, exit = p.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
			selectedCode(t, data, exit, "session_host_unavailable")
			if o.starts.Load() != 1 || o.admissions.Load() != 1 {
				t.Fatal("malformed snapshot replayed execution")
			}
			requireSameProcessJournal(t, before, witnessProcessJournal(t, p.daemon.journal.store.dir))
			if err := os.WriteFile(path, valid, 0600); err != nil {
				t.Fatal(err)
			}
			if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) == nil {
				t.Fatal("valid neighboring snapshot refused")
			}
		})
	}
}

// Ambiguous saved no-start remains unavailable after daemon replacement.
// Status, cancel and launch preserve its indexed obligation and perform no
// admission/start; a valid terminal record still replays after explicit repair.
func TestOperationJournalRestartKeepsAmbiguousDecisionUnknown(t *testing.T) {
	p, o, _, q := selectedFixture(t)
	delete(q, "notification")
	o.admitHook = func(context.Context, string, []byte) error { return ordinaryManagerRefusal{code: "policy_refused"} }
	id := q["operation_id"].(string)
	request := memberOperationBytes(t, q)
	data, exit := p.Call(processContext(t), selectedManagerProtocol, "launch", request)
	selectedCode(t, data, exit, "policy_refused")
	if !p.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("valid initial decision missing")
	}
	directory := p.daemon.journal.store.dir
	path := filepath.Join(directory, id+".json")
	valid, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	closeSelected(t, p)
	invalid := bytes.Replace(valid, []byte(`"no_start_proved":true`), []byte(`"no_start_proved":false,"no_start_proved":true`), 1)
	if bytes.Equal(invalid, valid) {
		t.Fatal("duplicate-proof control not applied")
	}
	if err := os.WriteFile(path, invalid, 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := registerHostPlugin(context.Background(), time.Now().Add(20*time.Second), directory, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeSelected(t, restarted) })
	o.plugin = restarted
	before := witnessProcessJournal(t, directory)
	if restarted.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("restart normalized ambiguous no-start")
	}
	for _, kind := range []string{"status", "cancel", "launch"} {
		input := operationRequest(t, id)
		if kind == "launch" {
			input = request
		}
		data, exit := restarted.Call(processContext(t), selectedManagerProtocol, kind, input)
		selectedCode(t, data, exit, "session_host_unavailable")
	}
	if !restarted.daemon.journal.known[id] || o.starts.Load() != 0 || o.admissions.Load() != 1 {
		t.Fatal("restart discarded or replayed obligation")
	}
	requireSameProcessJournal(t, before, witnessProcessJournal(t, directory))
	if err := os.WriteFile(path, valid, 0600); err != nil {
		t.Fatal(err)
	}
	if !restarted.daemon.provedNoStart(processContext(t), id) {
		t.Fatal("restored terminal control unavailable")
	}
	data, exit = restarted.Call(processContext(t), selectedManagerProtocol, "launch", request)
	selectedCode(t, data, exit, "policy_refused")
	if o.starts.Load() != 0 || o.admissions.Load() != 1 {
		t.Fatal("historical terminal replay re-entered admission")
	}
}

// Claim: concurrent observers of one valid retained proof all receive it; a
// second reader is never refused for contention alone, and sequential reads
// before and after still succeed.
func TestHeldProcessConcurrentObserversAllReceiveProof(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	selectedCall(t, p, "launch", q)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if got := p.daemon.observeHeldProcess(ctx, o.execution.session, o.execution.generation); got == nil || *got != o.admission.Kernel {
		t.Fatal("valid control observation omitted before concurrency")
	}
	const observers, each = 8, 25
	var wg sync.WaitGroup
	missing := make(chan int, observers)
	for i := 0; i < observers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nilCount := 0
			for j := 0; j < each; j++ {
				got := p.daemon.observeHeldProcess(ctx, o.execution.session, o.execution.generation)
				if got == nil || *got != o.admission.Kernel {
					nilCount++
				}
			}
			missing <- nilCount
		}()
	}
	wg.Wait()
	close(missing)
	total := 0
	for n := range missing {
		total += n
	}
	if total != 0 {
		t.Fatalf("concurrent observations omitted a valid proof %d of %d times", total, observers*each)
	}
	if got := p.daemon.observeHeldProcess(ctx, o.execution.session, o.execution.generation); got == nil || *got != o.admission.Kernel {
		t.Fatal("valid control observation omitted after concurrency")
	}
}

// Claim: optional process evidence that cannot be stored with the mandatory
// result is dropped and the result still persists; the matching valid result
// keeps its evidence as the positive control.
func TestOptionalEvidenceDoesNotVetoDurableResult(t *testing.T) {
	t.Run("mismatched-result-persists-without-process", func(t *testing.T) {
		p, o, q := selectedHeldFixture(t)
		o.executeHook = func(_ context.Context, _ string, request []byte) (*selectedExecution, []byte, error) {
			startSelectedHeldFixture(t, o, q)
			var operation struct {
				ID string `json:"operation_id"`
			}
			if err := json.Unmarshal(request, &operation); err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err := json.Unmarshal(selectedSessionBytes(o.execution, operation.ID), &record); err != nil {
				t.Fatal(err)
			}
			record["manager"] = "other-manager"
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			return o.execution, data, nil
		}
		result, exit := p.Call(processContext(t), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
		if exit != 0 {
			t.Fatalf("mandatory result vetoed by optional evidence: %s exit=%d", result, exit)
		}
		id := q["operation_id"].(string)
		entry, exists, err := p.daemon.journal.read(id)
		if err != nil || !exists || len(entry.Result) == 0 || entry.Started != nil {
			t.Fatal("result not persisted, or unpersistable evidence retained", err)
		}
		if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) != nil {
			t.Fatal("dropped evidence still exposed a process")
		}
	})
	t.Run("valid-result-keeps-process", func(t *testing.T) {
		p, o, q := selectedHeldFixture(t)
		selectedCall(t, p, "launch", q)
		id := q["operation_id"].(string)
		entry, exists, err := p.daemon.journal.read(id)
		if err != nil || !exists || entry.Started == nil {
			t.Fatal("valid control lost its started evidence", err)
		}
		if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) == nil {
			t.Fatal("valid control observation omitted")
		}
	})
}

// Claim: while the capability is not advertised outside fixtures, no ordinary
// describe, launch or outcome reply carries its extension members, even when
// a private retained proof exists; that private proof is the valid control.
func TestProcessObservationNotAdvertisedOutsideFixtures(t *testing.T) {
	p, o, q := selectedHeldFixture(t)
	launch := selectedCall(t, p, "launch", q)
	if p.daemon.observeHeldProcess(processContext(t), o.execution.session, o.execution.generation) == nil {
		t.Fatal("valid control: private retained proof missing")
	}
	description, exit := p.Call(processContext(t), selectedManagerProtocol, "describe", []byte("{}"))
	if exit != 0 || !bytes.Contains(description, []byte(`"status/1"`)) {
		t.Fatalf("valid control: base describe unavailable: %s exit=%d", description, exit)
	}
	id := q["operation_id"].(string)
	outcome, exit := p.Call(processContext(t), selectedManagerProtocol, "status", operationRequest(t, id))
	if exit != 0 || !bytes.Contains(outcome, []byte(`"outcome":"started"`)) {
		t.Fatalf("valid control: base outcome unavailable: %s exit=%d", outcome, exit)
	}
	for name, reply := range map[string][]byte{"describe": description, "launch": launch, "outcome": outcome} {
		for _, member := range []string{`"process"`, `"termination"`, `"no_start"`, "process-observation/1"} {
			if bytes.Contains(reply, []byte(member)) {
				t.Fatalf("%s reply leaked %s without the advertised capability: %s", name, member, reply)
			}
		}
	}
}
