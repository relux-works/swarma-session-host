package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
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
