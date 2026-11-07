package sessionhost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func preChildFixture(t *testing.T) (*preChildManager, map[string]any) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	record, _ := recordFixture(t)
	record["clone"] = workspace
	recordWrite(t, dir, "member-1.json", memberOperationBytes(t, record))
	owner, err := openPreChildManager(context.Background(), time.Now().Add(time.Minute), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	q := protocolChildRequestFixture(t)
	q["cwd"] = workspace
	q["profile"] = map[string]any{"name": "swarm-orch", "lock_sha256": strings.Repeat("a", 64), "fragment_digest": "sha256:" + strings.Repeat("b", 64)}
	return owner, q
}

// Real configured record/workspace handles precede any Host/runner. The retained
// claim supplies one fixed invocation, but never implements ordinary admission.
func TestPreChildOwnerPhysicalRetention(t *testing.T) {
	o, q := preChildFixture(t)
	data := memberOperationBytes(t, q)
	op, err := o.prepare(context.Background(), "launch", data)
	if err != nil {
		t.Fatal(err)
	}
	if o.records.owner != nil || o.records.preChild != o || op.retained.record.file == nil || op.retained.workspace == nil {
		t.Fatal("pre-child source was not physically retained independently of Host")
	}
	if _, ok := any(o).(managerOperationAdmission); ok {
		t.Fatal("resource owner granted ordinary admission")
	}
	if _, ok := any(op).(notificationManagerCustody); ok {
		t.Fatal("pre-child operation granted provider custody")
	}
	if err := op.retained.recheck(nil, o.records, "launch", data); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("nil Host owner selected pre-child source as ordinary admission", err)
	}
	if _, err := op.retained.childInvocation(nil, o.records, "launch", data); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("legacy Host-bound compiler admitted pre-child source", err)
	}
	reads := 0
	read := func(context.Context, string, []string, ...string) ([]byte, error) {
		reads++
		return nil, errors.New("fixture must not be called")
	}
	if _, err := admitManagerCustody(context.Background(), op.retained, data, nil, read); !errors.Is(err, errMemberRecordUnavailable) || reads != 0 {
		t.Fatal("pre-child source reached Core or broker custody", err, reads)
	}
	inv, err := op.takeInvocation(context.Background(), o)
	if err != nil || len(inv.args) != 19 || inv.args[17] != `--member-a2a-agent-json="fixture.orch"` || inv.args[18] != `--member-a2a-config-dir-json="/configured/source"` {
		t.Fatal("retained source did not supply fixed invocation", err)
	}
	inv.args[0] = "foreign"
	if op.invocation.args[0] != "protocol-child-v1" {
		t.Fatal("invocation aliases retained storage")
	}
	if _, err := op.takeInvocation(context.Background(), o); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("second child-start claim admitted", err)
	}
}

// Concurrent identical requests converge on one retained operation, and exactly
// one caller can consume its start. No process or broker is used by this test.
func TestPreChildOwnerConcurrentOnce(t *testing.T) {
	o, q := preChildFixture(t)
	data := memberOperationBytes(t, q)
	var wg sync.WaitGroup
	results := make(chan *preChildOperation, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op, err := o.prepare(context.Background(), "launch", data)
			if err != nil {
				t.Error(err)
				return
			}
			results <- op
		}()
	}
	wg.Wait()
	close(results)
	var selected *preChildOperation
	count := 0
	for op := range results {
		count++
		if selected == nil {
			selected = op
		}
		if op != selected {
			t.Fatal("duplicate created another operation")
		}
	}
	if count != 16 {
		t.Fatal("missing duplicate results", count)
	}
	starts := make(chan bool, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := selected.takeInvocation(context.Background(), o)
			starts <- err == nil
		}()
	}
	wg.Wait()
	close(starts)
	n := 0
	for ok := range starts {
		if ok {
			n++
		}
	}
	if n != 1 {
		t.Fatal("start was not consumed once", n)
	}
}

// A changed request cannot replace a claim; foreign-owner queries must not
// consume the real owner's start. A short request's completion does not close it.
func TestPreChildOwnerRequestBinding(t *testing.T) {
	o, q := preChildFixture(t)
	data := memberOperationBytes(t, q)
	ctx, cancel := context.WithCancel(context.Background())
	op, err := o.prepare(ctx, "launch", data)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := o.prepare(context.Background(), "launch", append(append([]byte(nil), data...), ' ')); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("changed bytes replaced claim", err)
	}
	q["operation_id"] = "00000000-0000-4000-8000-000000000099"
	if _, err := o.prepare(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("different operation replaced claim", err)
	}
	foreign, _ := preChildFixture(t)
	if _, err := op.takeInvocation(context.Background(), foreign); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("foreign owner consumed claim", err)
	}
	if _, err := op.takeInvocation(context.Background(), o); err != nil {
		t.Fatal("short request or foreign query damaged actual owner", err)
	}
}

// A valid request whose preparation fails retains consumption; adding a missing
// record afterwards must not silently retry it. A nearby complete fixture works.
func TestPreChildOwnerFailedPreparationIsConsumed(t *testing.T) {
	o, q := preChildFixture(t)
	path := filepath.Join(o.records.path, "member-1.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	data := memberOperationBytes(t, q)
	if _, err := o.prepare(context.Background(), "launch", data); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("missing source admitted", err)
	}
	recordWrite(t, o.records.path, "member-1.json", b)
	if _, err := o.prepare(context.Background(), "launch", data); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("failed preparation retried", err)
	}
	if o.operation == nil || o.operation.ready {
		t.Fatal("failed claim not retained")
	}
	valid, validQ := preChildFixture(t)
	if _, err := valid.prepare(context.Background(), "launch", memberOperationBytes(t, validQ)); err != nil {
		t.Fatal("nearby valid source refused", err)
	}
}

// Invalid wire input never allocates a claim. An otherwise valid unsupported
// create consumes its failed preparation instead of dropping requested meaning.
func TestPreChildOwnerInvalidAndUnsupported(t *testing.T) {
	o, q := preChildFixture(t)
	if _, err := o.prepare(context.Background(), "launch", []byte(`{}`)); !errors.Is(err, errPreChildUnavailable) || o.operation != nil {
		t.Fatal("invalid input created an operation", err)
	}
	q["native_tail"] = []string{"app-server"}
	if _, err := o.prepare(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errPreChildUnavailable) || o.operation == nil {
		t.Fatal("unsupported semantics were discarded or failed claim lost", err)
	}
	q["native_tail"] = []string{}
	if _, err := o.prepare(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("unsupported failed claim replaced by different input", err)
	}
}

// Identical-byte physical replacement, source drift, cancellation at the start
// boundary and expiration refuse without recycling the consumed start claim.
func TestPreChildOwnerStartRefusals(t *testing.T) {
	for _, name := range []string{"file-replacement", "root-replacement", "workspace-replacement", "cancel", "expiry"} {
		t.Run(name, func(t *testing.T) {
			o, q := preChildFixture(t)
			op, err := o.prepare(context.Background(), "launch", memberOperationBytes(t, q))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch name {
			case "file-replacement":
				path := filepath.Join(o.records.path, "member-1.json")
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path, path+".held"); err != nil {
					t.Fatal(err)
				}
				recordWrite(t, o.records.path, "member-1.json", b)
			case "root-replacement":
				path := o.records.path
				if err := os.Rename(path, path+".held"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(path + ".held") })
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "workspace-replacement":
				path := q["cwd"].(string)
				if err := os.Rename(path, path+".held"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(path + ".held") })
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "expiry":
				o.cancel()
				<-o.done
			}
			if got, err := op.takeInvocation(ctx, o); !errors.Is(err, errPreChildUnavailable) || !reflect.DeepEqual(got, protocolChildInvocation{}) {
				t.Fatal("invalid start returned partial invocation", err)
			}
			if _, err := op.takeInvocation(context.Background(), o); !errors.Is(err, errPreChildUnavailable) {
				t.Fatal("refused start was recycled", err)
			}
		})
	}
}

// Invalid finite lifetime/source and closed owner are resource refusals; none
// creates a native runtime. Close joins retained file cleanup despite waiter loss.
func TestPreChildOwnerLifetime(t *testing.T) {
	o, q := preChildFixture(t)
	if _, err := openPreChildManager(nil, time.Now().Add(time.Minute), o.records.path); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("nil lifetime admitted", err)
	}
	if _, err := openPreChildManager(context.Background(), time.Now().Add(-time.Second), o.records.path); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("expired owner admitted", err)
	}
	if _, err := openPreChildManager(context.Background(), time.Now().Add(time.Minute), "relative"); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("relative source admitted", err)
	}
	op, err := o.prepare(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	if err := o.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if op.retained.record.file != nil || op.retained.record.workspace != nil {
		t.Fatal("Close did not join retained file cleanup")
	}
	if _, err := o.prepare(context.Background(), "launch", memberOperationBytes(t, q)); !errors.Is(err, errPreChildUnavailable) {
		t.Fatal("closed owner reopened", err)
	}
}
