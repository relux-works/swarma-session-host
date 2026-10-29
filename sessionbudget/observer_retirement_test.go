package sessionbudget

import (
	"bytes"
	"context"
	"errors"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
)

// Only the typed feed IO is controlled; native observer/writer fixtures, actual
// service, durable store and factory worker execute their real paths.
type retirementFeed struct {
	closed, release, failed chan struct{}
	closeOnce, releaseOnce  sync.Once
}

func newRetirementFeed(t *testing.T) *retirementFeed {
	f := &retirementFeed{closed: make(chan struct{}), release: make(chan struct{}), failed: make(chan struct{})}
	t.Cleanup(func() { f.releaseOnce.Do(func() { close(f.release) }) })
	return f
}
func (f *retirementFeed) Next(ctx context.Context) (core.MuseObservation, error) {
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-f.closed:
		err = io.EOF
	case <-f.failed:
		err = io.EOF
	}
	<-f.release
	return core.MuseObservation{}, err
}
func (f *retirementFeed) Close() { f.closeOnce.Do(func() { close(f.closed) }) }

// Err returns the real underlying snapshot, then signals that the API observed
// a live context before it waits for the deliberately held registry mutex.
type observedRetirementContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (c *observedRetirementContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked) })
	return err
}

type retirementNative struct {
	*nativeFixture
	feed          *retirementFeed
	subscriptions int
}

func (n *retirementNative) SubscribeMuse(context.Context, core.ControllerLease) (core.MuseObservationFeed, error) {
	n.subscriptions++
	return n.feed, nil
}
func bindRetirementNative(t *testing.T, h *testHost, l core.ControllerLease) *retirementNative {
	t.Helper()
	n := &retirementNative{nativeFixture: newNative(l), feed: newRetirementFeed(t)}
	if l == h.lease {
		n.nativeFixture = h.native
	}
	if _, _, err := h.factory.BindGoal(context.Background(), l, "muse", n); err != nil {
		t.Fatal(err)
	}
	return n
}
func journalBytes(t *testing.T, h *testHost) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(h.root, "budget", "accounting.json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Joining is a real worker completion event, not a cancel request. Exact-lease
// retirement preserves full current budget/baseline/ledger/pause journal bytes,
// leaves another session alive, and prohibits same-lease silent re-subscription.
func TestGoalObserverRetirementJoinsExactWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := setup(t)
		n := bindRetirementNative(t, h, h.lease)
		other := bindRetirementNative(t, h, core.ControllerLease{SessionID: "other", Epoch: 1})
		if err := h.set(t, "actual-admitted", 0, goal.Budget{Tokens: 10}); err != nil {
			t.Fatal(err)
		}
		h.native.usage(150)
		if err := h.factory.CheckBudget(context.Background(), h.lease); err != nil {
			t.Fatal(err)
		}
		if a := h.account(t); a.Current == nil || a.Current.Baseline != 100 || !a.Current.BudgetPaused || len(a.Ledger) == 0 {
			t.Fatalf("actual ledger/baseline/pause fixture missing: %+v", a)
		}
		before := journalBytes(t, h)
		type result struct {
			joined bool
			err    error
		}
		done := make(chan result, 1)
		go func() {
			joined, err := h.factory.RetireGoalObserver(context.Background(), h.lease)
			done <- result{joined, err}
		}()
		<-n.feed.closed
		synctest.Wait()
		select {
		case r := <-done:
			t.Fatalf("retirement returned before worker joined: %+v", r)
		default:
		}
		n.feed.releaseOnce.Do(func() { close(n.feed.release) })
		synctest.Wait()
		r := <-done
		if !r.joined || r.err != nil {
			t.Fatalf("actual observer not joined: %+v", r)
		}
		if !bytes.Equal(before, journalBytes(t, h)) {
			t.Fatal("intentional retirement changed retained accounting")
		}
		select {
		case <-other.feed.closed:
			t.Fatal("exact retirement stopped unrelated session")
		default:
		}
		if _, _, err := h.factory.BindGoal(context.Background(), h.lease, "muse", n); !errors.Is(err, core.ErrStaleInputController) || n.subscriptions != 1 {
			t.Fatalf("retired lease silently restarted observer: %v subscriptions%d", err, n.subscriptions)
		}
		if err := h.factory.CheckBudget(context.Background(), h.lease); !errors.Is(err, core.ErrStaleInputController) {
			t.Fatalf("retired budget check admitted: %v", err)
		}
		if joined, err := h.factory.RetireGoalObserver(context.Background(), h.lease); !joined || err != nil {
			t.Fatalf("exact joined worker retry lost: %t %v", joined, err)
		}
	})
}

// Cancellation and stale/foreign lookup cannot stop another registered lease.
// A canceled join retains its actual retiring worker for a fresh exact retry.
func TestGoalObserverRetirementRefusalsAndCanceledJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := setup(t)
		n := bindRetirementNative(t, h, h.lease)
		other := bindRetirementNative(t, h, core.ControllerLease{SessionID: "other", Epoch: 1})
		before := journalBytes(t, h)
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if joined, err := h.factory.RetireGoalObserver(canceled, h.lease); joined || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled request retired worker: %t %v", joined, err)
		}
		// Cancellation while the actual registry lock is contended also has
		// no retirement effect; checking only before Lock is insufficient.
		h.factory.mu.Lock()
		waiting, cancelWaiting := context.WithCancel(context.Background())
		observedWaiting := &observedRetirementContext{Context: waiting, checked: make(chan struct{})}
		blocked := make(chan error, 1)
		go func() {
			joined, err := h.factory.RetireGoalObserver(observedWaiting, h.lease)
			if joined {
				err = errors.New("canceled blocked join reported success")
			}
			blocked <- err
		}()
		<-observedWaiting.checked
		cancelWaiting()
		h.factory.mu.Unlock()
		synctest.Wait()
		if err := <-blocked; !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked cancellation outcome: %v", err)
		}
		select {
		case <-n.feed.closed:
			t.Fatal("canceled blocked request retired observer")
		default:
		}
		wrong := h.lease
		wrong.Epoch++
		if joined, err := h.factory.RetireGoalObserver(context.Background(), wrong); joined || !errors.Is(err, core.ErrStaleInputController) {
			t.Fatalf("stale lease retired worker: %t %v", joined, err)
		}
		if joined, err := h.factory.RetireGoalObserver(context.Background(), core.ControllerLease{SessionID: "foreign", Epoch: 1}); joined || err != nil {
			t.Fatalf("absent foreign observer misreported: %t %v", joined, err)
		}
		for _, f := range []*retirementFeed{n.feed, other.feed} {
			select {
			case <-f.closed:
				t.Fatal("refusal closed registered observer")
			default:
			}
		}
		ctx, end := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			joined, err := h.factory.RetireGoalObserver(ctx, h.lease)
			if joined {
				err = errors.New("unjoined worker reported joined")
			}
			done <- err
		}()
		<-n.feed.closed
		end()
		synctest.Wait()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("interrupted join not retained: %v", err)
		}
		if _, _, err := h.factory.BindGoal(context.Background(), h.lease, "muse", n); !errors.Is(err, core.ErrStaleInputController) {
			t.Fatalf("pending retirement replaced worker: %v", err)
		}
		n.feed.releaseOnce.Do(func() { close(n.feed.release) })
		synctest.Wait()
		if joined, err := h.factory.RetireGoalObserver(context.Background(), h.lease); !joined || err != nil {
			t.Fatalf("retry did not join actual worker: %t %v", joined, err)
		}
		if !bytes.Equal(before, journalBytes(t, h)) {
			t.Fatal("canceled retirement rewrote accounting")
		}
		select {
		case <-other.feed.closed:
			t.Fatal("canceled retirement stopped unrelated observer")
		default:
		}
	})
}

// True unexplained feed loss marks retained accounting uncertain. Subsequent
// intentional retirement and a newer lease cannot erase that recovery barrier.
func TestGoalObserverUnknownLossRemainsUncertain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := setup(t)
		n := bindRetirementNative(t, h, h.lease)
		n.feed.releaseOnce.Do(func() { close(n.feed.release) })
		close(n.feed.failed)
		synctest.Wait()
		if !h.account(t).Uncertain {
			t.Fatal("unknown feed loss did not retain uncertainty")
		}
		before := journalBytes(t, h)
		if joined, err := h.factory.RetireGoalObserver(context.Background(), h.lease); !joined || err != nil {
			t.Fatalf("ended exact observer not joined: %t %v", joined, err)
		}
		if !bytes.Equal(before, journalBytes(t, h)) || !h.account(t).Uncertain {
			t.Fatal("retirement cleared unknown uncertainty")
		}
		next := h.lease
		next.Epoch++
		n.mu.Lock()
		n.lease = next
		n.native.SessionID = next.SessionID
		n.native.Epoch = next.Epoch
		n.mu.Unlock()
		if err := h.store.Restore(context.Background(), next, "native-s", n); !errors.Is(err, ErrAccountingUncertain) {
			t.Fatalf("new lease erased unexplained loss: %v", err)
		}
	})
}

// The neighboring valid new lease restores the actual retained account after
// the old worker joined; only its lease changes. Empty and active-paused
// allocations keep counters, baseline, ledger and pause state without a write.
func TestGoalObserverNewLeasePreservesRetainedAccounting(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "active-paused"}[active], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := setup(t)
				n := bindRetirementNative(t, h, h.lease)
				if active {
					if err := h.set(t, "retained", 0, goal.Budget{Tokens: 10}); err != nil {
						t.Fatal(err)
					}
					h.native.usage(150)
					if err := h.factory.CheckBudget(context.Background(), h.lease); err != nil {
						t.Fatal(err)
					}
				}
				before := h.account(t)
				n.feed.releaseOnce.Do(func() { close(n.feed.release) })
				if joined, err := h.factory.RetireGoalObserver(context.Background(), h.lease); !joined || err != nil {
					t.Fatalf("old observer did not join: %t %v", joined, err)
				}
				next := h.lease
				next.Epoch++
				h.native.mu.Lock()
				h.native.lease = next
				h.native.native.Epoch = next.Epoch
				writes := h.native.writes
				h.native.mu.Unlock()
				fresh := &retirementNative{nativeFixture: h.native, feed: newRetirementFeed(t)}
				if _, _, err := h.factory.BindGoal(context.Background(), next, "muse", fresh); err != nil {
					t.Fatalf("valid new observer lease refused: %v", err)
				}
				after := h.account(t)
				before.Lease = next
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("new lease reset retained accounting: before%+v after%+v", before, after)
				}
				if joined, err := h.factory.RetireGoalObserver(context.Background(), h.lease); joined || !errors.Is(err, core.ErrStaleInputController) {
					t.Fatalf("old lease retired current observer: %t %v", joined, err)
				}
				select {
				case <-fresh.feed.closed:
					t.Fatal("stale retirement closed current observer")
				default:
				}
				if err := h.factory.CheckBudget(context.Background(), next); err != nil {
					t.Fatalf("current observer refused: %v", err)
				}
				if h.native.writes != writes {
					t.Fatal("new observer lease repeated native budget effect")
				}
			})
		})
	}
}
