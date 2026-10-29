package sessionbudget

import (
	"bytes"
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
	"testing/synctest"
)

// A missing newer lease is not blocked by an older intentionally retired AND
// joined worker. Every live, unfinished, unexpectedly ended or newer sibling
// remains a refusal. Actual service/store/factory workers and typed feed IO run;
// no registry entry, completion channel, accounting or receipt is synthesized.
func TestGoalObserverRetirementSiblingTombstones(t *testing.T) {
	for _, mode := range []string{"older-retired-joined", "older-live", "older-retiring-unjoined", "older-unexpected-ended", "newer-live", "newer-retired-joined"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := setup(t)
				registered, requested := h.lease, h.lease
				requested.Epoch = 3
				if mode == "newer-live" || mode == "newer-retired-joined" {
					registered.Epoch = 3
					requested.Epoch = 1
				}
				n := bindRetirementNative(t, h, registered)
				other := bindRetirementNative(t, h, core.ControllerLease{SessionID: "other", Epoch: 1})
				switch mode {
				case "older-retired-joined", "newer-retired-joined":
					n.feed.releaseOnce.Do(func() { close(n.feed.release) })
					if joined, err := h.factory.RetireGoalObserver(context.Background(), registered); !joined || err != nil {
						t.Fatalf("real prior worker did not retire and join: %t %v", joined, err)
					}
				case "older-retiring-unjoined":
					ctx, cancel := context.WithCancel(context.Background())
					done := make(chan error, 1)
					go func() {
						joined, err := h.factory.RetireGoalObserver(ctx, registered)
						if joined {
							err = errors.New("unfinished join reported success")
						}
						done <- err
					}()
					<-n.feed.closed
					cancel()
					synctest.Wait()
					if err := <-done; !errors.Is(err, context.Canceled) {
						t.Fatalf("real unfinished retirement not retained: %v", err)
					}
				case "older-unexpected-ended":
					n.feed.releaseOnce.Do(func() { close(n.feed.release) })
					close(n.feed.failed)
					synctest.Wait()
					if !h.account(t).Uncertain {
						t.Fatal("unexpected loss fixture did not retain uncertainty")
					}
				}
				before := journalBytes(t, h)
				joined, err := h.factory.RetireGoalObserver(context.Background(), requested)
				if mode == "older-retired-joined" {
					if joined || err != nil {
						t.Fatalf("older retired joined tombstone blocked newer absent lease: joined%t %v", joined, err)
					}
				} else if joined || !errors.Is(err, core.ErrStaleInputController) {
					t.Fatalf("live or unresolved/newer sibling misreported absent: mode%s joined%t %v", mode, joined, err)
				}
				if !bytes.Equal(before, journalBytes(t, h)) {
					t.Fatal("sibling lookup changed retained accounting")
				}
				select {
				case <-other.feed.closed:
					t.Fatal("sibling lookup retired unrelated session")
				default:
				}
				if mode == "older-live" || mode == "newer-live" {
					select {
					case <-n.feed.closed:
						t.Fatal("sibling lookup retired live mismatched observer")
					default:
					}
				}
				if mode == "older-retired-joined" || mode == "newer-retired-joined" || mode == "older-retiring-unjoined" {
					if _, _, err := h.factory.BindGoal(context.Background(), registered, "muse", n); !errors.Is(err, core.ErrStaleInputController) || n.subscriptions != 1 {
						t.Fatalf("lookup discarded no-restart tombstone: %v subscriptions%d", err, n.subscriptions)
					}
				}
			})
		})
	}
}
