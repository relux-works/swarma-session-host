//go:build darwin && cgo

package runner

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/admission"
)

type testIdentifyObservation struct {
	process            admission.Process
	started, completed time.Time
	err                error
}

func testIdentifyObserve(ctx context.Context, pid int) testIdentifyObservation {
	o := testIdentifyObservation{started: time.Now()}
	o.process, o.err = admission.ObserveProcess(ctx, pid)
	o.completed = time.Now()
	return o
}

// This is test orchestration only. It is deliberately absent from production
// clients/admission and does not qualify either external record reader.
func testIdentifyLiveAcceptance(ctx context.Context, r identifyResponse, complete time.Time, expected admission.Process, read func(context.Context, int) testIdentifyObservation) error {
	if r.Result != "verified" || r.Child == nil {
		return errIdentifyUnavailable
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return errIdentifyUnavailable
	}
	seen := read(ctx, r.Child.PID)
	if !seen.started.After(complete) {
		return errIdentifyUnavailable
	}
	if seen.completed.After(deadline) || ctx.Err() != nil {
		return errIdentifyUnavailable
	}
	if seen.err != nil {
		return errIdentifyUnavailable
	}
	at, err := time.Parse(admission.ProcessTime, seen.process.StartedAt)
	if err != nil {
		return errIdentifyUnavailable
	}
	if seen.process != expected || seen.process.PID != r.Child.PID ||
		uint64(at.Unix()) != r.Child.Birth.Sec || uint64(at.Nanosecond()/1000) != r.Child.Birth.Usec {
		return errIdentifyConflict
	}
	return nil
}

// A successful preread cannot admit a snapshot delivered after normal reap.
// The live control performs an actual NEW kernel read after complete decoding,
// within the same original1000ms attempt; it compares full boot/UID/birth/image.
func TestIdentifyPostReplyLiveOrder(t *testing.T) {
	for _, reapBeforeReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "reapedDuringDelivery"}[reapBeforeReply], func(t *testing.T) {
			cfg := testConfig(t, "echo")
			s, openReap, err := startIdentifyTestRunner(t, cfg)
			if err != nil {
				t.Fatal(err)
			}
			preCtx, preCancel := context.WithTimeout(context.Background(), time.Second)
			pre := testIdentifyObserve(preCtx, s.pid)
			preCancel()
			if pre.err != nil {
				t.Fatal("IDENTIFY_PREREAD_CONTROL", pre.err)
			}
			release, openReply := identifyBarrier(t)
			entered := make(chan struct{}, 1)
			setIdentifyGate(s, func() { entered <- struct{}{}; <-release })
			ctx, cancel := identifyContext(t)
			defer cancel()
			type answer struct {
				r   identifyResponse
				err error
			}
			done := make(chan answer, 1)
			go func() {
				r, err := identifyForQualification(ctx, cfg.Socket, os.Getpid(), pre.process.UID, identifyMatchingRequest(s))
				done <- answer{r, err}
			}()
			awaitIdentifyEvent(t, entered, "IDENTIFY_ORDER_SNAPSHOT")
			if reapBeforeReply {
				if err := s.stdin.Close(); err != nil {
					t.Fatal(err)
				}
				openReap()
				awaitIdentifyEvent(t, s.done, "IDENTIFY_REAP_BEFORE_REPLY")
			}
			openReply()
			a := <-done
			if a.err != nil {
				t.Fatal("IDENTIFY_ORDER_WIRE", a.err)
			}
			requireIdentifyResult(t, a.r, "verified", "")
			complete := time.Now()
			reads := 0
			var actual testIdentifyObservation
			err = testIdentifyLiveAcceptance(ctx, a.r, complete, pre.process, func(ctx context.Context, pid int) testIdentifyObservation {
				reads++
				actual = testIdentifyObserve(ctx, pid)
				return actual
			})
			if reads != 1 || !actual.started.After(complete) {
				t.Fatal("IDENTIFY_FRESH_READ_ORDER")
			}
			if reapBeforeReply {
				if !errors.Is(err, errIdentifyUnavailable) || actual.err == nil {
					t.Fatal("IDENTIFY_STALE_PREREAD_REFUSED", err)
				}
			} else if err != nil {
				t.Fatal("IDENTIFY_ORDER_LIVE_CONTROL", err)
			}
		})
	}
}

// A real exited-unreaped child still has ownership, but the unchanged existing
// live/CDHASH observer refuses it; releasing the initial gate permits normal
// reap to publish child_reaped. No test consumes Wait4 or adopts a PID.
func TestIdentifyExitedUnreaped(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, openReap, err := startIdentifyTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.stdin.Close()
	if err := awaitOwnedZombie(s.pid); err != nil {
		t.Fatal(err)
	}
	r, err := identifyCall(t, s)
	if err != nil {
		t.Fatal(err)
	}
	requireIdentifyResult(t, r, "verified", "")
	ctx, cancel := identifyContext(t)
	defer cancel()
	if _, err := admission.ObserveProcess(ctx, s.pid); err == nil {
		t.Fatal("IDENTIFY_OWNERSHIP_IS_NOT_LIVE")
	}
	openReap()
	awaitIdentifyEvent(t, s.done, "IDENTIFY_ZOMBIE_NORMAL_REAP")
	r, err = identifyCall(t, s)
	if !errors.Is(err, errIdentifyUnavailable) {
		t.Fatal("IDENTIFY_ZOMBIE_AFTER_REAP")
	}
	requireIdentifyResult(t, r, "unavailable", "child_reaped")
}

// Cached/early/late observations never qualify, even with matching content.
// Exact PID, microsecond birth, UID, boot and image mismatch controls are
// comparison-sensitivity fixtures, not actual external verifier qualification.
func TestIdentifyOrderedReadGuards(t *testing.T) {
	r, err := decodeIdentifyResponse(identifySuccessWire())
	if err != nil {
		t.Fatal(err)
	}
	expected := admission.Process{PID: 123, UID: 501, BootUUID: "12345678-1234-1234-1234-123456789abc", StartedAt: "2027-01-15T08:00:00.123456Z", CDHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	at := time.Unix(int64(r.Child.Birth.Sec), int64(r.Child.Birth.Usec)*1000)
	expected.StartedAt = at.UTC().Format(admission.ProcessTime)
	for _, name := range []string{"valid", "cached", "early", "late", "pid", "birth", "uid", "boot", "image", "unavailable"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			complete := time.Now()
			seen := testIdentifyObservation{process: expected, started: complete.Add(time.Microsecond), completed: complete.Add(2 * time.Microsecond)}
			want := errIdentifyConflict
			switch name {
			case "valid":
				want = nil
			case "cached", "early":
				seen.started = complete.Add(-time.Microsecond)
				want = errIdentifyUnavailable
			case "late":
				deadline, _ := ctx.Deadline()
				seen.completed = deadline.Add(time.Nanosecond)
				want = errIdentifyUnavailable
			case "pid":
				seen.process.PID++
			case "birth":
				seen.process.StartedAt = at.Add(time.Microsecond).UTC().Format(admission.ProcessTime)
			case "uid":
				seen.process.UID++
			case "boot":
				seen.process.BootUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
			case "image":
				seen.process.CDHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			case "unavailable":
				seen.err = errors.New("unavailable")
				want = errIdentifyUnavailable
			}
			err := testIdentifyLiveAcceptance(ctx, r, complete, expected, func(context.Context, int) testIdentifyObservation { return seen })
			if !errors.Is(err, want) {
				t.Fatalf("IDENTIFY_ORDER_GUARD_%s: %v", name, err)
			}
		})
	}
}
