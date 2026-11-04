package sessionhost

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// An identical completed request finds the same actual attachment, including
// after terminal loss. Changed bytes or operation identity never prepare another
// provider. Ordinary admission is explicitly inert here, not native qualification.
func TestManagerOperationReuse(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	var calls atomic.Int32
	m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
		calls.Add(1)
		return &bridgePreparedFixture{origin: o}, nil
	}}
	data := memberOperationBytes(t, q)
	a, err := m.Prepare(context.Background(), "launch", data)
	if err != nil {
		t.Fatal(err)
	}
	for _, terminal := range []bool{false, true} {
		if terminal {
			_ = a.Close()
		}
		got, err := m.Prepare(context.Background(), "launch", data)
		if err != nil || got != a || calls.Load() != 1 {
			t.Fatal("operation was not reconciled to its actual attachment", terminal, err, calls.Load())
		}
		if terminal {
			if p, _ := got.Projection(); p.State != "unresolved" {
				t.Fatal("terminal operation became available again", p.State)
			}
		}
	}
	q["operation_id"] = "44444444-4444-4444-8444-444444444444"
	for _, changed := range [][]byte{append(append([]byte(nil), data...), ' '), memberOperationBytes(t, q)} {
		if _, err := parseMemberOperation("launch", changed); err != nil {
			t.Fatal("operation substitution control is not otherwise valid", err)
		}
		if got, err := m.Prepare(context.Background(), "launch", changed); got != nil || !errors.Is(err, errMemberRecordUnavailable) {
			t.Fatal("changed operation reused or replaced the claim", err)
		}
	}
	_, _, resume := memberOperationFixture(t, "resume")
	resumeData := memberOperationBytes(t, resume)
	if _, err := parseMemberOperation("resume", resumeData); err != nil {
		t.Fatal("resume substitution control is not otherwise valid", err)
	}
	if got, err := m.Prepare(context.Background(), "resume", resumeData); got != nil || !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("another operation kind replaced the claim", err)
	}
	if calls.Load() != 1 {
		t.Fatal("changed request repeated provider preparation", calls.Load())
	}
}

// Canceling a preparation request closes its returned owned provider and keeps
// its claim terminal, without canceling the continuing owner. No retry can turn
// that interrupted attempt into a second provider preparation.
func TestManagerOperationRequestCancellation(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	entered := make(chan struct{})
	var calls, cleaned atomic.Int32
	m.broker = bridgeBrokerFixture{prepare: func(ctx context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
		if calls.Add(1) != 1 {
			return nil, errors.New("unexpected duplicate provider preparation")
		}
		close(entered)
		<-ctx.Done()
		return &bridgePreparedFixture{origin: o, close: func() error { cleaned.Add(1); return nil }}, nil
	}}
	data := memberOperationBytes(t, q)
	request, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { _, err := m.Prepare(request, "launch", data); done <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("provider preparation not reached")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, errMemberRecordUnavailable) || cleaned.Load() != 1 {
			t.Fatal("canceled preparation did not clean up its actual provider", err, cleaned.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled preparation did not finish")
	}
	if m.owner.checkCurrentHost(m.owner.host) != nil {
		t.Fatal("request cancellation retired the continuing owner")
	}
	if a, err := m.Prepare(context.Background(), "launch", data); a != nil || !errors.Is(err, errMemberRecordUnavailable) || calls.Load() != 1 {
		t.Fatal("canceled operation repeated provider preparation", err, calls.Load())
	}
	recordNoAttachment(t, m.owner)
}

// A pending or failed attempt consumes this owner's one preparation claim. A
// concurrent retry and a retry after provider refusal cause no second callback;
// the successful nearby control above still reaches an actual attachment.
func TestManagerOperationPendingAndRefused(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	m.broker = bridgeBrokerFixture{prepare: func(ctx context.Context, _ ManagerOrigin) (PreparedNotificationBroker, error) {
		if calls.Add(1) != 1 {
			return nil, errors.New("unexpected duplicate provider preparation")
		}
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, errors.New("inert provider refusal")
	}}
	data := memberOperationBytes(t, q)
	done := make(chan error, 1)
	go func() { _, err := m.Prepare(context.Background(), "launch", data); done <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first actual provider preparation not reached")
	}
	if a, err := m.Prepare(context.Background(), "launch", data); a != nil || !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("pending operation admitted another attempt", err)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, errMemberRecordUnavailable) {
			t.Fatal("unexpected first refusal", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refused preparation did not finish")
	}
	if a, err := m.Prepare(context.Background(), "launch", data); a != nil || !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("refused operation admitted another attempt", err)
	}
	if calls.Load() != 1 {
		t.Fatal("pending/refused request repeated provider preparation", calls.Load())
	}
	recordNoAttachment(t, m.owner)
}

// Close cancels an in-flight preparation through the continuing owner and
// waits for its owned provider cleanup before returning. This uses an inert
// context-cooperative provider, not an unbounded or external process.
func TestManagerOperationCloseJoinsPreparation(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseProvider := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseProvider)
	var cleaned atomic.Int32
	m.broker = bridgeBrokerFixture{prepare: func(ctx context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return &bridgePreparedFixture{origin: o, close: func() error { cleaned.Add(1); return nil }}, nil
	}}
	prepareDone := make(chan error, 1)
	go func() {
		_, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
		prepareDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("provider preparation not entered")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- m.Close(context.Background()) }()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		releaseProvider()
		t.Fatal("manager Close did not cancel preparation")
	}
	select {
	case <-closeDone:
		releaseProvider()
		t.Fatal("manager Close returned before owned preparation cleanup")
	default:
	}
	releaseProvider()
	select {
	case err := <-closeDone:
		if err != nil || cleaned.Load() != 1 {
			t.Fatal("manager Close did not join exactly one cleanup", err, cleaned.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("manager Close did not join preparation")
	}
	select {
	case err := <-prepareDone:
		if !errors.Is(err, errMemberRecordUnavailable) {
			t.Fatal("canceled preparation produced a successful attachment", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled preparation did not return")
	}
}
