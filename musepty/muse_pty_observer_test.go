package musepty

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// The original captured log supplies session/process/run identities. The host
// is an in-process observation fixture: no executable, provider or PTY starts.
func museObserverFixture(t *testing.T) (*musePTYHost, string, museRunTerminal, *os.File) {
	t.Helper()
	path := filepath.Join("testdata/muse-1.4.7", muse147SubmittedLog)
	fold, err := readMuseLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(fold.Terminals) != 1 {
		t.Fatal("expected original captured completed run")
	}
	sink, err := os.CreateTemp(t.TempDir(), "input-sentinel")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	if _, err = sink.WriteString("input unchanged"); err != nil {
		t.Fatal(err)
	}
	h := &musePTYHost{snapshot: musePTYSnapshot{LogPath: path, SessionID: fold.SessionID, PID: fold.PID}, ptmx: sink, exited: make(chan struct{}), vt: newMuseVT(24, 80)}
	for id, terminal := range fold.Terminals {
		return h, id, terminal, sink
	}
	t.Fatal("captured run missing")
	return nil, "", museRunTerminal{}, nil
}
func museObserverNoInput(t *testing.T, sink *os.File) {
	t.Helper()
	b, err := os.ReadFile(sink.Name())
	if err != nil || string(b) != "input unchanged" {
		t.Fatal("observer wrote input", err)
	}
}

// Existing committed terminal bytes produce exactly the original result without
// another deliver/turn. Original terminal-before-cancel precedence is preserved.
func TestMusePTYObserveOwnerRunIsReadOnly(t *testing.T) {
	h, id, terminal, sink := museObserverFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := h.ObserveOwnerRun(ctx, id)
	if err != nil || !reflect.DeepEqual(result, museTextResult(terminal)) {
		t.Fatalf("captured result changed: %+v %v", result, err)
	}
	museObserverNoInput(t, sink)
	if h.pendingRun || h.stopped {
		t.Fatal("observer changed native lifetime/input state")
	}
}

// A foreign run cannot borrow the captured terminal. Cancellation and transport
// loss preserve the original distinct recoverable results, with no extra bytes.
func TestMusePTYObserveOwnerRunForeignAndExit(t *testing.T) {
	for _, name := range []string{"cancelled", "exited", "blank"} {
		t.Run(name, func(t *testing.T) {
			h, id, _, sink := museObserverFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "cancelled":
				cancel()
			case "exited":
				close(h.exited)
			case "blank":
				id = ""
			}
			if name != "blank" {
				id += "-foreign"
			}
			result, err := h.ObserveOwnerRun(ctx, id)
			if name == "blank" {
				if err != sm.ErrInvalidInput || result != nil {
					t.Fatalf("blank run accepted: %+v %v", result, err)
				}
			} else {
				want := "context_cancelled"
				if name == "exited" {
					want = "transport_lost"
				}
				if err != nil || result == nil || result.ProviderStatus != want || result.ExitCode != 1 || result.Status != sm.ProviderRunFailed || !result.Recoverable {
					t.Fatalf("wrong observation outcome: %+v %v", result, err)
				}
			}
			museObserverNoInput(t, sink)
		})
	}
}

// Existing fold identity/read errors remain nonrecoverable unknown_failure,
// rather than a success borrowed from a different log/process or a missing file.
func TestMusePTYObserveOwnerRunFoldFailure(t *testing.T) {
	for _, name := range []string{"foreign_session", "foreign_process", "missing_log"} {
		t.Run(name, func(t *testing.T) {
			h, id, _, sink := museObserverFixture(t)
			switch name {
			case "foreign_session":
				h.snapshot.SessionID += "-foreign"
			case "foreign_process":
				h.snapshot.PID++
			case "missing_log":
				h.snapshot.LogPath = filepath.Join(t.TempDir(), "missing.jsonl")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			result, err := h.ObserveOwnerRun(ctx, id)
			if err != nil || result == nil || result.ProviderStatus != "unknown_failure" || result.Recoverable || result.Detail == "" {
				t.Fatalf("fold failure promoted: %+v %v", result, err)
			}
			museObserverNoInput(t, sink)
		})
	}
}
