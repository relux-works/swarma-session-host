package sessionmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const (
	// claudeContextLeaseDirName holds the one-writer markers for Claude context
	// materializations. It sits beside the other manager-private state rather
	// than in the provider's own directory: the provider must never see manager
	// bookkeeping in a directory it enumerates as sessions.
	claudeContextLeaseDirName = "claude-context-leases"
)

// claudeContextLeaseMarker is the on-disk lease content.
//
// It is diagnostics and audit only: exclusion itself is the advisory file lock,
// not anything written here. That split is deliberate. An earlier revision made
// the marker authoritative and reclaimed it on a fixed TTL, which meant a live
// writer past that TTL could be displaced by a second one, and two racing
// reclaimers could each remove the other's freshly written marker. Both are
// impossible against a lock the kernel owns.
type claudeContextLeaseMarker struct {
	CtxID      string    `json:"ctx_id"`
	HolderPID  int       `json:"holder_pid"`
	HolderHost string    `json:"holder_host,omitempty"`
	Nonce      string    `json:"nonce"`
	AcquiredAt time.Time `json:"acquired_at"`
	RenewedAt  time.Time `json:"renewed_at"`
	ReleasedAt time.Time `json:"released_at,omitempty"`
}

// claudeContextLease is one exclusive writer registration for a materialization.
//
// The open file descriptor is the lease. While it is open and flocked no other
// open file description — in this process or any other — can take the same lock,
// and when the holder exits for any reason, including SIGKILL, the kernel drops
// it. There is no heartbeat to fall behind and no expiry to guess at.
type claudeContextLease struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	ctxID    string
	nonce    string
	released bool
}

// claudeContextLeasePath is the marker for one CTX under a manager layout.
func claudeContextLeasePath(layout Layout, ctxID string) (string, error) {
	normalized := normalizeClaudeContextKey(ctxID)
	if err := validateClaudeContextKey(normalized); err != nil {
		return "", err
	}
	return filepath.Join(
		layout.StateDir, claudeContextLeaseDirName, normalized+".writer",
	), nil
}

// acquireClaudeContextWriterLease admits exactly one writer per materialization.
//
// Claude has no lock of its own: the accepted research measured two concurrent
// non-fork resumes of one session id both succeeding, both writing, and the
// next resume reconstructing only one leaf — silent loss of a whole run's work.
// Every materialization here mints its own session identity so that branch
// cannot form, and this lease is the second half of the same invariant: it makes
// a second writer against one materialization a refusal with a reason instead of
// a race whose loser disappears.
//
// The marker file is created once and then never unlinked. Unlinking it would
// reintroduce exactly the race the lock removes: a second acquirer that already
// holds the old inode open would lock a file no longer reachable by name, while
// a third created a new inode at the same name and locked that, leaving two
// writers each convinced they were alone.
func acquireClaudeContextWriterLease(
	layout Layout,
	ctxID string,
	now func() time.Time,
) (*claudeContextLease, error) {
	path, err := claudeContextLeasePath(layout, ctxID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("creating Claude context lease directory: %w", err)
	}
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("securing Claude context lease directory: %w", err)
	}
	if now == nil {
		now = time.Now
	}
	normalized := normalizeClaudeContextKey(ctxID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening Claude context writer lease: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		holder := claudeContextLeaseHolder(path)
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, claudeContextWriterBusyError(normalized, holder)
		}
		return nil, fmt.Errorf("acquiring Claude context writer lease: %w", err)
	}
	nonce, err := newClaudeSessionUUID()
	if err != nil {
		_ = releaseClaudeFlock(file)
		return nil, err
	}
	lease := &claudeContextLease{file: file, path: path, ctxID: normalized, nonce: nonce}
	stamp := now().UTC()
	if err := lease.write(claudeContextLeaseMarker{
		CtxID:      normalized,
		HolderPID:  os.Getpid(),
		HolderHost: claudeContextLeaseHostname(),
		Nonce:      nonce,
		AcquiredAt: stamp,
		RenewedAt:  stamp,
	}); err != nil {
		_ = releaseClaudeFlock(file)
		return nil, err
	}
	return lease, nil
}

// write replaces the marker body under the held lock.
func (l *claudeContextLease) write(marker claudeContextLeaseMarker) error {
	encoded, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	// task-board-claude-removal-guard: allow l.file.Truncate; rewrites only the lease marker body under its held advisory lock, never a provider artifact.
	if err := l.file.Truncate(0); err != nil {
		return fmt.Errorf("recording Claude context writer lease: %w", err)
	}
	if _, err := l.file.WriteAt(encoded, 0); err != nil {
		return fmt.Errorf("recording Claude context writer lease: %w", err)
	}
	return l.file.Sync()
}

// heartbeat refreshes the marker's renewal stamp.
//
// Nothing depends on it for exclusion — the lock does that. It exists so an
// operator inspecting a lease can tell a working holder from a hung one, and so
// `session status` output about a long materialization is not silently frozen.
func (l *claudeContextLease) heartbeat(at time.Time) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.file == nil {
		return nil
	}
	marker := claudeContextLeaseMarker{
		CtxID:      l.ctxID,
		HolderPID:  os.Getpid(),
		HolderHost: claudeContextLeaseHostname(),
		Nonce:      l.nonce,
		AcquiredAt: claudeContextLeaseAcquiredAt(l.path, at),
		RenewedAt:  at.UTC(),
	}
	return l.write(marker)
}

// release drops the lease. It is safe to call more than once so a failure path,
// a natural process exit, and Stop can all call it.
func (l *claudeContextLease) release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.file == nil {
		return
	}
	l.released = true
	// Recording the release keeps the marker honest for the next acquirer's
	// diagnostics; the lock is what actually admits it.
	_ = l.write(claudeContextLeaseMarker{
		CtxID:      l.ctxID,
		HolderPID:  os.Getpid(),
		HolderHost: claudeContextLeaseHostname(),
		Nonce:      l.nonce,
		AcquiredAt: claudeContextLeaseAcquiredAt(l.path, time.Now()),
		RenewedAt:  time.Now().UTC(),
		ReleasedAt: time.Now().UTC(),
	})
	_ = releaseClaudeFlock(l.file)
	l.file = nil
}

func releaseClaudeFlock(file *os.File) error {
	if file == nil {
		return nil
	}
	// Closing the descriptor drops the lock on its own; unlocking first makes
	// the intent explicit and keeps the window between the two empty.
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return file.Close()
}

// claudeContextLeaseHolder reports the recorded holder pid of an existing
// marker, for the refusal message only. A marker that cannot be read yields 0
// and a generic holder description rather than a failure: the lock already
// established that someone holds it.
func claudeContextLeaseHolder(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var marker claudeContextLeaseMarker
	if err := json.Unmarshal(raw, &marker); err != nil {
		return 0
	}
	return marker.HolderPID
}

func claudeContextLeaseAcquiredAt(path string, fallback time.Time) time.Time {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fallback.UTC()
	}
	var marker claudeContextLeaseMarker
	if err := json.Unmarshal(raw, &marker); err != nil || marker.AcquiredAt.IsZero() {
		return fallback.UTC()
	}
	return marker.AcquiredAt.UTC()
}

func claudeContextLeaseHostname() string {
	name, err := os.Hostname()
	if err != nil {
		return ""
	}
	return name
}

func claudeContextWriterBusyError(ctxID string, holderPID int) error {
	holder := "another manager operation"
	if holderPID > 0 {
		holder = fmt.Sprintf("process %d", holderPID)
	}
	return &PublicError{
		Code: claudeContextWriterBusy,
		Message: fmt.Sprintf(
			"Claude context %s is already being materialized by %s; one writer per materialization is "+
				"enforced because concurrent writers to one Claude session branch its transcript and one "+
				"branch is then silently dropped. Take a second fork instead of a second writer",
			ctxID, holder,
		),
	}
}
