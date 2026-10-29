// Package historyguard binds complete admission images to durable committed
// and pending recovery copies in the existing lock witness. Callers hold the
// file fences during access. The old marker API remains for retained legacy
// low-level tests; production admission uses the bound-image protocol.
package historyguard

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const marker = "history-v1\n"
const pendingMarker = "pending-v1\n"

var ErrUncertain = errors.New("admission history witness is uncertain")
var stores sync.Map

// Guard shares an uncertainty latch among all live handles of one store. A new
// process has no admitted handles: each constructor must reconcile disk state.
type Guard struct{ closed atomic.Bool }

func For(lockPath string) *Guard {
	value, _ := stores.LoadOrStore(filepath.Clean(lockPath), &Guard{})
	return value.(*Guard)
}
func (g *Guard) Close()       { g.closed.Store(true) }
func (g *Guard) Reconciled()  { g.closed.Store(false) }
func (g *Guard) Closed() bool { return g.closed.Load() }

func open(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, ErrUncertain
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	actual, err := f.Stat()
	if err != nil || !os.SameFile(info, actual) {
		f.Close()
		return nil, ErrUncertain
	}
	return f, nil
}

func read(f *os.File) (bool, error) {
	raw, err := io.ReadAll(io.LimitReader(f, int64(len(marker)+1)))
	if err != nil {
		return false, err
	}
	if len(raw) == 0 {
		return false, nil
	}
	if !bytes.Equal(raw, []byte(marker)) && !bytes.Equal(raw, []byte(pendingMarker)) {
		return false, ErrUncertain
	}
	return true, nil
}

func HasHistory(lockPath string) (bool, error) {
	f, err := open(lockPath)
	if err != nil {
		return false, err
	}
	defer f.Close()
	return read(f)
}

// MarkHistory runs before the first nonempty image can commit. It never clears
// or truncates the witness. Every recommit syncs it, including reconciliation
// after an earlier uncertain fsync; malformed/partial bytes close admission.
func MarkHistory(lockPath string) error {
	f, err := open(lockPath)
	if err != nil {
		return err
	}
	defer f.Close()
	used, err := read(f)
	if err != nil {
		return err
	}
	if !used {
		if _, err := f.WriteAt([]byte(marker), 0); err != nil {
			return err
		}
	}
	return f.Sync()
}

// A dirty witness is durable before a registry commit can expose new keys. It
// closes already-reconciled handles in other processes after an uncertain write.
func BeginCommit(lockPath string) error  { return writeMarker(lockPath, pendingMarker) }
func FinishCommit(lockPath string) error { return writeMarker(lockPath, marker) }
func writeMarker(lockPath, value string) error {
	return writeMarkerWithSync(lockPath, value, (*os.File).Sync)
}

func writeMarkerWithSync(lockPath, value string, syncFile func(*os.File) error) (retErr error) {
	f, err := open(lockPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := read(f); err != nil {
		return err
	}
	defer func() {
		if retErr != nil && value == marker {
			// A failed clear remains visibly dirty for other live processes.
			// After restart every constructor still requires reconciliation.
			_, _ = f.WriteAt([]byte(pendingMarker), 0)
			_ = f.Sync()
		}
	}()
	if _, err := f.WriteAt([]byte(value), 0); err != nil {
		return err
	}
	return syncFile(f)
}
func CommitUncertain(lockPath string) (bool, error) {
	f, err := open(lockPath)
	if err != nil {
		return false, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, int64(len(marker)+1)))
	if err != nil {
		return false, err
	}
	if len(raw) == 0 || bytes.Equal(raw, []byte(marker)) {
		return false, nil
	}
	if bytes.Equal(raw, []byte(pendingMarker)) {
		return true, nil
	}
	return false, ErrUncertain
}
