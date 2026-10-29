package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

const controllerEpochFile = "controller-epochs.v1.json"

var ErrControllerEpochUncertain = errors.New("controller epoch journal requires reconciliation")

type controllerEpochJournal struct {
	mu             sync.Mutex
	dir, namespace string
	uncertain      bool
	recovering     bool
	syncFile       func(*os.File) error
	rename         func(string, string) error
	pending        map[string]ControllerEpoch
	guard          *historyguard.Guard
	coordinator    *epochCoordinator
}

type epochCoordinator struct {
	mu     sync.Mutex
	inputs map[string]map[*sessionInput]struct{}
}

var epochCoordinators sync.Map

func coordinatorFor(path string) *epochCoordinator {
	value, _ := epochCoordinators.LoadOrStore(filepath.Clean(path), &epochCoordinator{inputs: make(map[string]map[*sessionInput]struct{})})
	return value.(*epochCoordinator)
}

type controllerEpochEntry struct {
	SessionID string          `json:"session_id"`
	HighWater ControllerEpoch `json:"high_water"`
}

type controllerEpochDocument struct {
	Seal          historyguard.ImageSeal `json:"seal"`
	Version       int                    `json:"version"`
	Namespace     string                 `json:"namespace"`
	Entries       []controllerEpochEntry `json:"entries"`
	RecoveryFloor ControllerEpoch        `json:"recovery_floor,omitempty"`
}

// InitializeControllerEpochJournal prepares only empty admission metadata.
// Composition calls it before creating the fork's owner registry. It issues no
// lease and grants no capability to choose or advance a controller generation.
func InitializeControllerEpochJournal(stateDir, namespace string) error {
	_, err := newControllerEpochJournal(stateDir, namespace)
	return err
}

// ControllerEpochJournalHasReservations lets fork bootstrap distinguish the
// host's verified empty metadata from prior session state without decoding it.
// Missing, corrupt and insecure journal state is an error, never an empty map.
func ControllerEpochJournalHasReservations(stateDir, namespace string) (bool, error) {
	j := &controllerEpochJournal{dir: stateDir, namespace: namespace, guard: historyguard.For(filepath.Join(stateDir, controllerEpochFile+".lock"))}
	var reserved bool
	err := j.withLock(context.Background(), false, func() error { doc, err := j.load(); reserved = len(doc.Entries) != 0; return err })
	return reserved, err
}

func newControllerEpochJournal(stateDir, namespace string) (*controllerEpochJournal, error) {
	if !filepath.IsAbs(stateDir) || namespace == "" || strings.TrimSpace(namespace) != namespace {
		return nil, fmt.Errorf("absolute host state directory and exact namespace required")
	}
	j := &controllerEpochJournal{dir: stateDir, namespace: namespace, syncFile: (*os.File).Sync, rename: os.Rename}
	j.guard = historyguard.For(filepath.Join(stateDir, controllerEpochFile+".lock"))
	j.coordinator = coordinatorFor(stateDir)
	_, err := os.Lstat(filepath.Join(stateDir, controllerEpochFile+".lock"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	priorJournal := err == nil
	_, imageErr := os.Lstat(filepath.Join(stateDir, controllerEpochFile))
	err = j.withLock(context.Background(), errors.Is(imageErr, os.ErrNotExist), func() error {
		_, err := j.load()
		if !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return err
			}
			// A clean reopen is read-only. Pending or mismatched images require
			// explicit reconciliation before another generation is admitted.
			doc, err := j.load()
			if err != nil {
				return err
			}
			if len(doc.Entries) == 0 {
				prior, err := j.sessionArtifacts()
				if err != nil {
					return err
				}
				if prior {
					return ErrControllerEpochUncertain
				}
			}
			return nil
		}
		prior, err := j.priorState()
		if err != nil {
			return err
		}
		if prior || priorJournal {
			return ErrControllerEpochUncertain
		}
		return j.commit(controllerEpochDocument{Version: 1, Namespace: namespace, Entries: []controllerEpochEntry{}})
	})
	if err != nil {
		return nil, err
	}
	j.guard.Reconciled()
	j.uncertain = false
	return j, nil
}

func (j *controllerEpochJournal) locked(fn func() error) error {
	return j.withLock(context.Background(), true, fn)
}
func (j *controllerEpochJournal) withLock(ctx context.Context, exclusive bool, fn func() error) error {
	if err := os.MkdirAll(j.dir, 0700); err != nil {
		return err
	}
	unlock, err := lockControllerEpochsContext(ctx, filepath.Join(j.dir, controllerEpochFile+".lock"), exclusive)
	if err != nil {
		return err
	}
	defer unlock()
	// File locks precede the local mutex. A physical writer retains a shared
	// file fence while rechecking admission; an exclusive waiter must not own
	// mu while waiting for that writer to release its shared fence.
	if err := historyguard.LockMutex(ctx, &j.mu); err != nil {
		return err
	}
	defer j.mu.Unlock()
	if err := historyguard.ValidateTempDirectories(j.dir); err != nil {
		return err
	}
	return fn()
}

func (j *controllerEpochJournal) priorState() (bool, error) {
	prior := false
	err := filepath.WalkDir(j.dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if skip, err := historyguard.SkipTempDirectory(j.dir, path); err != nil {
			return err
		} else if skip {
			return filepath.SkipDir
		}
		if path == j.dir || path == filepath.Join(j.dir, controllerEpochFile+".lock") || path == filepath.Join(j.dir, controllerEpochFile+".lock.fence") || path == filepath.Join(j.dir, "manager.lock") {
			return nil
		}
		if !entry.IsDir() {
			prior = true
		}
		return nil
	})
	return prior, err
}

func (j *controllerEpochJournal) load() (controllerEpochDocument, error) {
	return j.loadImage(false)
}

func (j *controllerEpochJournal) loadImage(recoverAttempt bool) (result controllerEpochDocument, retErr error) {
	defer func() {
		if retErr != nil {
			j.uncertain = true
			j.guard.Close()
		}
	}()
	var doc controllerEpochDocument
	path := filepath.Join(j.dir, controllerEpochFile)
	info, err := os.Lstat(path)
	if err != nil {
		return doc, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return doc, ErrControllerEpochUncertain
	}
	f, err := os.Open(path)
	if err != nil {
		return doc, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return doc, ErrControllerEpochUncertain
	}
	const maximumBytes = 16 << 20
	raw, err := io.ReadAll(io.LimitReader(f, maximumBytes+1))
	if err != nil {
		return doc, err
	}
	if len(raw) > maximumBytes {
		return doc, ErrControllerEpochUncertain
	}
	doc, err = j.parseImage(raw)
	if err != nil {
		return doc, err
	}
	if err := historyguard.CheckBound(filepath.Join(j.dir, controllerEpochFile+".lock"), raw, j.storeIdentity(), recoverAttempt); err != nil {
		return doc, fmt.Errorf("%w: witness binding: %v", ErrControllerEpochUncertain, err)
	}
	return doc, nil
}

func (j *controllerEpochJournal) parseImage(raw []byte) (controllerEpochDocument, error) {
	var doc controllerEpochDocument
	if json.Unmarshal(raw, &doc) != nil || doc.Version != 1 || doc.Namespace != j.namespace || doc.Entries == nil {
		return doc, ErrControllerEpochUncertain
	}
	canonical, err := json.Marshal(doc)
	if err != nil || !bytes.Equal(raw, canonical) {
		return doc, ErrControllerEpochUncertain
	}
	if uint64(doc.RecoveryFloor) >= math.MaxUint64-1 {
		return doc, ErrControllerEpochUncertain
	}
	previous := ""

	for _, entry := range doc.Entries {
		if strings.TrimSpace(entry.SessionID) != entry.SessionID || entry.SessionID == "" || entry.SessionID <= previous || entry.HighWater == 0 || uint64(entry.HighWater) == math.MaxUint64 {
			return doc, ErrControllerEpochUncertain
		}
		previous = entry.SessionID
	}
	return doc, nil
}

func (j *controllerEpochJournal) commit(doc controllerEpochDocument) (retErr error) {
	defer func() {
		if retErr != nil {
			j.uncertain = true
			j.guard.Close()
		}
	}()
	var err error
	doc.Seal, err = historyguard.NextSeal(doc.Seal, j.storeIdentity())
	if err != nil {
		return err
	}
	unsigned, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	doc.Seal.SHA256 = historyguard.Digest(unsigned)
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if err := historyguard.PrepareBound(filepath.Join(j.dir, controllerEpochFile+".lock"), raw, j.storeIdentity(), j.recovering); err != nil {
		return err
	}
	f, err := historyguard.CreateTemp(filepath.Join(j.dir, controllerEpochFile), ".controller-epochs-")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	if err := f.Chmod(0600); err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		return err
	}
	if err := j.syncFile(f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := j.rename(f.Name(), filepath.Join(j.dir, controllerEpochFile)); err != nil {
		return err
	}
	dir, err := os.Open(j.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := j.syncFile(dir); err != nil {
		return err
	}
	if err := historyguard.SyncTempDirectory(filepath.Join(j.dir, controllerEpochFile), j.syncFile); err != nil {
		return err
	}
	return historyguard.FinishBound(filepath.Join(j.dir, controllerEpochFile+".lock"), raw, j.storeIdentity())
}

func (j *controllerEpochJournal) reserve(sessionID string) (ControllerEpoch, error) {
	return j.reserveContext(context.Background(), sessionID)
}

func (j *controllerEpochJournal) reserveContext(ctx context.Context, sessionID string) (ControllerEpoch, error) {
	if sessionID == "" || strings.TrimSpace(sessionID) != sessionID {
		return 0, ErrInvalidInput
	}
	if err := historyguard.LockMutex(ctx, &j.coordinator.mu); err != nil {
		return 0, err
	}
	defer j.coordinator.mu.Unlock()
	for input := range j.coordinator.inputs[sessionID] {
		if err := input.revokeAndDrain(ctx); err != nil {
			return 0, err
		}
	}
	delete(j.coordinator.inputs, sessionID)
	var reserved ControllerEpoch
	err := j.withLock(ctx, true, func() error {
		unlockIO, err := lockControllerEpochIOContext(ctx, filepath.Join(j.dir, controllerEpochFile+".io.lock"), true)
		if err != nil {
			return err
		}
		defer unlockIO()
		if j.uncertain || j.guard.Closed() {
			return ErrControllerEpochUncertain
		}
		doc, err := j.load()
		if err != nil {
			j.uncertain = true
			return err
		}
		if len(doc.Entries) == 0 {
			prior, err := j.sessionArtifacts()
			if err != nil {
				return err
			}
			if prior {
				j.uncertain = true
				j.guard.Close()
				return ErrControllerEpochUncertain
			}
		}
		index := sort.Search(len(doc.Entries), func(i int) bool { return doc.Entries[i].SessionID >= sessionID })
		if index == len(doc.Entries) || doc.Entries[index].SessionID != sessionID {
			doc.Entries = append(doc.Entries, controllerEpochEntry{})
			copy(doc.Entries[index+1:], doc.Entries[index:])
			doc.Entries[index] = controllerEpochEntry{SessionID: sessionID}
		}
		if uint64(doc.Entries[index].HighWater) >= math.MaxUint64-1 {
			j.uncertain = true
			return ErrControllerEpochUncertain
		}
		if doc.Entries[index].HighWater < doc.RecoveryFloor {
			doc.Entries[index].HighWater = doc.RecoveryFloor
		}
		doc.Entries[index].HighWater++
		if j.pending == nil {
			j.pending = make(map[string]ControllerEpoch)
		}
		j.pending[sessionID] = doc.Entries[index].HighWater
		if err := j.commit(doc); err != nil {
			return err
		}
		reserved = doc.Entries[index].HighWater
		delete(j.pending, sessionID)
		return nil
	})
	return reserved, err
}

func (j *controllerEpochJournal) reconcile() error {
	return j.reconcileContext(context.Background())
}

func (j *controllerEpochJournal) reconcileContext(ctx context.Context) error {
	return j.withLock(ctx, true, func() error {
		releaseIO, err := lockControllerEpochIOContext(ctx, filepath.Join(j.dir, controllerEpochFile+".io.lock"), true)
		if err != nil {
			return err
		}
		defer releaseIO()
		j.uncertain = true
		j.recovering = true
		defer func() { j.recovering = false }()
		doc, err := j.recoveryImage()
		if err != nil {
			return err
		}
		if len(doc.Entries) == 0 {
			prior, err := j.sessionArtifacts()
			if err != nil {
				return err
			}
			if prior {
				return ErrControllerEpochUncertain
			}
		}
		for sessionID, highWater := range j.pending {
			i := sort.Search(len(doc.Entries), func(i int) bool { return doc.Entries[i].SessionID >= sessionID })
			if i == len(doc.Entries) || doc.Entries[i].SessionID != sessionID {
				doc.Entries = append(doc.Entries, controllerEpochEntry{})
				copy(doc.Entries[i+1:], doc.Entries[i:])
				doc.Entries[i] = controllerEpochEntry{SessionID: sessionID}
			}
			if doc.Entries[i].HighWater < highWater {
				doc.Entries[i].HighWater = highWater
			}
		}
		// An uncertain rename may have exposed a reserved value. Retain the
		// high-water, resync it, and advance past it on the next reservation.
		if err := j.commit(doc); err != nil {
			return err
		}
		j.uncertain = false
		j.guard.Reconciled()
		j.pending = nil
		return nil
	})
}

// Only host composition calls this constructor. Reservation is durable before
// the writer exists; a failed reservation exposes neither a lease nor I/O.
func (j *controllerEpochJournal) admitInput(lifetime context.Context, sessionID string, backend hostInputBackend) (*sessionInput, error) {
	if lifetime == nil || lifetime.Err() != nil || backend == nil {
		return nil, ErrInvalidInput
	}
	epoch, err := j.reserveContext(lifetime, sessionID)
	if err != nil {
		return nil, err
	}
	h, err := newSessionInput(lifetime, sessionID, epoch, backend)
	if err != nil {
		return nil, err
	}
	h.admission = func(ctx context.Context) bool { return j.admitsContext(ctx, sessionID, epoch) }
	h.beginIO = j.beginIO
	if err := j.registerInput(lifetime, h, epoch); err != nil {
		return nil, err
	}
	return h, nil
}

func (j *controllerEpochJournal) admits(sessionID string, epoch ControllerEpoch) bool {
	return j.admitsContext(context.Background(), sessionID, epoch)
}
func (j *controllerEpochJournal) admitsContext(ctx context.Context, sessionID string, epoch ControllerEpoch) bool {
	return j.withLock(ctx, false, func() error {
		if j.uncertain || j.guard.Closed() {
			return ErrControllerEpochUncertain
		}
		doc, err := j.load()
		if err != nil {
			j.uncertain = true
			return err
		}
		i := sort.Search(len(doc.Entries), func(i int) bool { return doc.Entries[i].SessionID >= sessionID })
		if i == len(doc.Entries) || doc.Entries[i].SessionID != sessionID || doc.Entries[i].HighWater != epoch {
			return ErrStaleInputController
		}
		return nil
	}) == nil
}

func (j *controllerEpochJournal) renewInput(ctx context.Context, h *sessionInput) error {
	if h == nil {
		return ErrInvalidInput
	}
	epoch, err := j.reserveContext(ctx, h.sessionID)
	if err != nil {
		return err
	}
	// Reservation first revokes/drains old I/O, then commits the next generation.
	// Cancellation during local installation leaves that generation burned.
	if err := h.advanceEpoch(ctx, epoch); err != nil {
		return err
	}
	h.mu.Lock()
	h.admission = func(ctx context.Context) bool { return j.admitsContext(ctx, h.sessionID, epoch) }
	h.mu.Unlock()
	return j.registerInput(ctx, h, epoch)
}

func (j *controllerEpochJournal) registerInput(ctx context.Context, h *sessionInput, epoch ControllerEpoch) error {
	if err := historyguard.LockMutex(ctx, &j.coordinator.mu); err != nil {
		return err
	}
	defer j.coordinator.mu.Unlock()
	if !j.admitsContext(ctx, h.sessionID, epoch) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrStaleInputController
	}
	if j.coordinator.inputs[h.sessionID] == nil {
		j.coordinator.inputs[h.sessionID] = make(map[*sessionInput]struct{})
	}
	j.coordinator.inputs[h.sessionID][h] = struct{}{}
	return nil
}

func (j *controllerEpochJournal) sessionArtifacts() (bool, error) {
	prior := false
	err := filepath.WalkDir(j.dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == j.dir {
			return nil
		}
		if filepath.Dir(path) == j.dir {
			if skip, err := historyguard.SkipTempDirectory(j.dir, path); err != nil {
				return err
			} else if skip {
				return filepath.SkipDir
			}
			switch entry.Name() {
			case controllerEpochFile, controllerEpochFile + ".lock", controllerEpochFile + ".lock.intent", "session-owner-keys.v1.json.lock.intent", controllerEpochFile + ".lock.fence", controllerEpochFile + ".io.lock", "manager.lock", "session-owner-keys.v1.json", "session-owner-keys.v1.json.lock", "session-owner-keys.v1.json.lock.fence", "session-owner-keys.v1.json.lock.io.fence":
				return nil // Admission metadata can precede the first session.
			}
		}
		if !entry.IsDir() {
			prior = true
		}
		return nil
	})
	return prior, err
}

// The cross-process fence covers physical provider writes and retries. A
// reservation takes its exclusive side before changing any durable generation.
func (j *controllerEpochJournal) beginIO(ctx context.Context, lease ControllerLease) (func(), error) {
	// Both independently named stable shared locks span physical I/O. Reservation takes both exclusive
	// sides: replacing either single path cannot split the cross-process fence.
	releaseMetadata, err := lockControllerEpochsContext(ctx, filepath.Join(j.dir, controllerEpochFile+".lock"), false)
	if err != nil {
		return nil, err
	}
	releaseIO, err := lockControllerEpochIOContext(ctx, filepath.Join(j.dir, controllerEpochFile+".io.lock"), false)
	if err != nil {
		releaseMetadata()
		return nil, err
	}
	release := func() { releaseIO(); releaseMetadata() }
	if !j.admitsContext(ctx, lease.SessionID, lease.Epoch) {
		release()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, ErrStaleInputController
	}
	return release, nil
}

func (j *controllerEpochJournal) recoveryImage() (controllerEpochDocument, error) {
	raw, _ := historyguard.ReadImage(filepath.Join(j.dir, controllerEpochFile), 16<<20)
	images, err := historyguard.RecoveryImages(filepath.Join(j.dir, controllerEpochFile+".lock"), raw, j.storeIdentity())
	if err != nil {
		return controllerEpochDocument{}, ErrControllerEpochUncertain
	}
	var best controllerEpochDocument
	var selected []byte
	for _, image := range images {
		doc, err := j.parseImage(image)
		if err != nil {
			continue
		}
		if doc.Seal.Sequence > best.Seal.Sequence {
			best = doc
			selected = image
		} else if doc.Seal.Sequence == best.Seal.Sequence && !bytes.Equal(selected, image) {
			return controllerEpochDocument{}, ErrControllerEpochUncertain
		}
	}
	if best.Seal.Sequence == 0 {
		return best, ErrControllerEpochUncertain
	}
	// If the matching witness was lost or restored from an older copy, one
	// unexposed interrupted reservation may have existed only there. Reserve
	// beyond the entire known high-water rather than guessing that attempt's
	// session. A valid pending witness already retains its exact attempted value.
	if err := historyguard.CheckBound(filepath.Join(j.dir, controllerEpochFile+".lock"), raw, j.storeIdentity(), true); err != nil {
		floor := best.RecoveryFloor
		for _, entry := range best.Entries {
			if entry.HighWater > floor {
				floor = entry.HighWater
			}
		}
		if uint64(floor) >= math.MaxUint64-2 {
			return best, ErrControllerEpochUncertain
		}
		best.RecoveryFloor = floor + 1
	}
	return best, nil
}

// ReconcileControllerEpochJournal is an explicit recovery entry after ordinary
// construction refuses a lost/substituted image or witness. It issues no lease.
// A verified image must survive in one of the bound copies; absent evidence is
// never bootstrap permission. Actual provider work is drained by both fences.
func ReconcileControllerEpochJournal(stateDir, namespace string) error {
	if !filepath.IsAbs(stateDir) || namespace == "" || strings.TrimSpace(namespace) != namespace {
		return ErrControllerEpochUncertain
	}
	j := &controllerEpochJournal{dir: stateDir, namespace: namespace, syncFile: (*os.File).Sync, rename: os.Rename, guard: historyguard.For(filepath.Join(stateDir, controllerEpochFile+".lock")), coordinator: coordinatorFor(stateDir)}
	return j.reconcile()
}

// The location distinguishes independent stores for the same board/namespace.
// A single-file copy from a genuine higher-sequence foreign store cannot win
// recovery selection over this store's independently verified history.
func (j *controllerEpochJournal) storeIdentity() string {
	return historyguard.StoreIdentity("controller-epochs", j.namespace+"\x00"+filepath.Clean(j.dir))
}
