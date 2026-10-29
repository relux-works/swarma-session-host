package sessionmanager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
)

var ErrCodecRequired = errors.New("explicit fork runner codec is required")
var ErrLegacyViewStale = errors.New("legacy runner record view is stale")
var ErrLegacyCodecMismatch = errors.New("legacy runner codec is incompatible")
var ErrLegacyCommitUncertain = errors.New("legacy runner record commit requires recovery")

// codecRecords is the host-owned record transaction. Its caller holds the
// existing lifetime instance lock and controller fence. mu orders record work
// within that host; the codec never receives the path or a write capability.
// Only exact committed images are retained here, never a decoded private view.
type codecRecords struct {
	mu                 sync.Mutex
	root               string
	codec              ForkRunnerCodec
	bases              map[string]LegacyRecordInput
	uncertain          map[string]bool
	pendingInitial     map[string]LegacyRecordInput
	pendingReplacement map[string]LegacyRecordInput
	commit             func(string, []byte) error
	discardSync        func(string) error
}

func nilCodec(codec ForkRunnerCodec) bool {
	if codec == nil {
		return true
	}
	value := reflect.ValueOf(codec)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	}
	return false
}

func newCodecRecords(root string, codec ForkRunnerCodec) (*codecRecords, error) {
	if nilCodec(codec) {
		return nil, ErrCodecRequired
	}
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("absolute session record directory required")
	}
	return &codecRecords{root: root, codec: codec, bases: map[string]LegacyRecordInput{}, uncertain: map[string]bool{}, pendingInitial: map[string]LegacyRecordInput{}, commit: writePrivateFile}, nil
}

func legacyRecordImage(sessionID string, data []byte) LegacyRecordInput {
	hash := sha256.Sum256(data)
	return LegacyRecordInput{SessionID: sessionID, SHA256: hex.EncodeToString(hash[:]), Bytes: append([]byte(nil), data...)}
}
func copyLegacyRecordImage(src LegacyRecordInput) LegacyRecordInput {
	out := src
	out.Bytes = append([]byte(nil), src.Bytes...)
	return out
}
func validLegacySessionID(id string) bool {
	return strings.HasPrefix(id, "SES-") && len(id) >= len("SES-")+16 && filepath.Base(id) == id && !strings.ContainsAny(id, `/\`)
}
func (s *codecRecords) path(id string) (string, error) {
	if !validLegacySessionID(id) {
		return "", fmt.Errorf("invalid opaque session ID")
	}
	return filepath.Join(s.root, id, "record.json"), nil
}

func validateNeutralRunnerRecord(record RunnerRecord) error {
	if !validLegacySessionID(record.SessionID) || record.Owner == "" || record.Provider != record.ProviderHost.Provider {
		return fmt.Errorf("invalid neutral runner record identity")
	}
	if record.Contract != sessionRecordContract && record.Contract != legacySessionRecordContractV2 && record.Contract != legacySessionRecordContract {
		return fmt.Errorf("unsupported session record contract %q", record.Contract)
	}
	if (record.ProjectDir == "") != (record.NamespaceFingerprint == "") || record.ProjectDir != "" && !filepath.IsAbs(record.ProjectDir) {
		return fmt.Errorf("invalid session location")
	}
	switch record.State {
	case SessionRunning, SessionRecovering, SessionStopped:
	default:
		return fmt.Errorf("unsupported session state %q", record.State)
	}
	switch record.Liveness {
	case "", SessionLivenessLive, SessionLivenessDead, SessionLivenessUnknown:
	default:
		return fmt.Errorf("unsupported session liveness %q", record.Liveness)
	}
	if record.CreatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) {
		return fmt.Errorf("invalid ordered session timestamps")
	}
	if err := validateRunnerSnapshot(record.ProviderHost); err != nil {
		return err
	}
	if record.ConditionalStop != nil {
		intent := record.ConditionalStop
		if intent.Precondition != "not-busy" || (intent.Phase != "" && intent.Phase != "signal-attempt") || intent.Signalled && intent.Phase != "signal-attempt" {
			return fmt.Errorf("invalid conditional-stop recovery intent")
		}
	}
	_, err := cloneRunnerRecord(record)
	return err
}

func (s *codecRecords) decode(ctx context.Context, base LegacyRecordInput) (RunnerRecord, error) {
	if err := ctx.Err(); err != nil {
		return RunnerRecord{}, err
	}
	record, err := s.codec.DecodeLegacy(ctx, copyLegacyRecordImage(base))
	if err != nil {
		return RunnerRecord{}, err
	}
	if record.SessionID != base.SessionID {
		return RunnerRecord{}, fmt.Errorf("%w: decoded session identity", ErrLegacyViewStale)
	}
	if err := validateNeutralRunnerRecord(record); err != nil {
		return RunnerRecord{}, fmt.Errorf("%w: %v", ErrLegacyCodecMismatch, err)
	}
	return cloneRunnerRecord(record)
}

func (s *codecRecords) validateFixedPoint(ctx context.Context, base LegacyRecordInput, record RunnerRecord) error {
	owned, err := cloneRunnerRecord(record)
	if err != nil {
		return err
	}
	roundTrip, err := s.codec.EncodeMerge(ctx, copyLegacyRecordImage(base), owned)
	if err != nil {
		return err
	}
	if !bytes.Equal(base.Bytes, roundTrip) {
		return fmt.Errorf("%w: no-op encoding changed legacy bytes", ErrLegacyCodecMismatch)
	}
	return ctx.Err()
}

// load validates the exact no-op fixed point before returning anything a host
// could Restore. Re-loading is also the recovery route after uncertain commit;
// it observes the file actually committed, never the attempted neutral state.
func (s *codecRecords) load(ctx context.Context, id string) (RunnerRecord, error) {
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return RunnerRecord{}, err
	}
	defer s.mu.Unlock()
	path, err := s.path(id)
	if err != nil {
		return RunnerRecord{}, err
	}
	data, err := historyguard.ReadImage(path, 16<<20)
	if err != nil {
		return RunnerRecord{}, err
	}
	base := legacyRecordImage(id, data)
	record, err := s.decode(ctx, base)
	if err != nil {
		return RunnerRecord{}, err
	}
	if err = s.validateFixedPoint(ctx, base, record); err != nil {
		return RunnerRecord{}, err
	}
	if err = s.matchesCurrent(path, base); err != nil {
		return RunnerRecord{}, err
	}
	s.bases[id] = copyLegacyRecordImage(base)
	delete(s.uncertain, id)
	return cloneRunnerRecord(record)
}

func (s *codecRecords) matchesCurrent(path string, base LegacyRecordInput) error {
	data, err := historyguard.ReadImage(path, 16<<20)
	if err != nil {
		return fmt.Errorf("%w: reading current record: %w", ErrLegacyViewStale, err)
	}
	current := legacyRecordImage(base.SessionID, data)
	if current.SHA256 != base.SHA256 || !bytes.Equal(current.Bytes, base.Bytes) {
		return ErrLegacyViewStale
	}
	return nil
}

// acknowledgeCommitted is called only after this sole writer commits. A nil
// write result alone is insufficient: observe the exact visible image before
// giving the private codec authority to consume its encoded generation.
func (s *codecRecords) acknowledgeCommitted(ctx context.Context, path string, input LegacyRecordInput) error {
	if err := s.matchesCurrent(path, input); err != nil {
		s.uncertain[input.SessionID] = true
		return fmt.Errorf("%w: %w", ErrLegacyCommitUncertain, err)
	}
	if err := s.codec.AcknowledgeCommitted(ctx, copyLegacyRecordImage(input)); err != nil {
		s.uncertain[input.SessionID] = true
		return fmt.Errorf("%w: %w", ErrLegacyCommitUncertain, err)
	}
	return nil
}

// update is the single replacement route. The codec sees copies, and both
// validation and current-image checks complete before the atomic commit.
// Any commit error closes this session until a committed-image reload succeeds;
// after rename the visible record may already be new, so this is uncertain.
func (s *codecRecords) update(ctx context.Context, record RunnerRecord) error {
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	path, err := s.path(record.SessionID)
	if err != nil {
		return err
	}
	if s.uncertain[record.SessionID] {
		return ErrLegacyCommitUncertain
	}
	base, ok := s.bases[record.SessionID]
	if !ok {
		return ErrLegacyViewStale
	}
	if err = s.matchesCurrent(path, base); err != nil {
		return err
	}
	current, err := s.decode(ctx, base)
	if err != nil {
		return err
	}
	if current.Owner != record.Owner || current.Provider != record.Provider || current.ProviderHost.Snapshot.Kind != record.ProviderHost.Snapshot.Kind {
		return fmt.Errorf("%w: immutable record identity changed", ErrLegacyViewStale)
	}
	if err = validateNeutralRunnerRecord(record); err != nil {
		return err
	}
	owned, err := cloneRunnerRecord(record)
	if err != nil {
		return err
	}
	data, err := s.codec.EncodeMerge(ctx, copyLegacyRecordImage(base), owned)
	if err != nil {
		return err
	}
	data = append([]byte(nil), data...)
	encodedBase := legacyRecordImage(record.SessionID, data)
	decoded, err := s.decode(ctx, encodedBase)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(decoded, record) {
		return fmt.Errorf("%w: encoded record does not project requested state", ErrLegacyCodecMismatch)
	}
	if err = s.validateFixedPoint(ctx, encodedBase, decoded); err != nil {
		return err
	}
	if err = s.matchesCurrent(path, base); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if s.pendingReplacement == nil {
		s.pendingReplacement = map[string]LegacyRecordInput{}
	}
	s.pendingReplacement[record.SessionID] = copyLegacyRecordImage(encodedBase)
	if err = s.commit(path, data); err != nil {
		s.uncertain[record.SessionID] = true
		return fmt.Errorf("%w: %w", ErrLegacyCommitUncertain, err)
	}
	if err = s.acknowledgeCommitted(ctx, path, encodedBase); err != nil {
		return err
	}
	s.bases[record.SessionID] = copyLegacyRecordImage(encodedBase)
	delete(s.pendingReplacement, record.SessionID)
	return nil
}

// create is private to the host's new-session admission path. A loaded, seen,
// uncertain or existing identity cannot be treated as a new empty view.
func (s *codecRecords) create(ctx context.Context, record RunnerRecord) error {
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.createLocked(ctx, record)
}
func (s *codecRecords) createLocked(ctx context.Context, record RunnerRecord) error {
	path, err := s.path(record.SessionID)
	if err != nil {
		return err
	}
	if _, ok := s.bases[record.SessionID]; ok || s.uncertain[record.SessionID] {
		return ErrLegacyViewStale
	}
	if _, err = os.Lstat(filepath.Dir(path)); err == nil {
		return ErrLegacyViewStale
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = validateNeutralRunnerRecord(record); err != nil {
		return err
	}
	owned, err := cloneRunnerRecord(record)
	if err != nil {
		return err
	}
	data, err := s.codec.EncodeInitial(ctx, owned)
	if err != nil {
		return err
	}
	data = append([]byte(nil), data...)
	base := legacyRecordImage(record.SessionID, data)
	decoded, err := s.decode(ctx, base)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(decoded, record) {
		return fmt.Errorf("%w: initial record does not project admitted state", ErrLegacyCodecMismatch)
	}
	if err = s.validateFixedPoint(ctx, base, decoded); err != nil {
		return err
	}
	if _, err = os.Lstat(filepath.Dir(path)); err == nil {
		return ErrLegacyViewStale
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	s.pendingInitial[record.SessionID] = copyLegacyRecordImage(base)
	if err = s.commit(path, data); err != nil {
		s.uncertain[record.SessionID] = true
		return fmt.Errorf("%w: %w", ErrLegacyCommitUncertain, err)
	}
	if err = s.acknowledgeCommitted(ctx, path, base); err != nil {
		return err
	}
	s.bases[record.SessionID] = copyLegacyRecordImage(base)
	delete(s.pendingInitial, record.SessionID)
	return nil
}

// recoverInitial resolves only this live host's retained first-write attempt.
// A visible exact image is admitted by load validation. A provably absent
// record may be retried with the exact originally encoded bytes; foreign bytes
// or nonempty/symlink directory state refuse without a write or a new Launch.
func (s *codecRecords) recoverInitial(ctx context.Context, record RunnerRecord) (RunnerRecord, error) {
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return RunnerRecord{}, err
	}
	defer s.mu.Unlock()
	expected, attempted := s.pendingInitial[record.SessionID]
	if !attempted {
		expected, attempted = s.bases[record.SessionID]
	}
	if !attempted {
		if err := s.createLocked(ctx, record); err != nil {
			return RunnerRecord{}, err
		}
		return cloneRunnerRecord(record)
	}
	path, err := s.path(record.SessionID)
	if err != nil {
		return RunnerRecord{}, err
	}
	// The retained attempt belongs to this real session directory, including
	// when a linked directory happens to contain the expected record bytes.
	if info, statErr := os.Lstat(filepath.Dir(path)); statErr == nil {
		if !info.IsDir() {
			return RunnerRecord{}, ErrLegacyViewStale
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return RunnerRecord{}, statErr
	}
	actual, err := historyguard.ReadImage(path, 16<<20)
	if err == nil {
		if !bytes.Equal(actual, expected.Bytes) {
			return RunnerRecord{}, ErrLegacyViewStale
		}
		decoded, err := s.decode(ctx, expected)
		if err != nil {
			return RunnerRecord{}, err
		}
		if !reflect.DeepEqual(decoded, record) {
			return RunnerRecord{}, ErrLegacyCodecMismatch
		}
		if err = s.validateFixedPoint(ctx, expected, decoded); err != nil {
			return RunnerRecord{}, err
		}
		if err = s.matchesCurrent(path, expected); err != nil {
			return RunnerRecord{}, err
		}
		s.bases[record.SessionID] = copyLegacyRecordImage(expected)
		delete(s.uncertain, record.SessionID)
		delete(s.pendingInitial, record.SessionID)
		return cloneRunnerRecord(decoded)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return RunnerRecord{}, err
	}
	if err = emptyInitialLocation(path); err != nil {
		return RunnerRecord{}, err
	}
	owned, err := cloneRunnerRecord(record)
	if err != nil {
		return RunnerRecord{}, err
	}
	data, err := s.codec.EncodeInitial(ctx, owned)
	if err != nil {
		return RunnerRecord{}, err
	}
	if !bytes.Equal(data, expected.Bytes) {
		return RunnerRecord{}, ErrLegacyViewStale
	}
	decoded, err := s.decode(ctx, expected)
	if err != nil {
		return RunnerRecord{}, err
	}
	if !reflect.DeepEqual(decoded, record) {
		return RunnerRecord{}, ErrLegacyCodecMismatch
	}
	if err = s.validateFixedPoint(ctx, expected, decoded); err != nil {
		return RunnerRecord{}, err
	}
	if err = emptyInitialLocation(path); err != nil {
		return RunnerRecord{}, err
	}
	if err = ctx.Err(); err != nil {
		return RunnerRecord{}, err
	}
	if err = s.commit(path, append([]byte(nil), data...)); err != nil {
		s.uncertain[record.SessionID] = true
		return RunnerRecord{}, fmt.Errorf("%w: %w", ErrLegacyCommitUncertain, err)
	}
	if err = s.acknowledgeCommitted(ctx, path, expected); err != nil {
		return RunnerRecord{}, err
	}
	s.bases[record.SessionID] = copyLegacyRecordImage(expected)
	delete(s.uncertain, record.SessionID)
	delete(s.pendingInitial, record.SessionID)
	return cloneRunnerRecord(decoded)
}
func emptyInitialLocation(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return ErrLegacyViewStale
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(path)
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return ErrLegacyViewStale
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return ErrLegacyViewStale
	}
	return nil
}
