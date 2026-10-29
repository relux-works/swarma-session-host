package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

// Resolve only this writer's retained replacement attempt, never arbitrary
// decoded bytes. An observed old base permits retry; a visible candidate needs
// a fresh exact codec receipt before uncertainty can be cleared.
func (s *codecRecords) recoverAttachmentReplacement(ctx context.Context, id string) (RunnerRecord, error) {
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return RunnerRecord{}, err
	}
	defer s.mu.Unlock()
	old, ok := s.bases[id]
	expected, attempted := s.pendingReplacement[id]
	if !ok || !attempted || !s.uncertain[id] {
		return RunnerRecord{}, ErrLegacyCommitUncertain
	}
	path, err := s.path(id)
	if err != nil {
		return RunnerRecord{}, err
	}
	actual, err := historyguard.ReadImage(path, 16<<20)
	if err != nil {
		return RunnerRecord{}, err
	}
	if bytes.Equal(actual, old.Bytes) {
		r, err := s.decode(ctx, old)
		if err != nil {
			return RunnerRecord{}, err
		}
		if err := s.validateFixedPoint(ctx, old, r); err != nil {
			return RunnerRecord{}, err
		}
		if err := s.matchesCurrent(path, old); err != nil {
			return RunnerRecord{}, err
		}
		delete(s.uncertain, id)
		delete(s.pendingReplacement, id)
		return r, nil
	}
	if !bytes.Equal(actual, expected.Bytes) {
		return RunnerRecord{}, ErrLegacyViewStale
	}
	r, err := s.decode(ctx, expected)
	if err != nil {
		return RunnerRecord{}, err
	}
	if err := s.validateFixedPoint(ctx, expected, r); err != nil {
		return RunnerRecord{}, err
	}
	encoded, err := s.codec.EncodeMerge(ctx, copyLegacyRecordImage(old), copyRunnerRecord(r))
	if err != nil || !bytes.Equal(encoded, expected.Bytes) {
		return RunnerRecord{}, errors.Join(ErrLegacyViewStale, err)
	}
	if err := s.acknowledgeCommitted(ctx, path, expected); err != nil {
		return RunnerRecord{}, err
	}
	s.bases[id] = copyLegacyRecordImage(expected)
	delete(s.uncertain, id)
	delete(s.pendingReplacement, id)
	return r, nil
}

// The retained native operation owns these observations. A retry can finish
// disposal of its original directory or acknowledge its own observed absence;
// it cannot claim a replacement empty directory merely because the path matches.
type attachmentDiscardState struct {
	removed          bool
	directory        os.FileInfo
	directoryRemoved bool
}

// Only the sole writer calls this after its owned final commit and native join.
// State advances at actual filesystem boundaries even when a later step fails.
func (s *codecRecords) discardAttachment(ctx context.Context, namespace string, base LegacyRecordInput, state *attachmentDiscardState) error {
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	ack, ok := s.codec.(LegacyRecordDiscardAcknowledger)
	if !ok || absentHostPort(ack) {
		return ErrHostCapabilityUnavailable
	}
	path, err := s.path(base.SessionID)
	if err != nil {
		return err
	}
	if base.SHA256 == "" || s.bases[base.SessionID].SHA256 != base.SHA256 || s.uncertain[base.SessionID] {
		return ErrLegacyViewStale
	}
	dir := filepath.Dir(path)
	info, statErr := os.Lstat(dir)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	if statErr == nil {
		if !info.IsDir() || state.directoryRemoved || state.directory != nil && !os.SameFile(state.directory, info) {
			return ErrLegacyViewStale
		}
		if state.directory == nil {
			state.directory = info
		}
	} else if !state.removed {
		return ErrLegacyViewStale
	}
	if !state.removed {
		if err := s.matchesCurrent(path, base); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		state.removed = true
	} else if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(ErrLegacyViewStale, err)
	}
	if statErr == nil {
		if err := s.syncDiscardDirectory(dir); err != nil {
			return err
		}
		// Remove only this exact empty owned directory. Never recurse into contents.
		latest, err := os.Lstat(dir)
		if err != nil || !os.SameFile(state.directory, latest) {
			return errors.Join(ErrLegacyViewStale, err)
		}
		if err := os.Remove(dir); err != nil {
			return err
		}
	}
	state.directoryRemoved = true
	if err := s.syncDiscardDirectory(s.root); err != nil {
		return err
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(ErrLegacyViewStale, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ack.AcknowledgeDiscarded(ctx, LegacyRecordDiscard{NamespaceFingerprint: namespace, SessionID: base.SessionID, DiscardedSHA256: base.SHA256}); err != nil {
		return err
	}
	delete(s.bases, base.SessionID)
	delete(s.uncertain, base.SessionID)
	return nil
}
func syncAttachmentDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (s *codecRecords) syncDiscardDirectory(path string) error {
	if s.discardSync != nil {
		return s.discardSync(path)
	}
	return syncAttachmentDirectory(path)
}
