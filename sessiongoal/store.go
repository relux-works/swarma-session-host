package sessiongoal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"time"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type journal struct {
	Schema    int                       `json:"schema"`
	Sessions  map[string]*sessionState  `json:"sessions"`
	Envelopes map[string]envelopeRecord `json:"envelopes"`
}
type sessionState struct {
	Holder     string              `json:"holder"`
	Generation uint64              `json:"generation"`
	Heads      map[Namespace]*Head `json:"heads"`
}
type envelopeRecord struct {
	Digest     string  `json:"digest"`
	Principal  string  `json:"principal"`
	Generation uint64  `json:"generation"`
	SessionID  string  `json:"session_id"`
	Receipt    Receipt `json:"receipt"`
}
type Service struct {
	root      string
	authority Authority
	mu        sync.Mutex
	uncertain bool
}

func Open(directory string, authority Authority) (*Service, error) {
	root, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("%w: goal directory must be a private0700 directory", ErrJournalInvalid)
	}
	s := &Service{root: root, authority: authority}
	if err = s.transaction(context.Background(), false, func(*journal) error { return nil }); err != nil {
		return nil, err
	}
	return s, nil
}

// Transactions use an OS file lock shared by independent Service instances.
// Provider/authority I/O is never performed under this journal lock.
func (s *Service) transaction(ctx context.Context, write bool, fn func(*journal) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain {
		return ErrCommitUncertain
	}
	lockPath := filepath.Join(s.root, "journal.lock")
	if info, err := os.Lstat(lockPath); err == nil && (!info.Mode().IsRegular() || info.Mode().Perm() != 0600) {
		return ErrJournalInvalid
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	data, err := readJournal(filepath.Join(s.root, "journal.json"))
	if err != nil {
		return err
	}
	if err = fn(data); errors.Is(err, errNoChange) {
		return nil
	} else if err != nil {
		return err
	}
	if !write {
		return nil
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return s.commit(data)
}
func readJournal(path string) (*journal, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return &journal{Schema: 1, Sessions: map[string]*sessionState{}, Envelopes: map[string]envelopeRecord{}}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() >= 64<<20 {
		return nil, ErrJournalInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 64<<20))
	decoder.DisallowUnknownFields()
	var data journal
	if err = decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJournalInvalid, err)
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return nil, ErrJournalInvalid
	}
	if err = validateJournal(&data); err != nil {
		return nil, err
	}
	return &data, nil
}
func validateJournal(j *journal) error {
	if j.Schema != 1 || j.Sessions == nil || j.Envelopes == nil {
		return ErrJournalInvalid
	}
	for id, session := range j.Sessions {
		if id == "" || session == nil || session.Holder == "" || session.Generation == 0 || session.Heads == nil {
			return ErrJournalInvalid
		}
		for namespace, head := range session.Heads {
			if !validNamespace(namespace) || head == nil || head.Latest.SessionID != id || head.Latest.Namespace != namespace || head.Latest.Ref.ID != referenceID(id, namespace) || head.Latest.Ref.Revision < 1 || len(head.History) != head.Latest.Ref.Revision {
				return ErrJournalInvalid
			}
			for i, r := range head.History {
				if r.Ref.ID != head.Latest.Ref.ID || r.Ref.Revision != i+1 || r.SessionID != id || r.Namespace != namespace || r.SourceEnvelopeID == "" || r.Principal == "" || r.Holder == "" || r.HolderGeneration == 0 || r.HolderGeneration > session.Generation || !validContent(r.Content) || (r.State != Active && r.State != Accepted && r.State != Cleared) {
					return ErrJournalInvalid
				}
				record, ok := j.Envelopes[r.SourceEnvelopeID]
				if !ok || record.Principal != r.Principal || record.Generation != r.HolderGeneration || record.SessionID != id || record.Receipt.Ref != r.Ref || record.Receipt.Holder != r.Holder || record.Receipt.SourceEnvelopeID != r.SourceEnvelopeID || len(record.Digest) != 64 {
					return ErrJournalInvalid
				}
			}
			if !reflect.DeepEqual(head.History[len(head.History)-1], head.Latest) {
				return ErrJournalInvalid
			}
			if head.Applied != nil && (head.Applied.ID != head.Latest.Ref.ID || head.Applied.Revision < 1 || head.Applied.Revision > head.Latest.Ref.Revision || head.AppliedReceipt == nil || head.AppliedReceipt.ID == "" || head.AppliedReceipt.SessionID != id || head.AppliedReceipt.Epoch == 0) {
				return ErrJournalInvalid
			}
			if head.Attempt != nil && (head.Attempt.Ref != head.Latest.Ref || head.Attempt.Lease.SessionID != id || head.Attempt.Lease.Epoch == 0 || head.Attempt.InputID == "") {
				return ErrJournalInvalid
			}
			if head.Rollback != nil && (head.Rollback.Lease.SessionID != id || head.Rollback.Lease.Epoch == 0 || head.Rollback.InputID == "") {
				return ErrJournalInvalid
			}
			if head.Acknowledged != nil && (head.Acknowledged.Ref != head.Latest.Ref || head.Acknowledged.Receipt.ID == "" || head.Acknowledged.Receipt.SessionID != id) {
				return ErrJournalInvalid
			}
			for _, attempt := range []*deliveryAttempt{head.Attempt, head.Rollback} {
				if attempt != nil && (attempt.Serial == 0 || attempt.Serial > head.DeliverySerial || attempt.Input.ID != attempt.InputID || attempt.Input.Lease != attempt.Lease) {
					return ErrJournalInvalid
				}
			}
			rollbackReceipts := map[core.InputReceipt]bool{}
			for _, rollback := range head.Rollbacks {
				if rollback.Receipt.ID == "" || rollback.Receipt.SessionID != id || rollback.Receipt.Epoch == 0 || rollbackReceipts[rollback.Receipt] {
					return ErrJournalInvalid
				}
				if rollback.Target != nil && (rollback.Target.ID != head.Latest.Ref.ID || rollback.Target.Revision < 1 || rollback.Target.Revision > len(head.History)) {
					return ErrJournalInvalid
				}
				rollbackReceipts[rollback.Receipt] = true
			}
			applied := map[core.InputReceipt]bool{}
			currentApplied := head.Applied == nil
			for _, a := range head.Applications {
				if a.Ref.ID != head.Latest.Ref.ID || a.Ref.Revision < 1 || a.Ref.Revision > len(head.History) || applied[a.Receipt] || a.Receipt.ID == "" || a.Receipt.SessionID != id || a.Receipt.Epoch == 0 {
					return ErrJournalInvalid
				}
				revision := head.History[a.Ref.Revision-1]
				if a.SourceEnvelopeID != revision.SourceEnvelopeID || a.Holder != revision.Holder || a.HolderGeneration != revision.HolderGeneration {
					return ErrJournalInvalid
				}
				applied[a.Receipt] = true
				if head.Applied != nil && a.Ref == *head.Applied && head.AppliedReceipt != nil && a.Receipt == *head.AppliedReceipt {
					currentApplied = true
				}
			}
			if !currentApplied {
				return ErrJournalInvalid
			}
		}
	}
	return nil
}
func (s *Service) commit(data *journal) error {
	if err := validateJournal(data); err != nil {
		return err
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if len(payload) >= 64<<20 {
		return fmt.Errorf("goal journal capacity reached")
	}
	temp, err := os.CreateTemp(s.root, ".goal-journal-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if _, err = temp.Write(payload); err != nil {
		_ = temp.Close()
		return err
	}
	if err = temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(s.root, "journal.json")); err != nil {
		return err
	}
	dir, err := os.Open(s.root)
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	if err != nil {
		s.uncertain = true
		return errors.Join(ErrCommitUncertain, err)
	}
	return nil
}
