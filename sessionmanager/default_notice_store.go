package sessionmanager

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

var ErrNoticeReceiptUnknown = errors.New("notice queue receipt is not issued for this session")
var ErrNoticeReceiptChanged = errors.New("notice queue receipt no longer names the observed bytes")

// SessionNoticeStore preserves the original queue format and inbox operations.
// Payload stays structured until the host renders the complete pending burst.
// Receipts are exact readable filenames; they convey no goal or run authority.
type SessionNoticeStore struct {
	mu       sync.Mutex
	inbox    *sessionNoticeInbox
	receipts map[string]map[string][sha256.Size]byte
}

var _ NoticeStore = (*SessionNoticeStore)(nil)

// NewSessionNoticeStore does not create state before NewHost admits its ports.
func NewSessionNoticeStore(layout Layout) (*SessionNoticeStore, error) {
	if !filepath.IsAbs(layout.NoticesDir) {
		return nil, fmt.Errorf("absolute notice inbox directory required")
	}
	return &SessionNoticeStore{inbox: newSessionNoticeInbox(layout.NoticesDir), receipts: make(map[string]map[string][sha256.Size]byte)}, nil
}

func (s *SessionNoticeStore) Enqueue(ctx context.Context, sessionID string, notice Notice) error {
	id, err := validateSessionNoticeSessionID(sessionID)
	if err != nil {
		return fmt.Errorf("%w: notice session", ErrInvalidInput)
	}
	if notice.Payload == nil || notice.Payload.SessionID != id || notice.ID != "" || notice.Text != "" || notice.SessionID != "" && notice.SessionID != id || !notice.CreatedAt.IsZero() && !notice.CreatedAt.Equal(notice.Payload.CreatedAt) {
		return fmt.Errorf("%w: structured notice payload and exact session required; receipts are returned by Pending", ErrInvalidInput)
	}
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Keep validation ahead of filesystem effects, as at the original producer
	// boundary. The original enqueue still owns contract/time/random identity.
	payload := *notice.Payload
	if strings.TrimSpace(payload.RunID) == "" {
		return fmt.Errorf("session notice requires a run id")
	}
	if strings.TrimSpace(payload.State) == "" {
		return fmt.Errorf("session notice requires a terminal state")
	}
	if err := secureNoticeStoreDirectory(s.inbox.dir); err != nil {
		return err
	}
	dir, err := s.inbox.sessionDir(id)
	if err != nil {
		return err
	}
	if err := secureNoticeStoreDirectory(dir); err != nil {
		return err
	}
	return s.inbox.enqueue(payload)
}

func (s *SessionNoticeStore) Pending(ctx context.Context, sessionID string) ([]Notice, error) {
	id, err := validateSessionNoticeSessionID(sessionID)
	if err != nil {
		return nil, fmt.Errorf("%w: notice session", ErrInvalidInput)
	}
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	payloads, paths, pendingErr := s.inbox.pending(id)
	var rows []Notice
	for i, payload := range payloads {
		if err := ctx.Err(); err != nil {
			return rows, errors.Join(pendingErr, err)
		}
		if payload.SessionID != id {
			pendingErr = errors.Join(pendingErr, fmt.Errorf("%w: foreign addressed notice retained", ErrInvalidInput))
			continue
		}
		// A receipt must describe the same readable payload actually returned,
		// rather than a replacement published between the inbox read and here.
		data, err := readNoticeReceiptFile(paths[i])
		var observed SessionNotice
		if err != nil || json.Unmarshal(data, &observed) != nil || !reflect.DeepEqual(observed, payload) {
			pendingErr = errors.Join(pendingErr, ErrNoticeReceiptChanged)
			continue
		}
		name := filepath.Base(paths[i])
		if s.receipts[id] == nil {
			s.receipts[id] = make(map[string][sha256.Size]byte)
		}
		s.receipts[id][name] = sha256.Sum256(data)
		copy := payload
		rows = append(rows, Notice{Payload: &copy, ID: name, SessionID: id, CreatedAt: payload.CreatedAt})
	}
	return rows, pendingErr
}

func (s *SessionNoticeStore) Acknowledge(ctx context.Context, sessionID, receipt string) error {
	id, err := validateSessionNoticeSessionID(sessionID)
	if err != nil {
		return ErrNoticeReceiptUnknown
	}
	if filepath.Base(receipt) != receipt || strings.HasPrefix(receipt, ".") || !strings.HasSuffix(receipt, ".json") {
		return ErrNoticeReceiptUnknown
	}
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	expected, issued := s.receipts[id][receipt]
	if !issued {
		return ErrNoticeReceiptUnknown
	}
	dir, err := s.inbox.sessionDir(id)
	if err != nil {
		return err
	}
	name := filepath.Join(dir, receipt)
	data, err := readNoticeReceiptFile(name)
	if err != nil || sha256.Sum256(data) != expected {
		return ErrNoticeReceiptChanged
	}
	// The original queue removes only paths returned as readable. Report a
	// failed removal instead of pretending that the notice was acknowledged.
	if err := os.Remove(name); err != nil {
		return err
	}
	delete(s.receipts[id], receipt)
	return nil
}

func readNoticeReceiptFile(name string) ([]byte, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrNoticeReceiptChanged
	}
	return os.ReadFile(name)
}

func secureNoticeStoreDirectory(name string) error {
	if err := os.MkdirAll(name, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("notice inbox is not a directory")
	}
	return os.Chmod(name, 0700)
}
