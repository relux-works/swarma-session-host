package sessioncontext

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
)

const (
	materializationRecordFileName     = "record.json"
	materializationQuarantineFileName = "quarantine.json"
)

type materializationStore struct {
	hostCodec      MaterializationHostCodec
	root           string
	quarantineRoot string
}

type materializationLoadError struct {
	CtxID  string
	Action string
	Err    error
}

func (e *materializationLoadError) Error() string {
	return fmt.Sprintf("%s materialization %s: %v", e.Action, e.CtxID, e.Err)
}

func (e *materializationLoadError) Unwrap() error {
	return e.Err
}

type materializationQuarantineMetadata struct {
	Contract      string    `json:"contract"`
	CtxID         string    `json:"ctx_id"`
	Reason        string    `json:"reason"`
	QuarantinedAt time.Time `json:"quarantined_at"`
}

func newMaterializationStore(root, quarantineRoot string) *materializationStore {
	return &materializationStore{root: root, quarantineRoot: quarantineRoot, hostCodec: sm.NewLegacyRecordCodec()}
}

func (s *materializationStore) load() ([]*materializationRecord, error) {
	entries, err := os.ReadDir(s.root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading materialization catalog: %w", err)
	}
	records := make([]*materializationRecord, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "CTX-") {
			continue
		}
		path := filepath.Join(s.root, entry.Name(), materializationRecordFileName)
		if err := rejectSymlinkAncestors(path); err != nil {
			return nil, &materializationLoadError{CtxID: entry.Name(), Action: "validating", Err: err}
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil, &materializationLoadError{CtxID: entry.Name(), Action: "reading", Err: readErr}
		}
		var record materializationRecord
		if decodeErr := json.Unmarshal(data, &record); decodeErr != nil {
			return nil, &materializationLoadError{CtxID: entry.Name(), Action: "decoding", Err: decodeErr}
		}
		var wire struct {
			ProviderHost json.RawMessage `json:"provider_host"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return nil, &materializationLoadError{CtxID: entry.Name(), Action: "decoding host", Err: err}
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(wire.ProviderHost, &fields); err != nil {
			return nil, &materializationLoadError{CtxID: entry.Name(), Action: "decoding host", Err: err}
		}
		if opaque, old := fields["opaque"]; old && len(opaque) > 0 && string(opaque) != "null" {
			if s.hostCodec == nil {
				return nil, &materializationLoadError{CtxID: entry.Name(), Action: "decoding legacy host", Err: fmt.Errorf("original provider-host codec supplier unavailable")}
			}
			h, err := s.hostCodec.DecodeProviderHost(context.Background(), wire.ProviderHost)
			if err != nil {
				return nil, &materializationLoadError{CtxID: entry.Name(), Action: "decoding legacy host", Err: err}
			}
			record.ProviderHost = h
			record.legacyImage = append([]byte(nil), data...)
			record.legacyProjection, _ = json.Marshal(&record)
		}
		if validateErr := validateMaterializationRecord(&record); validateErr != nil {
			return nil, &materializationLoadError{CtxID: entry.Name(), Action: "validating", Err: validateErr}
		}
		if record.CtxID != entry.Name() {
			return nil, &materializationLoadError{
				CtxID:  entry.Name(),
				Action: "validating",
				Err:    fmt.Errorf("catalog directory contains record %s", record.CtxID),
			}
		}
		records = append(records, &record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].CtxID < records[j].CtxID })
	return records, nil
}

func (s *materializationStore) save(record *materializationRecord) error {
	if err := validateMaterializationRecord(record); err != nil {
		return err
	}
	dir := filepath.Join(s.root, record.CtxID)
	if err := rejectSymlinkAncestors(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating materialization directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("securing materialization directory: %w", err)
	}
	data, err := s.encode(record)
	if err != nil {
		return fmt.Errorf("encoding materialization record: %w", err)
	}
	if err = writePrivateFile(filepath.Join(dir, materializationRecordFileName), data); err != nil {
		return err
	}
	if len(record.legacyImage) > 0 {
		record.legacyImage = append([]byte(nil), data...)
		record.legacyProjection, _ = json.Marshal(record)
	}
	return nil
}

// A lifecycle update owns the catalog fields, not the provider's historical
// private image. The real codec merges that subrecord against its original;
// omitted known lease fields are deleted rather than resurrected by an overlay.
func (s *materializationStore) encode(record *materializationRecord) ([]byte, error) {
	current, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(record.legacyImage) == 0 {
		b, e := json.MarshalIndent(record, "", "  ")
		return append(b, '\n'), e
	}
	if bytes.Equal(current, record.legacyProjection) {
		return append([]byte(nil), record.legacyImage...), nil
	}
	var original, updated map[string]json.RawMessage
	if err = json.Unmarshal(record.legacyImage, &original); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(current, &updated); err != nil {
		return nil, err
	}
	if s.hostCodec == nil {
		return nil, fmt.Errorf("historical provider-host codec unavailable")
	}
	provider, err := s.hostCodec.EncodeProviderHost(context.Background(), original["provider_host"], record.ProviderHost)
	if err != nil {
		return nil, err
	}
	// Only this owned persisted record's exported schema is replaced. Unknown
	// catalog members remain intact, including when omitempty clears a lease.
	typ := reflect.TypeOf(materializationRecord{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath != "" {
			continue
		}
		key := strings.Split(f.Tag.Get("json"), ",")[0]
		if key != "" && key != "-" {
			delete(original, key)
		}
	}
	for k, v := range updated {
		original[k] = v
	}
	original["provider_host"] = provider
	b, err := json.MarshalIndent(original, "", "  ")
	return append(b, '\n'), err
}

func (s *materializationStore) remove(ctxID string) error {
	ctxID = strings.TrimSpace(ctxID)
	if !strings.HasPrefix(ctxID, "CTX-") ||
		filepath.Base(ctxID) != ctxID ||
		strings.ContainsAny(ctxID, `/\`) {
		return fmt.Errorf("invalid materialization removal target %q", ctxID)
	}
	if err := rejectSymlinkAncestors(filepath.Join(s.root, ctxID)); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(s.root, ctxID)); err != nil {
		return fmt.Errorf("removing materialization %s: %w", ctxID, err)
	}
	if _, err := os.Stat(s.root); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("checking materialization catalog after removal: %w", err)
	}
	return syncDirectory(s.root)
}

func (s *materializationStore) quarantineInvalid(ctxID string, cause error, at time.Time) error {
	ctxID = strings.TrimSpace(ctxID)
	if !strings.HasPrefix(ctxID, "CTX-") ||
		filepath.Base(ctxID) != ctxID ||
		strings.ContainsAny(ctxID, `/\`) {
		return fmt.Errorf("invalid materialization quarantine target %q", ctxID)
	}
	if err := os.MkdirAll(s.quarantineRoot, 0o700); err != nil {
		return fmt.Errorf("creating materialization quarantine directory: %w", err)
	}
	if err := os.Chmod(s.quarantineRoot, 0o700); err != nil {
		return fmt.Errorf("securing materialization quarantine directory: %w", err)
	}
	source := filepath.Join(s.root, ctxID)
	destination, err := nextMaterializationQuarantinePath(s.quarantineRoot, ctxID)
	if err != nil {
		return err
	}
	if err := os.Rename(source, destination); err != nil {
		return fmt.Errorf("moving materialization %s to quarantine: %w", ctxID, err)
	}
	if err := syncDirectory(s.root); err != nil {
		return fmt.Errorf("syncing materialization catalog after quarantine: %w", err)
	}
	if err := syncDirectory(s.quarantineRoot); err != nil {
		return fmt.Errorf("syncing materialization quarantine directory: %w", err)
	}
	reason := "materialization record is invalid"
	if cause != nil {
		reason = cause.Error()
	}
	metadata := materializationQuarantineMetadata{
		Contract:      MaterializationContractVersion,
		CtxID:         ctxID,
		Reason:        reason,
		QuarantinedAt: at.UTC(),
	}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding materialization quarantine metadata: %w", err)
	}
	return writePrivateFile(
		filepath.Join(destination, materializationQuarantineFileName),
		append(data, '\n'),
	)
}

func (s *materializationStore) quarantinedCount() (int, error) {
	entries, err := os.ReadDir(s.quarantineRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading materialization quarantine: %w", err)
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "CTX-") {
			count++
		}
	}
	return count, nil
}

func nextMaterializationQuarantinePath(root, ctxID string) (string, error) {
	for suffix := 0; ; suffix++ {
		name := ctxID
		if suffix > 0 {
			name = fmt.Sprintf("%s-%d", ctxID, suffix)
		}
		path := filepath.Join(root, name)
		_, err := os.Lstat(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return path, nil
		case err != nil:
			return "", fmt.Errorf("checking materialization quarantine target: %w", err)
		}
	}
}

// A raw/corrupt record moved aside still reserves its original context identity.
// No caller can silently recycle the CTX while its writer custody is unresolved.
func (s *materializationStore) quarantinedIdentity(id string) bool {
	entries, err := os.ReadDir(s.quarantineRoot)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		return true
	}
	id = normalizeContextID(id)
	for _, e := range entries {
		if e.IsDir() && (e.Name() == id || strings.HasPrefix(e.Name(), id+"-")) {
			return true
		}
	}
	return false
}

// quarantinedIDs recovers only conservative custody holds from the original
// quarantine metadata. Unreadable or unbound metadata is unknown visibility,
// never an observed empty set and never permission to reclaim artifacts.
func (s *materializationStore) quarantinedIDs() (map[string]struct{}, error) {
	ids := map[string]struct{}{}
	entries, err := os.ReadDir(s.quarantineRoot)
	if errors.Is(err, os.ErrNotExist) {
		return ids, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "CTX-") {
			continue
		}
		path := filepath.Join(s.quarantineRoot, entry.Name(), materializationQuarantineFileName)
		if !entry.IsDir() {
			return nil, fmt.Errorf("quarantine visibility contains a non-directory identity")
		}
		if err = rejectSymlinkAncestors(path); err != nil {
			return nil, err
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil, readErr
		}
		var meta materializationQuarantineMetadata
		if err = json.Unmarshal(data, &meta); err != nil {
			return nil, err
		}
		if meta.Contract != MaterializationContractVersion || meta.CtxID != normalizeContextID(meta.CtxID) || validateContextID(meta.CtxID) != nil || !(entry.Name() == meta.CtxID || strings.HasPrefix(entry.Name(), meta.CtxID+"-")) {
			return nil, fmt.Errorf("quarantine identity metadata is not bound to its directory")
		}
		ids[meta.CtxID] = struct{}{}
	}
	return ids, nil
}
