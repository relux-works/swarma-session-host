package musebuilds

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// ProbeContractID identifies the first-probe contract a STATE verdict
// was recorded under. Pass 2b owns the probe; this pass writes the
// COPIED state that the probe resumes. A state naming another
// contract refuses for explicit migration, never silent
// reinterpretation.
const ProbeContractID = "muse-first-probe-v1"

// Build state values. Pass 2a writes COPIED and reads all three;
// pass 2b adds the probe verdict transitions.
const (
	StateCopied  = "COPIED"
	StateProbeOK = "PROBE_OK"
	StateBAD     = "BAD"
)

const (
	buildStateSchema = 1
	stateMaxBytes    = 64 * 1024
)

var errProbeContractMismatch = errors.New("probe contract mismatch")

// BadVerdict is the deterministic persistent verdict for exact bytes
// under one probe contract: only a clean first-probe identity
// contradiction mints one, never a timeout or a transient failure.
// Pass 2b owns minting; this pass reads and routes recorded verdicts.
type BadVerdict struct {
	Code       string `json:"code"`
	Detail     string `json:"detail"`
	RecordedAt string `json:"recorded_at"`
}

// BuildState is the one authoritative STATE document for a frozen
// build: schema, identity, size, publish transaction, probe
// contract, lifecycle state and the optional BAD verdict. There is
// no second file that can disagree with it.
type BuildState struct {
	Schema        int         `json:"schema"`
	Build         string      `json:"build"`
	SHA256        string      `json:"sha256"`
	Size          int64       `json:"size"`
	Transaction   string      `json:"transaction"`
	ProbeContract string      `json:"probe_contract"`
	State         string      `json:"state"`
	Verdict       *BadVerdict `json:"verdict"`
}

// agreesWith requires the state's identity to name exactly this key
// and discovery tuple: full build text and full digest, never a
// prefix, and a size the frozen bytes must match.
func (s BuildState) agreesWith(key string, id FrozenIdentity) error {
	if s.Build != id.Build || s.SHA256 != id.SHA256 {
		return fmt.Errorf("state names %s/%s, want the %s tuple", s.Build, s.SHA256, key)
	}
	if key != freezeKey(s.Build, s.SHA256) {
		return fmt.Errorf("state key disagrees with its directory")
	}
	if s.Size < 0 {
		return fmt.Errorf("state size %d is negative", s.Size)
	}
	if s.State == StateBAD && s.Verdict == nil {
		return fmt.Errorf("BAD state carries no verdict")
	}
	if s.State != StateBAD && s.Verdict != nil {
		return fmt.Errorf("%s state carries a verdict", s.State)
	}
	return nil
}

func isLowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		digit := (s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f')
		if !digit {
			return false
		}
	}
	return true
}

// decodeStrictJSON decodes one JSON object with the state's reader
// contract: bounded size, no duplicate keys at any depth, no unknown
// fields, exact types. encoding/json alone takes the last duplicate
// silently, so duplicates are scanned first with a token walk.
func decodeStrictJSON(data []byte, bound int64, v any) error {
	if int64(len(data)) > bound {
		return fmt.Errorf("document is %d bytes, past the %d bound", len(data), bound)
	}
	if err := rejectDuplicateKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("document carries trailing bytes")
	}
	return nil
}

func rejectDuplicateKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not a string")
				}
				if seen[key] {
					return fmt.Errorf("duplicate key %q", key)
				}
				seen[key] = true
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		default:
			return nil
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("document carries trailing values")
	}
	return nil
}

func validateBuildState(s BuildState) error {
	if s.Schema != buildStateSchema {
		return fmt.Errorf("schema is %d, want %d", s.Schema, buildStateSchema)
	}
	if _, err := muse.ParseBuildID(s.Build); err != nil {
		return fmt.Errorf("build %q does not parse: %w", s.Build, err)
	}
	if !isLowerHex64(s.SHA256) {
		return fmt.Errorf("sha256 is not 64 lowercase hex")
	}
	if s.Size < 0 {
		return fmt.Errorf("size %d is negative", s.Size)
	}
	if s.Transaction == "" {
		return fmt.Errorf("transaction is empty")
	}
	if s.ProbeContract != ProbeContractID {
		return fmt.Errorf("%w: state names %q, this binary speaks %q", errProbeContractMismatch, s.ProbeContract, ProbeContractID)
	}
	switch s.State {
	case StateCopied, StateProbeOK, StateBAD:
	default:
		return fmt.Errorf("state %q is not COPIED, PROBE_OK or BAD", s.State)
	}
	if s.State == StateBAD {
		if s.Verdict == nil || s.Verdict.Code == "" {
			return fmt.Errorf("BAD state carries no verdict code")
		}
	} else if s.Verdict != nil {
		return fmt.Errorf("%s state carries a verdict", s.State)
	}
	return nil
}

// readBuildState reads one authoritative STATE document. Absence of
// the file is (zero, false, nil); every other failure — unreadable,
// malformed, disagreeing — is an error, never absence. The read is a
// no-follow, nonblocking, size-bounded, owner-checked descriptor
// read: symlinks, FIFOs, oversized documents and foreign-owned state
// refuse instead of decoding.
func readBuildStateAt(ctx context.Context, meta *pinnedDir, key string) (BuildState, bool, error) {
	if err := ctx.Err(); err != nil {
		return BuildState{}, false, err
	}
	dir, err := meta.openChildDir(key)
	if os.IsNotExist(err) {
		return BuildState{}, false, nil
	}
	if err != nil {
		return BuildState{}, false, err
	}
	defer dir.close()
	if err := requireDirMode(dir, 0o700); err != nil {
		return BuildState{}, false, err
	}
	file, err := dir.openChildFile("STATE.json")
	if os.IsNotExist(err) {
		return BuildState{}, false, nil
	}
	if err != nil {
		return BuildState{}, false, err
	}
	defer file.Close()
	raw, info, _, err := readMetadataFD(ctx, file, stateMaxBytes, true)
	if err != nil {
		return BuildState{}, false, err
	}
	if info.Mode().Perm() != 0o600 {
		return BuildState{}, false, fmt.Errorf("STATE mode is %s", info.Mode())
	}
	state, err := buildStateDocument.parse(raw)
	return state, err == nil, err
}

// The caller holds the metadata lock. All operations are relative to the
// retained metadata directory, including the temporary and final rename.
func writeBuildStateAt(ctx context.Context, meta *pinnedDir, key string, state BuildState) error {
	raw, err := buildStateDocument.encode(state)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := meta.mkdirChild(key); err != nil {
		return err
	}
	dir, err := meta.openChildDir(key)
	if err != nil {
		return err
	}
	defer dir.close()
	if err := requireDirMode(dir, 0o700); err != nil {
		return err
	}
	return atomicDocumentAt(ctx, dir, "STATE.json", ".state-", raw)
}

var errPublicationUnknown = errors.New("durability unknown after rename")

// Kept for filesystem-fixture setup; publication retains the open store.
func ensureFrozenStore(ctx context.Context, root string) error {
	store, err := openFrozenStore(ctx, root)
	if err != nil {
		return freezeRefusal(ctx, StageDedupe, CodeFreezeUnavailable, err)
	}
	store.close()
	return nil
}

// openFrozenBytes binds leaf and binary gates to the same descriptors used
// for verification and (for crash adoption) permission protection.
func openFrozenBytes(store *frozenStore, key, build string, protected bool) (*pinnedDir, *os.File, error) {
	leaf, err := store.builds.openChildDir(key)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*pinnedDir, *os.File, error) { _ = leaf.close(); return nil, nil, err }
	info, err := leaf.file.Stat()
	if err != nil {
		return fail(err)
	}
	mode := info.Mode().Perm()
	if (protected && mode != 0o555) || (!protected && mode != 0o555 && mode != 0o700) {
		return fail(fmt.Errorf("frozen leaf mode is %s", info.Mode()))
	}
	file, err := leaf.openChildFile(frozenBinaryName(build))
	if err != nil {
		return fail(err)
	}
	if err := checkFrozenFile(file); err != nil {
		_ = file.Close()
		return fail(err)
	}
	info, err = file.Stat()
	if err != nil {
		_ = file.Close()
		return fail(err)
	}
	mode = info.Mode().Perm()
	if (protected && mode != 0o555) || (!protected && (mode&0o111 == 0 || mode&0o022 != 0)) {
		_ = file.Close()
		return fail(fmt.Errorf("frozen binary mode is %s", info.Mode()))
	}
	return leaf, file, nil
}

func verifyFrozenTuple(ctx context.Context, store *frozenStore, key string, id FrozenIdentity) (FrozenIdentity, error) {
	if err := ctx.Err(); err != nil {
		return FrozenIdentity{}, err
	}
	leaf, file, err := openFrozenBytes(store, key, id.Build, true)
	if err != nil {
		return FrozenIdentity{}, err
	}
	defer leaf.close()
	defer file.Close()
	digest, err := hashFileDescriptor(ctx, StageDedupe, file)
	if err != nil {
		return FrozenIdentity{}, err
	}
	if digest != id.SHA256 {
		return FrozenIdentity{}, fmt.Errorf("frozen bytes disagree with the %s tuple", key)
	}
	return FrozenIdentity{Build: id.Build, SHA256: digest, Path: file.Name()}, nil
}

func frozenBytesPresent(ctx context.Context, store *frozenStore, key string, id FrozenIdentity) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	leaf, err := store.builds.openChildDir(key)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer leaf.close()
	exists, err := leaf.childExists(frozenBinaryName(id.Build))
	if err != nil || !exists {
		return false, err
	}
	pinnedLeaf, file, err := openFrozenBytes(store, key, id.Build, false)
	if err != nil {
		return false, err
	}
	defer pinnedLeaf.close()
	defer file.Close()
	digest, err := hashFileDescriptor(ctx, StageDedupe, file)
	if err != nil {
		return false, err
	}
	if digest != id.SHA256 {
		return false, fmt.Errorf("content at %s disagrees with the key digest", key)
	}
	return true, nil
}

// hashFileBytes streams a file's SHA-256 in bounded chunks with a
// context check per chunk. Frozen binaries are hundreds of megabytes;
// they are never buffered whole. The open never follows a trailing
// symlink and the descriptor must be a regular file: a symlink or
// FIFO at a frozen path refuses instead of hashing another object's
// bytes.
func hashFileBytes(ctx context.Context, stage, path string) (string, error) {
	file, err := openMetadataFile(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	if info, err := file.Stat(); err != nil {
		return "", err
	} else if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", path)
	}
	return hashFileDescriptor(ctx, stage, file)
}

func hashFileDescriptor(ctx context.Context, stage string, file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	hash := sha256.New()
	chunk := make([]byte, hashReadChunk)
	for {
		if err := freezeRefuseIfCanceled(ctx, stage); err != nil {
			return "", err
		}
		n, err := file.Read(chunk)
		if n > 0 {
			_, _ = hash.Write(chunk[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	if err := freezeRefuseIfCanceled(ctx, stage); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
