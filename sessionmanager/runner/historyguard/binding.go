package historyguard

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

// ImageSeal is the first field of each canonical admission JSON document. Its
// digest covers the exact whole document with SHA256 empty. Witness copies are
// recovery images, never canonical owner authority or provider record shadows.
type ImageSeal struct {
	StoreID  string `json:"store_id"`
	Sequence uint64 `json:"sequence"`
	SHA256   string `json:"sha256"`
}

type boundWitness struct {
	Version   int    `json:"version"`
	StoreID   string `json:"store_id"`
	Committed []byte `json:"committed"`
	Pending   []byte `json:"pending"`
	SHA256    string `json:"sha256"`
}

func StoreIdentity(kind, namespace string) string {
	h := sha256.Sum256([]byte(kind + "\x00" + namespace))
	return hex.EncodeToString(h[:])
}
func Digest(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func NextSeal(current ImageSeal, identity string) (ImageSeal, error) {
	if identity == "" || current.StoreID != "" && current.StoreID != identity || current.Sequence == math.MaxUint64 {
		return ImageSeal{}, ErrUncertain
	}
	return ImageSeal{StoreID: identity, Sequence: current.Sequence + 1}, nil
}

// VerifyImage hashes exact bytes, preserving the caller's independent canonical
// decoder check. It cannot silently reserialize a partially lost document.
func VerifyImage(raw []byte, identity string) (ImageSeal, error) {
	var header struct {
		Seal ImageSeal `json:"seal"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &header) != nil {
		return ImageSeal{}, ErrUncertain
	}
	s := header.Seal
	if s.StoreID != identity || s.Sequence == 0 || len(s.SHA256) != 64 {
		return ImageSeal{}, ErrUncertain
	}
	encoded, _ := json.Marshal(s)
	prefix := append(append([]byte(`{"seal":`), encoded...), ',')
	if !bytes.HasPrefix(raw, prefix) {
		return ImageSeal{}, ErrUncertain
	}
	blank := s
	blank.SHA256 = ""
	encodedBlank, _ := json.Marshal(blank)
	unsigned := append(append(append([]byte(`{"seal":`), encodedBlank...), ','), raw[len(prefix):]...)
	if Digest(unsigned) != s.SHA256 {
		return ImageSeal{}, ErrUncertain
	}
	return s, nil
}

func readBound(lockPath, identity string) (boundWitness, error) {
	var w boundWitness
	f, err := open(lockPath)
	if err != nil {
		return w, err
	}
	defer f.Close()
	const maximumWitnessBytes = 48 << 20
	raw, err := io.ReadAll(io.LimitReader(f, maximumWitnessBytes+1))
	if err != nil {
		return w, err
	}
	if len(raw) > maximumWitnessBytes || json.Unmarshal(raw, &w) != nil || w.Version != 2 || w.StoreID != identity {
		return w, ErrUncertain
	}
	canonical, err := json.Marshal(w)
	if err != nil || !bytes.Equal(raw, canonical) {
		return w, ErrUncertain
	}
	digest := w.SHA256
	w.SHA256 = ""
	unsigned, _ := json.Marshal(w)
	w.SHA256 = digest
	if len(digest) != 64 || Digest(unsigned) != digest {
		return w, ErrUncertain
	}
	if len(w.Committed) == 0 && len(w.Pending) == 0 {
		return w, ErrUncertain
	}
	for _, image := range [][]byte{w.Committed, w.Pending} {
		if len(image) != 0 {
			if _, err := VerifyImage(image, identity); err != nil {
				return w, err
			}
		}
	}
	return w, nil
}

func writeBound(lockPath string, w boundWitness) error {
	return writeBoundWithSync(lockPath, w, (*os.File).Sync)
}

func writeBoundWithSync(lockPath string, w boundWitness, syncFile func(*os.File) error) error {
	w.Version = 2
	w.SHA256 = ""
	unsigned, _ := json.Marshal(w)
	w.SHA256 = Digest(unsigned)
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	// The witness is a recovery replica, not a lock inode. Atomic replacement
	// preserves the old committed image across a torn/failed pending write.
	// Callers hold two independently named stable fences during mutation.
	f, err := CreateTemp(lockPath, ".admission-witness-")
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
	if err := syncFile(f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), lockPath); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(lockPath))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := syncFile(dir); err != nil {
		return err
	}
	return SyncTempDirectory(lockPath, syncFile)
}

// CheckBound requires both independent replicas to attest the exact committed
// image. A lost/stale replica cannot erase the other replica's pending intent.
// A checked reconciliation read may inspect pending images. RecoveryImages
// separately collects surviving copies when a checked read itself refuses.
func CheckBound(lockPath string, raw []byte, identity string, reconciling bool) error {
	if _, err := VerifyImage(raw, identity); err != nil {
		return err
	}
	for _, path := range []string{lockPath, lockPath + ".intent"} {
		w, err := readBound(path, identity)
		if err != nil {
			return err
		}
		if reconciling && (bytes.Equal(raw, w.Committed) || bytes.Equal(raw, w.Pending)) {
			continue
		}
		if len(w.Pending) != 0 || !bytes.Equal(raw, w.Committed) {
			return ErrUncertain
		}
	}
	return nil
}

// PrepareBound persists a full pending recovery image before the JSON commit.
// Only first bootstrap or an explicitly validated reconciliation may replace an
// absent/corrupt witness; ordinary commits require its matching committed base.
func PrepareBound(lockPath string, raw []byte, identity string, reconciling bool) error {
	next, err := VerifyImage(raw, identity)
	if err != nil {
		return err
	}
	w, readErr := readBound(lockPath, identity)
	if readErr != nil {
		if !reconciling && next.Sequence != 1 {
			return ErrUncertain
		}
		w = boundWitness{StoreID: identity}
	} else if !reconciling {
		if err := CheckBound(lockPath, w.Committed, identity, false); err != nil {
			return err
		}
		if len(w.Pending) != 0 {
			return ErrUncertain
		}
		old, err := VerifyImage(w.Committed, identity)
		if err != nil || old.Sequence == math.MaxUint64 || next.Sequence != old.Sequence+1 {
			return ErrUncertain
		}
	}
	w.Pending = append([]byte(nil), raw...)
	// The JSON attempt starts only after both pending copies are durable.
	// An interruption between these writes closes admission through disagreement;
	// recovery keeps every surviving pending image before recommitting both.
	if err := writeBound(lockPath, w); err != nil {
		return err
	}
	return writeBound(lockPath+".intent", w)
}

func FinishBound(lockPath string, raw []byte, identity string) error {
	return finishBoundWithSync(lockPath, raw, identity, (*os.File).Sync)
}
func finishBoundWithSync(lockPath string, raw []byte, identity string, syncFile func(*os.File) error) error {
	w, err := readBound(lockPath, identity)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, w.Pending) {
		return ErrUncertain
	}
	other, err := readBound(lockPath+".intent", identity)
	if err != nil || !bytes.Equal(raw, other.Pending) {
		return ErrUncertain
	}
	pending := w
	w.Committed = append([]byte(nil), raw...)
	w.Pending = nil
	for _, path := range []string{lockPath, lockPath + ".intent"} {
		if err := writeBoundWithSync(path, w, syncFile); err != nil {
			// Either pending copy closes admission if finalization is interrupted.
			_ = writeBound(lockPath, pending)
			_ = writeBound(lockPath+".intent", pending)
			return err
		}
	}
	return nil
}

// RecoveryImages returns independently verified candidates, including a
// pending pre-rename attempt. The store validates namespace/entries, retains
// high-water/tombstones, checks canonical owners, then explicitly recommits.
// A malformed witness contributes no candidate; it never legitimizes an image.
func RecoveryImages(lockPath string, raw []byte, identity string) ([][]byte, error) {
	var images [][]byte
	if _, err := VerifyImage(raw, identity); err == nil {
		images = append(images, append([]byte(nil), raw...))
	}
	for _, path := range []string{lockPath, lockPath + ".intent"} {
		if w, err := readBound(path, identity); err == nil {
			for _, v := range [][]byte{w.Committed, w.Pending} {
				if len(v) != 0 {
					images = append(images, append([]byte(nil), v...))
				}
			}
		}
	}

	if len(images) == 0 {
		return nil, fmt.Errorf("%w: no verified recovery image", ErrUncertain)
	}
	return images, nil
}

// BoundPending distinguishes a checked transaction witness from the legacy
// marker API retained only for its pre-existing low-level regression tests.
func BoundPending(lockPath, identity string) (bool, error) {
	var pending bool
	for _, path := range []string{lockPath, lockPath + ".intent"} {
		w, err := readBound(path, identity)
		if err != nil {
			return false, err
		}
		pending = pending || len(w.Pending) != 0
	}
	return pending, nil
}

// SyncBound exposes no bypass: a successful call proves both witness and exact
// image binding; it does not create a witness or mark an unknown image clean.
func SyncBound(lockPath string, raw []byte, identity string) error {
	if err := CheckBound(lockPath, raw, identity, false); err != nil {
		return err
	}
	for _, path := range []string{lockPath, lockPath + ".intent"} {
		f, err := open(path)
		if err != nil {
			return err
		}
		err = f.Sync()
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

// ReadImage refuses symlink/insecure, oversized and replaced inodes. A recovery
// caller may use a separate verified witness if this single image was lost.
func ReadImage(path string, maximum int64) ([]byte, error) {
	f, err := open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, ErrUncertain
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, current) {
		return nil, ErrUncertain
	}
	return data, nil
}
