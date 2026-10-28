package musebuilds

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	muse "github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

var currentEUID = os.Geteuid

const hashReadChunk = 32 * 1024

type pinnedSource struct {
	lease  *fileLease
	digest string
}

func checkLinkOwner(path string, id fileIdentity) error {
	if id.nlink > 1 {
		return fmt.Errorf("%s has %d hard links", path, id.nlink)
	}
	if id.uid != uint32(currentEUID()) {
		return fmt.Errorf("%s is owned by another user", path)
	}
	return nil
}

// The source gates and hash operate on ONE descriptor owned by the snapshot.
// A swap can neither donate unchecked bytes nor release the original lease.
func openValidateAndHashSelectedSource(ctx context.Context, s *discoverySnapshot, dir *fileLease, path string, id muse.BuildIdentity) (*pinnedSource, error) {
	if err := refuseIfCanceled(ctx, StageSource); err != nil {
		return nil, err
	}
	if filepath.Base(path) != pinnedNamePrefix+id.Build {
		return nil, refusal(ctx, StageSource, CodeSelectionUnusable, fmt.Errorf("%s does not name build %s", path, id.Build))
	}
	lease, err := s.open(ctx, dir, filepath.Base(path), StageSource)
	if err != nil {
		return nil, refusal(ctx, StageSource, CodeSelectionUnusable, err)
	}
	if !lease.id.mode.IsRegular() {
		return nil, refusal(ctx, StageSource, CodeSelectionUnusable, fmt.Errorf("%s is not regular", path))
	}
	if lease.id.mode.Perm()&0o111 == 0 {
		return nil, refusal(ctx, StageSource, CodeSelectionUnusable, fmt.Errorf("%s is not executable", path))
	}
	if err := checkLinkOwner(path, lease.id); err != nil {
		return nil, refusal(ctx, StageSource, CodeSelectionUnusable, err)
	}
	interpose(interposeAfterValidate)
	digest, err := hashOpenSource(ctx, lease, StageSource, interposeInitialRead)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, changed(err)
		}
		return nil, refusal(ctx, StageSource, CodeSelectionUnusable, err)
	}
	return &pinnedSource{lease: lease, digest: digest}, nil
}

// readChunks bounds total work by the fstat size and checks ctx around every
// read and hash/consume step. A continuously growing file cannot extend the
// read; its size/ctime change is refused by the final identity sweep.
// Kernel-uninterruptible syscalls remain the design's explicit platform bound.
func readChunks(ctx context.Context, lease *fileLease, stage string, point interposePoint, consume func([]byte)) error {
	chunk := make([]byte, hashReadChunk)
	for offset := int64(0); offset < lease.id.size; {
		if err := refuseIfCanceled(ctx, stage); err != nil {
			return err
		}
		want := min(int64(len(chunk)), lease.id.size-offset)
		n, err := lease.file.ReadAt(chunk[:want], offset)
		interpose(point)
		if canceled := refuseIfCanceled(ctx, stage); canceled != nil {
			return canceled
		}
		if n != int(want) {
			return fmt.Errorf("partial read of %s: %w", lease.path, io.ErrUnexpectedEOF)
		}
		if err != nil && err != io.EOF {
			return err
		}
		consume(chunk[:n])
		offset += int64(n)
	}
	return refuseIfCanceled(ctx, stage)
}

func readLease(ctx context.Context, lease *fileLease, stage string, point interposePoint, capBytes int64) ([]byte, error) {
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return nil, err
	}
	if lease.id.size < 0 || lease.id.size > capBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", lease.path, capBytes)
	}
	var data bytes.Buffer
	err := readChunks(ctx, lease, stage, point, func(chunk []byte) { _, _ = data.Write(chunk) })
	return data.Bytes(), err
}

func hashOpenSource(ctx context.Context, lease *fileLease, stage string, point interposePoint) (string, error) {
	hash := sha256.New()
	if err := readChunks(ctx, lease, stage, point, func(chunk []byte) { _, _ = hash.Write(chunk) }); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func recheckSource(ctx context.Context, pinned *pinnedSource) error {
	digest, err := hashOpenSource(ctx, pinned.lease, StageRecheck, interposeFinalRead)
	if err != nil {
		return refusal(ctx, StageRecheck, CodeDiscoveryChanged, err)
	}
	if digest != pinned.digest {
		return changed(errors.New("selected source bytes changed"))
	}
	return nil
}
