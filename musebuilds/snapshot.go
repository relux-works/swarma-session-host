package musebuilds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

// snapshotSeedVersion pins the snapshot seed encoding. A future encoding
// change mints a new version rather than reusing these bytes.
const snapshotSeedVersion = "musebuilds-snapshot-v1"

// deriveSnapshotID derives the selection linearization point: a stable
// digest over the install directory, the sidecar selector, the full ranked
// candidate set and the selected source digest. Repeated discovery over an
// unchanged install returns the same ID; any added, removed or reordered
// candidate, selector change or source-byte change moves it. Callers compare
// IDs for equality only; the encoding is opaque.
func deriveSnapshotID(ctx context.Context, installDir, sidecarBuild string, sidecarPresent bool, ordered []candidate, digest string) (string, error) {
	fields := make([]string, 0, len(ordered)+4)
	fields = append(fields, snapshotSeedVersion, installDir)
	if sidecarPresent {
		fields = append(fields, "present:"+sidecarBuild)
	} else {
		fields = append(fields, "absent")
	}
	for _, cand := range ordered {
		if err := refuseIfCanceled(ctx, StageSelect); err != nil {
			return "", err
		}
		fields = append(fields, cand.id.Build)
	}
	fields = append(fields, digest)
	hash := sha256.New()
	for i, field := range fields {
		if err := refuseIfCanceled(ctx, StageSelect); err != nil {
			return "", err
		}
		if i > 0 {
			_, _ = hash.Write([]byte{0})
		}
		for len(field) > 0 {
			if err := refuseIfCanceled(ctx, StageSelect); err != nil {
				return "", err
			}
			n := min(len(field), hashReadChunk)
			_, _ = hash.Write([]byte(field[:n]))
			field = field[n:]
		}
	}
	if err := refuseIfCanceled(ctx, StageSelect); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
