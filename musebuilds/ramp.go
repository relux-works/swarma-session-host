package musebuilds

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Ramp bindings from G2 Q1: the state file carries ramp_down=
// (0 CLEAR, 1 HOLD) and both the state mtime and the watch log's
// last sample must be fresh, or the ramp is UNKNOWN and refuses.
const (
	rampKey           = "ramp_down="
	rampStateMaxBytes = 64 * 1024
	rampLogMaxBytes   = 1024 * 1024
	rampFreshness     = 900 * time.Second
)

// requireRampClear reads the host ramp through the configured paths
// and clock. CLEAR proceeds; HOLD and every UNKNOWN shape refuse
// with their own codes. Unknown is never CLEAR.
func requireRampClear(ctx context.Context, cfg HostConfig) error {
	if err := freezeRefuseIfCanceled(ctx, StageAdmit); err != nil {
		return err
	}
	now := cfg.clock()()
	clear, unknown := readRamp(ctx, now, cfg.RampStatePath, cfg.RampLogPath)
	if err := freezeRefuseIfCanceled(ctx, StageAdmit); err != nil {
		return err
	}
	switch {
	case unknown != nil:
		return &FreezeError{Stage: StageAdmit, Code: CodeRampUnknown, Cause: unknown}
	case !clear:
		return &FreezeError{Stage: StageAdmit, Code: CodeRampHold, Cause: errRampHold}
	default:
		return nil
	}
}

var errRampHold = fmt.Errorf("ramp_down=1")

func readRamp(ctx context.Context, now time.Time, statePath, logPath string) (clear bool, unknown error) {
	value, mtime, err := readRampStateDoc(ctx, statePath)
	if err != nil {
		return false, err
	}
	if err := requireFresh(now, mtime, rampFreshness); err != nil {
		return false, err
	}
	if err := requireLogFresh(ctx, now, logPath); err != nil {
		return false, err
	}
	return value == "0", nil
}

// readRampStateDoc returns the last ramp_down= value in the state
// file with the descriptor's own mtime, matching the watch script's
// last-line-wins read. The file is opened without following
// symlinks and read within its bound; a missing file, a symlink, a
// FIFO, an over-bound file, a missing key and any value but 0 or 1
// are UNKNOWN, never CLEAR and never HOLD.
func readRampStateDoc(ctx context.Context, statePath string) (string, time.Time, error) {
	raw, info, present, err := readMetadataDocument(ctx, statePath, rampStateMaxBytes, true)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("ramp state unreadable: %w", err)
	}
	if !present {
		return "", time.Time{}, fmt.Errorf("ramp state unreadable: no such file")
	}
	value := ""
	found := false
	for _, line := range strings.Split(string(raw), "\n") {
		rest, ok := strings.CutPrefix(line, rampKey)
		if !ok {
			continue
		}
		value, found = rest, true
	}
	if !found {
		return "", time.Time{}, fmt.Errorf("ramp state carries no %s key", rampKey)
	}
	if value != "0" && value != "1" {
		return "", time.Time{}, fmt.Errorf("ramp_down=%q is not 0 or 1", value)
	}
	return value, info.ModTime(), nil
}

func requireFresh(now time.Time, mtime time.Time, window time.Duration) error {
	if mtime.After(now) {
		return fmt.Errorf("ramp mtime %s is ahead of the clock", mtime.UTC().Format(time.RFC3339))
	}
	if now.Sub(mtime) > window {
		return fmt.Errorf("ramp mtime %s is older than %s", mtime.UTC().Format(time.RFC3339), window)
	}
	return nil
}

// requireLogFresh requires the watch log's last sample to be fresh.
// A missing, empty, over-bound or malformed log is UNKNOWN; only the
// last non-empty line's leading timestamp counts, and a future stamp
// refuses like a stale one. The log is opened without following
// symlinks and read within its bound.
func requireLogFresh(ctx context.Context, now time.Time, logPath string) error {
	raw, _, present, err := readMetadataDocument(ctx, logPath, rampLogMaxBytes, true)
	if err != nil {
		return fmt.Errorf("ramp log unreadable: %w", err)
	}
	if !present {
		return fmt.Errorf("ramp log unreadable: no such file")
	}
	lines := strings.Split(string(raw), "\n")
	stamp := ""
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) == "" {
			continue
		}
		stamp, _, _ = strings.Cut(lines[i], "\t")
		break
	}
	if stamp == "" {
		return fmt.Errorf("ramp log holds no sample")
	}
	sampled, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		return fmt.Errorf("ramp log stamp %q does not parse: %w", stamp, err)
	}
	if sampled.After(now) {
		return fmt.Errorf("ramp log stamp %s is ahead of the clock", stamp)
	}
	if now.Sub(sampled) > rampFreshness {
		return fmt.Errorf("ramp log stamp %s is older than %s", stamp, rampFreshness)
	}
	return nil
}
