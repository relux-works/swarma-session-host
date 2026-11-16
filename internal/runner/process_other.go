//go:build !linux || !amd64

package runner

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/relux-works/swarma-session-host/internal/admission"
)

func preflightIdentifyV2() error { return errChildBirthUnsupported }

func startOriginalProcess(selected bool, binary string, args []string, attr *os.ProcAttr) (*os.Process, linuxOriginal, error) {
	if selected {
		return nil, linuxOriginal{err: errChildBirthUnsupported}, errChildBirthUnsupported
	}
	p, err := os.StartProcess(binary, args, attr)
	owned := linuxOriginal{err: errChildBirthUnsupported}
	if err == nil {
		read := func(ctx context.Context) (OwnedProcessEvidence, error) {
			raw, err := admission.ObserveKernelProcess(ctx, p.Pid)
			if err != nil {
				return OwnedProcessEvidence{}, err
			}
			return OwnedProcessEvidence{PID: raw.Process.PID, RealUID: raw.RealUID, EffectiveUID: raw.Process.UID,
				BootID: strings.ToLower(raw.Process.BootUUID), Platform: "darwin", StartSec: strconv.FormatUint(raw.StartSec, 10),
				StartUsec: raw.StartUsec, Executable: raw.Process.CDHash}, nil
		}
		// Captured before the Server's reaper begins. Failure is permanent for
		// this child; a later status never tries to recapture by numeric PID.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var readErr error
		owned.started, readErr = read(ctx)
		cancel()
		if readErr == nil {
			owned.process = read
		}
	}
	// Observation failure does not change ordinary launch/cleanup semantics.
	return p, owned, err
}
