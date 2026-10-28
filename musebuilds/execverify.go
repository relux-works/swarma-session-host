package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// StageExecVerify is the pre-exec verification stage. Every refusal names
// it so a caller can tell the frozen gate refused without matching prose.
const StageExecVerify = "exec-verify"

// Pre-exec refusal codes. A failed, partial or malformed read is never a
// legitimate absence: each code below refuses a distinct fact.
const (
	// CodeExecVerifyFailed refuses a frozen tuple whose bytes, modes or
	// path disagree with the tuple: the design section-7 pre-exec gate.
	CodeExecVerifyFailed = "muse_exec_verify_failed"
	// CodeExecLeaseFailed refuses a use-lease acquisition: contention
	// past the cap or an unreadable store, never a skipped lease.
	CodeExecLeaseFailed = "muse_exec_lease_failed"
	// CodeExecCanceled refuses verification or lease work whose context
	// ended.
	CodeExecCanceled = "muse_exec_canceled"
	// CodeExecUnsupportedPlatform refuses pre-exec enforcement where
	// the host cannot pin directories or hold advisory locks.
	CodeExecUnsupportedPlatform = "muse_exec_unsupported_platform"
)

// ExecError is a typed pre-exec refusal: the stage that refused and the
// machine-readable reason, with the underlying cause chained.
type ExecError struct {
	Stage string
	Code  string
	Cause error
}

// Error renders the refusal with its stage and code.
func (e *ExecError) Error() string {
	if e == nil {
		return "muse exec error"
	}
	if e.Cause != nil {
		return "muse exec " + e.Stage + " " + e.Code + ": " + e.Cause.Error()
	}
	return "muse exec " + e.Stage + " " + e.Code
}

// Unwrap returns the underlying cause.
func (e *ExecError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// AsExecError matches an ExecError in err's chain for code and stage
// assertions.
func AsExecError(err error) (*ExecError, bool) {
	var refusal *ExecError
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return nil, false
}

func execRefuseIfCanceled(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return &ExecError{Stage: StageExecVerify, Code: CodeExecCanceled, Cause: err}
	}
	return nil
}

func execRefusal(ctx context.Context, code string, cause error) error {
	if err := execRefuseIfCanceled(ctx); err != nil {
		return err
	}
	if existing, ok := AsExecError(cause); ok && existing.Code == CodeExecCanceled {
		return cause
	}
	return &ExecError{Stage: StageExecVerify, Code: code, Cause: cause}
}

// VerifyFrozenForExec is the design section-7 pre-exec gate: it re-verifies
// one frozen tuple against the bytes about to run, immediately before the
// caller starts its child. It checks canonical fixed-root ancestry (the
// path must be the tuple's own frozen copy), root/builds/staging/meta
// ownership and 0700 modes, non-symlink FD-relative components, a regular
// owner-held binary in the protected 0555 leaf, the exact versioned name
// for the build, and the full byte SHA-256. Probe state is deliberately
// NOT consulted here: state admission belongs to selection, the permit
// path and the deferral path, which all re-verify before authorizing;
// this gate binds bytes to path at the last moment. The caller must hold
// a UseLease across verify, start and reap; this call itself takes none.
func VerifyFrozenForExec(ctx context.Context, cfg HostConfig, id FrozenIdentity) error {
	if err := execRefuseIfCanceled(ctx); err != nil {
		return err
	}
	if !supportedPlatform {
		return &ExecError{Stage: StageExecVerify, Code: CodeExecUnsupportedPlatform}
	}
	if err := validateStoreRoot(cfg.FrozenRoot); err != nil {
		return execRefusal(ctx, CodeExecVerifyFailed, err)
	}
	if err := validateFrozenReference(cfg.FrozenRoot, id); err != nil {
		return execRefusal(ctx, CodeExecVerifyFailed, err)
	}
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return execRefusal(ctx, CodeExecVerifyFailed, err)
	}
	defer store.close()
	key := freezeKey(id.Build, id.SHA256)
	leaf, file, err := openFrozenBytes(store, key, id.Build, true)
	if err != nil {
		return execRefusal(ctx, CodeExecVerifyFailed, err)
	}
	defer leaf.close()
	defer file.Close()
	digest, err := hashFileDescriptor(ctx, StageExecVerify, file)
	if err != nil {
		return execRefusal(ctx, CodeExecVerifyFailed, err)
	}
	if digest != id.SHA256 {
		return execRefusal(ctx, CodeExecVerifyFailed, fmt.Errorf("frozen bytes hash %s, want the tuple %s", digest, id.SHA256))
	}
	return nil
}

// UseLease is one held shared maintenance lease on the frozen store: the
// design section-3.5 use exclusion the host holds across pre-exec
// verification, child startup and teardown/reap, so reviewed exclusive
// cleanup cannot delete bytes a live child runs. Acquire it before
// reading or verifying executable bytes; release it after the child is
// reaped on every path, including failed starts. The zero value holds
// nothing; only AcquireUseLease issues a lease.
type UseLease struct {
	mu      sync.Mutex
	store   *frozenStore
	release func() error
}

// AcquireUseLease takes the shared maintenance lease on the frozen
// store. The wait is bounded by the design lease cap and the caller's
// context; contention refuses, it never waits indefinitely and never
// skips the lease.
func AcquireUseLease(ctx context.Context, cfg HostConfig) (*UseLease, error) {
	if err := execRefuseIfCanceled(ctx); err != nil {
		return nil, err
	}
	if !supportedPlatform {
		return nil, &ExecError{Stage: StageExecVerify, Code: CodeExecUnsupportedPlatform}
	}
	if err := validateStoreRoot(cfg.FrozenRoot); err != nil {
		return nil, execRefusal(ctx, CodeExecLeaseFailed, err)
	}
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return nil, execRefusal(ctx, CodeExecLeaseFailed, err)
	}
	release, err := acquireMaintenanceLeaseAt(ctx, store.root)
	if err != nil {
		store.close()
		if ferr, ok := AsFreezeError(err); ok && ferr.Code == CodeFreezeCanceled {
			return nil, &ExecError{Stage: StageExecVerify, Code: CodeExecCanceled, Cause: err}
		}
		return nil, execRefusal(ctx, CodeExecLeaseFailed, err)
	}
	return &UseLease{store: store, release: release}, nil
}

// Release drops the held lease and its pinned store handles. It is
// idempotent and nil-safe: a second release is a no-op, so deferred
// releases on overlapping teardown paths never corrupt the store.
func (l *UseLease) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.release == nil {
		return nil
	}
	release := l.release
	l.release = nil
	err := release()
	if l.store != nil {
		l.store.close()
		l.store = nil
	}
	return err
}
