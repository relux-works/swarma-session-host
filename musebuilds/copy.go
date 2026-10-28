package musebuilds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

// checkInstallSource re-verifies the discovery's installed source
// before freeze touches admission: no symlink, regular, executable,
// owner-controlled, no hard-link alias. Digest agreement is proven
// during the copy itself, from this same descriptor.
func checkInstallSource(path string) error {
	file, err := openInstallSource(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	id, err := descriptorIdentity(file)
	if err != nil {
		return err
	}
	if !id.mode.IsRegular() {
		return fmt.Errorf("%s is not regular", path)
	}
	if id.mode.Perm()&0o111 == 0 {
		return fmt.Errorf("%s is not executable", path)
	}
	return checkLinkOwner(path, id)
}

// copyAndPublishHeld copies the verified source and publishes it as
// COPIED with its authoritative state, all under the global lock and
// the shared maintenance lease. The lease is released before any
// probe runs; probes belong to pass 2b.
func copyAndPublishHeld(ctx context.Context, cfg HostConfig, d Discovery, held HostLockHeld, reservation Reservation) (FrozenCopy, error) {
	freezeInterpose(freezeBeforeCopy, hookFor(d, reservation.Transaction))
	if err := freezeRefuseIfCanceled(ctx, StageCopy); err != nil {
		failCopy(cfg, reservation, err)
		return FrozenCopy{}, err
	}
	releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok {
			failCopy(cfg, reservation, err)
			return FrozenCopy{}, ferr
		}
		failCopy(cfg, reservation, err)
		return FrozenCopy{}, freezeRefusal(ctx, StageCopy, CodeFreezeLeaseFailed, err)
	}
	staged, size, err := copySourceToStaging(ctx, cfg, d, reservation)
	if err != nil {
		_ = releaseLease()
		failCopy(cfg, reservation, err)
		return FrozenCopy{}, err
	}
	defer staged.close()
	if err := held.Check(); err != nil {
		_ = releaseLease()
		lockErr := &FreezeError{Stage: StageCopy, Code: CodeFreezeLockFailed, Cause: err}
		failCopy(cfg, reservation, lockErr)
		return FrozenCopy{}, lockErr
	}
	result, err := publishStaging(ctx, cfg, d, reservation, staged, size)
	releaseErr := releaseLease()
	if err != nil {
		if releaseErr != nil {
			if ferr, ok := AsFreezeError(err); ok && ferr.Cause != nil {
				ferr.Cause = errors.Join(ferr.Cause, releaseErr)
			}
		}
		return FrozenCopy{}, err
	}
	if releaseErr != nil {
		return FrozenCopy{}, &FreezeError{Stage: StagePublish, Code: CodeFreezeLeaseFailed, Cause: releaseErr}
	}
	return result, nil
}

// failCopy records a failed reservation outcome. The staging is
// retained for reviewed cleanup, never deleted.
func failCopy(cfg HostConfig, reservation Reservation, err error) {
	detail := err.Error()
	if len(detail) > 512 {
		detail = detail[:512]
	}
	_ = recordReservationOutcome(cfg, reservation, outcomeFailed, detail)
}

// copySourceToStaging streams the installed source into a private
// staging directory, verifying the discovery digest on the stream
// and again on read-back. Mode and mtime are preserved from the
// source; the 0555 freeze protection is applied at publication, not
// here. Any failure retains the staging and publishes nothing.
var errQuarantinedCopy = errors.New("quarantine on copied attributes")

type stagedCopy struct {
	writer *os.File
	dir    *pinnedDir
	file   *os.File
	attrs  map[string][]byte
}

func (s *stagedCopy) close() { _ = s.file.Close(); _ = s.dir.close() }

func copySourceToStaging(ctx context.Context, cfg HostConfig, d Discovery, reservation Reservation) (*stagedCopy, int64, error) {
	if err := freezeRefuseIfCanceled(ctx, StageCopy); err != nil {
		return nil, 0, err
	}
	src, err := openInstallSource(d.Identity.Path)
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, err)
	}
	defer src.Close()
	srcID, err := descriptorIdentity(src)
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, err)
	}
	if !srcID.mode.IsRegular() || srcID.mode.Perm()&0o111 == 0 {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, fmt.Errorf("source is not executable regular bytes"))
	}
	if err := checkLinkOwner(d.Identity.Path, srcID); err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, err)
	}
	attrs, err := captureXattrs(ctx, src)
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, err)
	}
	if _, quarantined := attrs[quarantineAttr]; quarantined {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceQuarantined, errQuarantinedCopy)
	}
	srcInfo, err := src.Stat()
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, err)
	}
	created, err := cfg.store.staging.mkdirChild(reservation.Transaction)
	if err != nil || !created {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, fmt.Errorf("staging transaction exists or failed: %v", err))
	}
	dir, err := cfg.store.staging.openChildDir(reservation.Transaction)
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	dst, err := dir.openFile(frozenBinaryName(d.Identity.Build), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		_ = dir.close()
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	staged := &stagedCopy{dir: dir, file: dst, writer: dst, attrs: attrs}
	accepted := false
	defer func() {
		if !accepted {
			staged.close()
		}
	}()
	streamDigest, err := streamCopy(ctx, src, dst)
	if err != nil {
		// Preserve the public copy-stage wrapper and its typed cancellation cause.
		return nil, 0, &FreezeError{Stage: StageCopy, Code: CodeFreezeCopyFailed, Cause: err}
	}
	if streamDigest != d.Identity.SHA256 {
		return nil, 0, &FreezeError{Stage: StageCopy, Code: CodeSourceUnusable, Cause: fmt.Errorf("installed bytes disagree with the discovery digest")}
	}
	freezeInterpose(freezeAfterCopyStream, hookFor(d, reservation.Transaction))
	// ctime includes attribute changes; compare the held source after streaming.
	// Late quarantine cannot enter the captured attribute set or be accepted as
	// an unchanged source. No second source-attribute read mixes snapshots.
	after, err := descriptorIdentity(src)
	if err != nil || after != srcID {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, fmt.Errorf("source changed during copy: %v", err))
	}
	if err := dst.Chmod(srcID.mode.Perm()); err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	if err := copyXattrs(ctx, attrs, dst); err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	freezeInterpose(freezeAfterXattrCopy, hookFor(d, reservation.Transaction))
	if err := verifyStagedXattrs(ctx, attrs, dst); err != nil {
		code := CodeFreezeCopyFailed
		if errors.Is(err, errQuarantinedCopy) {
			code = CodeSourceQuarantined
		}
		return nil, 0, freezeRefusal(ctx, StageCopy, code, err)
	}
	mtime := srcInfo.ModTime()
	if err := dir.chtimesChild(frozenBinaryName(d.Identity.Build), mtime); err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	if err := fsyncFile(dst); err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	readBack, err := hashFileDescriptor(ctx, StageCopy, dst)
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	if readBack != d.Identity.SHA256 {
		return nil, 0, &FreezeError{Stage: StageCopy, Code: CodeFreezeCopyFailed, Cause: fmt.Errorf("staged bytes changed after the copy stream")}
	}
	if err := dir.sync(); err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	info, err := dst.Stat()
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	readHandle, err := pinStagedReadHandle(dir, frozenBinaryName(d.Identity.Build), dst)
	if err != nil {
		return nil, 0, freezeRefusal(ctx, StageCopy, CodeFreezeCopyFailed, err)
	}
	staged.file = readHandle
	accepted = true
	return staged, info.Size(), nil
}

// streamCopy streams the source into the staging file in bounded
// chunks with a context check per chunk, hashing what it writes.
// The returned digest covers exactly the staged bytes.
func streamCopy(ctx context.Context, src, dst *os.File) (string, error) {
	hash := sha256.New()
	chunk := make([]byte, hashReadChunk)
	for {
		if err := freezeRefuseIfCanceled(ctx, StageCopy); err != nil {
			return "", err
		}
		freezeInterpose(freezeCopyChunk, FreezeHook{})
		n, err := src.Read(chunk)
		if n > 0 {
			if _, werr := dst.Write(chunk[:n]); werr != nil {
				return "", werr
			}
			_, _ = hash.Write(chunk[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	if err := freezeRefuseIfCanceled(ctx, StageCopy); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// publishStaging atomically publishes a verified staged copy: fresh
// or empty-adopted key directory, rename, syncs, the explicit 0555
// protection step, COPIED state under the metadata lock, and the
// reservation outcome. Existing content is never overwritten.
func publishStaging(ctx context.Context, cfg HostConfig, d Discovery, reservation Reservation, staged *stagedCopy, size int64) (FrozenCopy, error) {
	hook := hookFor(d, reservation.Transaction)
	hook.stagedFile = staged.file
	hook.closedWriter = staged.writer
	freezeInterpose(freezeBeforePublish, hook)
	if err := freezeRefuseIfCanceled(ctx, StagePublish); err != nil {
		failCopy(cfg, reservation, err)
		return FrozenCopy{}, err
	}
	key := freezeKey(d.Identity.Build, d.Identity.SHA256)
	binaryName := frozenBinaryName(d.Identity.Build)
	target := frozenBinaryPath(cfg.FrozenRoot, key, d.Identity.Build)
	buildsPin := cfg.store.builds
	created, err := buildsPin.mkdirChild(key)
	if err != nil {
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}
	keyPin, err := buildsPin.openChildDir(key)
	if err != nil {
		collErr := &FreezeError{Stage: StagePublish, Code: CodeFrozenCollision, Cause: err}
		failCopy(cfg, reservation, collErr)
		return FrozenCopy{}, collErr
	}
	defer func() { _ = keyPin.close() }()
	if !created {
		if err := ensureEmptyKeyDir(keyPin); err != nil {
			collErr := &FreezeError{Stage: StagePublish, Code: CodeFrozenCollision, Cause: err}
			failCopy(cfg, reservation, collErr)
			return FrozenCopy{}, collErr
		}
	}
	exists, err := keyPin.childExists(binaryName)
	if err != nil {
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}
	if exists {
		collErr := &FreezeError{Stage: StagePublish, Code: CodeFrozenCollision, Cause: fmt.Errorf("%s already exists", target)}
		failCopy(cfg, reservation, collErr)
		return FrozenCopy{}, collErr
	}
	// The staging descriptor is still held; the publication hook cannot
	// substitute its parent. Recheck attributes just before the rename.
	if err := verifyStagedXattrs(ctx, staged.attrs, staged.file); err != nil {
		return FrozenCopy{}, freezeRefusal(ctx, StagePublish, CodeFreezeCopyFailed, err)
	}
	stagingPin := staged.dir
	if err := sameChildFile(stagingPin, binaryName, staged.file); err != nil {
		return FrozenCopy{}, freezeRefusal(ctx, StagePublish, CodeFrozenCollision, err)
	}
	if err := renamePinnedChild(stagingPin, keyPin, binaryName, binaryName); err != nil {
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}
	if err := keyPin.chmodSelf(0o555); err != nil {
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}
	if err := keyPin.chmodChild(binaryName, 0o555); err != nil {
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}
	if err := fsyncPinnedFile(keyPin, binaryName); err != nil {
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}
	if err := keyPin.sync(); err != nil {
		return FrozenCopy{}, &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationUnknown, Cause: err}
	}
	if err := buildsPin.sync(); err != nil {
		return FrozenCopy{}, &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationUnknown, Cause: err}
	}
	releaseState, err := acquireStateLockAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok {
			return FrozenCopy{}, ferr
		}
		return FrozenCopy{}, &FreezeError{Stage: StagePublish, Code: CodeFreezeLeaseFailed, Cause: err}
	}
	state := BuildState{
		Schema:        buildStateSchema,
		Build:         d.Identity.Build,
		SHA256:        d.Identity.SHA256,
		Size:          size,
		Transaction:   reservation.Transaction,
		ProbeContract: ProbeContractID,
		State:         StateCopied,
	}
	writeErr := writeBuildStateAt(ctx, cfg.store.meta, key, state)
	releaseErr := releaseState()
	if writeErr != nil {
		if errors.Is(writeErr, errPublicationUnknown) {
			return FrozenCopy{}, &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationUnknown, Cause: writeErr}
		}
		pubErr := &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: writeErr}
		failCopy(cfg, reservation, pubErr)
		return FrozenCopy{}, pubErr
	}
	if releaseErr != nil {
		return FrozenCopy{}, &FreezeError{Stage: StagePublish, Code: CodeFreezeLeaseFailed, Cause: releaseErr}
	}
	if err := recordReservationOutcome(cfg, reservation, outcomePublished, "bytes and COPIED state published"); err != nil {
		if errors.Is(err, errPublicationUnknown) {
			return FrozenCopy{}, &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationUnknown, Cause: err}
		}
		return FrozenCopy{}, &FreezeError{Stage: StagePublish, Code: CodeFreezePublicationFailed, Cause: err}
	}
	freezeInterpose(freezeAfterPublish, hookFor(d, reservation.Transaction))
	if err := freezeRefuseIfCanceled(ctx, StagePublish); err != nil {
		return FrozenCopy{}, err
	}
	return FrozenCopy{
		Frozen:  FrozenIdentity{Build: d.Identity.Build, SHA256: d.Identity.SHA256, Path: target},
		Outcome: OutcomePublished,
	}, nil
}

// ensureEmptyKeyDir adopts a leftover empty key directory from a
// crashed publish without
// deletion. Any content refuses: only emptiness is adoptable. The
// inventory reads through the pinned directory FD, never by path.
func ensureEmptyKeyDir(keyPin *pinnedDir) error {
	entries, err := keyPin.entries()
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("%s holds %d foreign entries", keyPin.path, len(entries))
	}
	return nil
}

// fsyncPinnedFile flushes a published file through a no-follow open
// under its pinned parent.
func fsyncPinnedFile(dir *pinnedDir, name string) error {
	file, err := dir.openChildFile(name)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return fsyncFile(file)
}

// adoptPublishedCopy recovers complete verified published bytes left
// without state by a crash: it applies the 0555 protection the crash
// may have skipped and publishes the missing COPIED state under the
// bounded state lock, without copying or billing again. The caller
// holds the shared maintenance lease across the byte lookup and this
// adoption. The bytes are authoritative — they must verify — and the
// reservation proves the transaction; any outcome adopts, because a
// failed outcome with complete bytes means the failure was
// elsewhere. Partial bytes, missing transactions and malformed state
// never adopt.
func adoptPublishedCopy(ctx context.Context, cfg HostConfig, d Discovery, key string) (*FrozenCopy, error) {
	if err := freezeRefuseIfCanceled(ctx, StageDedupe); err != nil {
		return nil, err
	}
	reservation, err := findAdoptionReservation(ctx, cfg.LedgerDir, d.Identity.Build, d.Identity.SHA256, cfg.Host)
	if err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeUnavailable, err)
	}
	if reservation == nil {
		return nil, &FreezeError{Stage: StageDedupe, Code: CodeFreezeUnavailable, Cause: fmt.Errorf("published bytes without a trustworthy transaction are unknown")}
	}
	releaseState, err := acquireStateLockAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok {
			return nil, ferr
		}
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeLeaseFailed, err)
	}
	defer func() { _ = releaseState() }()
	if state, present, err := readBuildStateAt(ctx, cfg.store.meta, key); err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
	} else if present {
		// A concurrent publisher won while this reader waited for state.lock.
		if err := state.agreesWith(key, d.Identity); err != nil {
			return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
		}
		if state.State == StateBAD {
			return nil, &FreezeError{Stage: StageDedupe, Code: CodeRecordedBad, Bad: state.Verdict}
		}
		frozen, err := verifyFrozenTuple(ctx, cfg.store, key, d.Identity)
		if err != nil {
			return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
		}
		outcome := OutcomeResumed
		if state.State == StateProbeOK {
			outcome = OutcomeReused
		}
		return &FrozenCopy{Frozen: frozen, Outcome: outcome}, nil
	}
	keyPin, file, err := openFrozenBytes(cfg.store, key, d.Identity.Build, false)
	if err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
	}
	defer keyPin.close()
	defer file.Close()
	digest, err := hashFileDescriptor(ctx, StageDedupe, file)
	if err != nil || digest != d.Identity.SHA256 {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeUnavailable, fmt.Errorf("published bytes no longer verify: %v", err))
	}
	if err := keyPin.chmodSelf(0o555); err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezePublicationFailed, err)
	}
	if err := file.Chmod(0o555); err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezePublicationFailed, err)
	}
	if err := fsyncFile(file); err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezePublicationFailed, err)
	}
	if err := keyPin.sync(); err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezePublicationUnknown, err)
	}
	frozen := FrozenIdentity{Build: d.Identity.Build, SHA256: digest, Path: file.Name()}
	info, err := file.Stat()
	if err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
	}
	state := BuildState{
		Schema:        buildStateSchema,
		Build:         d.Identity.Build,
		SHA256:        d.Identity.SHA256,
		Size:          info.Size(),
		Transaction:   reservation.Transaction,
		ProbeContract: ProbeContractID,
		State:         StateCopied,
	}
	if err := writeBuildStateAt(ctx, cfg.store.meta, key, state); err != nil {
		if errors.Is(err, errPublicationUnknown) {
			return nil, &FreezeError{Stage: StageDedupe, Code: CodeFreezePublicationUnknown, Cause: err}
		}
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezePublicationFailed, err)
	}
	if err := recordReservationOutcome(cfg, *reservation, outcomePublished, "adopted after a crash"); err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezePublicationFailed, err)
	}
	return &FrozenCopy{Frozen: frozen, Outcome: OutcomeAdopted}, nil
}

// Close every writable handle before a published COPIED state can authorize
// a concurrent probe. The read handle pins the same inode (no pathname open),
// so attributes and final publication still use descriptor-bound evidence.
func pinStagedReadHandle(dir *pinnedDir, name string, writer *os.File) (*os.File, error) {
	reader, err := dir.openFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	rinfo, err := reader.Stat()
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	winfo, err := writer.Stat()
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	if !rinfo.Mode().IsRegular() || !os.SameFile(rinfo, winfo) {
		_ = reader.Close()
		return nil, fmt.Errorf("staging inode changed before writer close")
	}
	if err := writer.Close(); err != nil {
		_ = reader.Close()
		return nil, err
	}
	return reader, nil
}
