package sessionmanager

import (
	"errors"
	"syscall"
)

const ErrorSessionProcessIdentityUnverified = "session_process_identity_unverified"

// providerProcessIdentity binds a persisted PID to its kernel-observed owner,
// birth time and process group. Missing identity is never a signal capability.
// This private evidence is recorded at launch, not reconstructed at restart.
type providerProcessIdentity struct {
	PID   int    `json:"pid"`
	UID   uint32 `json:"uid"`
	Start string `json:"start"`
	PGID  int    `json:"pgid"`
}

func verifyProviderProcessIdentity(pid int, expected *providerProcessIdentity) (providerProcessIdentity, error) {
	refuse := func() (providerProcessIdentity, error) {
		return providerProcessIdentity{}, &PublicError{Code: ErrorSessionProcessIdentityUnverified, Message: "persisted provider process identity cannot be verified; no signal was sent"}
	}
	if expected == nil || expected.PID != pid || expected.Start == "" || pid <= 1 {
		return refuse()
	}
	actual, err := readProviderProcessIdentity(pid)
	if err != nil || actual.PID != expected.PID || actual.UID != expected.UID || actual.Start != expected.Start || actual.PGID != expected.PGID {
		return refuse()
	}
	return actual, nil
}

// signalPersistedProviderGroup is the one restart signal primitive. Only a
// verified original group leader can authorize signaling its persisted group.
// A kernel-read failure, reused PID or changed group sends no signal.
func signalPersistedProviderGroup(pid int, expected *providerProcessIdentity, signal syscall.Signal) error {
	actual, err := verifyProviderProcessIdentity(pid, expected)
	if err != nil {
		return err
	}
	if actual.PGID != pid {
		return &PublicError{Code: ErrorSessionProcessIdentityUnverified, Message: "persisted provider is not its recorded process-group leader; no signal was sent"}
	}
	if err := syscall.Kill(-pid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
