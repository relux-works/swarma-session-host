package musebuilds

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// HostLock is the injectable global admission lock. The production
// implementation shells out to the canonical mini-build-lock script
// through minilock.Client; tests use fakes that never touch the real
// global lock. One Acquire/Release pair serializes one freeze
// critical section: ramp re-read, reservation and copy/publication.
type HostLock interface {
	Acquire(ctx context.Context) (HostLockHeld, error)
}

// HostLockHeld is one acquired global lock. Check reports lock loss
// without releasing; Release drops the hold and reports lock loss
// when the hold already ended.
type HostLockHeld interface {
	Check() error
	Release() error
}

// HostConfig carries every host binding freeze needs. Production uses
// DefaultHostConfig, which resolves the account home once; tests
// build HostConfig literally with temporary directories and fakes.
// There is no ambient-home fallback: an empty path is invalid, never
// a request to guess the caller's home.
type HostConfig struct {
	store *frozenStore // request-local directory authority; never supplied by callers

	// FrozenRoot is the fixed per-account frozen root
	// (~/.local/share/muse-frozen). Freeze creates its layout.
	FrozenRoot string
	// LedgerDir is the infra-provisioned host reservation ledger
	// (~/.local/share/mini-infra/muse-freeze-ledger). An absent
	// directory means not provisioned and refuses; freeze never
	// creates it.
	LedgerDir string
	// RampStatePath and RampLogPath are the G2 Q1 ramp signal
	// (~/.config/syspolicyd-watch.state and
	// ~/Library/Logs/syspolicyd-watch.tsv), read through these
	// injected paths, never a hard-coded home.
	RampStatePath string
	RampLogPath   string
	// MaxFreezesPerDay is the host runaway guard: at most this many
	// NEW freezes per UTC day. Zero means unset, the product
	// default, which caps nothing. The mini deployment sets 3;
	// that wiring belongs to the launch path (L2C/L4), not to this
	// package, which only enforces the configured value.
	MaxFreezesPerDay int
	// Host and User name the ledger reservation. Both are required.
	Host string
	User string
	// Now supplies the trusted UTC clock; nil means time.Now.
	Now func() time.Time
	// Locker is the global admission lock. Nil refuses: production
	// sets the minilock client through DefaultHostConfig and tests
	// set a fake, so a missing lock is a wiring fault, not a hint
	// to skip serialization.
	Locker HostLock
	// Quarantine reports whether path carries a Gatekeeper
	// quarantine xattr. Nil means the production xattr check; tests
	// inject scripted answers. Quarantine is a macOS concept; the
	// production check is defined in the unix file.
	Quarantine func(path string) (bool, error)
}

// accountHome resolves the authenticated account's home directory
// from the password database, never from the HOME environment
// variable. Design section 3.1 fixes the freeze root per account
// with no environment override; os.UserHomeDir follows HOME on Unix,
// so a caller-controlled HOME must not redirect the frozen root,
// ledger, ramp bindings or lock script.
func accountHome() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	if u.HomeDir == "" || !filepath.IsAbs(u.HomeDir) {
		return "", errors.New("the account home directory is not absolute")
	}
	return u.HomeDir, nil
}

// DefaultHostConfig resolves the production host bindings from the
// account home once. It returns the product default of no daily cap;
// the mini deployment overrides MaxFreezesPerDay with 3.
func DefaultHostConfig() (HostConfig, error) {
	home, err := accountHome()
	if err != nil {
		return HostConfig{}, err
	}
	host, err := os.Hostname()
	if err != nil {
		return HostConfig{}, err
	}
	account := currentAccount()
	if account == "" {
		return HostConfig{}, errNoAccountName
	}
	return HostConfig{
		FrozenRoot:       filepath.Join(home, ".local", "share", "muse-frozen"),
		LedgerDir:        filepath.Join(home, ".local", "share", "mini-infra", "muse-freeze-ledger"),
		RampStatePath:    filepath.Join(home, ".config", "syspolicyd-watch.state"),
		RampLogPath:      filepath.Join(home, "Library", "Logs", "syspolicyd-watch.tsv"),
		MaxFreezesPerDay: 0,
		Host:             host,
		User:             account,
		Locker:           productionLocker(filepath.Join(home, ".local", "bin", "mini-build-lock"), home),
	}, nil
}

func currentAccount() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return ""
}

func (c HostConfig) clock() func() time.Time {
	if c.Now != nil {
		return c.Now
	}
	return time.Now
}

func (c HostConfig) quarantineCheck(path string) (bool, error) {
	if c.Quarantine != nil {
		return c.Quarantine(path)
	}
	return defaultQuarantineCheck(path)
}

// validateHostConfig refuses a config that cannot honestly admit a
// freeze. Every field is checked; the first failure names its field.
func validateHostConfig(c HostConfig) error {
	if c.FrozenRoot == "" || !filepath.IsAbs(c.FrozenRoot) {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidFrozenRoot}
	}
	if c.LedgerDir == "" || !filepath.IsAbs(c.LedgerDir) {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidLedgerDir}
	}
	if c.RampStatePath == "" || !filepath.IsAbs(c.RampStatePath) {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidRampState}
	}
	if c.RampLogPath == "" || !filepath.IsAbs(c.RampLogPath) {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidRampLog}
	}
	if c.MaxFreezesPerDay < 0 {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidDayCap}
	}
	if c.Host == "" {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidHostName}
	}
	if c.User == "" {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errInvalidUserName}
	}
	if c.Locker == nil {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeInvalid, Cause: errMissingLocker}
	}
	return nil
}

// validateDiscoveryInput refuses a discovery value freeze cannot bind
// to bytes: an unparseable build, a malformed digest or a
// non-absolute source path. Existence and gates belong to the later
// source checks, which carry their own codes.
func validateDiscoveryInput(d Discovery) error {
	if _, err := muse.ParseBuildID(d.Identity.Build); err != nil {
		return &FreezeError{Stage: StageDedupe, Code: CodeFreezeInvalid, Cause: errInvalidDiscoveryBuild}
	}
	if !isLowerHex64(d.Identity.SHA256) {
		return &FreezeError{Stage: StageDedupe, Code: CodeFreezeInvalid, Cause: errInvalidDiscoveryDigest}
	}
	if d.Identity.Path == "" || !filepath.IsAbs(d.Identity.Path) {
		return &FreezeError{Stage: StageDedupe, Code: CodeFreezeInvalid, Cause: errInvalidDiscoveryPath}
	}
	return nil
}
