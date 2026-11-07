package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
)

const maxCreatePolicyBytes = 64 << 10
const maxCreatePolicyGrants = 128

var createPolicyID = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// All fields are selected by trusted composition, never by an operation DTO.
// execProfile names THIS composition's fixed executable/environment policy;
// the file cannot select another launcher, args, environment or native mode.
// callerUID must come from an authenticated OS caller boundary. Supplying a
// different number is not proof of isolation. No production route installs
// this candidate; adoption must establish that boundary independently first.
type operatorCreatePolicyConfig struct {
	path, execProfile      string
	operatorUID, callerUID uint32
}

// Version 1 is exactly json.Marshal(document) plus LF, in this field order.
// Byte equality rejects unknown/case-folded/duplicate keys, nulls, alternate
// numeric/string encodings, whitespace, missing keys and trailing documents.
type createPolicyDocument struct {
	Version int                 `json:"version"`
	Grants  []createPolicyGrant `json:"grants"`
}
type createPolicyGrant struct {
	Profile     string `json:"profile"`
	LockSHA256  string `json:"lock_sha256"`
	ExecProfile string `json:"exec_profile"`
}

func parseCreatePolicy(data []byte) (createPolicyDocument, error) {
	var d createPolicyDocument
	if len(data) > maxCreatePolicyBytes || json.Unmarshal(data, &d) != nil || d.Version != 1 || len(d.Grants) == 0 || len(d.Grants) > maxCreatePolicyGrants {
		return createPolicyDocument{}, errOrdinaryWireUnavailable
	}
	encoded, err := json.Marshal(d)
	if err != nil || !bytes.Equal(data, append(encoded, '\n')) {
		return createPolicyDocument{}, errOrdinaryWireUnavailable
	}
	seen := make(map[[2]string]bool)
	for _, g := range d.Grants {
		key := [2]string{g.Profile, g.LockSHA256}
		if !createPolicyID.MatchString(g.Profile) || !notificationSHA256(g.LockSHA256) || !createPolicyID.MatchString(g.ExecProfile) || seen[key] {
			return createPolicyDocument{}, errOrdinaryWireUnavailable
		}
		seen[key] = true
	}
	return d, nil
}

// A policy belongs to one handler/actual operation. First successful checking
// pins the file, directory ancestry, bytes and selected grant. All later loss
// is terminal, even if the operator restores the original file afterwards.
type operatorCreatePolicy struct {
	mu                  sync.Mutex
	config              operatorCreatePolicyConfig
	operation           *ordinaryWireOperation
	root                *os.Root
	file                *os.File
	identity            os.FileInfo
	parents             []os.FileInfo // parent directory through filesystem root
	digest              [32]byte
	grant               createPolicyGrant
	lost                bool
	singleTrust         *privateCompositionConfig
	configurationDigest string
}

func newOperatorCreatePolicy(c operatorCreatePolicyConfig) (*operatorCreatePolicy, error) {
	// Owner-only files do not protect against same-UID host agents. Refuse a
	// known shared caller UID, root callers, and callers running as this host.
	if !filepath.IsAbs(c.path) || filepath.Clean(c.path) != c.path || !createPolicyID.MatchString(c.execProfile) ||
		(c.operatorUID != 0 && c.operatorUID != uint32(os.Geteuid())) ||
		c.callerUID == 0 || c.callerUID == c.operatorUID || c.callerUID == uint32(os.Geteuid()) {
		return nil, errOrdinaryWireUnavailable
	}
	return &operatorCreatePolicy{config: c}, nil
}

func createPolicyFileSafe(info os.FileInfo, uid uint32) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode() & ^os.ModePerm != 0 ||
		(info.Mode().Perm() != 0400 && info.Mode().Perm() != 0600) || info.Size() <= 0 || info.Size() > maxCreatePolicyBytes {
		return false
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	return ok && s.Uid == uid && s.Nlink == 1
}

func createPolicyDirectorySafe(info os.FileInfo, uid uint32) bool {
	if info == nil || !info.IsDir() || info.Mode() & ^(os.ModeDir|os.ModePerm|os.ModeSticky) != 0 {
		return false
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || (s.Uid != 0 && s.Uid != uid) {
		return false
	}
	// Root-owned sticky ancestors (e.g. /tmp) cannot let a distinct caller
	// replace operator-owned children. All other ancestors exclude writes.
	return info.Mode().Perm()&0022 == 0 || (s.Uid == 0 && info.Mode()&os.ModeSticky != 0)
}

func (p *operatorCreatePolicy) directories() ([]os.FileInfo, error) {
	if !canonicalRecordDirectory(p.config.path) {
		return nil, errOrdinaryWireUnavailable
	}
	var parents []os.FileInfo
	for path := filepath.Dir(p.config.path); ; path = filepath.Dir(path) {
		info, err := os.Lstat(path)
		if err != nil || !createPolicyDirectorySafe(info, p.config.operatorUID) {
			return nil, errOrdinaryWireUnavailable
		}
		parents = append(parents, info)
		if path == filepath.Dir(path) {
			break
		}
	}
	if p.parents != nil {
		if len(parents) != len(p.parents) {
			return nil, errOrdinaryWireUnavailable
		}
		for i := range parents {
			if !os.SameFile(parents[i], p.parents[i]) {
				return nil, errOrdinaryWireUnavailable
			}
		}
	}
	if p.root != nil {
		held, err := p.root.Stat(".")
		if err != nil || !createPolicyDirectorySafe(held, p.config.operatorUID) || !os.SameFile(held, parents[0]) {
			return nil, errOrdinaryWireUnavailable
		}
	}
	return parents, nil
}

func (p *operatorCreatePolicy) sameFile(a, b os.FileInfo) bool {
	return createPolicyFileSafe(a, p.config.operatorUID) && createPolicyFileSafe(b, p.config.operatorUID) &&
		os.SameFile(a, b) && a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

// Caller holds p.mu. NOFOLLOW/NONBLOCK and named/held observations on both
// sides of a bounded read refuse observed replacement, FIFO and symlink races.
// These checks are optimistic; they do not lock out an operator filesystem
// writer or make the final check atomic with process creation.
func (p *operatorCreatePolicy) read() ([]byte, error) {
	parents, err := p.directories()
	if err != nil {
		return nil, err
	}
	if p.root == nil {
		p.parents = parents
		p.root, err = os.OpenRoot(filepath.Dir(p.config.path))
		if err != nil {
			return nil, errOrdinaryWireUnavailable
		}
	}
	named, err := os.Lstat(p.config.path)
	if err != nil || !createPolicyFileSafe(named, p.config.operatorUID) {
		return nil, errOrdinaryWireUnavailable
	}
	if p.file == nil {
		p.identity = named
		p.file, err = p.root.OpenFile(filepath.Base(p.config.path), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, errOrdinaryWireUnavailable
		}
	}
	held, err := p.file.Stat()
	if err != nil || !p.sameFile(named, p.identity) || !p.sameFile(held, p.identity) {
		return nil, errOrdinaryWireUnavailable
	}
	data, err := io.ReadAll(io.NewSectionReader(p.file, 0, maxCreatePolicyBytes+1))
	after, e1 := p.file.Stat()
	namedAfter, e2 := os.Lstat(p.config.path)
	_, e3 := p.directories()
	if err != nil || e1 != nil || e2 != nil || e3 != nil || int64(len(data)) != p.identity.Size() ||
		!p.sameFile(after, p.identity) || !p.sameFile(namedAfter, p.identity) {
		return nil, errOrdinaryWireUnavailable
	}
	return data, nil
}

func (p *operatorCreatePolicy) checkCreate(ctx context.Context, op *ordinaryWireOperation) error {
	if p == nil {
		return errOrdinaryWireUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fail := func() error { p.release(); return errOrdinaryWireUnavailable }
	if p.lost || ctx == nil || ctx.Err() != nil || op == nil || op.manager == nil || op.manager.owner == nil || op.claim == nil || (p.operation != nil && p.operation != op) {
		return fail()
	}
	// Recheck actual custody before deriving the key from its retained record.
	// The request's profile/lock fields alone never select a grant.
	m := op.manager
	m.mu.Lock()
	m.owner.mu.Lock()
	valid := !m.closed && !m.owner.closed && m.owner.ctx.Err() == nil && m.operation == op && op.ready &&
		p.matchesSingleTrustLauncher(m) &&
		op.claim == m.owner.operation && op.claim.owner == m.owner && op.claim.ready && op.claim.kind == "launch" &&
		op.claim.retained.recheckPhysical(m.owner.records, op.claim.kind, op.claim.data) == nil
	var profile, lock string
	if valid {
		profile, lock = op.claim.retained.record.record.Profile, op.claim.retained.record.record.ProfileLock
	}
	m.owner.mu.Unlock()
	m.mu.Unlock()
	if !valid {
		return fail()
	}
	data, err := p.read()
	if err != nil {
		return fail()
	}
	digest := sha256.Sum256(data)
	if p.operation != nil {
		if digest != p.digest || p.grant.Profile != profile || p.grant.LockSHA256 != lock || ctx.Err() != nil || m.owner.ctx.Err() != nil {
			return fail()
		}
		return nil
	}
	var document createPolicyDocument
	if p.singleTrust != nil {
		document, err = parseSingleTrustUnitPolicy(data, p.configurationDigest)
	} else {
		document, err = parseCreatePolicy(data)
	}
	if err != nil {
		return fail()
	}
	for _, g := range document.Grants {
		if g.Profile == profile && g.LockSHA256 == lock && g.ExecProfile == p.config.execProfile && ctx.Err() == nil && m.owner.ctx.Err() == nil {
			p.operation, p.digest, p.grant = op, digest, g
			return nil
		}
	}
	return fail()
}

func (p *operatorCreatePolicy) release() {
	p.lost = true
	if p.file != nil {
		_ = p.file.Close()
		p.file = nil
	}
	if p.root != nil {
		_ = p.root.Close()
		p.root = nil
	}
}

func (p *operatorCreatePolicy) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.release()
}
