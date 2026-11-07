package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func policyBytes(t *testing.T, grants ...createPolicyGrant) []byte {
	t.Helper()
	b, err := json.Marshal(createPolicyDocument{Version: 1, Grants: grants})
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}

func fixtureGrant() createPolicyGrant {
	return createPolicyGrant{"swarm-orch", strings.Repeat("a", 64), "inert-v1"}
}

// Distinct UID is a synthetic composition assumption, not a production caller
// authentication claim. No test changes identity or executes a native runtime.
func operatorPolicyFixture(t *testing.T, data []byte) (*operatorCreatePolicy, string) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "create-policy.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	p, err := newOperatorCreatePolicy(operatorCreatePolicyConfig{
		path: path, execProfile: "inert-v1", operatorUID: uint32(os.Geteuid()), callerUID: uint32(os.Geteuid()) + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	return p, path
}

func writePolicy(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCreateOperatorPolicyStrictEncoding(t *testing.T) {
	good := policyBytes(t, fixtureGrant())
	wrongProfile, wrongLock, wrongExec := fixtureGrant(), fixtureGrant(), fixtureGrant()
	wrongProfile.Profile, wrongLock.LockSHA256, wrongExec.ExecProfile = "other-profile", strings.Repeat("b", 64), "other-exec"
	duplicate := fixtureGrant()
	conflict := duplicate
	conflict.ExecProfile = "other-exec"
	for name, data := range map[string][]byte{
		"malformed":           []byte(`{"version":`),
		"unknown-version":     bytes.Replace(good, []byte(`"version":1`), []byte(`"version":2`), 1),
		"unknown-field":       bytes.Replace(good, []byte(`"version":1`), []byte(`"version":1,"allow":true`), 1),
		"unknown-grant-field": bytes.Replace(good, []byte(`"profile":`), []byte(`"allow":true,"profile":`), 1),
		"case-folded-key":     bytes.Replace(good, []byte(`"version"`), []byte(`"Version"`), 1),
		"duplicate-key":       bytes.Replace(good, []byte(`"version":1`), []byte(`"version":2,"version":1`), 1),
		"duplicate-nested":    bytes.Replace(good, []byte(`"profile":"swarm-orch"`), []byte(`"profile":"other","profile":"swarm-orch"`), 1),
		"duplicate-grant":     policyBytes(t, duplicate, duplicate),
		"conflicting-grant":   policyBytes(t, duplicate, conflict),
		"wrong-profile":       policyBytes(t, wrongProfile),
		"wrong-lock":          policyBytes(t, wrongLock),
		"wrong-exec-profile":  policyBytes(t, wrongExec),
		"uppercase-lock":      bytes.ReplaceAll(good, []byte(strings.Repeat("a", 64)), []byte(strings.Repeat("A", 64))),
		"missing-key":         bytes.Replace(good, []byte(`"version":1,`), nil, 1),
		"null-grants":         []byte("{\"version\":1,\"grants\":null}\n"),
		"empty-grants":        policyBytes(t),
		"null-grant":          []byte("{\"version\":1,\"grants\":[null]}\n"),
		"trailing-document":   append(append([]byte(nil), good...), []byte("{}")...),
		"whitespace":          append([]byte(" "), good...),
		"missing-lf":          good[:len(good)-1],
		"oversize":            bytes.Repeat([]byte(" "), maxCreatePolicyBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			h, body, _, starts := createFixture(t)
			p, path := operatorPolicyFixture(t, data)
			h.policy = p
			if got, err := h.create(context.Background(), "launch", body); err == nil || got != createRefused || *starts != 0 {
				t.Fatal("invalid policy started", got, err, *starts)
			}
			// Repair cannot resurrect this actual handler or its policy object.
			writePolicy(t, path, good)
			got, _ := h.create(context.Background(), "launch", body)
			if got != createRefused || *starts != 0 || !p.lost {
				t.Fatal("repair retried refusal", got, *starts, p.lost)
			}
		})
	}
	// Even an unrelated duplicate must invalidate the entire configuration.
	other := fixtureGrant()
	other.Profile = "other"
	if _, err := parseCreatePolicy(policyBytes(t, fixtureGrant(), other, other)); err == nil {
		t.Fatal("ignored unrelated duplicate")
	}
	grants := make([]createPolicyGrant, maxCreatePolicyGrants+1)
	if _, err := parseCreatePolicy(policyBytes(t, grants...)); err == nil {
		t.Fatal("unbounded grants")
	}
}

func TestCreateOperatorPolicyUnsafeFiles(t *testing.T) {
	for _, name := range []string{"group-read", "group-write", "other-read", "executable", "special-mode", "directory", "fifo", "hardlink", "symlink", "ancestor-symlink", "writable-parent", "writable-ancestor", "foreign-owner"} {
		t.Run(name, func(t *testing.T) {
			h, body, _, starts := createFixture(t)
			p, path := operatorPolicyFixture(t, policyBytes(t, fixtureGrant()))
			h.policy = p
			switch name {
			case "group-read", "group-write", "other-read", "executable", "special-mode":
				mode := map[string]os.FileMode{"group-read": 0640, "group-write": 0620, "other-read": 0604, "executable": 0700, "special-mode": 0600 | os.ModeSetuid}[name]
				if err := os.Chmod(path, mode); err != nil {
					t.Fatal(err)
				}
			case "directory", "fifo", "symlink":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				var err error
				if name == "directory" {
					err = os.Mkdir(path, 0700)
				} else if name == "fifo" {
					err = syscall.Mkfifo(path, 0600)
				} else {
					target := path + ".target"
					writePolicy(t, target, policyBytes(t, fixtureGrant()))
					err = os.Symlink(target, path)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, path+".link"); err != nil {
					t.Fatal(err)
				}
			case "ancestor-symlink":
				link := filepath.Join(t.TempDir(), "linked-parent")
				if err := os.Symlink(filepath.Dir(path), link); err != nil {
					t.Fatal(err)
				}
				p.config.path = filepath.Join(link, filepath.Base(path))
			case "writable-parent", "writable-ancestor":
				if name == "writable-ancestor" {
					sub := filepath.Join(filepath.Dir(path), "sub")
					if err := os.Mkdir(sub, 0700); err != nil {
						t.Fatal(err)
					}
					p.config.path = filepath.Join(sub, filepath.Base(path))
					writePolicy(t, p.config.path, policyBytes(t, fixtureGrant()))
				}
				if err := os.Chmod(filepath.Dir(path), 0777); err != nil {
					t.Fatal(err)
				}
			case "foreign-owner":
				// Expected root ownership mismatches the actual operator-owned file.
				// This exercises real stat UID evidence without chown/root access.
				if os.Geteuid() == 0 {
					t.Skip("qualification requires the unprivileged agent boundary")
				}
				p.config.operatorUID = 0
			}
			if got, err := h.create(context.Background(), "launch", body); err == nil || got != createRefused || *starts != 0 {
				t.Fatal("unsafe policy accepted", got, err, *starts)
			}
		})
	}
}

func TestCreateOperatorPolicyCompositionBoundary(t *testing.T) {
	p, path := operatorPolicyFixture(t, policyBytes(t, fixtureGrant()))
	info, err := os.Stat(path)
	if err != nil || !createPolicyFileSafe(info, uint32(os.Geteuid())) || createPolicyFileSafe(info, uint32(os.Geteuid())+1) {
		t.Fatal("file ownership gate did not check actual stat UID", err)
	}
	for _, name := range []string{"same-uid", "root-caller", "host-uid", "unknown-operator", "relative-path", "unclean-path", "unknown-exec-profile"} {
		t.Run(name, func(t *testing.T) {
			c := p.config
			switch name {
			case "same-uid":
				c.callerUID = c.operatorUID
			case "root-caller":
				c.callerUID = 0
			case "host-uid":
				c.operatorUID, c.callerUID = 0, uint32(os.Geteuid())
			case "unknown-operator":
				c.operatorUID = uint32(os.Geteuid()) + 10
			case "relative-path":
				c.path = "policy.json"
			case "unclean-path":
				c.path = filepath.Dir(c.path) + "/../policy.json"
			case "unknown-exec-profile":
				c.execProfile = ""
			}
			if candidate, err := newOperatorCreatePolicy(c); err == nil || candidate != nil {
				t.Fatal("unproven composition boundary accepted")
			}
		})
	}
}

// A deterministic mutation after check one but before the final Start gate.
// It wraps the real adapter; it cannot supply a permissive decision of its own.
type changingOperatorPolicy struct {
	*operatorCreatePolicy
	calls  int
	change func()
}

func (p *changingOperatorPolicy) checkCreate(ctx context.Context, op *ordinaryWireOperation) error {
	p.calls++
	err := p.operatorCreatePolicy.checkCreate(ctx, op)
	if err == nil && p.calls == 1 {
		p.change()
	}
	return err
}

func TestCreateOperatorPolicyFinalTOCTOURefusals(t *testing.T) {
	for _, name := range []string{"digest-drift", "same-bytes-replacement", "symlink-swap", "parent-replacement", "unsafe-mode", "record-drift"} {
		t.Run(name, func(t *testing.T) {
			h, body, _, starts := createFixture(t)
			good := policyBytes(t, fixtureGrant())
			p, path := operatorPolicyFixture(t, good)
			changing := &changingOperatorPolicy{operatorCreatePolicy: p}
			h.policy = changing
			changing.change = func() {
				switch name {
				case "digest-drift":
					before, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					// Same inode/size/mode/mtime, so digest is the decisive control.
					writePolicy(t, path, bytes.Replace(good, []byte("inert-v1"), []byte("inert-v2"), 1))
					if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
						t.Fatal(err)
					}
				case "same-bytes-replacement", "symlink-swap":
					if err := os.Rename(path, path+".original"); err != nil {
						t.Fatal(err)
					}
					if name == "symlink-swap" {
						if err := os.Symlink(path+".original", path); err != nil {
							t.Fatal(err)
						}
					} else {
						writePolicy(t, path, good)
					}
				case "parent-replacement":
					dir := filepath.Dir(path)
					if err := os.Rename(dir, dir+".original"); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.RemoveAll(dir + ".original") })
					if err := os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
					writePolicy(t, path, good)
				case "unsafe-mode":
					if err := os.Chmod(path, 0666); err != nil {
						t.Fatal(err)
					}
				case "record-drift":
					writePolicy(t, filepath.Join(h.wire.owner.records.path, "member-1.json"), []byte("{}"))
				}
			}
			got, err := h.create(context.Background(), "launch", body)
			if err == nil || got != createRefused || *starts != 0 || !h.operation.claim.attempted {
				t.Fatal("TOCTOU permitted Start", got, err, *starts)
			}
			if name != "record-drift" && (changing.calls != 2 || !p.lost || p.file != nil || p.root != nil) {
				t.Fatal("final adapter check or cleanup missing", changing.calls, p.lost)
			}
		})
	}
}

func TestCreateOperatorPolicyRetainedOrigin(t *testing.T) {
	h, body, _, starts := createFixture(t)
	p, _ := operatorPolicyFixture(t, policyBytes(t, fixtureGrant()))
	op, err := h.wire.dispatch(context.Background(), "launch", body)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.checkCreate(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	if p.digest != sha256.Sum256(policyBytes(t, fixtureGrant())) || p.operation != op || p.grant != fixtureGrant() {
		t.Fatal("did not pin actual operation, digest and grant")
	}
	// Identical DTO and pointers in a copied ordinary operation are insufficient.
	copy := *op
	if err := p.checkCreate(context.Background(), &copy); err == nil || *starts != 0 {
		t.Fatal("copied operation substituted for actual custody")
	}
	if err := p.checkCreate(context.Background(), op); err == nil {
		t.Fatal("terminal policy loss repaired")
	}
	// A grant matching only the in-memory request projection must not authorize.
	for _, key := range []string{"profile", "lock"} {
		t.Run(key, func(t *testing.T) {
			h, body, _, starts := createFixture(t)
			op, err := h.wire.dispatch(context.Background(), "launch", body)
			if err != nil {
				t.Fatal(err)
			}
			g := fixtureGrant()
			if key == "profile" {
				g.Profile = "request-only"
				op.claim.retained.request.Profile.Name = g.Profile
			} else {
				g.LockSHA256 = strings.Repeat("c", 64)
				op.claim.retained.request.Profile.Lock = g.LockSHA256
			}
			p, _ := operatorPolicyFixture(t, policyBytes(t, g))
			h.policy = p
			if _, err := h.create(context.Background(), "launch", body); err == nil || *starts != 0 {
				t.Fatal("expected DTO became policy authority")
			}
		})
	}
}

func TestCreateOperatorPolicyValidInertPositive(t *testing.T) {
	h, body, _, starts := createFixture(t)
	p, path := operatorPolicyFixture(t, policyBytes(t, fixtureGrant()))
	h.policy = p
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	if got, err := h.create(context.Background(), "launch", body); err != nil || got != createRunnerOwned || *starts != 1 || p.grant.ExecProfile != "inert-v1" {
		t.Fatal(got, err, *starts)
	}
	if err := h.close(context.Background()); err != nil || p.file != nil || p.root != nil || !p.lost {
		t.Fatal("policy handle cleanup lost", err)
	}
}

func TestCreateOperatorPolicyRealInertCustody(t *testing.T) {
	m, q := ordinaryWireFixture(t)
	script := []byte("#!/bin/sh\nread ignored\n")
	writePolicy(t, m.launcher.path, script)
	if err := os.Chmod(m.launcher.path, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(script)
	dir, err := os.MkdirTemp("", "d2-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	cfg := ordinaryLauncherConfig{binary: m.launcher.path, sha256: hex.EncodeToString(sum[:]), environment: m.environment.entries}
	p, _ := operatorPolicyFixture(t, policyBytes(t, fixtureGrant()))
	h, err := openOrdinaryCreateHandler(context.Background(), time.Now().Add(10*time.Second), m.owner.records.path, cfg, filepath.Join(dir, "runner.sock"), 100*time.Millisecond, p)
	if err != nil {
		t.Fatal(err)
	}
	defer h.close(context.Background())
	if got, err := h.create(context.Background(), "launch", memberOperationBytes(t, q)); err != nil || got != createRunnerOwned {
		t.Fatal(got, err)
	}
	receipt, err := h.custody()
	if err != nil || receipt.operation != h.operation || receipt.claim != h.operation.claim || receipt.retained != h.operation.claim.retained || receipt.source != h.wire.owner.records || receipt.server != h.child {
		t.Fatal("T2 observation did not preserve exact actual handles", err)
	}
}
