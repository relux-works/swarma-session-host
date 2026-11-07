package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func singleTrustPolicyFixture(t *testing.T, cfg privateCompositionConfig, grant createPolicyGrant) *operatorCreatePolicy {
	t.Helper()
	p, report, err := newSingleTrustUnitCreatePolicy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	grant.ExecProfile = PlainPathnameBackend
	b, err := json.Marshal(singleTrustUnitPolicyDocument{2, SingleTrustUnitProfile, PlainPathnameBackend, report.SelectedConfigurationHash, []createPolicyGrant{grant}})
	if err != nil {
		t.Fatal(err)
	}
	writePolicy(t, cfg.profile.policyPath, append(b, '\n'))
	return p
}

func TestSingleTrustUnitExplicitReport(t *testing.T) {
	r, err := DescribeRuntimeProfile(SingleTrustUnitProfile, PlainPathnameBackend)
	if err != nil || r.Profile != "single-trust-unit" || r.ServiceUID != uint32(os.Geteuid()) || r.CallerUID != r.ServiceUID ||
		r.PerAgentIsolation || r.ProductionAdmission || r.AtomicPathnameExec || r.SealedProcfdQualified ||
		!strings.Contains(r.Limits, "shared UID, no per-agent isolation") || !strings.Contains(r.ProductionBlocker, "A2-035") ||
		!strings.Contains(r.CutoverRequires, "Source factory/ACK") || !strings.Contains(r.CutoverRequires, "Linux v2") {
		t.Fatal(r, err)
	}
	for _, c := range [][2]string{{"", ""}, {"", PlainPathnameBackend}, {"production", PlainPathnameBackend}, {SingleTrustUnitProfile, ""}, {SingleTrustUnitProfile, "linux-sealed-procfd-v1"}} {
		if _, err := DescribeRuntimeProfile(c[0], c[1]); err == nil {
			t.Fatal("implicit profile or backend downgrade", c)
		}
	}
}

func TestSingleTrustUnitConfigurationBinding(t *testing.T) {
	cfg, _, _, _, _ := t4CompositionFixture(t)
	frozen, report, err := freezePrivateProfile(cfg)
	if err != nil || !notificationSHA256(report.SelectedConfigurationHash) {
		t.Fatal(report, err)
	}
	changes := map[string]func(*privateCompositionConfig){
		"binary":           func(c *privateCompositionConfig) { c.launcher.binary = c.profile.policyPath },
		"binary-pin":       func(c *privateCompositionConfig) { c.launcher.sha256 = strings.Repeat("b", 64) },
		"environment":      func(c *privateCompositionConfig) { c.launcher.environment = append(c.launcher.environment, "LANG=C") },
		"core-environment": func(c *privateCompositionConfig) { c.coreEnvironment = []string{"LANG=C"} },
		"records":          func(c *privateCompositionConfig) { c.records += "-other" },
		"state":            func(c *privateCompositionConfig) { c.stateDir += "-other" },
		"socket":           func(c *privateCompositionConfig) { c.socket += "-other" },
		"policy":           func(c *privateCompositionConfig) { c.profile.policyPath += "-other" },
		"model":            func(c *privateCompositionConfig) { c.selection.Model = "other" },
		"provider":         func(c *privateCompositionConfig) { c.selection.Vendor = "other" },
		"effort":           func(c *privateCompositionConfig) { c.selection.Reasoning = "other" },
		"native-release":   func(c *privateCompositionConfig) { c.nativeRelease = "other" },
		"permission":       func(c *privateCompositionConfig) { c.permission = "other" },
		"timeout":          func(c *privateCompositionConfig) { c.ioTimeout += time.Millisecond },
		"poll":             func(c *privateCompositionConfig) { c.poll += time.Millisecond },
		"ready":            func(c *privateCompositionConfig) { c.ready += time.Millisecond },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			c := cfg
			change(&c)
			_, got, err := freezePrivateProfile(c)
			if err != nil || got.SelectedConfigurationHash == report.SelectedConfigurationHash {
				t.Fatal("changed configuration retained selection", got, err)
			}
		})
	}
	cfg.launcher.environment[1] = "PATH=/other"
	if frozen.launcher.environment[1] != "PATH=/usr/bin:/bin" {
		t.Fatal("frozen config aliases caller slice")
	}
}

func TestSingleTrustUnitPolicySelectionAndTerminalLoss(t *testing.T) {
	for _, mode := range []string{"accepted", "v1", "profile", "lock", "exec", "config", "backend", "duplicate-field", "request-only", "launcher-path", "launcher-pin", "launcher-env", "same-uid-mode", "policy-replaced"} {
		t.Run(mode, func(t *testing.T) {
			m, q := ordinaryWireFixture(t)
			dir := filepath.Dir(m.launcher.path)
			cfg := privateCompositionConfig{
				profile: privateRuntimeProfileConfig{SingleTrustUnitProfile, PlainPathnameBackend, filepath.Join(dir, "policy.json")},
				records: m.owner.records.path, stateDir: filepath.Join(dir, "state"), socket: filepath.Join(dir, "runner.sock"),
				launcher: ordinaryLauncherConfig{m.launcher.path, m.launcher.digest, m.environment.entries},
			}
			p := singleTrustPolicyFixture(t, cfg, fixtureGrant())
			if p.config.operatorUID != uint32(os.Geteuid()) || p.config.callerUID != uint32(os.Geteuid()) {
				t.Fatal("manufactured caller principal")
			}
			op, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q))
			if err != nil {
				t.Fatal(err)
			}
			good, err := os.ReadFile(cfg.profile.policyPath)
			if err != nil {
				t.Fatal(err)
			}
			bad := good
			switch mode {
			case "v1":
				bad = policyBytes(t, createPolicyGrant{"swarm-orch", strings.Repeat("a", 64), PlainPathnameBackend})
			case "profile":
				bad = bytes.Replace(good, []byte("swarm-orch"), []byte("other"), 1)
			case "lock":
				bad = bytes.Replace(good, []byte(strings.Repeat("a", 64)), []byte(strings.Repeat("b", 64)), 1)
			case "exec":
				bad = bytes.Replace(good, []byte(`"exec_profile":"plain-pathname-v1"`), []byte(`"exec_profile":"other"`), 1)
			case "config":
				bad = bytes.Replace(good, []byte(p.configurationDigest), []byte(strings.Repeat("c", 64)), 1)
			case "backend":
				bad = bytes.Replace(good, []byte(`"backend":"plain-pathname-v1"`), []byte(`"backend":"linux-sealed-procfd-v1"`), 1)
			case "duplicate-field":
				bad = bytes.Replace(good, []byte(`"version":2`), []byte(`"version":2,"version":2`), 1)
			case "request-only":
				op.claim.retained.request.Profile.Name = "request-only"
				bad = bytes.Replace(good, []byte("swarm-orch"), []byte("request-only"), 1)
			case "launcher-path":
				m.launcher.path += "-other"
			case "launcher-pin":
				m.launcher.digest = strings.Repeat("f", 64)
			case "launcher-env":
				m.environment.entries = []string{"PATH=/other"}
			case "same-uid-mode":
				if err := os.Chmod(cfg.profile.policyPath, 0660); err != nil {
					t.Fatal(err)
				}
			case "policy-replaced":
				if err := p.checkCreate(context.Background(), op); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(cfg.profile.policyPath); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "same-uid-mode" {
				writePolicy(t, cfg.profile.policyPath, bad)
			}
			err = p.checkCreate(context.Background(), op)
			if mode == "accepted" {
				if err != nil || p.grant.ExecProfile != PlainPathnameBackend {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("policy accepted changed selection")
			}
			writePolicy(t, cfg.profile.policyPath, good)
			if err := p.checkCreate(context.Background(), op); err == nil {
				t.Fatal("terminal loss repaired")
			}
		})
	}
}

func TestSingleTrustUnitCompositionNoDowngrade(t *testing.T) {
	for _, mode := range []string{"missing", "production", "sealed", "missing-source", "missing-core"} {
		t.Run(mode, func(t *testing.T) {
			cfg, _, body, selected, fragment := t4CompositionFixture(t)
			deps := privateCompositionDependencies{source: bridgeBrokerFixture{prepare: func(context.Context, ManagerOrigin) (PreparedNotificationBroker, error) {
				t.Fatal("refused composition reached Source")
				return nil, nil
			}}, core: t4CoreReader(selected, fragment)}
			switch mode {
			case "missing":
				cfg.profile.name = ""
			case "production":
				cfg.profile.name = "production"
			case "sealed":
				cfg.profile.backend = "linux-sealed-procfd-v1"
			case "missing-source":
				deps.source = nil
			case "missing-core":
				deps.core = nil
			}
			r, err := openPrivateComposition(context.Background(), context.Background(), time.Now().Add(time.Second), cfg, deps, "launch", body)
			if err == nil || r != nil {
				t.Fatal("composition downgraded")
			}
			if _, err := os.Lstat(cfg.stateDir); !os.IsNotExist(err) {
				t.Fatal("refusal created runtime footprint", err)
			}
		})
	}
}
