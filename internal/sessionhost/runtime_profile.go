package sessionhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

const SingleTrustUnitProfile = "single-trust-unit"
const PlainPathnameBackend = "plain-pathname-v1"

// RuntimeProfileReport describes a selected posture, never an admission receipt
// or qualification of an installed launcher or its downstream native images.
type RuntimeProfileReport struct {
	Profile                   string `json:"profile"`
	Backend                   string `json:"backend"`
	ServiceUID                uint32 `json:"service_uid"`
	CallerUID                 uint32 `json:"caller_uid"`
	PerAgentIsolation         bool   `json:"per_agent_isolation"`
	ProductionAdmission       bool   `json:"production_admission"`
	AtomicPathnameExec        bool   `json:"atomic_pathname_exec"`
	SealedProcfdQualified     bool   `json:"sealed_procfd_qualified"`
	Custody                   string `json:"custody"`
	Limits                    string `json:"limits"`
	ProductionBlocker         string `json:"production_blocker"`
	CutoverRequires           string `json:"cutover_requires"`
	SelectedConfigurationHash string `json:"selected_configuration_sha256,omitempty"`
}

// DescribeRuntimeProfile has no default, production fallback, credential input,
// filesystem effects or activation. UIDs describe this process's one principal;
// they do not authenticate an individual agent sharing that principal.
func DescribeRuntimeProfile(profile, backend string) (RuntimeProfileReport, error) {
	if profile != SingleTrustUnitProfile || backend != PlainPathnameBackend || os.Geteuid() == 0 || os.Getuid() != os.Geteuid() {
		return RuntimeProfileReport{}, errors.New("runtime_profile_unavailable")
	}
	uid := uint32(os.Geteuid())
	return RuntimeProfileReport{
		Profile: profile, Backend: backend, ServiceUID: uid, CallerUID: uid,
		Custody:           "shared UID; one trusted OS principal; no custody boundary against same-UID writers",
		Limits:            "shared UID, no per-agent isolation; non-atomic pathname/exec; launcher hash checks do not cover interpreters, loaders or downstream native images",
		ProductionBlocker: "A2-035: O/S/C principals and UID-switch helper",
		CutoverRequires:   "Source factory/ACK contract and Linux v2 identity qualification; independent Muse review and Source landing",
	}, nil
}

type privateRuntimeProfileConfig struct {
	name, backend, policyPath string
}

// All effect-relevant composition choices are copied and hashed. The digest is
// a selection binding, not evidence of exclusive custody of these paths.
func freezePrivateProfile(cfg privateCompositionConfig) (privateCompositionConfig, RuntimeProfileReport, error) {
	report, err := DescribeRuntimeProfile(cfg.profile.name, cfg.profile.backend)
	if err != nil || !filepath.IsAbs(cfg.profile.policyPath) || filepath.Clean(cfg.profile.policyPath) != cfg.profile.policyPath {
		return cfg, RuntimeProfileReport{}, errPrivateComposition
	}
	env, err := parseProtocolChildEnvironment(cfg.launcher.environment)
	if err != nil || !canonicalRecordDirectory(cfg.launcher.binary) || !notificationSHA256(cfg.launcher.sha256) {
		return cfg, RuntimeProfileReport{}, errPrivateComposition
	}
	cfg.launcher.environment = env.entries
	cfg.coreEnvironment = append([]string(nil), cfg.coreEnvironment...)
	// An explicit wire projection avoids hashing unexported Go fields as {}.
	selection := struct {
		Profile, Backend, PolicyPath, Records, StateDir, Socket string
		Binary, BinarySHA256                                    string
		Environment, CoreEnvironment                            []string
		Selection                                               Selection
		NativeRelease, Permission                               string
		IOTimeout, Poll, Ready                                  int64
	}{cfg.profile.name, cfg.profile.backend, cfg.profile.policyPath, cfg.records, cfg.stateDir, cfg.socket,
		cfg.launcher.binary, cfg.launcher.sha256, cfg.launcher.environment, cfg.coreEnvironment,
		cfg.selection, cfg.nativeRelease, cfg.permission, int64(cfg.ioTimeout), int64(cfg.poll), int64(cfg.ready)}
	data, err := json.Marshal(selection)
	if err != nil {
		return cfg, RuntimeProfileReport{}, errPrivateComposition
	}
	digest := sha256.Sum256(data)
	report.SelectedConfigurationHash = hex.EncodeToString(digest[:])
	return cfg, report, nil
}

// Version 2 is a separate explicit single-trust-unit policy envelope. Version 1
// remains unchanged for the older distinct-caller candidate; it cannot admit
// this profile. Grants retain the record profile/lock -> fixed exec relation.
type singleTrustUnitPolicyDocument struct {
	Version        int                 `json:"version"`
	RuntimeProfile string              `json:"runtime_profile"`
	Backend        string              `json:"backend"`
	Configuration  string              `json:"selected_configuration_sha256"`
	Grants         []createPolicyGrant `json:"grants"`
}

func parseSingleTrustUnitPolicy(data []byte, digest string) (createPolicyDocument, error) {
	var d singleTrustUnitPolicyDocument
	if len(data) > maxCreatePolicyBytes || json.Unmarshal(data, &d) != nil || d.Version != 2 ||
		d.RuntimeProfile != SingleTrustUnitProfile || d.Backend != PlainPathnameBackend || d.Configuration != digest {
		return createPolicyDocument{}, errOrdinaryWireUnavailable
	}
	b, err := json.Marshal(d)
	if err != nil || !bytes.Equal(data, append(b, '\n')) {
		return createPolicyDocument{}, errOrdinaryWireUnavailable
	}
	// Reuse all v1 grant validation, including duplicate profile/lock refusal.
	b, err = json.Marshal(createPolicyDocument{Version: 1, Grants: d.Grants})
	if err != nil {
		return createPolicyDocument{}, errOrdinaryWireUnavailable
	}
	return parseCreatePolicy(append(b, '\n'))
}

func newSingleTrustUnitCreatePolicy(cfg privateCompositionConfig) (*operatorCreatePolicy, RuntimeProfileReport, error) {
	frozen, report, err := freezePrivateProfile(cfg)
	if err != nil {
		return nil, RuntimeProfileReport{}, err
	}
	// This constructor alone accepts S=C=current UID. No caller UID argument
	// exists and no production admission can flow through this selection.
	uid := uint32(os.Geteuid())
	p := &operatorCreatePolicy{config: operatorCreatePolicyConfig{
		path: frozen.profile.policyPath, execProfile: PlainPathnameBackend, operatorUID: uid, callerUID: uid,
	}, singleTrust: &frozen, configurationDigest: report.SelectedConfigurationHash}
	return p, report, nil
}

func (p *operatorCreatePolicy) matchesSingleTrustLauncher(m *ordinaryWireManager) bool {
	if p.singleTrust == nil {
		return true
	}
	c := p.singleTrust
	if m.owner == nil || m.owner.records == nil || m.owner.records.path != c.records ||
		m.launcher == nil || m.launcher.path != c.launcher.binary || m.launcher.digest != c.launcher.sha256 ||
		len(m.environment.entries) != len(c.launcher.environment) {
		return false
	}
	for i, entry := range c.launcher.environment {
		if m.environment.entries[i] != entry {
			return false
		}
	}
	return true
}
