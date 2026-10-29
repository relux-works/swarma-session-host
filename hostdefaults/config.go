// Package hostdefaults supplies filesystem-backed host configuration and notices.
package hostdefaults

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// FileConfig reads an explicitly selected host configuration file. Optional is
// only for a default path: an explicitly selected missing file must be refused.
// Path discovery and provider launch-plan construction belong to composition.
type FileConfig struct {
	Path               string
	Optional           bool
	Version            string
	Namespace          sm.NamespaceKey
	EnforcementKeyFile string
}

type CodexDefaults struct {
	SleepTool       bool
	GoalTokenBudget int64
	AutoContinue    sm.CodexAutoContinueOptions
}

type LoadedConfig struct {
	Host  sm.HostSettings
	Codex CodexDefaults
	// Retention is retained for the context service; HostSettings has no retention
	// field. Snapshot refuses it until composition supplies that actual service.
	ContextRetention json.RawMessage
	// Retention contains the validated optional knobs. The context runtime owns
	// defaults and policy application; Snapshot still refuses an unbound policy.
	Retention *ContextRetentionConfig
}

var ErrContextRetentionSupplierRequired = errors.New("host context retention configuration supplier required")

var _ sm.HostConfig = FileConfig{}

func (f FileConfig) Snapshot(ctx context.Context) (sm.HostSettings, error) {
	loaded, err := f.Load(ctx)
	if err != nil {
		return sm.HostSettings{}, err
	}
	if len(loaded.ContextRetention) > 0 {
		return sm.HostSettings{}, ErrContextRetentionSupplierRequired
	}
	return loaded.Host, nil
}

// Load retains provider settings beyond HostSettings rather than silently
// dropping sleep, budget or auto-continue fields before launch construction.
func (f FileConfig) Load(ctx context.Context) (LoadedConfig, error) {
	if err := ctx.Err(); err != nil {
		return LoadedConfig{}, err
	}
	loaded := LoadedConfig{Host: sm.HostSettings{DaemonVersion: f.Version, GoalAckTimeout: 30 * time.Second, LocalPolicy: sm.LocalPolicy{PayloadMode: "off", Namespace: f.Namespace, EnforcementKeyFile: f.EnforcementKeyFile}}, Codex: CodexDefaults{SleepTool: true, GoalTokenBudget: 50000000}}
	if f.Path == "" {
		return LoadedConfig{}, fmt.Errorf("selected host configuration path required")
	}
	data, err := os.ReadFile(f.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && f.Optional {
			return loaded, nil
		}
		return LoadedConfig{}, fmt.Errorf("reading host config: %w", err)
	}
	root, err := object(data, false)
	if err != nil {
		return LoadedConfig{}, err
	}
	for _, key := range keys(root) {
		switch key {
		case "session_manager":
			fields, e := object(root[key], true)
			if e != nil {
				return LoadedConfig{}, e
			}
			for k, raw := range fields {
				if k != "providers" {
					return LoadedConfig{}, unknown("session_manager." + k)
				}
				if e := parseProviders(raw, &loaded); e != nil {
					return LoadedConfig{}, e
				}
			}
		case "contexts":
			if err := parseContexts(root[key], &loaded); err != nil {
				return LoadedConfig{}, err
			}
		default:
			return LoadedConfig{}, unknown(key)
		}
	}
	if err := ctx.Err(); err != nil {
		return LoadedConfig{}, err
	}
	return loaded, nil
}

func parseProviders(raw json.RawMessage, loaded *LoadedConfig) error {
	providers, err := object(raw, true)
	if err != nil {
		return err
	}
	for _, provider := range keys(providers) {
		fields, err := object(providers[provider], true)
		if err != nil {
			return err
		}
		if provider != "claude" && provider != "codex" {
			return unknown("session_manager.providers." + provider)
		}
		for _, key := range keys(fields) {
			value := fields[key]
			section := "session_manager.providers." + provider + "." + key
			if provider == "claude" {
				if key != "goal_ack_timeout" {
					return unknown(section)
				}
				var duration string
				if err := scalar(value, &duration); err != nil {
					return fmt.Errorf("%s must be a positive duration", section)
				}
				parsed, err := time.ParseDuration(strings.TrimSpace(duration))
				if err != nil || parsed <= 0 {
					return fmt.Errorf("%s must be a positive duration", section)
				}
				loaded.Host.GoalAckTimeout = parsed
				continue
			}
			switch key {
			case "sleep_tool":
				if err := scalar(value, &loaded.Codex.SleepTool); err != nil {
					return fmt.Errorf("%s must be a boolean", section)
				}
			case "goal_token_budget":
				if err := scalar(value, &loaded.Codex.GoalTokenBudget); err != nil || loaded.Codex.GoalTokenBudget <= 0 {
					return fmt.Errorf("%s must be a positive integer", section)
				}
			case "auto_continue":
				if err := parseAutoContinue(value, &loaded.Codex.AutoContinue); err != nil {
					return err
				}
			default:
				return unknown(section)
			}
		}
	}
	return nil
}

func parseAutoContinue(raw json.RawMessage, options *sm.CodexAutoContinueOptions) error {
	fields, err := object(raw, true)
	if err != nil {
		return err
	}
	for _, key := range keys(fields) {
		var target any
		switch key {
		case "enabled":
			target = &options.Enabled
		case "delay_seconds":
			target = &options.DelaySeconds
		case "max_nudges_per_turn":
			target = &options.MaxNudgesPerTurn
		case "prompt":
			target = &options.Prompt
		default:
			return unknown("auto_continue." + key)
		}
		if err := scalar(fields[key], target); err != nil {
			return fmt.Errorf("invalid auto_continue.%s: %w", key, err)
		}
	}
	return nil
}

func parseContexts(raw json.RawMessage, loaded *LoadedConfig) error {
	fields, err := object(raw, true)
	if err != nil {
		return err
	}
	for _, key := range keys(fields) {
		switch key {
		case "clearance":
			roles, err := object(fields[key], true)
			if err != nil {
				return err
			}
			clearance := map[string][]sm.ContextSecretClass{}
			for _, role := range keys(roles) {
				normalized := strings.ToLower(strings.TrimSpace(role))
				if normalized == "" || normalized == "." || normalized == ".." || strings.ContainsRune(normalized, 0) || strings.ContainsAny(normalized, `/\`) || filepath.IsAbs(normalized) || filepath.VolumeName(normalized) != "" || filepath.Clean(normalized) != normalized {
					return fmt.Errorf("invalid clearance role")
				}
				if normalized == "coordinator" {
					normalized = "orchestrator"
				}
				entry, err := object(roles[role], true)
				if err != nil {
					return err
				}
				if entry == nil {
					continue
				}
				for k := range entry {
					if k != "allowed_secret_classes" {
						return unknown("contexts.clearance." + role + "." + k)
					}
				}
				var classes []string
				if err := scalar(entry["allowed_secret_classes"], &classes); err != nil || len(classes) == 0 {
					return fmt.Errorf("clearance must list secret classes")
				}
				clearance[normalized] = nil // Original canonical map replaces the preceding alias entry.
				seen := map[string]bool{}
				for _, class := range classes {
					c := strings.ToLower(strings.TrimSpace(class))
					if c != "restricted" && c != "unrestricted" || seen[c] {
						return fmt.Errorf("invalid or duplicate secret class")
					}
					seen[c] = true
					clearance[normalized] = append(clearance[normalized], sm.ContextSecretClass(c))
				}
				sort.Slice(clearance[normalized], func(i, j int) bool { return clearance[normalized][i] < clearance[normalized][j] })
			}
			loaded.Host.LocalPolicy.Clearance = clearance
		case "builder_allowlist":
			if bytes.Equal(bytes.TrimSpace(fields[key]), []byte("null")) {
				continue
			}
			var entries []string
			if err := json.Unmarshal(fields[key], &entries); err != nil {
				return fmt.Errorf("invalid builder allowlist")
			}
			seen := map[string]bool{}
			for _, entry := range entries {
				spec := strings.TrimSpace(entry)
				if spec == "" || strings.HasPrefix(spec, "/") || strings.ContainsRune(spec, 0) {
					return fmt.Errorf("path spec must be repo-relative")
				}
				spec = strings.TrimSuffix(strings.TrimPrefix(strings.ReplaceAll(spec, `\`, "/"), "./"), "/")
				if spec == "" || spec == "." {
					return fmt.Errorf("path spec must name a path")
				}
				for _, segment := range strings.Split(spec, "/") {
					if segment == ".." {
						return fmt.Errorf("path spec escapes repository")
					}
				}
				if _, err := path.Match(spec, "probe"); err != nil {
					return fmt.Errorf("invalid path spec: %w", err)
				}
				if !seen[spec] {
					seen[spec] = true
					loaded.Host.LocalPolicy.BuilderAllowlist.Paths = append(loaded.Host.LocalPolicy.BuilderAllowlist.Paths, spec)
				}
			}
			sort.Strings(loaded.Host.LocalPolicy.BuilderAllowlist.Paths)
		case "payloads":
			payloads, err := object(fields[key], true)
			if err != nil {
				return err
			}
			for k, value := range payloads {
				if k != "mode" {
					return unknown("contexts.payloads." + k)
				}
				var mode string
				if err := scalar(value, &mode); err != nil {
					return fmt.Errorf("invalid payload mode")
				}
				mode = strings.ToLower(strings.TrimSpace(mode))
				if mode != "off" && mode != "unrestricted" {
					return fmt.Errorf("unsupported payload mode")
				}
				loaded.Host.LocalPolicy.PayloadMode = sm.ContextPayloadMode(mode)
			}
		case "retention":
			retention, err := ParseContextRetentionConfig(fields[key])
			if err != nil {
				return err
			}
			loaded.Retention = retention
			if !bytes.Equal(bytes.TrimSpace(fields[key]), []byte("null")) {
				loaded.ContextRetention = append(json.RawMessage(nil), fields[key]...)
			}
		default:
			return unknown("contexts." + key)
		}
	}
	return nil
}

// Each consumed object rejects repeated member names before a value can win.
// Unknown case aliases are refused by the exact field dispatch above.
func object(raw []byte, nullable bool) (map[string]json.RawMessage, error) {
	if nullable && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("configuration section must be an object")
	}
	fields := map[string]json.RawMessage{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("configuration member must be a string")
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate configuration member %q", key)
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		fields[key] = value
	}
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("trailing configuration data")
	}
	return fields, nil
}
func scalar(raw json.RawMessage, target any) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("explicit non-null value required")
	}
	return json.Unmarshal(raw, target)
}
func keys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func unknown(key string) error { return fmt.Errorf("host configuration key %s is not recognized", key) }
