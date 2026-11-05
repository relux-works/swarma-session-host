package sessionhost

import (
	"errors"
	"os/user"
	"reflect"
	"strings"
	"testing"
)

func protocolChildBaselineFixture(t *testing.T) []string {
	t.Helper()
	operator, err := user.Current()
	if err != nil || operator.HomeDir == "" {
		t.Fatal("operator home metadata unavailable")
	}
	return []string{"HOME=" + operator.HomeDir, "PATH=/usr/bin:/bin"}
}

// Only the configured, exact allowed names survive, in stable order. Optional
// deployment fields stay byte-exact; no ambient environment is enumerated.
// The home lookup reads user metadata, never a credential/native-home payload.
func TestProtocolChildEnvironmentPositive(t *testing.T) {
	base := protocolChildBaselineFixture(t)
	configured := append(append([]string(nil), base...), "TMPDIR=/fixture/tmp space", "LANG=en_US.UTF-8", "LC_ALL=C", "LC_CTYPE=UTF-8", "TZ=UTC", "USER=fixture", "LOGNAME=fixture", "SHELL=/bin/zsh", "CURATOR_HOME=/fixture/curator")
	got, err := parseProtocolChildEnvironment(configured)
	if err != nil || !reflect.DeepEqual(got.entries, configured) {
		t.Fatal("explicit deployment baseline changed", err)
	}
	configured[0] = "HOME=/fixture/foreign"
	if got.entries[0] != base[0] {
		t.Fatal("baseline retained caller-owned slice")
	}
	t.Setenv("PROTOCOL_CHILD_SYNTHETIC_AMBIENT", "not-a-secret")
	current, err := parseProtocolChildEnvironment(base)
	if err != nil || !reflect.DeepEqual(current.entries, base) {
		t.Fatal("ambient environment contaminated explicit configuration", err)
	}
}

// Each otherwise unknown name refuses, including a synthetic credential marker,
// alone and in combination. Nothing is scrubbed and continued; no real token or
// parent environment is read. Nearby clean input still passes after rejection.
func TestProtocolChildEnvironmentForeignNames(t *testing.T) {
	base := protocolChildBaselineFixture(t)
	names := []string{"A2A_AGENT", "A2A_CONFIG_DIR", "CODEX_HOME", "CODEX_THREAD_ID", "CLAUDE_CONFIG_DIR", "CLAUDECODE", "GH_TOKEN", "HTTP_PROXY", "DYLD_INSERT_LIBRARIES", "UNKNOWN"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			injected := append(append([]string(nil), base...), name+"=synthetic-placeholder")
			got, err := parseProtocolChildEnvironment(injected)
			if !errors.Is(err, errProtocolChildEnvironment) || !reflect.DeepEqual(got, protocolChildEnvironment{}) {
				t.Fatal("foreign input was scrubbed or forwarded", err)
			}
			if _, err := parseProtocolChildEnvironment(base); err != nil {
				t.Fatal("nearby clean baseline refused", err)
			}
		})
	}
	injected := append([]string(nil), base...)
	for _, name := range names {
		injected = append(injected, name+"=synthetic-placeholder")
	}
	if got, err := parseProtocolChildEnvironment(injected); !errors.Is(err, errProtocolChildEnvironment) || !reflect.DeepEqual(got, protocolChildEnvironment{}) {
		t.Fatal("combined parent-passthrough witness admitted", err)
	}
}

// Required/unique names and bounded, framed values are checked before publishing
// any baseline. The real operator home is the valid control, never a fake HOME.
func TestProtocolChildEnvironmentRefusals(t *testing.T) {
	base := protocolChildBaselineFixture(t)
	cases := [][]string{
		nil, base[:1], base[1:], {base[0], "PATH="},
		{"HOME=/fixture/foreign", base[1]}, {base[0], base[1], base[0]},
		{base[0], base[1], "PATH=/fixture/other"},
		{base[0], base[1], "LANG"}, {base[0], base[1], "lang=C"},
		{base[0], base[1], "LANG=line\nbreak"}, {base[0], base[1], "LANG=nul\x00byte"},
		{base[0], base[1], "LANG=" + string([]byte{0xff})},
		{base[0], base[1], "LANG=" + strings.Repeat("a", 4097)},
	}
	for _, configured := range cases {
		got, err := parseProtocolChildEnvironment(configured)
		if !errors.Is(err, errProtocolChildEnvironment) || !reflect.DeepEqual(got, protocolChildEnvironment{}) {
			t.Fatal("invalid baseline partially published", err)
		}
	}
	if _, err := parseProtocolChildEnvironment(base); err != nil {
		t.Fatal("clean control refused", err)
	}
}
