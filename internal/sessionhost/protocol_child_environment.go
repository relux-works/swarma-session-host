package sessionhost

import (
	"errors"
	"os/user"
	"path/filepath"
	"strings"
)

var errProtocolChildEnvironment = errors.New("protocol_child_environment_refused")

var protocolChildEnvironmentNames = [...]string{
	"HOME", "PATH", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "USER", "LOGNAME", "SHELL", "CURATOR_HOME",
}

// protocolChildEnvironment contains a frozen deployment projection, not the
// ambient Host environment or an arbitrary request map. Parsing does not prove
// that the installation/deployment config itself was independently admitted.
type protocolChildEnvironment struct{ entries []string }

func parseProtocolChildEnvironment(configured []string) (protocolChildEnvironment, error) {
	values := make(map[string]string, len(configured))
	for _, entry := range configured {
		name, value, ok := strings.Cut(entry, "=")
		allowed := false
		for _, known := range protocolChildEnvironmentNames {
			allowed = allowed || name == known
		}
		_, duplicate := values[name]
		if !ok || !allowed || duplicate || !protocolChildValue(value) {
			return protocolChildEnvironment{}, errProtocolChildEnvironment
		}
		values[name] = value
	}
	// User database metadata, not HOME from this or the parent's environment,
	// identifies the real operator home. No native home or credential is opened.
	operator, err := user.Current()
	if err != nil || operator.HomeDir == "" || !filepath.IsAbs(operator.HomeDir) ||
		values["HOME"] != operator.HomeDir || filepath.Clean(values["HOME"]) != values["HOME"] || values["PATH"] == "" {
		return protocolChildEnvironment{}, errProtocolChildEnvironment
	}
	entries := make([]string, 0, len(values))
	for _, name := range protocolChildEnvironmentNames {
		if value, present := values[name]; present {
			entries = append(entries, name+"="+value)
		}
	}
	return protocolChildEnvironment{entries: entries}, nil
}
