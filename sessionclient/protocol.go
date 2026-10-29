package sessionclient

import (
	"fmt"
	"strconv"
	"strings"
)

// These identifiers retain the published control protocol bytes.
const protocolName = "task-board-session-manager"
const ProtocolVersion = 8

type ProtocolStatus struct {
	Protocol        string `json:"protocol,omitempty"`
	ProtocolVersion int    `json:"protocol_version,omitempty"`
	DaemonVersion   string `json:"daemon_version,omitempty"`
	StartupHealthy  bool   `json:"startup_healthy"`
}

func ProtocolIdentifier(version int) string { return fmt.Sprintf("%s-v%d", protocolName, version) }
func CheckCompatibility(status ProtocolStatus, wrapperBuildVersion string) error {
	actual := status.ProtocolVersion
	if actual <= 0 {
		actual = ProtocolVersionFromIdentifier(status.Protocol)
	}
	if actual < ProtocolVersion {
		return &ProtocolUpgradeError{
			ActualVersion:  actual,
			DaemonVersion:  status.DaemonVersion,
			WrapperVersion: wrapperBuildVersion,
		}
	}
	if actual == ProtocolVersion {
		if skew := BuildSkew(status.DaemonVersion, wrapperBuildVersion); skew != nil {
			return skew
		}
	}
	if actual > ProtocolVersion {
		return fmt.Errorf(
			"Session Manager protocol v%d is newer than this wrapper supports (v%d); update task-board before retrying",
			actual,
			ProtocolVersion,
		)
	}
	if status.Protocol != "" && status.Protocol != ProtocolIdentifier(actual) {
		return fmt.Errorf("unsupported Session Manager protocol identifier %q", status.Protocol)
	}
	return nil
}

// managerBuildSkew reports an equal-protocol build mismatch, and nil whenever
// the comparison cannot be made.
//
// Both sides must name a real build. A daemon that advertises no version, or
// either side reporting the un-stamped `dev` placeholder, is an absent
// measurement — and an absent measurement must not be laundered into a
// mismatch, because the consequence of a false positive here is replacing a
// healthy daemon.
func BuildSkew(daemonVersion, wrapperBuildVersion string) *ProtocolUpgradeError {
	daemon := strings.TrimSpace(daemonVersion)
	wrapper := strings.TrimSpace(wrapperBuildVersion)
	if daemon == "" || wrapper == "" {
		return nil
	}
	if daemon == "dev" || wrapper == "dev" {
		return nil
	}
	if daemon == wrapper {
		return nil
	}
	return &ProtocolUpgradeError{
		ActualVersion:  ProtocolVersion,
		DaemonVersion:  daemon,
		BuildSkew:      true,
		WrapperVersion: wrapper,
	}
}

func ProtocolVersionFromIdentifier(identifier string) int {
	identifier = strings.TrimSpace(identifier)
	prefix := protocolName + "-v"
	if !strings.HasPrefix(identifier, prefix) {
		return 0
	}
	version, err := strconv.Atoi(strings.TrimPrefix(identifier, prefix))
	if err != nil || version < 1 {
		return 0
	}
	return version
}
