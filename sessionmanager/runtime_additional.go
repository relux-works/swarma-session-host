package sessionmanager

import (
	"time"
)

const claudeGoalTokenCoverageThreshold = 0.85

func musePickStatedTerm(launch string, stated ...string) string {
	for _, term := range stated {
		if term != "" && launch != "" && term != launch {
			return term
		}
	}
	for _, term := range stated {
		if term != "" {
			return term
		}
	}
	return ""
}

var (
	transportProbeTimeout     = 150 * time.Millisecond
	providerProcessStopGrace  = 3 * time.Second
	providerProcessForceGrace = 2 * time.Second
	providerProcessPollPeriod = 50 * time.Millisecond
)
