package sessionmanager

import "time"

const codexAutoContinuePollInterval = time.Second

type codexAutoContinueState struct {
	turnID       string
	waitingSince time.Time
	lastNudged   time.Time
	nudges       int
}

const codexConsumerNotificationLimit = 20000
