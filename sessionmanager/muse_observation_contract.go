package sessionmanager

import (
	"context"
	"time"
)

// Contract delta D4: read-only native evidence is separate from writer acks.
type MuseObservationSource interface {
	SubscribeMuse(context.Context, ControllerLease) (MuseObservationFeed, error)
}
type MuseObservationFeed interface {
	Next(context.Context) (MuseObservation, error)
	Close()
}
type MuseObservation struct {
	Lease       ControllerLease
	Sequence    uint64
	ObservedAt  time.Time
	Event       MSPNotification
	Gap         *MuseObservationGap
	RestoreFold *MuseRestoreFold
}
type MuseObservationGap struct {
	FromSequence    uint64
	ThroughSequence uint64
	Reason          MuseObservationGapReason
}
type MuseObservationGapReason string
