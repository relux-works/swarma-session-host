package sessionmanager

import (
	"fmt"
	"reflect"
)

// NewLegacyRecordCodecWithMuseAccounting couples the actual runtime consumer
// to the host's record codec. Structural presence never proves native restore:
// the host invokes its real preflight and hydration before exposing a writer.
// The default codec retains its refusal for unsupported active state.
func NewLegacyRecordCodecWithMuseAccounting(consumer MuseAccountingRestorer) (*LegacyRecordCodec, error) {
	if absentHostPort(consumer) {
		return nil, fmt.Errorf("%w: actual native accounting consumer required", ErrHostCapabilityUnavailable)
	}
	return &LegacyRecordCodec{museAccounting: consumer}, nil
}
func validateNativeAccountingCodecCoupling(options HostOptions) error {
	codec, ok := options.Codec.(*LegacyRecordCodec)
	if !ok || absentHostPort(codec.museAccounting) {
		return nil
	}
	runtime, ok := options.OwnerRuns.(MuseAccountingRestorer)
	if !ok || absentHostPort(runtime) || reflect.TypeOf(runtime) != reflect.TypeOf(codec.museAccounting) || !reflect.TypeOf(runtime).Comparable() || runtime != codec.museAccounting {
		return fmt.Errorf("%w: codec and owner runtime require the same native accounting consumer", ErrHostCapabilityUnavailable)
	}
	return nil
}
