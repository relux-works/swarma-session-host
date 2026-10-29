package sessionmanager

import (
	"context"
	"errors"
	"testing"
)

type nativeCodecConfigProbe struct{ calls int }

func (p *nativeCodecConfigProbe) Snapshot(context.Context) (HostSettings, error) {
	p.calls++
	return HostSettings{}, nil
}

// Explicit codec/runtime coupling is structural admission before config/files
// or factories. It does not itself qualify or enable any native Restore state.
func TestNativeAccountingCodecRequiresExactRuntime(t *testing.T) {
	if _, e := NewLegacyRecordCodecWithMuseAccounting(nil); !errors.Is(e, ErrHostCapabilityUnavailable) {
		t.Fatalf("nil consumer admitted: %v", e)
	}
	old, _, _ := accountingRecordsProbe(t)
	options := old.options
	old.Close()
	consumer := &nativeRestoreBoundaryProbe{}
	codec, e := NewLegacyRecordCodecWithMuseAccounting(consumer)
	if e != nil {
		t.Fatal(e)
	}
	options.Codec = codec
	for _, other := range []CapturedOwnerRunner{nil, &nativeRestoreBoundaryProbe{}} {
		config := &nativeCodecConfigProbe{}
		options.Config = config
		options.OwnerRuns = other
		if h, e := NewHost(options); !errors.Is(e, ErrHostCapabilityUnavailable) || h != nil || config.calls != 0 {
			t.Fatalf("mismatched consumer reached config: %v calls%d", e, config.calls)
		}
	}
	options.Config = hostCommitProbeConfig{}
	options.OwnerRuns = consumer
	h, e := NewHost(options)
	if e != nil {
		t.Fatalf("actual paired consumer refused: %v", e)
	}
	h.Close()
}
