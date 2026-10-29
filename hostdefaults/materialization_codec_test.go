package hostdefaults

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

func materializationProviderFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "sessionmanager", "testdata", "legacy-records", name+"-initial.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	if len(record["provider_host"]) == 0 {
		t.Fatal("original fixture has no provider_host")
	}
	return record["provider_host"]
}

// The concrete composition bridge preserves the actual legacy converter's
// admitted snapshots and active-state refusals. It supplies no native runtime.
func TestMaterializationHostCodecUsesActualLegacySubrecord(t *testing.T) {
	ctx := context.Background()
	codec := sm.NewLegacyRecordCodec()
	bridge, err := NewMaterializationHostCodec(ctx, codec)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"muse-pty", "muse-pty-provenance"} {
		t.Run(name, func(t *testing.T) {
			raw := materializationProviderFixture(t, name)
			before := bytes.Clone(raw)
			want, err := codec.DecodeProviderHost(ctx, raw)
			if err != nil {
				t.Fatal(err)
			}
			got, err := bridge.DecodeProviderHost(ctx, raw)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("bridge changed actual provider record: %v", err)
			}
			roundtrip, err := bridge.EncodeProviderHost(ctx, raw, got)
			if err != nil || !bytes.Equal(roundtrip, before) || !bytes.Equal(raw, before) {
				t.Fatalf("original provider bytes changed: %v", err)
			}
		})
	}
	for _, name := range []string{"claude", "codex", "muse-native", "muse-text"} {
		t.Run(name, func(t *testing.T) {
			raw := materializationProviderFixture(t, name)
			before := bytes.Clone(raw)
			got, err := bridge.DecodeProviderHost(ctx, raw)
			if !errors.Is(err, sm.ErrHostCapabilityUnavailable) || !reflect.DeepEqual(got, sm.ProviderHostRecord{}) || !bytes.Equal(raw, before) {
				t.Fatalf("active native state lost its actual refusal: %v", err)
			}
		})
	}
}

// Missing real dependencies produce no bridge; no default context or codec is
// substituted. A valid pair remains the nearby positive control.
func TestMaterializationHostCodecRequiresRealDependencies(t *testing.T) {
	codec := sm.NewLegacyRecordCodec()
	for _, test := range []struct {
		life  context.Context
		codec *sm.LegacyRecordCodec
	}{{nil, codec}, {context.Background(), nil}, {nil, nil}} {
		bridge, err := NewMaterializationHostCodec(test.life, test.codec)
		if err == nil || bridge != nil {
			t.Fatal("missing dependency produced materialization bridge")
		}
	}
	if bridge, err := NewMaterializationHostCodec(context.Background(), codec); err != nil || bridge == nil {
		t.Fatalf("real dependencies refused: %v", err)
	}
}

// An expired lifetime retains the codec's cancellation refusal without reading
// an otherwise valid handle; malformed and truncated input retain their exact
// decoder error classes while live.
func TestMaterializationHostCodecPreservesCancellationAndDecodeErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bridge, err := NewMaterializationHostCodec(ctx, sm.NewLegacyRecordCodec())
	if err != nil {
		t.Fatal(err)
	}
	raw := materializationProviderFixture(t, "muse-pty")
	if _, err := bridge.DecodeProviderHost(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var syntax *json.SyntaxError
	if _, err := bridge.DecodeProviderHost(ctx, []byte(`{"provider":?}`)); !errors.As(err, &syntax) {
		t.Fatalf("malformed input lost syntax refusal: %v", err)
	}
	if _, err := bridge.DecodeProviderHost(ctx, []byte(`{"provider":`)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated input lost unexpected-EOF refusal: %v", err)
	}
	cancel()
	got, err := bridge.DecodeProviderHost(ctx, raw)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, sm.ProviderHostRecord{}) {
		t.Fatalf("cancelled bridge returned a snapshot: %v", err)
	}
}

// Request cancellation and lifetime cancellation both refuse decoding and
// encoding without returning bytes; live conversion above is the valid control.
func TestMaterializationHostCodecRequestAndLifetimeFenceWrites(t *testing.T) {
	life, stop := context.WithCancel(context.Background())
	defer stop()
	bridge, err := NewMaterializationHostCodec(life, sm.NewLegacyRecordCodec())
	if err != nil {
		t.Fatal(err)
	}
	raw := materializationProviderFixture(t, "muse-pty")
	record, err := bridge.DecodeProviderHost(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	request, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{request, nil} {
		if _, err := bridge.DecodeProviderHost(ctx, raw); err == nil {
			t.Fatal("cancelled or missing request decoded")
		}
		if got, err := bridge.EncodeProviderHost(ctx, raw, record); err == nil || got != nil {
			t.Fatal("cancelled or missing request encoded")
		}
	}
	stop()
	if got, err := bridge.EncodeProviderHost(context.Background(), raw, record); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("expired lifetime encoded: %v", err)
	}
}
