//go:build unix

package hostdefaults

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/sessioncontext"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

const configuredRetentionJSON = `{"contexts":{"retention":{"ready_ttl":"14d","stale_ttl":"72h","building_ttl":"90m","retired_ttl":"2d","unpublished_materialization_ttl":"12h","materialization_idle_ttl":"36h","payload_ttl":"3d","max_ready_per_binding":5,"max_descendants_per_base":16}}}`

func configuredRetentionWant() core.ContextRetentionSettings {
	return core.ContextRetentionSettings{ReadyTTL: 14 * 24 * time.Hour, StaleTTL: 72 * time.Hour, BuildingTTL: 90 * time.Minute, RetiredTTL: 48 * time.Hour, UnpublishedMaterializationTTL: 12 * time.Hour, MaterializationIdleTTL: 36 * time.Hour, PayloadTTL: 72 * time.Hour, MaxReadyPerBinding: 5, MaxDescendantsPerBase: 16}
}

// The actual parsed file supplies all nine configured fields. The standalone
// installs the real consumer, while public unbound Snapshot still refuses.
func TestFileRetentionSnapshotBindsAllOriginalSettings(t *testing.T) {
	o := standaloneOptions(t)
	o.Config.Optional = false
	if err := os.WriteFile(o.Config.Path, []byte(configuredRetentionJSON), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := o.Config.RetentionSnapshot(context.Background())
	if err != nil || !got.Known || got.Settings != configuredRetentionWant() {
		t.Fatalf("actual parser/snapshot: %+v %v", got, err)
	}
	if _, err := o.Config.Snapshot(context.Background()); !errors.Is(err, ErrContextRetentionSupplierRequired) {
		t.Fatalf("unbound config lost refusal: %v", err)
	}
	s, err := OpenStandalone(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	settings, err := s.Contexts.ContextRetentionSettings(context.Background())
	if err != nil || settings != configuredRetentionWant() {
		t.Fatalf("real bound resolver: %+v %v", settings, err)
	}
}

// This explicit registry fixture only records the exact request at the real
// Service boundary. It implements no registry/board algorithm or live authority.
type configuredSweepAuthority struct {
	core.ContextAuthority
	requests []core.RetentionSweepRequest
}

func (a *configuredSweepAuthority) SweepContextRetention(_ context.Context, r core.RetentionSweepRequest) (*core.RetentionSweepResult, error) {
	a.requests = append(a.requests, r)
	return &core.RetentionSweepResult{DryRun: r.DryRun}, nil
}

type unqueriedContextReferences struct{ t *testing.T }

func (r unqueriedContextReferences) Referenced(context.Context, string) (bool, bool, error) {
	r.t.Fatal("empty real catalog unexpectedly queried a context reference")
	return false, false, errors.New("unexpected reference")
}

// The configured caller propagates resolution failure BEFORE registry effects,
// then passes a whole resolved policy. The explicit HTTP/service path keeps its
// old independent normalization even if the installed config later fails.
func TestConfiguredRetentionSweepResolvesBeforeEffectsWithoutMerging(t *testing.T) {
	o := standaloneOptions(t)
	o.Config.Optional = false
	if err := os.WriteFile(o.Config.Path, []byte(configuredRetentionJSON), 0600); err != nil {
		t.Fatal(err)
	}
	a := &configuredSweepAuthority{}
	o.ContextAuthority = a
	o.ContextReferences = unqueriedContextReferences{t}
	s, err := OpenStandalone(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	report, err := s.ConfiguredContextRetentionSweep(context.Background(), sessioncontext.ContextRetentionSweepRequest{Settings: core.ContextRetentionSettings{ReadyTTL: time.Second}, IncludePinned: true, DryRun: true})
	if err != nil || !report.DryRun || report.Registry == nil || len(a.requests) != 1 || a.requests[0].Settings != configuredRetentionWant() || !a.requests[0].IncludePinned || !a.requests[0].DryRun || a.requests[0].LeaseVisibility != "observed" {
		t.Fatalf("configured real sweep lost full policy: %+v %v %+v", report, err, a.requests)
	}
	if err := os.WriteFile(o.Config.Path, []byte(`{"contexts":{"retention":{"unknown":3}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	bad, err := s.ConfiguredContextRetentionSweep(context.Background(), sessioncontext.ContextRetentionSweepRequest{})
	if err == nil || !strings.Contains(err.Error(), "unknown") || !reflect.DeepEqual(bad, sessioncontext.ContextRetentionSweepReport{}) || len(a.requests) != 1 {
		t.Fatalf("failed config reached authority: %+v %v calls=%d", bad, err, len(a.requests))
	}
	_, err = s.Contexts.SweepContextRetention(context.Background(), sessioncontext.ContextRetentionSweepRequest{Settings: core.ContextRetentionSettings{ReadyTTL: time.Hour}, DryRun: true})
	want := sessioncontext.DefaultContextRetentionSettings()
	want.ReadyTTL = time.Hour
	if err != nil || len(a.requests) != 2 || a.requests[1].Settings != want {
		t.Fatalf("explicit request was merged with config: %v %+v", err, a.requests)
	}
	if _, err := s.ConfiguredContextRetentionSweep(nil, sessioncontext.ContextRetentionSweepRequest{}); !errors.Is(err, core.ErrInvalidInput) {
		t.Fatalf("nil request context: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ConfiguredContextRetentionSweep(context.Background(), sessioncontext.ContextRetentionSweepRequest{}); !errors.Is(err, core.ErrHostClosed) || len(a.requests) != 2 {
		t.Fatalf("closed host launched configured sweep: %v", err)
	}
}

// A failed/canceled/missing request source can never become Known/defaults.
// Optional verified absence remains the nearby valid control.
func TestFileRetentionSnapshotRefusesBeforeKnownDefaults(t *testing.T) {
	o := standaloneOptions(t)
	o.Config.Optional = false
	for _, body := range []string{`{`, `{"contexts":{"retention":{"unknown":1}}}`, `{"contexts":{"retention":{"ready_ttl":"0s"}}}`, `{"contexts":{"retention":{"max_ready_per_binding":-1}}}`} {
		if err := os.WriteFile(o.Config.Path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := o.Config.RetentionSnapshot(context.Background())
		if err == nil || got.Known || got.Settings != (core.ContextRetentionSettings{}) {
			t.Fatalf("invalid source became usable defaults: %+v %v", got, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := o.Config.RetentionSnapshot(ctx); !errors.Is(err, context.Canceled) || got.Known {
		t.Fatalf("canceled source: %+v %v", got, err)
	}
	if got, err := o.Config.RetentionSnapshot(nil); err == nil || got.Known {
		t.Fatal("nil context became known")
	}
	if err := os.Remove(o.Config.Path); err != nil {
		t.Fatal(err)
	}
	o.Config.Optional = true
	if got, err := o.Config.RetentionSnapshot(context.Background()); err != nil || !got.Known || got.Settings != (core.ContextRetentionSettings{}) {
		t.Fatalf("verified optional absence refused: %+v %v", got, err)
	}
}
