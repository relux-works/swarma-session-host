package sessioncontext

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

type retentionConfigSource struct {
	snapshot RetentionConfigSnapshot
	err      error
	calls    int
	context  context.Context
}

func (s *retentionConfigSource) RetentionSnapshot(ctx context.Context) (RetentionConfigSnapshot, error) {
	s.calls++
	s.context = ctx
	return s.snapshot, s.err
}

type retentionConfigAuthority struct {
	*fakeMaterializationAuthority
	sweeps []sm.RetentionSweepRequest
}

func (a *retentionConfigAuthority) SweepContextRetention(ctx context.Context, r sm.RetentionSweepRequest) (*sm.RetentionSweepResult, error) {
	a.sweeps = append(a.sweeps, r)
	return a.fakeMaterializationAuthority.SweepContextRetention(ctx, r)
}
func openRetentionConfigService(t *testing.T, source RetentionConfig) (*Service, *retentionConfigAuthority) {
	t.Helper()
	a := &retentionConfigAuthority{fakeMaterializationAuthority: newFakeMaterializationAuthority()}
	m, err := Open(context.Background(), Options{Layout: testLayout(t, "retention-config"), Authority: a, References: references{known: true}, RetentionConfig: source, Now: func() time.Time { return materializationTestClock }})
	if err != nil {
		t.Fatal(err)
	}
	return m, a
}
func originalConfiguredRetentionSettings() sm.ContextRetentionSettings {
	return sm.ContextRetentionSettings{ReadyTTL: 14 * 24 * time.Hour, StaleTTL: 72 * time.Hour, BuildingTTL: 90 * time.Minute, RetiredTTL: 2 * 24 * time.Hour, UnpublishedMaterializationTTL: 12 * time.Hour, MaterializationIdleTTL: 36 * time.Hour, PayloadTTL: 3 * 24 * time.Hour, MaxReadyPerBinding: 5, MaxDescendantsPerBase: 16}
}

// The runtime resolves all nine original config knobs and only verified unset
// fields become documented defaults. This does not test the owner's JSON parser.
func TestContextRetentionConfigResolvesOriginalNineSettings(t *testing.T) {
	defaults := sm.ContextRetentionSettings{ReadyTTL: 30 * 24 * time.Hour, StaleTTL: 7 * 24 * time.Hour, BuildingTTL: 6 * time.Hour, RetiredTTL: 24 * time.Hour, UnpublishedMaterializationTTL: 24 * time.Hour, MaterializationIdleTTL: 24 * time.Hour, PayloadTTL: 7 * 24 * time.Hour, MaxReadyPerBinding: 3, MaxDescendantsPerBase: 64}
	partial := defaults
	partial.ReadyTTL = 14 * 24 * time.Hour
	for _, tc := range []struct {
		name   string
		source *retentionConfigSource
		want   sm.ContextRetentionSettings
	}{
		{name: "explicit defaults", want: defaults},
		{name: "verified absent section", source: &retentionConfigSource{snapshot: RetentionConfigSnapshot{Known: true}}, want: defaults},
		{name: "all nine original configured values", source: &retentionConfigSource{snapshot: RetentionConfigSnapshot{Known: true, Settings: originalConfiguredRetentionSettings()}}, want: originalConfiguredRetentionSettings()},
		{name: "partial configuration", source: &retentionConfigSource{snapshot: RetentionConfigSnapshot{Known: true, Settings: sm.ContextRetentionSettings{ReadyTTL: 14 * 24 * time.Hour}}}, want: partial},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var source RetentionConfig
			if tc.source != nil {
				source = tc.source
			}
			m, a := openRetentionConfigService(t, source)
			ctx := context.WithValue(context.Background(), struct{}{}, tc.name)
			got, err := m.ContextRetentionSettings(ctx)
			if err != nil || got != tc.want {
				t.Fatalf("original config resolution = %+v, %v; want %+v", got, err, tc.want)
			}
			if tc.source != nil && (tc.source.calls != 1 || tc.source.context != ctx) {
				t.Fatal("configuration snapshot did not receive the operation context exactly once")
			}
			if len(a.sweeps) != 0 {
				t.Fatal("resolving settings invoked a registry sweep")
			}
		})
	}
	t.Run("fresh snapshot per resolution", func(t *testing.T) {
		s := &retentionConfigSource{snapshot: RetentionConfigSnapshot{Known: true, Settings: originalConfiguredRetentionSettings()}}
		m, _ := openRetentionConfigService(t, s)
		if _, err := m.ContextRetentionSettings(context.Background()); err != nil {
			t.Fatal(err)
		}
		s.snapshot.Settings.ReadyTTL = 9 * time.Hour
		got, err := m.ContextRetentionSettings(context.Background())
		if err != nil || got.ReadyTTL != 9*time.Hour || s.calls != 2 {
			t.Fatalf("fresh config = %+v, %v; calls=%d", got, err, s.calls)
		}
	})
}

// Unknown/error/invalid typed source facts never become a successful default
// policy. Resolution refuses before any registry call or local custody change;
// callers must propagate this error rather than launching a configured sweep.
func TestContextRetentionConfigRefusalPreservesCustody(t *testing.T) {
	cause := errors.New("fixture config load failed")
	cases := []struct {
		name   string
		source retentionConfigSource
		want   string
	}{
		{"load error", retentionConfigSource{snapshot: RetentionConfigSnapshot{Known: true, Settings: originalConfiguredRetentionSettings()}, err: cause}, cause.Error()},
		{"unknown empty source", retentionConfigSource{}, "retention configuration visibility unavailable"},
		{"unknown populated source", retentionConfigSource{snapshot: RetentionConfigSnapshot{Settings: originalConfiguredRetentionSettings()}}, "retention configuration visibility unavailable"},
	}
	for _, f := range []struct {
		name string
		set  func(*sm.ContextRetentionSettings)
	}{
		{"ready_ttl", func(s *sm.ContextRetentionSettings) { s.ReadyTTL = -time.Hour }},
		{"stale_ttl", func(s *sm.ContextRetentionSettings) { s.StaleTTL = -time.Hour }},
		{"building_ttl", func(s *sm.ContextRetentionSettings) { s.BuildingTTL = -time.Hour }},
		{"retired_ttl", func(s *sm.ContextRetentionSettings) { s.RetiredTTL = -time.Hour }},
		{"unpublished_materialization_ttl", func(s *sm.ContextRetentionSettings) { s.UnpublishedMaterializationTTL = -time.Hour }},
		{"materialization_idle_ttl", func(s *sm.ContextRetentionSettings) { s.MaterializationIdleTTL = -time.Hour }},
		{"payload_ttl", func(s *sm.ContextRetentionSettings) { s.PayloadTTL = -time.Hour }},
		{"max_ready_per_binding", func(s *sm.ContextRetentionSettings) { s.MaxReadyPerBinding = -1 }},
		{"max_descendants_per_base", func(s *sm.ContextRetentionSettings) { s.MaxDescendantsPerBase = -1 }},
	} {
		settings := originalConfiguredRetentionSettings()
		f.set(&settings)
		cases = append(cases, struct {
			name   string
			source retentionConfigSource
			want   string
		}{"negative " + f.name, retentionConfigSource{snapshot: RetentionConfigSnapshot{Known: true, Settings: settings}}, f.name})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, a := openRetentionConfigService(t, &tc.source)
			record := seedMaterialization(m, "CTX-260801-n00001", MaterializationDetached, materializationTestClock.Add(-48*time.Hour), func(r *materializationRecord) { r.DetachReason = MaterializationDetachUnpublished })
			before := cloneMaterialization(record)
			sentinel := filepath.Join(m.layout.StateDir, "provider-custody-evidence")
			if err := os.WriteFile(sentinel, []byte("original provider bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := m.ContextRetentionSettings(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) || got != (sm.ContextRetentionSettings{}) {
				t.Fatalf("invalid config must refuse without usable settings: got=%+v err=%v want=%q", got, err, tc.want)
			}
			if tc.source.err != nil && !errors.Is(err, cause) {
				t.Fatal("configuration error cause was lost")
			}
			b, readErr := os.ReadFile(sentinel)
			if readErr != nil || string(b) != "original provider bytes" || !reflect.DeepEqual(m.materializations[record.CtxID], before) || len(a.sweeps) != 0 || len(a.reads) != 0 || len(a.publications) != 0 || len(a.confirmAttempts) != 0 {
				t.Fatal("configuration refusal changed provider/catalog/authority custody")
			}
		})
	}
	t.Run("canceled resolution", func(t *testing.T) {
		source := &retentionConfigSource{snapshot: RetentionConfigSnapshot{Known: true}}
		m, a := openRetentionConfigService(t, source)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got, err := m.ContextRetentionSettings(ctx)
		if !errors.Is(err, context.Canceled) || got != (sm.ContextRetentionSettings{}) || source.calls != 0 || len(a.sweeps) != 0 {
			t.Fatalf("canceled config operation = %+v %v calls=%d", got, err, source.calls)
		}
	})
}

// Configuration is resolved before composing the request, exactly as the original
// CLI did. Explicit partial requests remain independent and use sweep defaults,
// not a newly invented config/request merge. The real layer-2 idle verdict runs.
func TestContextRetentionResolvedSettingsDriveSweepWithoutRequestMerge(t *testing.T) {
	source := &retentionConfigSource{snapshot: RetentionConfigSnapshot{Known: true, Settings: originalConfiguredRetentionSettings()}}
	source.snapshot.Settings.MaterializationIdleTTL = 72 * time.Hour
	m, a := openRetentionConfigService(t, source)
	record := seedMaterialization(m, "CTX-260801-n00001", MaterializationReady, materializationTestClock.Add(-48*time.Hour), nil)
	settings, err := m.ContextRetentionSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	report, err := m.SweepContextRetention(context.Background(), ContextRetentionSweepRequest{Settings: settings, IncludePinned: true})
	if err != nil || report.Registry == nil || len(a.sweeps) != 1 || a.sweeps[0].Settings != source.snapshot.Settings || !a.sweeps[0].IncludePinned || a.sweeps[0].LeaseVisibility != "observed" || m.materializations[record.CtxID] == nil || len(report.Materializations) != 0 {
		t.Fatalf("configured whole request was not applied unchanged: %+v %v sweeps=%+v", report, err, a.sweeps)
	}
	source.err = errors.New("source now unavailable")
	report, err = m.SweepContextRetention(context.Background(), ContextRetentionSweepRequest{Settings: sm.ContextRetentionSettings{ReadyTTL: time.Hour, MaxReadyPerBinding: -1}, DryRun: true})
	want := DefaultContextRetentionSettings()
	want.ReadyTTL = time.Hour
	if err != nil || len(a.sweeps) != 2 || a.sweeps[1].Settings != want || !a.sweeps[1].DryRun || source.calls != 1 {
		t.Fatalf("explicit request merged config or changed original normalization: %+v %v calls=%d", report, err, source.calls)
	}
	outcome := materializationOutcomeFor(report, record.CtxID)
	if outcome == nil || outcome.Action != ContextMaterializationPlanned || outcome.Rule != "materialization_idle_ttl" || m.materializations[record.CtxID] == nil {
		t.Fatalf("default idle TTL/dry-run custody = %+v", outcome)
	}
}
