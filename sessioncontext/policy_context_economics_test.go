package sessioncontext

import (
	"strings"
	"testing"
	"time"
)

var economicsClock = time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)

func economicsRow(
	id string,
	source ContextMaterializationSource,
	mutate func(*ContextRunEconomics),
) ContextRunEconomics {
	row := ContextRunEconomics{
		CtxID:      id,
		RunID:      "RUN-260801-000001",
		Provider:   ContextProviderClaude,
		Source:     source,
		ObservedAt: economicsClock,
	}
	if mutate != nil {
		mutate(&row)
	}
	return NormalizeContextRunEconomics(row)
}

func measuredSpend(cost float64, cacheRead int64) func(*ContextRunEconomics) {
	return func(row *ContextRunEconomics) {
		row.TokensMeasured = true
		row.Tokens = ContextTokenUsage{
			InputTokens:         1_000,
			CacheReadTokens:     cacheRead,
			CacheCreationTokens: 5_000,
			OutputTokens:        20_000,
		}
		row.CostMeasured = true
		row.ProviderCostUSD = cost
		row.CostBasis = "list_dated"
	}
}

// --- The gate on claiming more than was measured ----------------------------

// TestAnUnmeasuredRowCannotCarryNumbers attacks the honesty gate directly: every
// case sets a "measured" flag false and a value non-zero, which is exactly what
// a caller inventing a plausible figure would produce. If validation admitted
// any of them, every aggregate downstream would be measuring an assumption.
func TestAnUnmeasuredRowCannotCarryNumbers(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ContextRunEconomics)
		want   string
	}{
		{
			name: "tokens without a measurement",
			mutate: func(row *ContextRunEconomics) {
				row.Tokens = ContextTokenUsage{CacheReadTokens: 100_000}
			},
			want: "token counts it did not measure",
		},
		{
			name:   "cost without a measurement",
			mutate: func(row *ContextRunEconomics) { row.ProviderCostUSD = 0.42 },
			want:   "cost it did not measure",
		},
		{
			name:   "wall time without a measurement",
			mutate: func(row *ContextRunEconomics) { row.WallTime = time.Minute },
			want:   "wall time it did not measure",
		},
		{
			name:   "bytes without a measurement",
			mutate: func(row *ContextRunEconomics) { row.SourceBytes = 442_908 },
			want:   "source bytes it did not measure",
		},
		{
			name: "a measured cost with no basis",
			mutate: func(row *ContextRunEconomics) {
				row.CostMeasured = true
				row.ProviderCostUSD = 0.42
			},
			want: "must name the basis",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Build the row without normalization, because normalization is what
			// clears an unmeasured field: the gate has to hold on its own.
			row := ContextRunEconomics{
				ContractVersion: ContextEconomicsContract,
				CtxID:           "CTX-260801-a1b2c3",
				Source:          ContextSourceForked,
				ObservedAt:      economicsClock,
			}
			test.mutate(&row)
			err := ValidateContextRunEconomics(row)
			if err == nil {
				t.Fatalf("a row claiming an unmeasured value was admitted")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("refusal = %q, want it to name %q", err.Error(), test.want)
			}
		})
	}
}

// TestNormalizationClearsWhatWasNotMeasured is the second half of the same rule.
// A leftover number on an unmeasured row must not survive to reach an aggregate
// through some path that only checked whether the field was non-zero.
func TestNormalizationClearsWhatWasNotMeasured(t *testing.T) {
	row := NormalizeContextRunEconomics(ContextRunEconomics{
		CtxID:           "CTX-260801-a1b2c3",
		Source:          ContextSourceForked,
		SourceBytes:     442_908,
		Tokens:          ContextTokenUsage{CacheReadTokens: 100_000},
		WallTime:        time.Minute,
		ProviderCostUSD: 0.42,
		ObservedAt:      economicsClock,
	})
	if row.SourceBytes != 0 || row.Tokens.Total() != 0 || row.WallTime != 0 ||
		row.ProviderCostUSD != 0 {
		t.Fatalf("normalization kept values nobody measured: %+v", row)
	}
	if err := ValidateContextRunEconomics(row); err != nil {
		t.Fatalf("a normalized unmeasured row was refused: %v", err)
	}
}

// --- Cache honesty ----------------------------------------------------------

// TestACacheMissIsNeverReportedAsASaving is the single most load-bearing
// property here.
//
// Both cohorts are measured and the shared arm cost more than the cold one,
// because its forks realized no cache reads. The realized saving must therefore
// be negative, and the cache statement must report the misses. An implementation
// that folded an assumed cache rate into the arithmetic — or that suppressed a
// negative result — would report a saving nobody received.
func TestACacheMissIsNeverReportedAsASaving(t *testing.T) {
	rows := []ContextRunEconomics{
		economicsRow("CTX-260801-c00001", ContextSourceCold, measuredSpend(0.50, 400_000)),
		economicsRow("CTX-260801-c00002", ContextSourceForked, measuredSpend(0.90, 0)),
		economicsRow("CTX-260801-c00003", ContextSourceForked, measuredSpend(0.90, 0)),
	}
	report := DeriveContextEconomics(rows, ContextPayloadCostRates{}, economicsClock)

	if report.Saving.State != ContextSavingMeasured {
		t.Fatalf("saving state = %s, want measured", report.Saving.State)
	}
	if report.Saving.AmountPerRunUSD >= 0 {
		t.Fatalf("a shared arm that cost more reported a saving of $%.4f",
			report.Saving.AmountPerRunUSD)
	}
	if !strings.Contains(report.Saving.Statement, "more expensive") {
		t.Fatalf("the statement hid the direction: %s", report.Saving.Statement)
	}
	if report.Cache.RunsWithCacheReads != 1 || report.Cache.MeasuredRuns != 3 {
		t.Fatalf("cache realization = %d of %d measured runs, want 1 of 3",
			report.Cache.RunsWithCacheReads, report.Cache.MeasuredRuns)
	}
	if !strings.Contains(report.Cache.Statement, "2 measured misses") {
		t.Fatalf("the measured misses were not reported: %s", report.Cache.Statement)
	}
	if !strings.Contains(report.Cache.Statement, "never used to predict") {
		t.Fatalf("the cache statement did not say it is an observation: %s", report.Cache.Statement)
	}
}

// TestASavingWithNoBaselineIsUnmeasuredRatherThanZero separates the two claims
// that a naive implementation collapses. With no cold arm there is nothing to
// compare against, and "we do not know" must not print as "$0.0000 saved".
func TestASavingWithNoBaselineIsUnmeasuredRatherThanZero(t *testing.T) {
	rows := []ContextRunEconomics{
		economicsRow("CTX-260801-d00001", ContextSourceForked, measuredSpend(0.40, 300_000)),
		economicsRow("CTX-260801-d00002", ContextSourceForked, measuredSpend(0.42, 310_000)),
	}
	report := DeriveContextEconomics(rows, ContextPayloadCostRates{}, economicsClock)
	if report.Saving.State != ContextSavingUnmeasured {
		t.Fatalf("saving state = %s, want unmeasured", report.Saving.State)
	}
	if report.Saving.AmountPerRunUSD != 0 || report.Saving.SharedCostPerRunUSD != 0 {
		t.Fatalf("an unmeasured saving carried numbers: %+v", report.Saving)
	}
	if !strings.Contains(report.Saving.Statement, "unmeasured") {
		t.Fatalf("the statement did not say the saving is unmeasured: %s", report.Saving.Statement)
	}
}

// TestUnmeasuredRunsAreCountedAndExcluded is the "40 forks, 12 of them we could
// not see" property. A run whose provider reported nothing must not be averaged
// in as a cheap one, and its absence must be visible rather than inferred.
func TestUnmeasuredRunsAreCountedAndExcluded(t *testing.T) {
	rows := []ContextRunEconomics{
		economicsRow("CTX-260801-e00001", ContextSourceCold, measuredSpend(1.00, 200_000)),
		economicsRow("CTX-260801-e00002", ContextSourceForked, measuredSpend(0.50, 100_000)),
		economicsRow("CTX-260801-e00003", ContextSourceForked, nil),
		economicsRow("CTX-260801-e00004", ContextSourceForked, nil),
	}
	report := DeriveContextEconomics(rows, ContextPayloadCostRates{}, economicsClock)
	if report.Runs != 4 || report.UnmeasuredRuns != 2 {
		t.Fatalf("runs = %d, unmeasured = %d, want 4 and 2", report.Runs, report.UnmeasuredRuns)
	}
	if report.Saving.SharedRuns != 1 {
		t.Fatalf("shared cohort = %d runs, want only the one with measured spend",
			report.Saving.SharedRuns)
	}
	if report.Saving.SharedCostPerRunUSD != 0.50 {
		t.Fatalf("shared cost per run = $%.4f; unmeasured runs diluted the mean",
			report.Saving.SharedCostPerRunUSD)
	}
}

// TestSourcesAreReportedSeparately keeps build, fetch and fork from being folded
// together. They pay for different things — exploration, transfer, replay — and
// one number over all three is uninterpretable.
func TestSourcesAreReportedSeparately(t *testing.T) {
	rows := []ContextRunEconomics{
		economicsRow("CTX-260801-f00001", ContextSourceBuilt, func(row *ContextRunEconomics) {
			row.SourceBytesMeasured = true
			row.SourceBytes = 276_933
			row.WallTimeMeasured = true
			row.WallTime = 3 * time.Minute
		}),
		economicsRow("CTX-260801-f00002", ContextSourceForked, func(row *ContextRunEconomics) {
			row.SourceBytesMeasured = true
			row.SourceBytes = 442_908
			row.WallTimeMeasured = true
			row.WallTime = 4 * time.Minute
		}),
		economicsRow("CTX-260801-f00003", ContextSourceFetched, func(row *ContextRunEconomics) {
			row.SourceBytesMeasured = true
			row.SourceBytes = 442_908
		}),
	}
	report := DeriveContextEconomics(rows, ContextPayloadCostRates{}, economicsClock)
	if len(report.BySource) != 3 {
		t.Fatalf("sources = %d, want built, fetched and forked separately", len(report.BySource))
	}
	byName := map[ContextMaterializationSource]ContextSourceEconomics{}
	for _, bucket := range report.BySource {
		byName[bucket.Source] = bucket
	}
	if byName[ContextSourceForked].Bytes != 442_908 ||
		byName[ContextSourceBuilt].Bytes != 276_933 {
		t.Fatalf("byte attribution collapsed across sources: %+v", report.BySource)
	}
	if byName[ContextSourceFetched].WallTimeRuns != 0 {
		t.Fatalf("an unmeasured wall time counted toward the measured run count")
	}
}

// --- Payload cost attribution (BR-11) ---------------------------------------

// TestPayloadBytesAreAttributedAndPricedOnlyAgainstANamedBasis is the BR-11
// operator surface. With no rate the bytes are still attributed to their context
// and no dollar figure is invented; with a named rate the figure appears and
// carries the basis it was priced on.
func TestPayloadBytesAreAttributedAndPricedOnlyAgainstANamedBasis(t *testing.T) {
	rows := []ContextRunEconomics{
		economicsRow("CTX-260801-g00001", ContextSourceFetched, func(row *ContextRunEconomics) {
			row.BindingElementID = "STORY-260728-aaaaaa"
			row.PayloadDownloadedBytes = 442_908
			row.PayloadStoredBytes = 442_908
		}),
	}
	unpriced := DeriveContextEconomics(rows, ContextPayloadCostRates{}, economicsClock)
	if unpriced.Payload.Priced {
		t.Fatalf("payload cost was priced with no rate basis configured")
	}
	if unpriced.Payload.TotalStoredBytes != 442_908 {
		t.Fatalf("bytes were not attributed without a price: %+v", unpriced.Payload)
	}
	if unpriced.Payload.TotalStorageUSDPerMonth != 0 {
		t.Fatalf("a dollar figure was invented without a rate")
	}
	if unpriced.Payload.Rows[0].BindingElementID != "STORY-260728-aaaaaa" {
		t.Fatalf("payload bytes were not attributed to their bound element")
	}

	priced := DeriveContextEconomics(rows, ContextPayloadCostRates{
		Basis:                 "aws-s3-standard-us-east-1",
		StorageUSDPerGiBMonth: 0.023,
	}, economicsClock)
	if !priced.Payload.Priced || priced.Payload.Basis != "aws-s3-standard-us-east-1" {
		t.Fatalf("a named rate did not produce a priced attribution: %+v", priced.Payload)
	}
	if priced.Payload.TotalStorageUSDPerMonth <= 0 {
		t.Fatalf("a priced attribution produced no storage cost")
	}
}

// TestARateWithNoBasisPricesNothing attacks the gate the previous test walked
// past.
//
// It covers the case the rule exists for: a rate is supplied and the basis is
// not. Both neighbouring cases — no rate at all, and rate plus basis — are
// satisfied by a Priced() that only looked at the numbers, so neither of them
// fails when the basis requirement is deleted. This one does: the operator gets
// a dollar column whose provenance nobody can check, which is the same
// unattributable figure the CostBasis field on a run row exists to refuse.
func TestARateWithNoBasisPricesNothing(t *testing.T) {
	rates := ContextPayloadCostRates{
		StorageUSDPerGiBMonth: 0.023,
		TransferUSDPerGiB:     0.09,
	}
	if rates.Priced() {
		t.Fatalf("a rate with no basis reported itself priceable")
	}
	// A basis of nothing but whitespace is the same claim spelled differently.
	if (ContextPayloadCostRates{Basis: "   ", TransferUSDPerGiB: 0.09}).Priced() {
		t.Fatalf("a blank basis was accepted as provenance")
	}

	report := DeriveContextEconomics([]ContextRunEconomics{
		economicsRow("CTX-260801-n00001", ContextSourceFetched, func(row *ContextRunEconomics) {
			row.PayloadStoredBytes = 442_908
			row.PayloadDownloadedBytes = 442_908
		}),
	}, rates, economicsClock)

	if report.Payload.Priced {
		t.Fatalf("payload bytes were priced against a rate with no named source")
	}
	if report.Payload.TotalStorageUSDPerMonth != 0 || report.Payload.TotalTransferUSD != 0 {
		t.Fatalf("a dollar figure survived a rate with no basis: %+v", report.Payload)
	}
	if report.Payload.Rows[0].StorageUSDPerMonth != 0 || report.Payload.Rows[0].TransferUSD != 0 {
		t.Fatalf("a per-context dollar figure survived a rate with no basis: %+v",
			report.Payload.Rows[0])
	}
	if report.Payload.TotalStoredBytes != 442_908 {
		t.Fatalf("bytes stopped being attributed when the price was refused: %+v", report.Payload)
	}
	if !strings.Contains(report.Payload.Statement, "no rate basis is configured") {
		t.Fatalf("the report did not say why nothing was priced: %s", report.Payload.Statement)
	}
}

// TestASavingIsNeverFoldedAcrossDifferentCostBases is the arithmetic honesty
// rule for the saving itself.
//
// The cold arm is priced on provider telemetry and the shared arm on a dated
// list price. Subtracting them produces a number in no currency, and labelling
// it with whichever basis happened to be read first is worse than reporting
// nothing: it reads as a measured result. Both arms are still reported, because
// the comparison is real once one cohort is re-priced.
func TestASavingIsNeverFoldedAcrossDifferentCostBases(t *testing.T) {
	pricedOn := func(basis string, cost float64) func(*ContextRunEconomics) {
		return func(row *ContextRunEconomics) {
			measuredSpend(cost, 100_000)(row)
			row.CostBasis = basis
		}
	}
	report := DeriveContextEconomics([]ContextRunEconomics{
		economicsRow("CTX-260801-m00001", ContextSourceCold,
			pricedOn("provider-telemetry-2026-07", 1.00)),
		economicsRow("CTX-260801-m00002", ContextSourceForked,
			pricedOn("list-price-2025-11", 0.40)),
	}, ContextPayloadCostRates{}, economicsClock)

	if report.Saving.State != ContextSavingIncomparable {
		t.Fatalf("saving state = %s, want incomparable across two price bases: %+v",
			report.Saving.State, report.Saving)
	}
	if report.Saving.AmountPerRunUSD != 0 {
		t.Fatalf("a saving figure was folded across two bases: %+v", report.Saving)
	}
	if report.Saving.Basis != "" {
		t.Fatalf("an incomparable saving was labelled with one basis: %q", report.Saving.Basis)
	}
	if len(report.Saving.Bases) != 2 ||
		report.Saving.Bases[0] != "list-price-2025-11" ||
		report.Saving.Bases[1] != "provider-telemetry-2026-07" {
		t.Fatalf("the bases were not reported in a stable order: %+v", report.Saving.Bases)
	}
	for _, want := range []string{"list-price-2025-11", "provider-telemetry-2026-07", "no currency"} {
		if !strings.Contains(report.Saving.Statement, want) {
			t.Fatalf("statement %q does not name %q", report.Saving.Statement, want)
		}
	}
	// The arms are still measured, so they are still reported: the defect is the
	// subtraction, not the observation.
	if report.Saving.BaselineCostPerRunUSD != 1.00 || report.Saving.SharedCostPerRunUSD != 0.40 {
		t.Fatalf("the measured arms were dropped along with the difference: %+v", report.Saving)
	}
}

// TestOneBasisAcrossBothArmsStillComputesASaving is the other side of the same
// gate: refusing mixed bases must not refuse the ordinary case.
func TestOneBasisAcrossBothArmsStillComputesASaving(t *testing.T) {
	report := DeriveContextEconomics([]ContextRunEconomics{
		economicsRow("CTX-260801-m10001", ContextSourceCold, measuredSpend(1.00, 100_000)),
		economicsRow("CTX-260801-m10002", ContextSourceForked, measuredSpend(0.40, 100_000)),
	}, ContextPayloadCostRates{}, economicsClock)

	if report.Saving.State != ContextSavingMeasured {
		t.Fatalf("saving state = %s, want measured on one shared basis", report.Saving.State)
	}
	if report.Saving.Basis != "list_dated" {
		t.Fatalf("basis = %q, want the single basis both arms were priced on", report.Saving.Basis)
	}
	if len(report.Saving.Bases) != 0 {
		t.Fatalf("a single-basis saving reported a basis list: %+v", report.Saving.Bases)
	}
}

// TestStoredPayloadBytesAreNotSummedPerRun catches the inflation a flow/level
// confusion would cause: one context read by ten runs occupies its size once,
// and summing it would scale the storage column by popularity.
func TestStoredPayloadBytesAreNotSummedPerRun(t *testing.T) {
	rows := []ContextRunEconomics{}
	for range 10 {
		rows = append(rows, economicsRow("CTX-260801-h00001", ContextSourceFetched,
			func(row *ContextRunEconomics) {
				row.PayloadStoredBytes = 442_908
				row.PayloadDownloadedBytes = 442_908
			}))
	}
	report := DeriveContextEconomics(rows, ContextPayloadCostRates{}, economicsClock)
	if report.Payload.TotalStoredBytes != 442_908 {
		t.Fatalf("stored bytes = %d, want the context's size once rather than once per run",
			report.Payload.TotalStoredBytes)
	}
	// Transfer genuinely is a flow: ten downloads moved ten times the bytes.
	if report.Payload.TotalDownloadedBytes != 4_429_080 {
		t.Fatalf("downloaded bytes = %d, want the sum across runs",
			report.Payload.TotalDownloadedBytes)
	}
}

// TestAnInvalidRowNeverReachesTheAggregate proves the validation gate is on the
// aggregation path and not only on the write path.
func TestAnInvalidRowNeverReachesTheAggregate(t *testing.T) {
	rows := []ContextRunEconomics{
		economicsRow("CTX-260801-i00001", ContextSourceCold, measuredSpend(1.00, 200_000)),
		{
			ContractVersion: ContextEconomicsContract,
			CtxID:           "CTX-260801-i00002",
			Source:          ContextSourceForked,
			ProviderCostUSD: -5,
			CostMeasured:    true,
			CostBasis:       "list_dated",
			ObservedAt:      economicsClock,
		},
	}
	report := DeriveContextEconomics(rows, ContextPayloadCostRates{}, economicsClock)
	if report.Runs != 1 {
		t.Fatalf("runs = %d, want the invalid row dropped", report.Runs)
	}
	if report.Saving.State != ContextSavingUnmeasured {
		t.Fatalf("a dropped row still produced a shared cohort: %+v", report.Saving)
	}
}

// TestNoMeasurementAtAllSaysSoRatherThanReportingZero is the empty case, which
// is the one an operator sees first on a fresh machine.
func TestNoMeasurementAtAllSaysSoRatherThanReportingZero(t *testing.T) {
	report := DeriveContextEconomics(nil, ContextPayloadCostRates{}, economicsClock)
	if !strings.Contains(report.Cache.Statement, "unknown") ||
		!strings.Contains(report.Cache.Statement, "assumes a cache hit") {
		t.Fatalf("an empty report did not say cache behaviour is unknown: %s", report.Cache.Statement)
	}
	if report.Saving.State != ContextSavingUnmeasured {
		t.Fatalf("an empty report claimed a measured saving")
	}
}
