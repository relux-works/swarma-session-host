package sessioncontext

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Realized economics is the honest counterpart to the projected saving.
//
// The original economics measurement that justified this feature found one
// result that has to be structural here rather than remembered: prompt-cache
// reuse is not budgetable. Sibling forks of the same completed base realized
// 97.9% and 19.2% cache reads with no caller-visible difference, and the arm
// totals move by more than a factor of two between "cache hit as measured" and
// "cache never hits". A telemetry layer that folded an assumed cache rate into
// its numbers would report a saving that nobody paid and nobody received.
//
// So the rules encoded in this file are:
//
//   - every dollar and every token is measured or absent, never inferred;
//   - a cache miss is a measured zero that lowers the realized saving, never an
//     omission that quietly raises it;
//   - a saving with no measured baseline is reported as unmeasured, with no
//     number attached, because "we could not measure it" and "it was zero" are
//     different claims and only one of them is true;
//   - payload bytes are always attributed; their price is reported only against
//     a named rate basis an operator supplied.

// ContextEconomicsContract is the realized per-run economics row contract.
const ContextEconomicsContract = "context-realized-economics-v1"

// ContextMaterializationSource is how a run obtained its context.
//
// It mirrors the private materialization catalog's own source enum. It is
// repeated here because cost is attributed by it: a build pays exploration, a
// fetch pays transfer, and a fork pays a transcript replay, and folding the
// three together is what makes an aggregate uninterpretable.
type ContextMaterializationSource string

const (
	ContextSourceBuilt   ContextMaterializationSource = "built"
	ContextSourceFetched ContextMaterializationSource = "fetched"
	ContextSourceForked  ContextMaterializationSource = "forked"
	// ContextSourceCold is a run that had no context at all. It is the baseline
	// arm: without measured cold runs there is nothing to compare a fork against,
	// and the report says so rather than inventing one.
	ContextSourceCold ContextMaterializationSource = "cold"
)

// ContextTokenUsage is one run's measured token classes.
//
// The four are kept separate because they are priced differently and because
// their ratio is the only evidence of realized cache behaviour there is. Folding
// them into a single "tokens" number would delete exactly the measurement this
// file exists to preserve.
type ContextTokenUsage struct {
	InputTokens         int64 `json:"input_tokens"`
	CacheReadTokens     int64 `json:"cache_read_tokens"`
	CacheCreationTokens int64 `json:"cache_creation_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
}

// Total is every token the model processed, however billed.
func (u ContextTokenUsage) Total() int64 {
	return u.InputTokens + u.CacheReadTokens + u.CacheCreationTokens + u.OutputTokens
}

// Ingested is every input-side token, which is the number a context is supposed
// to reduce.
func (u ContextTokenUsage) Ingested() int64 {
	return u.InputTokens + u.CacheReadTokens + u.CacheCreationTokens
}

func (u ContextTokenUsage) add(other ContextTokenUsage) ContextTokenUsage {
	u.InputTokens += other.InputTokens
	u.CacheReadTokens += other.CacheReadTokens
	u.CacheCreationTokens += other.CacheCreationTokens
	u.OutputTokens += other.OutputTokens
	return u
}

// ContextRunEconomics is one run's realized cost record.
//
// Every measurement carries its own "was this observed" flag rather than
// relying on a zero value. A provider that reported no usage and a provider that
// reported zero usage are different facts, and only the second one is evidence.
type ContextRunEconomics struct {
	ContractVersion string                       `json:"contract_version"`
	CtxID           string                       `json:"ctx_id,omitempty"`
	RunID           string                       `json:"run_id,omitempty"`
	Role            string                       `json:"role,omitempty"`
	Provider        ContextProvider              `json:"provider,omitempty"`
	Source          ContextMaterializationSource `json:"source"`

	// SourceBytes is what obtaining the context moved on this machine: the
	// transcript a build wrote, the payload a fetch downloaded, or the copy a fork
	// made. Zero with SourceBytesMeasured false means nobody looked.
	SourceBytes         int64 `json:"source_bytes,omitempty"`
	SourceBytesMeasured bool  `json:"source_bytes_measured,omitempty"`

	// PayloadUploadedBytes and PayloadDownloadedBytes are the cross-machine
	// transport this run caused, attributed to this context per BR-11.
	PayloadUploadedBytes   int64 `json:"payload_uploaded_bytes,omitempty"`
	PayloadDownloadedBytes int64 `json:"payload_downloaded_bytes,omitempty"`
	// PayloadStoredBytes is what this context currently occupies in the payload
	// store. It is a level, not a flow, and is deduplicated per CTX when folded.
	PayloadStoredBytes int64 `json:"payload_stored_bytes,omitempty"`

	Tokens         ContextTokenUsage `json:"tokens"`
	TokensMeasured bool              `json:"tokens_measured,omitempty"`

	WallTime         time.Duration `json:"wall_time_ns,omitempty"`
	WallTimeMeasured bool          `json:"wall_time_measured,omitempty"`

	// ProviderCostUSD is the provider's own reported cost for this run. It is not
	// recomputed from tokens and a price table: a wrapper's cost telemetry was
	// measured to cover more models than the one being priced, and reconciling to
	// "close enough" hid that.
	ProviderCostUSD  float64 `json:"provider_cost_usd,omitempty"`
	CostMeasured     bool    `json:"cost_measured,omitempty"`
	CostBasis        string  `json:"cost_basis,omitempty"`
	BindingElementID string  `json:"binding_element_id,omitempty"`

	ObservedAt time.Time `json:"observed_at"`
}

// CacheRealized reports whether this run actually read anything from cache.
//
// It is a measured predicate, and false is a real answer. The distinction it
// makes is the one the whole file turns on: a run with zero cache reads did not
// save anything on replay, and must not be counted as though it had.
func (r ContextRunEconomics) CacheRealized() bool {
	return r.TokensMeasured && r.Tokens.CacheReadTokens > 0
}

// NormalizeContextRunEconomics fills the contract version and canonical
// spellings, and clears every measurement whose "measured" flag is false.
//
// Clearing is deliberate rather than tidy. An unmeasured row that still carried
// a leftover number would let that number reach an aggregate through some later
// path that only checked whether the field was non-zero.
func NormalizeContextRunEconomics(row ContextRunEconomics) ContextRunEconomics {
	row.ContractVersion = ContextEconomicsContract
	row.CtxID = NormalizeContextID(row.CtxID)
	row.RunID = strings.ToUpper(strings.TrimSpace(row.RunID))
	row.Role = NormalizeContextRole(row.Role)
	row.BindingElementID = NormalizeContextElementID(row.BindingElementID)
	if strings.TrimSpace(string(row.Source)) == "" {
		row.Source = ContextSourceCold
	}
	if !row.SourceBytesMeasured {
		row.SourceBytes = 0
	}
	if !row.TokensMeasured {
		row.Tokens = ContextTokenUsage{}
	}
	if !row.WallTimeMeasured {
		row.WallTime = 0
	}
	if !row.CostMeasured {
		row.ProviderCostUSD = 0
		row.CostBasis = ""
	}
	row.ObservedAt = row.ObservedAt.UTC()
	return row
}

// ValidateContextRunEconomics refuses a row that claims more than it measured.
//
// This is the gate. Without it "measured" is a field a caller can set next to
// numbers it invented, and every honesty property downstream rests on a flag
// nobody checked. A negative token count, a negative cost, or a cost asserted
// as measured with no basis are all the same defect: a claim with no observation
// behind it.
func ValidateContextRunEconomics(row ContextRunEconomics) error {
	if row.ContractVersion != ContextEconomicsContract {
		return contextError(
			ContextErrorContractInvalid,
			fmt.Sprintf(
				"realized economics row declares contract %q, want %s",
				row.ContractVersion, ContextEconomicsContract,
			),
			nil,
		)
	}
	switch row.Source {
	case ContextSourceBuilt, ContextSourceFetched, ContextSourceForked, ContextSourceCold:
	default:
		return contextError(
			ContextErrorContractInvalid,
			fmt.Sprintf("unsupported materialization source %q", row.Source),
			nil,
		)
	}
	if row.SourceBytes < 0 || row.PayloadUploadedBytes < 0 ||
		row.PayloadDownloadedBytes < 0 || row.PayloadStoredBytes < 0 {
		return contextError(ContextErrorContractInvalid, "byte counters cannot be negative", nil)
	}
	if row.Tokens.InputTokens < 0 || row.Tokens.CacheReadTokens < 0 ||
		row.Tokens.CacheCreationTokens < 0 || row.Tokens.OutputTokens < 0 {
		return contextError(ContextErrorContractInvalid, "token counters cannot be negative", nil)
	}
	if row.WallTime < 0 {
		return contextError(ContextErrorContractInvalid, "wall time cannot be negative", nil)
	}
	if row.ProviderCostUSD < 0 {
		return contextError(ContextErrorContractInvalid, "provider cost cannot be negative", nil)
	}
	if !row.TokensMeasured && row.Tokens.Total() != 0 {
		return contextError(
			ContextErrorContractInvalid,
			"realized economics row carries token counts it did not measure; an unmeasured row "+
				"reports no tokens rather than plausible ones",
			nil,
		)
	}
	if !row.CostMeasured && row.ProviderCostUSD != 0 {
		return contextError(
			ContextErrorContractInvalid,
			"realized economics row carries a cost it did not measure; spend is reported only where "+
				"the provider reported it",
			nil,
		)
	}
	if row.CostMeasured && strings.TrimSpace(row.CostBasis) == "" {
		return contextError(
			ContextErrorContractInvalid,
			"a measured cost must name the basis it was priced on; a bare dollar figure cannot be "+
				"reconciled against provider telemetry or a dated list price",
			nil,
		)
	}
	if !row.WallTimeMeasured && row.WallTime != 0 {
		return contextError(
			ContextErrorContractInvalid,
			"realized economics row carries a wall time it did not measure",
			nil,
		)
	}
	if !row.SourceBytesMeasured && row.SourceBytes != 0 {
		return contextError(
			ContextErrorContractInvalid,
			"realized economics row carries source bytes it did not measure",
			nil,
		)
	}
	return nil
}

// ContextCacheRealization is what the cache actually did, as observed.
//
// It has no predicted counterpart on purpose. Selection, retention, and every
// budget in this system are computed without it, and this value exists only so
// an operator can see the spread rather than plan against a rate.
type ContextCacheRealization struct {
	// MeasuredRuns is how many rows carried provider usage at all.
	MeasuredRuns int `json:"measured_runs"`
	// RunsWithCacheReads is how many of those actually read from cache. The
	// difference between this and MeasuredRuns is the measured miss count, and it
	// is reported because a miss is the case that must never turn into a saving.
	RunsWithCacheReads int   `json:"runs_with_cache_reads"`
	CacheReadTokens    int64 `json:"cache_read_tokens"`
	IngestedTokens     int64 `json:"ingested_tokens"`
	// ReadShare is cache reads over ingested tokens across measured runs only. It
	// is a description of what happened, never an input to anything.
	ReadShare float64 `json:"read_share"`
	Statement string  `json:"statement"`
}

// ContextSavingState says what kind of claim the saving figure is.
type ContextSavingState string

const (
	// ContextSavingMeasured means both arms were observed and the difference is
	// arithmetic on measured spend.
	ContextSavingMeasured ContextSavingState = "measured"
	// ContextSavingUnmeasured means there is no measured baseline to compare
	// against. No number is attached, because the honest answer to "how much did
	// we save" without a baseline is "we do not know", not zero and not an
	// estimate.
	ContextSavingUnmeasured ContextSavingState = "unmeasured"
	// ContextSavingIncomparable means both arms were measured but not on the same
	// price basis, so subtracting them would produce a figure in no currency.
	//
	// It is a separate verdict from unmeasured because the two say different
	// things to an operator: unmeasured means "look at more runs", incomparable
	// means "these runs were priced against different sources and one of them has
	// to be re-priced". Folding them anyway and labelling the result with
	// whichever basis was seen first is the exact failure CostBasis exists to
	// prevent — a wrapper's telemetry priced models nobody was measuring, and
	// reconciling to close enough hid it.
	ContextSavingIncomparable ContextSavingState = "incomparable"
)

// ContextRealizedSaving is the measured spend difference per run, or the
// explicit statement that it could not be measured.
type ContextRealizedSaving struct {
	State ContextSavingState `json:"state"`
	// BaselineRuns and SharedRuns are the two measured cohorts. They are reported
	// so a reader can see how thin the comparison is.
	BaselineRuns int `json:"baseline_runs"`
	SharedRuns   int `json:"shared_runs"`
	// BaselineCostPerRunUSD and SharedCostPerRunUSD are measured means.
	BaselineCostPerRunUSD float64 `json:"baseline_cost_per_run_usd,omitempty"`
	SharedCostPerRunUSD   float64 `json:"shared_cost_per_run_usd,omitempty"`
	// AmountPerRunUSD is positive when sharing was cheaper and negative when it
	// was not. A negative realized saving is reported as a negative number rather
	// than suppressed: the measurement found shapes where sharing loses, and a
	// report that could only ever say "saved" would hide them.
	AmountPerRunUSD float64 `json:"amount_per_run_usd,omitempty"`
	Basis           string  `json:"basis,omitempty"`
	// Bases is every distinct price basis the measured rows carried. It is
	// reported whenever there is more than one, because that is the case where no
	// single Basis can honestly label the figure.
	Bases     []string `json:"bases,omitempty"`
	Statement string   `json:"statement"`
}

// ContextPayloadCostRates prices cross-machine payload bytes.
//
// The zero value prices nothing, and that is the shipped default. A price is a
// deployment fact — region, tier, self-hosted or not — and a report that
// defaulted to somebody else's rate would attribute a cost the operator does not
// pay. Bytes are attributed either way; only the dollar column depends on this.
type ContextPayloadCostRates struct {
	// Basis names where the rates came from. It is required whenever a rate is
	// set, so a dollar figure always carries its own provenance.
	Basis string `json:"basis,omitempty"`
	// StorageUSDPerGiBMonth prices bytes at rest.
	StorageUSDPerGiBMonth float64 `json:"storage_usd_per_gib_month,omitempty"`
	// TransferUSDPerGiB prices upload plus download.
	TransferUSDPerGiB float64 `json:"transfer_usd_per_gib,omitempty"`
}

// Priced reports whether a dollar column can be produced at all.
func (r ContextPayloadCostRates) Priced() bool {
	return strings.TrimSpace(r.Basis) != "" &&
		(r.StorageUSDPerGiBMonth > 0 || r.TransferUSDPerGiB > 0)
}

// ContextPayloadCostRow attributes payload bytes to one context.
type ContextPayloadCostRow struct {
	CtxID            string `json:"ctx_id"`
	BindingElementID string `json:"binding_element_id,omitempty"`
	StoredBytes      int64  `json:"stored_bytes,omitempty"`
	UploadedBytes    int64  `json:"uploaded_bytes,omitempty"`
	DownloadedBytes  int64  `json:"downloaded_bytes,omitempty"`
	// StorageUSDPerMonth and TransferUSD are present only against a named rate
	// basis.
	StorageUSDPerMonth float64 `json:"storage_usd_per_month,omitempty"`
	TransferUSD        float64 `json:"transfer_usd,omitempty"`
}

// ContextPayloadCostAttribution is the operator-facing BR-11 payload cost view.
type ContextPayloadCostAttribution struct {
	Rows                    []ContextPayloadCostRow `json:"rows"`
	TotalStoredBytes        int64                   `json:"total_stored_bytes"`
	TotalUploadedBytes      int64                   `json:"total_uploaded_bytes"`
	TotalDownloadedBytes    int64                   `json:"total_downloaded_bytes"`
	TotalStorageUSDPerMonth float64                 `json:"total_storage_usd_per_month,omitempty"`
	TotalTransferUSD        float64                 `json:"total_transfer_usd,omitempty"`
	Priced                  bool                    `json:"priced"`
	Basis                   string                  `json:"basis,omitempty"`
	Statement               string                  `json:"statement"`
}

// ContextSourceEconomics is one materialization source's measured totals.
type ContextSourceEconomics struct {
	Source          ContextMaterializationSource `json:"source"`
	Runs            int                          `json:"runs"`
	MeasuredRuns    int                          `json:"measured_runs"`
	Bytes           int64                        `json:"bytes"`
	BytesMeasured   int                          `json:"bytes_measured_runs"`
	Tokens          ContextTokenUsage            `json:"tokens"`
	WallTime        time.Duration                `json:"wall_time_ns"`
	WallTimeRuns    int                          `json:"wall_time_measured_runs"`
	ProviderCostUSD float64                      `json:"provider_cost_usd"`
	CostRuns        int                          `json:"cost_measured_runs"`
}

// ContextEconomicsReport is the folded realized-economics view.
type ContextEconomicsReport struct {
	ContractVersion string                        `json:"contract_version"`
	Runs            int                           `json:"runs"`
	BySource        []ContextSourceEconomics      `json:"by_source"`
	Cache           ContextCacheRealization       `json:"cache"`
	Saving          ContextRealizedSaving         `json:"saving"`
	Payload         ContextPayloadCostAttribution `json:"payload"`
	// UnmeasuredRuns is how many rows carried no provider usage. It is a
	// first-class number: an aggregate over 40 runs where 12 were invisible is a
	// different claim from an aggregate over 28.
	UnmeasuredRuns int       `json:"unmeasured_runs"`
	EvaluatedAt    time.Time `json:"evaluated_at"`
}

// DeriveContextEconomics folds realized rows into the operator report.
//
// Invalid rows are dropped rather than absorbed, and the drop is visible in the
// run counts. Every aggregate is computed from measured rows only; nothing here
// fills a gap with a plausible value.
func DeriveContextEconomics(
	rows []ContextRunEconomics,
	rates ContextPayloadCostRates,
	at time.Time,
) ContextEconomicsReport {
	if at.IsZero() {
		at = time.Now()
	}
	report := ContextEconomicsReport{
		ContractVersion: ContextEconomicsContract,
		BySource:        []ContextSourceEconomics{},
		EvaluatedAt:     at.UTC(),
	}
	bySource := make(map[ContextMaterializationSource]*ContextSourceEconomics)
	payload := make(map[string]*ContextPayloadCostRow)
	bases := make(map[string]struct{})
	var baselineCost, sharedCost float64
	var baselineRuns, sharedRuns int

	for _, raw := range rows {
		row := NormalizeContextRunEconomics(raw)
		if err := ValidateContextRunEconomics(row); err != nil {
			continue
		}
		report.Runs++
		bucket := bySource[row.Source]
		if bucket == nil {
			bucket = &ContextSourceEconomics{Source: row.Source}
			bySource[row.Source] = bucket
		}
		bucket.Runs++
		if row.SourceBytesMeasured {
			bucket.Bytes += row.SourceBytes
			bucket.BytesMeasured++
		}
		if row.TokensMeasured {
			bucket.MeasuredRuns++
			bucket.Tokens = bucket.Tokens.add(row.Tokens)
			report.Cache.MeasuredRuns++
			report.Cache.CacheReadTokens += row.Tokens.CacheReadTokens
			report.Cache.IngestedTokens += row.Tokens.Ingested()
			if row.Tokens.CacheReadTokens > 0 {
				report.Cache.RunsWithCacheReads++
			}
		} else {
			report.UnmeasuredRuns++
		}
		if row.WallTimeMeasured {
			bucket.WallTime += row.WallTime
			bucket.WallTimeRuns++
		}
		if row.CostMeasured {
			bucket.ProviderCostUSD += row.ProviderCostUSD
			bucket.CostRuns++
			if row.Source == ContextSourceCold {
				baselineCost += row.ProviderCostUSD
				baselineRuns++
			} else {
				sharedCost += row.ProviderCostUSD
				sharedRuns++
			}
			bases[strings.TrimSpace(row.CostBasis)] = struct{}{}
		}
		accumulateContextPayloadCost(payload, row)
	}

	for _, bucket := range bySource {
		report.BySource = append(report.BySource, *bucket)
	}
	sort.SliceStable(report.BySource, func(i, j int) bool {
		return report.BySource[i].Source < report.BySource[j].Source
	})
	report.Cache = finishContextCacheRealization(report.Cache)
	report.Saving = deriveContextRealizedSaving(
		sortedContextCostBases(bases), baselineCost, baselineRuns, sharedCost, sharedRuns,
	)
	report.Payload = finishContextPayloadAttribution(payload, rates)
	return report
}

func accumulateContextPayloadCost(
	rows map[string]*ContextPayloadCostRow,
	row ContextRunEconomics,
) {
	if row.CtxID == "" ||
		(row.PayloadUploadedBytes == 0 && row.PayloadDownloadedBytes == 0 && row.PayloadStoredBytes == 0) {
		return
	}
	entry := rows[row.CtxID]
	if entry == nil {
		entry = &ContextPayloadCostRow{CtxID: row.CtxID, BindingElementID: row.BindingElementID}
		rows[row.CtxID] = entry
	}
	entry.UploadedBytes += row.PayloadUploadedBytes
	entry.DownloadedBytes += row.PayloadDownloadedBytes
	// Stored bytes are a level rather than a flow: the same context observed by
	// ten runs occupies its size once, not ten times. Summing them would inflate
	// the storage column by the popularity of the context, which is precisely
	// backwards.
	if row.PayloadStoredBytes > entry.StoredBytes {
		entry.StoredBytes = row.PayloadStoredBytes
	}
}

func finishContextCacheRealization(cache ContextCacheRealization) ContextCacheRealization {
	if cache.IngestedTokens > 0 {
		cache.ReadShare = float64(cache.CacheReadTokens) / float64(cache.IngestedTokens)
	}
	misses := cache.MeasuredRuns - cache.RunsWithCacheReads
	switch {
	case cache.MeasuredRuns == 0:
		cache.Statement = "no run reported provider usage, so realized cache behaviour is unknown; " +
			"nothing in this report assumes a cache hit"
	default:
		cache.Statement = fmt.Sprintf(
			"%d of %d measured runs read from cache (%d measured misses), realizing %.1f%% of ingested "+
				"tokens from cache; this is an observation of what happened and is never used to "+
				"predict, budget, or select",
			cache.RunsWithCacheReads, cache.MeasuredRuns, misses, cache.ReadShare*100,
		)
	}
	return cache
}

// deriveContextRealizedSaving computes the saving only where both arms were
// measured.
//
// A missing baseline produces the unmeasured verdict with no number. That is the
// rule the accepted measurement forces: the difference between the "as billed"
// and "cache never hits" columns was larger than the saving itself, so a figure
// derived from one arm and an assumption about the other would be off by more
// than the quantity it was reporting.
func deriveContextRealizedSaving(
	bases []string,
	baselineCost float64,
	baselineRuns int,
	sharedCost float64,
	sharedRuns int,
) ContextRealizedSaving {
	saving := ContextRealizedSaving{
		BaselineRuns: baselineRuns,
		SharedRuns:   sharedRuns,
	}
	if len(bases) == 1 {
		saving.Basis = bases[0]
	}
	if len(bases) > 1 {
		saving.Bases = append([]string(nil), bases...)
	}
	if baselineRuns == 0 || sharedRuns == 0 {
		saving.State = ContextSavingUnmeasured
		saving.Statement = fmt.Sprintf(
			"realized saving is unmeasured: %d measured cold runs and %d measured shared runs, and a "+
				"saving needs both arms; no figure is reported rather than one derived from an assumed "+
				"cache rate",
			baselineRuns, sharedRuns,
		)
		return saving
	}
	if len(bases) > 1 {
		// Both arms were observed, and subtracting them anyway would produce a
		// figure in no currency. The arms are reported so the operator can see the
		// comparison exists and what has to be re-priced before it means anything.
		saving.State = ContextSavingIncomparable
		saving.BaselineCostPerRunUSD = baselineCost / float64(baselineRuns)
		saving.SharedCostPerRunUSD = sharedCost / float64(sharedRuns)
		saving.Statement = fmt.Sprintf(
			"realized saving is not computed: %d measured cold and %d measured shared runs were priced "+
				"on %d different bases (%s), and a difference folded across them would be a number in no "+
				"currency; re-price one cohort or report them separately",
			baselineRuns, sharedRuns, len(bases), strings.Join(bases, ", "),
		)
		return saving
	}
	saving.State = ContextSavingMeasured
	saving.BaselineCostPerRunUSD = baselineCost / float64(baselineRuns)
	saving.SharedCostPerRunUSD = sharedCost / float64(sharedRuns)
	saving.AmountPerRunUSD = saving.BaselineCostPerRunUSD - saving.SharedCostPerRunUSD
	direction := "cheaper"
	if saving.AmountPerRunUSD < 0 {
		direction = "more expensive"
	}
	saving.Statement = fmt.Sprintf(
		"measured on %d cold and %d shared runs%s: a shared run was $%.4f %s than a cold run, from "+
			"provider-reported spend with no assumed cache reuse",
		baselineRuns, sharedRuns, contextBasisSuffix(saving.Basis),
		abs(saving.AmountPerRunUSD), direction,
	)
	return saving
}

// sortedContextCostBases is every distinct basis the measured rows named.
//
// The empty basis can never appear: ValidateContextRunEconomics refuses a
// measured cost with no basis, so every row that reaches here carries one.
func sortedContextCostBases(bases map[string]struct{}) []string {
	sorted := make([]string, 0, len(bases))
	for basis := range bases {
		if trimmed := strings.TrimSpace(basis); trimmed != "" {
			sorted = append(sorted, trimmed)
		}
	}
	sort.Strings(sorted)
	return sorted
}

func contextBasisSuffix(basis string) string {
	if basis == "" {
		return ""
	}
	return " on basis " + basis
}

func abs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

const contextGiB = float64(1 << 30)

func finishContextPayloadAttribution(
	rows map[string]*ContextPayloadCostRow,
	rates ContextPayloadCostRates,
) ContextPayloadCostAttribution {
	attribution := ContextPayloadCostAttribution{
		Rows:   []ContextPayloadCostRow{},
		Priced: rates.Priced(),
		Basis:  strings.TrimSpace(rates.Basis),
	}
	for _, row := range rows {
		entry := *row
		if attribution.Priced {
			entry.StorageUSDPerMonth = float64(entry.StoredBytes) / contextGiB * rates.StorageUSDPerGiBMonth
			entry.TransferUSD = float64(entry.UploadedBytes+entry.DownloadedBytes) /
				contextGiB * rates.TransferUSDPerGiB
		}
		attribution.Rows = append(attribution.Rows, entry)
		attribution.TotalStoredBytes += entry.StoredBytes
		attribution.TotalUploadedBytes += entry.UploadedBytes
		attribution.TotalDownloadedBytes += entry.DownloadedBytes
		attribution.TotalStorageUSDPerMonth += entry.StorageUSDPerMonth
		attribution.TotalTransferUSD += entry.TransferUSD
	}
	sort.SliceStable(attribution.Rows, func(i, j int) bool {
		return attribution.Rows[i].CtxID < attribution.Rows[j].CtxID
	})
	if attribution.Priced {
		attribution.Statement = fmt.Sprintf(
			"cross-machine payload bytes attributed to %d contexts and priced on basis %s",
			len(attribution.Rows), attribution.Basis,
		)
	} else {
		attribution.Statement = fmt.Sprintf(
			"cross-machine payload bytes attributed to %d contexts; no rate basis is configured, so "+
				"bytes are reported and no dollar figure is invented for them",
			len(attribution.Rows),
		)
	}
	return attribution
}
