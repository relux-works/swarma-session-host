package sessioncontext

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ContextCompletenessFeedbackContract is the non-blocking economic signal
// derived from how forked runs actually behaved.
//
// CV-6 is explicit that completeness is feedback and never a gate, and the
// contract is built to make that structurally true rather than merely intended:
// there is no failing verdict in this file, no consequence, and no transition.
// The strongest thing it can emit is a warning and a recommendation nobody is
// required to take.
//
// The reason is that "the base did not cover enough" is an economic judgement,
// not a correctness one. A base that says less than it could is honest and
// cheap; a base that says more than it checked is the failure the publication
// gate exists to catch. Turning a coverage gap into a refusal would trade the
// second problem for the first — builders would widen boundaries to pass a gate,
// and the widened claims would be exactly the ones nobody verified.
const ContextCompletenessFeedbackContract = "context-completeness-feedback-v1"

const (
	// ContextCompletenessMinObservableForks is how many observable forks a base
	// needs before any warning is reported.
	//
	// A small sample of re-exploring forks is indistinguishable from a small
	// sample of thorough developers, and a warning fired on three forks would
	// train operators to ignore the signal long before it ever meant anything.
	ContextCompletenessMinObservableForks = 20
	// ContextCompletenessWindow is how far back the rate is measured.
	ContextCompletenessWindow = 7 * 24 * time.Hour
	// ContextCompletenessEpisodeRate is the share of observable forks that had to
	// leave the boundary before the base is called economically weak.
	ContextCompletenessEpisodeRate = 0.30
	// ContextCompletenessMedianExtraReads is the median per-fork cost of leaving
	// the boundary that triggers the same warning.
	ContextCompletenessMedianExtraReads = 3
	// ContextBoundaryWideningMinForks and ContextBoundaryWideningMinRoles are the
	// evidence a widening recommendation needs: repeated, and not from one role.
	//
	// The role requirement is what stops a single specialist's habits becoming a
	// permanent widening of a boundary every other role then pays to carry.
	ContextBoundaryWideningMinForks = 3
	ContextBoundaryWideningMinRoles = 2
)

// ContextCompletenessWarningCode is a stable economic-warning code.
type ContextCompletenessWarningCode string

const (
	// ContextCompletenessWarningEpisodeRate is too many forks having to explore.
	ContextCompletenessWarningEpisodeRate ContextCompletenessWarningCode = "re_exploration_rate_high"
	// ContextCompletenessWarningExtraReads is exploration being expensive when it
	// happens, even if it happens rarely.
	ContextCompletenessWarningExtraReads ContextCompletenessWarningCode = "extra_reads_median_high"
)

// ContextCompletenessWarning is one economic observation. It never blocks.
type ContextCompletenessWarning struct {
	Code ContextCompletenessWarningCode `json:"code"`
	// Detail is a bounded, path-free sentence. Paths belong in the widening
	// recommendation, where they are evidence; here they would just be content
	// leaking into an aggregate.
	Detail string `json:"detail"`
	// Blocking is present and always false. A consumer reading this struct should
	// not have to know the contract to know it is safe to ignore.
	Blocking bool `json:"blocking"`
}

// ContextBoundaryWideningRecommendation proposes one area the base could cover
// next time. It is never applied automatically.
type ContextBoundaryWideningRecommendation struct {
	Prefix string `json:"prefix"`
	// Forks and Roles are the evidence thresholds, reported so the recommendation
	// can be judged rather than merely obeyed.
	Forks int      `json:"forks"`
	Roles []string `json:"roles"`
	// AutoApplied is present and always false. The whole point of a
	// recommendation is that a human or a later builder run decides.
	AutoApplied bool `json:"auto_applied"`
}

// ContextCompletenessFeedback is the aggregate over one base's forks.
type ContextCompletenessFeedback struct {
	ContractVersion string `json:"contract_version"`
	CtxID           string `json:"ctx_id"`
	// WindowStart and WindowEnd bound what was measured, so a reader is never
	// left guessing whether a quiet week meant a good base or no forks.
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`

	TotalForks       int `json:"total_forks"`
	ObservableForks  int `json:"observable_forks"`
	PartialForks     int `json:"partial_forks"`
	UnavailableForks int `json:"unavailable_forks"`

	ReExplorationEpisodes int     `json:"re_exploration_episodes"`
	EpisodeRate           float64 `json:"episode_rate"`
	MedianExtraReads      float64 `json:"median_extra_reads"`
	TotalSpotChecks       int     `json:"total_spot_checks"`
	TotalUnclassified     int     `json:"total_unclassified_access"`
	TotalAssayAbstentions int     `json:"total_assay_abstentions"`

	// SampleSufficient reports whether the thresholds were even eligible to fire.
	// Without it a feedback block with no warnings reads as "this base is fine",
	// when it may only mean "nobody has used it yet".
	SampleSufficient bool                                    `json:"sample_sufficient"`
	Warnings         []ContextCompletenessWarning            `json:"warnings,omitempty"`
	Widening         []ContextBoundaryWideningRecommendation `json:"widening_recommendations,omitempty"`
	// Blocking is present and always false, for the same reason it is on the
	// warning: the guarantee should be readable off the wire.
	Blocking bool `json:"blocking"`
}

// AggregateContextCompleteness folds per-fork telemetry into the CV-6 signal.
//
// Unavailable forks are counted and then excluded from every rate. That single
// rule is what keeps the metric honest: including them as zero-read forks would
// make a provider whose stream this build cannot parse look like a provider
// whose bases are perfect, and dropping them silently would hide how much of the
// picture is missing.
func AggregateContextCompleteness(
	ctxID string,
	telemetry []ContextForkTelemetry,
	now time.Time,
) *ContextCompletenessFeedback {
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	windowStart := now.Add(-ContextCompletenessWindow)

	feedback := &ContextCompletenessFeedback{
		ContractVersion: ContextCompletenessFeedbackContract,
		CtxID:           NormalizeContextID(ctxID),
		WindowStart:     windowStart,
		WindowEnd:       now,
		Blocking:        false,
	}
	extraReads := make([]int, 0, len(telemetry))
	prefixForks := make(map[string]int)
	prefixRoles := make(map[string]map[string]struct{})

	for _, row := range telemetry {
		at := row.ObservedAt.UTC()
		if at.Before(windowStart) || at.After(now) {
			continue
		}
		feedback.TotalForks++
		if !row.Observable {
			feedback.UnavailableForks++
			continue
		}
		feedback.ObservableForks++
		if row.Status == ContextConsumerObservationPartial {
			feedback.PartialForks++
		}
		feedback.TotalSpotChecks += row.SpotChecks
		feedback.TotalUnclassified += row.UnclassifiedAccess
		feedback.TotalAssayAbstentions += row.AssayAbstentions
		extraReads = append(extraReads, row.ExtraReads)
		if !row.ReExplorationEpisode {
			continue
		}
		feedback.ReExplorationEpisodes++
		for _, prefix := range row.OutsidePrefixes {
			prefixForks[prefix]++
			if prefixRoles[prefix] == nil {
				prefixRoles[prefix] = make(map[string]struct{})
			}
			if role := NormalizeContextRole(row.Role); role != "" {
				prefixRoles[prefix][role] = struct{}{}
			}
		}
	}

	if feedback.ObservableForks > 0 {
		feedback.EpisodeRate = float64(feedback.ReExplorationEpisodes) / float64(feedback.ObservableForks)
		feedback.MedianExtraReads = contextMedian(extraReads)
	}
	feedback.SampleSufficient = feedback.ObservableForks >= ContextCompletenessMinObservableForks
	if feedback.SampleSufficient {
		if feedback.EpisodeRate > ContextCompletenessEpisodeRate {
			feedback.Warnings = append(feedback.Warnings, ContextCompletenessWarning{
				Code: ContextCompletenessWarningEpisodeRate,
				Detail: fmt.Sprintf(
					"%d of %d observable forks had to read outside the declared boundary (%.0f%%)",
					feedback.ReExplorationEpisodes, feedback.ObservableForks, feedback.EpisodeRate*100,
				),
				Blocking: false,
			})
		}
		if feedback.MedianExtraReads > ContextCompletenessMedianExtraReads {
			feedback.Warnings = append(feedback.Warnings, ContextCompletenessWarning{
				Code: ContextCompletenessWarningExtraReads,
				Detail: fmt.Sprintf(
					"the median fork read %.1f uncovered paths; a base that saved them would pay for itself",
					feedback.MedianExtraReads,
				),
				Blocking: false,
			})
		}
	}

	// The widening recommendation has its own evidence bar and is deliberately
	// not gated on SampleSufficient. Three forks across two roles all reaching the
	// same area is a concrete, checkable observation about that area; it does not
	// become more true after seventeen more forks somewhere else.
	prefixes := make([]string, 0, len(prefixForks))
	for prefix := range prefixForks {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	for _, prefix := range prefixes {
		roles := prefixRoles[prefix]
		if prefixForks[prefix] < ContextBoundaryWideningMinForks || len(roles) < ContextBoundaryWideningMinRoles {
			continue
		}
		named := make([]string, 0, len(roles))
		for role := range roles {
			named = append(named, role)
		}
		sort.Strings(named)
		feedback.Widening = append(feedback.Widening, ContextBoundaryWideningRecommendation{
			Prefix:      prefix,
			Forks:       prefixForks[prefix],
			Roles:       named,
			AutoApplied: false,
		})
	}
	return feedback
}

// contextMedian is the ordinary median of a small integer sample.
func contextMedian(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return float64(sorted[middle])
	}
	return float64(sorted[middle-1]+sorted[middle]) / 2
}

// Render produces the operator-facing summary.
//
// It always names the unavailable count, even when it is zero. A completeness
// report that only mentioned its blind spots when it had them would be read as
// complete every time it was silent.
func (f *ContextCompletenessFeedback) Render() string {
	if f == nil {
		return ""
	}
	var out strings.Builder
	fmt.Fprintf(&out, "context completeness %s (non-blocking)\n", f.CtxID)
	fmt.Fprintf(&out,
		"forks: %d total, %d observable (%d partial), %d unavailable and excluded from every rate\n",
		f.TotalForks, f.ObservableForks, f.PartialForks, f.UnavailableForks)
	if !f.SampleSufficient {
		fmt.Fprintf(&out,
			"sample below %d observable forks: rates are reported, warnings are not evaluated\n",
			ContextCompletenessMinObservableForks)
	}
	fmt.Fprintf(&out, "re-exploration: %d episodes, rate %.2f, median extra reads %.1f\n",
		f.ReExplorationEpisodes, f.EpisodeRate, f.MedianExtraReads)
	for _, warning := range f.Warnings {
		fmt.Fprintf(&out, "warning (%s): %s\n", warning.Code, warning.Detail)
	}
	for _, widening := range f.Widening {
		fmt.Fprintf(&out,
			"recommendation: consider covering %q next build (%d forks, roles %s) — not applied\n",
			widening.Prefix, widening.Forks, strings.Join(widening.Roles, ", "))
	}
	return out.String()
}
