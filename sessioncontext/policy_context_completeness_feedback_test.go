package sessioncontext

import (
	"strings"
	"testing"
	"time"
)

func feedbackRow(
	role string,
	episode bool,
	extraReads int,
	prefixes []string,
	at time.Time,
) ContextForkTelemetry {
	return ContextForkTelemetry{
		ContractVersion:      ContextForkTelemetryContract,
		CtxID:                "CTX-260728-a1b2c3",
		Role:                 role,
		Provider:             ContextProviderCodex,
		Status:               ContextConsumerObservationComplete,
		Observable:           true,
		AnyRepositoryRead:    true,
		ReExplorationEpisode: episode,
		ExtraReads:           extraReads,
		OutsidePrefixes:      prefixes,
		ObservedAt:           at,
	}
}

func unobservableRow(at time.Time) ContextForkTelemetry {
	return ContextForkTelemetry{
		ContractVersion: ContextForkTelemetryContract,
		CtxID:           "CTX-260728-a1b2c3",
		Role:            "developer",
		Provider:        ContextProviderClaude,
		Status:          ContextConsumerObservationUnavailable,
		Observable:      false,
		ObservedAt:      at,
	}
}

// TestUnavailableForksAreExcludedFromEveryRate is the honesty rule that decides
// whether this metric is worth having.
//
// Counting them as zero-read forks would make a provider whose stream this build
// cannot parse look like a provider whose bases are perfect, which is the most
// flattering possible misreading and the easiest bug to ship.
func TestUnavailableForksAreExcludedFromEveryRate(t *testing.T) {
	now := registryTestClock
	rows := make([]ContextForkTelemetry, 0, 40)
	for i := 0; i < 10; i++ {
		rows = append(rows, feedbackRow("developer", true, 5, []string{"tools"}, now.Add(-time.Hour)))
	}
	for i := 0; i < 10; i++ {
		rows = append(rows, feedbackRow("developer", false, 0, nil, now.Add(-time.Hour)))
	}
	// Thirty runs nobody could observe. If they leaked into the denominator the
	// rate would fall from 0.50 to 0.20 and the warning would vanish.
	for i := 0; i < 30; i++ {
		rows = append(rows, unobservableRow(now.Add(-time.Hour)))
	}

	feedback := AggregateContextCompleteness("CTX-260728-a1b2c3", rows, now)
	if feedback.TotalForks != 50 || feedback.ObservableForks != 20 || feedback.UnavailableForks != 30 {
		t.Fatalf("counts = %+v", feedback)
	}
	if feedback.EpisodeRate != 0.5 {
		t.Fatalf("episode rate = %v, want 0.5 over observable forks only", feedback.EpisodeRate)
	}
	if len(feedback.Warnings) == 0 {
		t.Fatal("a 50%% episode rate over a sufficient sample produced no warning")
	}
	rendered := feedback.Render()
	if !strings.Contains(rendered, "30 unavailable") {
		t.Fatalf("the report hides how much it could not see:\n%s", rendered)
	}
}

// TestFeedbackIsNeverBlocking is CV-6 stated as a property of the output rather
// than of the caller's discipline.
func TestFeedbackIsNeverBlocking(t *testing.T) {
	now := registryTestClock
	rows := make([]ContextForkTelemetry, 0, 25)
	for i := 0; i < 25; i++ {
		rows = append(rows, feedbackRow("developer", true, 9, []string{"tools"}, now.Add(-time.Hour)))
	}
	feedback := AggregateContextCompleteness("CTX-260728-a1b2c3", rows, now)
	if feedback.Blocking {
		t.Fatal("the completeness aggregate reported itself as blocking")
	}
	if len(feedback.Warnings) == 0 {
		t.Fatal("the worst possible sample produced no warning at all")
	}
	for _, warning := range feedback.Warnings {
		if warning.Blocking {
			t.Fatalf("warning %s is blocking", warning.Code)
		}
	}
}

// TestASmallSampleReportsRatesAndFiresNoWarning keeps the thresholds from
// training operators to ignore the signal.
func TestASmallSampleReportsRatesAndFiresNoWarning(t *testing.T) {
	now := registryTestClock
	rows := []ContextForkTelemetry{
		feedbackRow("developer", true, 9, []string{"tools"}, now.Add(-time.Hour)),
		feedbackRow("developer", true, 9, []string{"tools"}, now.Add(-time.Hour)),
		feedbackRow("reviewer", true, 9, []string{"tools"}, now.Add(-time.Hour)),
	}
	feedback := AggregateContextCompleteness("CTX-260728-a1b2c3", rows, now)
	if feedback.SampleSufficient {
		t.Fatalf("three forks were treated as a sufficient sample: %+v", feedback)
	}
	if len(feedback.Warnings) != 0 {
		t.Fatalf("a three-fork sample fired %d warning(s)", len(feedback.Warnings))
	}
	if feedback.EpisodeRate != 1 {
		t.Fatalf("the rate itself is still reported: %v", feedback.EpisodeRate)
	}
	if !strings.Contains(feedback.Render(), "warnings are not evaluated") {
		t.Fatalf("a quiet small sample reads as a healthy base:\n%s", feedback.Render())
	}
}

// TestTheEpisodeRateAndTheExtraReadThresholdFireIndependently: the AC names two
// conditions joined by "or", and a single combined check would satisfy neither
// case cleanly.
func TestTheEpisodeRateAndTheExtraReadThresholdFireIndependently(t *testing.T) {
	now := registryTestClock

	// Rare but expensive: 20% of forks explore, and each costs 9 extra reads.
	rare := make([]ContextForkTelemetry, 0, 20)
	for i := 0; i < 4; i++ {
		rare = append(rare, feedbackRow("developer", true, 9, []string{"tools"}, now.Add(-time.Hour)))
	}
	for i := 0; i < 16; i++ {
		rare = append(rare, feedbackRow("developer", false, 4, nil, now.Add(-time.Hour)))
	}
	rareFeedback := AggregateContextCompleteness("CTX-260728-a1b2c3", rare, now)
	if rareFeedback.EpisodeRate > ContextCompletenessEpisodeRate {
		t.Fatalf("fixture episode rate %v is above the threshold; this case is about the other one",
			rareFeedback.EpisodeRate)
	}
	if !completenessHasWarning(rareFeedback, ContextCompletenessWarningExtraReads) {
		t.Fatalf("a high median extra-read count fired no warning: %+v", rareFeedback)
	}

	// Frequent but cheap: half the forks explore, each costing one read.
	frequent := make([]ContextForkTelemetry, 0, 20)
	for i := 0; i < 10; i++ {
		frequent = append(frequent, feedbackRow("developer", true, 1, []string{"tools"}, now.Add(-time.Hour)))
	}
	for i := 0; i < 10; i++ {
		frequent = append(frequent, feedbackRow("developer", false, 0, nil, now.Add(-time.Hour)))
	}
	frequentFeedback := AggregateContextCompleteness("CTX-260728-a1b2c3", frequent, now)
	if frequentFeedback.MedianExtraReads > ContextCompletenessMedianExtraReads {
		t.Fatalf("fixture median %v is above the threshold; this case is about the other one",
			frequentFeedback.MedianExtraReads)
	}
	if !completenessHasWarning(frequentFeedback, ContextCompletenessWarningEpisodeRate) {
		t.Fatalf("a high episode rate fired no warning: %+v", frequentFeedback)
	}
}

// TestAHealthyBaseFiresNothing is the positive control: a rule that warned about
// everything would pass every case above.
func TestAHealthyBaseFiresNothing(t *testing.T) {
	now := registryTestClock
	rows := make([]ContextForkTelemetry, 0, 30)
	for i := 0; i < 30; i++ {
		rows = append(rows, feedbackRow("developer", false, 0, nil, now.Add(-time.Hour)))
	}
	feedback := AggregateContextCompleteness("CTX-260728-a1b2c3", rows, now)
	if !feedback.SampleSufficient {
		t.Fatal("thirty observable forks is not a sufficient sample")
	}
	if len(feedback.Warnings) != 0 || len(feedback.Widening) != 0 {
		t.Fatalf("a base nobody had to look past produced %+v", feedback)
	}
}

// TestWideningNeedsRepeatedEvidenceFromMoreThanOneRole is the rule that stops
// one specialist's habits becoming a boundary every role pays to carry.
func TestWideningNeedsRepeatedEvidenceFromMoreThanOneRole(t *testing.T) {
	now := registryTestClock

	oneRole := []ContextForkTelemetry{
		feedbackRow("developer", true, 2, []string{"tools"}, now.Add(-time.Hour)),
		feedbackRow("developer", true, 2, []string{"tools"}, now.Add(-time.Hour)),
		feedbackRow("developer", true, 2, []string{"tools"}, now.Add(-time.Hour)),
	}
	if got := AggregateContextCompleteness("CTX-260728-a1b2c3", oneRole, now); len(got.Widening) != 0 {
		t.Fatalf("three forks from one role recommended a widening: %+v", got.Widening)
	}

	tooFew := []ContextForkTelemetry{
		feedbackRow("developer", true, 2, []string{"tools"}, now.Add(-time.Hour)),
		feedbackRow("reviewer", true, 2, []string{"tools"}, now.Add(-time.Hour)),
	}
	if got := AggregateContextCompleteness("CTX-260728-a1b2c3", tooFew, now); len(got.Widening) != 0 {
		t.Fatalf("two forks recommended a widening: %+v", got.Widening)
	}

	qualifying := []ContextForkTelemetry{
		feedbackRow("developer", true, 2, []string{"tools"}, now.Add(-time.Hour)),
		feedbackRow("developer", true, 2, []string{"tools"}, now.Add(-time.Hour)),
		feedbackRow("reviewer", true, 2, []string{"tools"}, now.Add(-time.Hour)),
	}
	feedback := AggregateContextCompleteness("CTX-260728-a1b2c3", qualifying, now)
	if len(feedback.Widening) != 1 {
		t.Fatalf("qualifying evidence produced %+v", feedback.Widening)
	}
	recommendation := feedback.Widening[0]
	if recommendation.Prefix != "tools" || recommendation.Forks != 3 || len(recommendation.Roles) != 2 {
		t.Fatalf("recommendation = %+v", recommendation)
	}
	if recommendation.AutoApplied {
		t.Fatal("a boundary widening was applied automatically")
	}
	if !strings.Contains(feedback.Render(), "not applied") {
		t.Fatalf("the report does not say the recommendation is advisory:\n%s", feedback.Render())
	}
}

// TestOnlyTheSevenDayWindowIsMeasured keeps an old burst of exploration from
// warning about a base that has since been fixed.
func TestOnlyTheSevenDayWindowIsMeasured(t *testing.T) {
	now := registryTestClock
	rows := make([]ContextForkTelemetry, 0, 40)
	for i := 0; i < 25; i++ {
		rows = append(rows, feedbackRow(
			"developer", true, 9, []string{"tools"}, now.Add(-ContextCompletenessWindow-time.Hour)))
	}
	for i := 0; i < 25; i++ {
		rows = append(rows, feedbackRow("developer", false, 0, nil, now.Add(-time.Hour)))
	}
	feedback := AggregateContextCompleteness("CTX-260728-a1b2c3", rows, now)
	if feedback.TotalForks != 25 || feedback.ObservableForks != 25 {
		t.Fatalf("the window did not drop the old burst: %+v", feedback)
	}
	if len(feedback.Warnings) != 0 {
		t.Fatalf("an out-of-window burst still warned: %+v", feedback.Warnings)
	}
}

func completenessHasWarning(
	feedback *ContextCompletenessFeedback,
	code ContextCompletenessWarningCode,
) bool {
	for _, warning := range feedback.Warnings {
		if warning.Code == code {
			return true
		}
	}
	return false
}
