package sessionbudget

import (
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	"testing"
)

// Preserve the original native provider's selection: absent uses its fallback,
// a positive selection replaces it, and explicit unlimited removes the ceiling.
// Invalid selections never acquire unlimited semantics.
func TestMuseRevisionBudget(t *testing.T) {
	for _, tc := range []struct {
		name           string
		selected       *goal.Budget
		fallback, want int64
		invalid        bool
	}{
		{"fallback", nil, 19, 19, false},
		{"positive", &goal.Budget{Tokens: 7}, 19, 7, false},
		{"explicit_none", &goal.Budget{Unlimited: true}, 19, 0, false},
		{"zero", &goal.Budget{}, 19, 0, true},
		{"negative", &goal.Budget{Tokens: -1}, 19, 0, true},
		{"ambiguous_none", &goal.Budget{Unlimited: true, Tokens: 7}, 19, 0, true},
		{"bad_fallback", nil, -1, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SelectBudget(tc.selected, tc.fallback)
			if (err != nil) != tc.invalid || got != tc.want {
				t.Fatalf("selected budget = %d, %v; want %d invalid=%t", got, err, tc.want, tc.invalid)
			}
		})
	}
}
