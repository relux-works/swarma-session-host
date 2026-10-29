package musepty

import (
	"encoding/json"
	"os"
	"testing"
)

// The consumer-produced, pinned input corpus is data. Canonical goal conversion
// is tested by the consumer; these tests prove the real terminal sees its bytes.
func productionParityRevisions(t *testing.T) (string, string) {
	t.Helper()
	rows := recordedGoalInputs(t)
	return rows[0].Submitted, rows[1].Submitted
}

type recordedGoalInput struct {
	Operation string `json:"operation"`
	Submitted string `json:"submitted"`
}

func recordedGoalInputs(t *testing.T) []recordedGoalInput {
	t.Helper()
	raw, err := os.ReadFile("testdata/baseline/goal-input.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []recordedGoalInput
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatal("incomplete input corpus")
	}
	return rows
}
