package sessionmanager

import (
	"encoding/json"
	"errors"
)

// Native snapshots state all three cumulative counters. Backward goal pages
// may state none. Missing counters never acquire invented zero values.
func museNativeCumulativeUsage(raw json.RawMessage) (*MuseRestoreUsage, error) {
	var value struct {
		Output *int64 `json:"outputTokens"`
		Prompt *int64 `json:"promptTokens"`
		Total  *int64 `json:"totalTokens"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	for _, counter := range []*int64{value.Output, value.Prompt, value.Total} {
		if counter != nil && *counter < 0 {
			return nil, errors.New("resume snapshot tokenUsage is negative")
		}
	}
	if value.Output == nil || value.Prompt == nil || value.Total == nil {
		return nil, nil
	}
	return &MuseRestoreUsage{OutputTokens: *value.Output, PromptTokens: *value.Prompt, TotalTokens: *value.Total}, nil
}

func copyMuseNativeUsage(value *MuseRestoreUsage) *MuseRestoreUsage {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
