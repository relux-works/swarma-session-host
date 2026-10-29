package contextpolicy

import "strings"

// Provider spellings are local resource vocabulary, with no canonical owner or
// goal policy. Untyped constants can be used with the caller's concrete enum.
const (
	ContextProviderClaude = "claude"
	ContextProviderCodex  = "codex"
)

// NormalizeContextID preserves the original context-resource spelling rule.
// This is normalization only: it neither validates an ID nor grants authority.
func NormalizeContextID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	parts := strings.Split(id, "-")
	if len(parts) != 3 {
		return strings.ToUpper(id)
	}
	return strings.ToUpper(parts[0]) + "-" + parts[1] + "-" + strings.ToLower(parts[2])
}
