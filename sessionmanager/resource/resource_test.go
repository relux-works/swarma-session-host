package contextpolicy

import "testing"

// Independent vectors preserve valid resource spelling and malformed-input
// normalization. This helper does not reject malformed IDs or mint owner keys.
func TestNormalizeContextIDOriginalSpellingOnly(t *testing.T) {
	for _, c := range []struct{ input, want string }{
		{"", ""}, {" \t\u2003", ""},
		{" ctx-261025-Ab12Cd ", "CTX-261025-ab12cd"},
		{"CTX-261025-ab12cd", "CTX-261025-ab12cd"},
		{"ctx", "CTX"}, {"ctx-261025", "CTX-261025"},
		{"ctx-261025-ab-cd", "CTX-261025-AB-CD"},
		{"ctx-invalid-date", "CTX-invalid-date"},
		{"ctx-261025-../FILE", "CTX-261025-../file"},
	} {
		t.Run(c.input, func(t *testing.T) {
			if got := NormalizeContextID(c.input); got != c.want {
				t.Fatalf("context resource spelling %q = %q, want %q", c.input, got, c.want)
			}
		})
	}
	if ContextProviderCodex != "codex" || ContextProviderClaude != "claude" {
		t.Fatal("provider spelling changed")
	}
}
