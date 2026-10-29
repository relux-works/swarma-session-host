package contextpolicy

import "testing"

// Independent vectors bind the original repository-relative spelling rules.
// In particular, any parent segment and slash-rooted path refuses; this is not
// filepath.Clean, Windows-volume validation, or an actual filesystem allowlist.
func TestNormalizeContextPathSpecOriginalRules(t *testing.T) {
	for _, row := range []struct{ input, want string }{
		{"", ""}, {" \t\u2003", ""}, {".", ""}, {"./", ""},
		{"/repo/file", ""}, {`\repo\file`, ""}, {`\\server\file`, ""},
		{"..", ""}, {"../file", ""}, {"a/../b", ""}, {`a\..\b`, ""},
		{"docs", "docs"}, {" docs/file \t", "docs/file"}, {"./docs/file", "docs/file"},
		{`docs\file`, "docs/file"}, {"docs/", "docs"}, {"docs///", "docs//"},
		{"a//b", "a//b"}, {"././docs", "./docs"}, {"a/./b", "a/./b"},
		{".../file", ".../file"}, {"docs/**", "docs/**"}, {"Case/File", "Case/File"},
		{`C:\docs\file`, "C:/docs/file"}, {"docs\x00file", "docs\x00file"},
	} {
		t.Run(row.input, func(t *testing.T) {
			if got := NormalizeContextPathSpec(row.input); got != row.want {
				t.Fatalf("normalize %q = %q, want %q", row.input, got, row.want)
			}
		})
	}
}
