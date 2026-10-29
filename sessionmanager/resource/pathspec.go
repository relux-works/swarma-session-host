package contextpolicy

import "strings"

// NormalizeContextPathSpec reduces a path spec to a clean repo-relative form and
// returns "" for anything that escapes the repository.
//
// It is exported so the Session Manager can normalize the read scope a provider
// launch boundary reports with exactly the rules the allowlist is matched under.
// Two normalizations that disagree would be a hole: a root that looks outside
// the allowlist to one and inside to the other.
func NormalizeContextPathSpec(spec string) string {
	return normalizeContextPathSpec(spec)
}

// normalizeContextPathSpec reduces a path spec to a clean repo-relative form and
// rejects anything that escapes the repository.
func normalizeContextPathSpec(spec string) string {
	trimmed := strings.TrimSpace(spec)
	if trimmed == "" {
		return ""
	}
	trimmed = strings.ReplaceAll(trimmed, `\`, "/")
	if strings.HasPrefix(trimmed, "/") {
		return ""
	}
	trimmed = strings.TrimPrefix(trimmed, "./")
	if trimmed == "" || trimmed == "." {
		return ""
	}
	for _, segment := range strings.Split(trimmed, "/") {
		if segment == ".." {
			return ""
		}
	}
	return strings.TrimSuffix(trimmed, "/")
}
