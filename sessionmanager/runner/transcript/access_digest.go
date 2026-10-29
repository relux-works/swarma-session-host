// Package transcript owns provider transcript and repository-access evidence
// computations. It has no canonical board, task or store dependency.
package transcript

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
)

// ContextRepositoryAccessContentDigest fingerprints returned file bytes under
// one anchor.
func ContextRepositoryAccessContentDigest(anchor string, payload []byte) string {
	sum := sha256.Sum256(append(
		[]byte(strings.ToLower(strings.TrimSpace(anchor))+"\x00content\x00"),
		payload...,
	))
	return hex.EncodeToString(sum[:])
}

// ContextRepositoryAccessEnumerationDigest fingerprints one enumeration under
// one anchor.
//
// The entries are sorted and de-duplicated first, so the digest describes the
// set of paths the builder saw rather than the order the filesystem happened to
// return them in. An empty enumeration has a digest of its own rather than none:
// "the directory was empty" is a real observation, and the coverage consumer has
// to be able to tell it from "no enumeration happened".
func ContextRepositoryAccessEnumerationDigest(anchor string, entries []string) string {
	cleaned := normalizeContextAccessEntries(entries)
	parts := append(
		[]string{strings.ToLower(strings.TrimSpace(anchor)) + "\x00enumeration"},
		cleaned...,
	)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

// ContextRepositoryAccessStatDigest fingerprints the facts a stat returned.
//
// A stat returns no content, but it does return an observation — whether the
// path exists, what kind of node it is, how large it is — and a trace that
// recorded no digest for it would let the answer be changed after the fact.
func ContextRepositoryAccessStatDigest(
	anchor string,
	exists bool,
	kind string,
	size int64,
) string {
	payload := strings.Join([]string{
		strings.ToLower(strings.TrimSpace(anchor)) + "\x00stat",
		"exists=" + strconv.FormatBool(exists),
		"kind=" + strings.ToLower(strings.TrimSpace(kind)),
		"size=" + strconv.FormatInt(size, 10),
	}, "\n")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func normalizeContextAccessEntries(entries []string) []string {
	cleaned := make([]string, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		normalized := normalizeContextPathSpec(entry)
		if normalized == "" {
			continue
		}
		if _, duplicate := seen[normalized]; duplicate {
			continue
		}
		seen[normalized] = struct{}{}
		cleaned = append(cleaned, normalized)
	}
	sort.Strings(cleaned)
	return cleaned
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
