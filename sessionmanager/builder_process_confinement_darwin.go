//go:build darwin

package sessionmanager

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const sandboxExecPath = "/usr/bin/sandbox-exec"

func platformConfinedProviderCommand(
	projectDir string,
	executable string,
	args []string,
	roots []string,
	runtimeReadRoots []string,
) (string, []string, error) {
	if _, err := os.Stat(sandboxExecPath); err != nil {
		return "", nil, fmt.Errorf(
			"%w: %s is not usable: %v",
			errBuilderProcessConfinementUnavailable,
			sandboxExecPath,
			err,
		)
	}
	profile, err := renderBuilderSeatbeltProfile(
		projectDir,
		executable,
		roots,
		runtimeReadRoots,
	)
	if err != nil {
		return "", nil, err
	}
	wrapped := []string{"-p", profile, executable}
	wrapped = append(wrapped, args...)
	return sandboxExecPath, wrapped, nil
}

// renderBuilderSeatbeltProfile denies filesystem reads globally and restores
// only immutable system runtime paths, the selected provider runtime, and the
// configured project roots. Project writes are denied as part of the same
// boundary: otherwise rename(2) could move an out-of-scope file into an allowed
// root without reading it, defeating the read policy before a later read.
func renderBuilderSeatbeltProfile(
	projectDir string,
	executable string,
	roots []string,
	runtimeReadRoots []string,
) (string, error) {
	if !filepath.IsAbs(projectDir) {
		return "", fmt.Errorf("Seatbelt project directory must be absolute")
	}
	readRoots, err := builderSeatbeltRuntimeReadRoots(executable, runtimeReadRoots)
	if err != nil {
		return "", err
	}
	var profile strings.Builder
	cleanProjectDir := filepath.Clean(projectDir)
	profile.WriteString("(version 1)\n")
	profile.WriteString("(allow default)\n")
	profile.WriteString("(deny file-read*)\n")
	// Dynamic loaders and path traversal need ancestor metadata even when file
	// contents are denied. Restore that narrow capability globally, then remove
	// it again for the user's home and project before adding explicit runtime
	// and allowlisted exceptions below.
	profile.WriteString("(allow file-read-metadata)\n")
	profile.WriteString("(allow file-read-data (literal \"/\"))\n")
	if userHome, homeErr := os.UserHomeDir(); homeErr == nil {
		writeSeatbeltPathRule(&profile, "deny", "file-read*", userHome)
	}
	for _, ancestor := range pathAncestors(cleanProjectDir) {
		profile.WriteString("(allow file-read-data (literal ")
		profile.WriteString(strconv.Quote(ancestor))
		profile.WriteString("))\n")
	}
	for _, root := range readRoots {
		writeSeatbeltPathRule(&profile, "allow", "file-read*", root)
	}
	writeSeatbeltPathRule(&profile, "deny", "file-read*", cleanProjectDir)
	writeSeatbeltPathRule(&profile, "deny", "file-write*", cleanProjectDir)
	profile.WriteString("(allow file-read* (literal ")
	profile.WriteString(strconv.Quote(cleanProjectDir))
	profile.WriteString("))\n")
	for _, root := range roots {
		absolute := filepath.Join(projectDir, filepath.FromSlash(root))
		if hasPathGlob(root) {
			pattern, err := seatbeltGlobPattern(absolute)
			if err != nil {
				return "", err
			}
			for _, parent := range globTraversalRoots(projectDir, root) {
				profile.WriteString("(allow file-read-metadata (literal ")
				profile.WriteString(strconv.Quote(parent))
				profile.WriteString("))\n")
			}
			profile.WriteString("(allow file-read* (regex #\"")
			profile.WriteString(escapeSeatbeltRegex(pattern))
			profile.WriteString("\"))\n")
			continue
		}
		writeSeatbeltPathRule(&profile, "allow", "file-read*", absolute)
	}
	return profile.String(), nil
}

func builderSeatbeltRuntimeReadRoots(executable string, extra []string) ([]string, error) {
	if !filepath.IsAbs(executable) {
		return nil, fmt.Errorf("Seatbelt provider executable must be absolute")
	}
	roots := []string{
		"/System",
		"/Library",
		"/usr",
		"/bin",
		"/sbin",
		"/dev",
		"/private/etc",
		"/private/var/db/timezone",
		filepath.Dir(filepath.Clean(executable)),
	}
	if _, err := os.Stat("/opt/homebrew"); err == nil {
		roots = append(roots, "/opt/homebrew")
	}
	if resolved, err := filepath.EvalSymlinks(executable); err == nil {
		roots = append(roots, filepath.Dir(resolved))
	}
	roots = append(roots, extra...)

	seen := make(map[string]struct{}, len(roots))
	normalized := make([]string, 0, len(roots))
	for _, root := range roots {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		if !filepath.IsAbs(root) {
			return nil, fmt.Errorf("Seatbelt runtime read root must be absolute: %q", root)
		}
		root = filepath.Clean(root)
		if resolved, err := filepath.EvalSymlinks(root); err == nil {
			root = resolved
		}
		if root == string(filepath.Separator) {
			return nil, fmt.Errorf("Seatbelt runtime read root must not cover the filesystem")
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		normalized = append(normalized, root)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func pathAncestors(path string) []string {
	var reversed []string
	for parent := filepath.Dir(filepath.Clean(path)); parent != string(filepath.Separator); parent = filepath.Dir(parent) {
		reversed = append(reversed, parent)
	}
	ancestors := make([]string, 0, len(reversed))
	for index := len(reversed) - 1; index >= 0; index-- {
		ancestors = append(ancestors, reversed[index])
	}
	return ancestors
}

func writeSeatbeltPathRule(profile *strings.Builder, effect, operation, root string) {
	quoted := strconv.Quote(filepath.Clean(root))
	profile.WriteString("(")
	profile.WriteString(effect)
	profile.WriteString(" ")
	profile.WriteString(operation)
	profile.WriteString(" (literal ")
	profile.WriteString(quoted)
	profile.WriteString(") (subpath ")
	profile.WriteString(quoted)
	profile.WriteString("))\n")
}

func globTraversalRoots(projectDir, spec string) []string {
	parts := strings.Split(filepath.ToSlash(spec), "/")
	current := filepath.Clean(projectDir)
	roots := []string{current}
	for _, part := range parts {
		if hasPathGlob(part) {
			break
		}
		current = filepath.Join(current, filepath.FromSlash(part))
		roots = append(roots, current)
	}
	return roots
}

func hasPathGlob(spec string) bool {
	return strings.ContainsAny(spec, "*?[")
}

func seatbeltGlobPattern(absolute string) (string, error) {
	absolute = filepath.ToSlash(filepath.Clean(absolute))
	var pattern strings.Builder
	pattern.WriteString("^")
	for index := 0; index < len(absolute); {
		switch absolute[index] {
		case '*':
			pattern.WriteString("[^/]*")
			index++
		case '?':
			pattern.WriteString("[^/]")
			index++
		case '[':
			end := strings.IndexByte(absolute[index+1:], ']')
			if end < 0 {
				return "", fmt.Errorf("invalid builder allowlist glob %q", absolute)
			}
			end += index + 1
			class := absolute[index+1 : end]
			pattern.WriteByte('[')
			pattern.WriteString(class)
			pattern.WriteByte(']')
			index = end + 1
		default:
			start := index
			for index < len(absolute) && !strings.ContainsRune("*?[", rune(absolute[index])) {
				index++
			}
			pattern.WriteString(regexp.QuoteMeta(absolute[start:index]))
		}
	}
	pattern.WriteString("$")
	return pattern.String(), nil
}

func escapeSeatbeltRegex(pattern string) string {
	return strings.ReplaceAll(pattern, `"`, `\"`)
}
