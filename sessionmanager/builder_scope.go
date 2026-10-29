package sessionmanager

import (
	"fmt"
	contextpolicy "github.com/relux-works/swarma-session-host/sessionmanager/resource"
	"sort"
)

func normalizeConfinedRoots(roots []string) ([]string, error) {
	normalized := make([]string, 0, len(roots))
	for _, root := range roots {
		clean := contextpolicy.NormalizeContextPathSpec(root)
		if clean == "" {
			return nil, fmt.Errorf("read scope entry %q is not a repo-relative path", root)
		}
		normalized = append(normalized, clean)
	}
	if len(normalized) == 0 {
		return nil, fmt.Errorf("read scope is empty")
	}
	sort.Strings(normalized)
	return normalized, nil
}
