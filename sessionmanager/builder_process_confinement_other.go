//go:build !darwin

package sessionmanager

import "fmt"

func platformConfinedProviderCommand(
	_ string,
	_ string,
	_ []string,
	_ []string,
	_ []string,
) (string, []string, error) {
	return "", nil, fmt.Errorf(
		"%w on this platform",
		errBuilderProcessConfinementUnavailable,
	)
}
