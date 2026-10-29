//go:build !darwin && !linux

package sessionmanager

import "errors"

func readProviderProcessIdentity(int) (providerProcessIdentity, error) {
	return providerProcessIdentity{}, errors.New("provider process identity is unavailable on this platform")
}
