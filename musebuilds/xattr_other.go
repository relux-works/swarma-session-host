//go:build !darwin && !linux

package musebuilds

import (
	"context"
	"errors"
	"os"
)

func captureXattrs(context.Context, *os.File) (map[string][]byte, error) {
	return nil, errors.New("xattrs need darwin/linux")
}
func copyXattrs(context.Context, map[string][]byte, *os.File) error {
	return errors.New("xattrs need darwin/linux")
}
func verifyStagedXattrs(context.Context, map[string][]byte, *os.File) error {
	return errors.New("xattrs need darwin/linux")
}
