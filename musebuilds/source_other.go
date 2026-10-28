//go:build !darwin && !linux

package musebuilds

import (
	"errors"
	"os"
)

const supportedPlatform = false

var errNoFileIdentity = errors.New("host does not support descriptor-relative discovery")

// ResolveNewest refuses before any filesystem operation on these platforms.
// These stubs keep cross-compilation possible without a blocking-open fallback.
func openDirectoryRoot() (*os.File, error)               { return nil, errNoFileIdentity }
func openDirectoryAt(*os.File, string) (*os.File, error) { return nil, errNoFileIdentity }
func openFileAt(*os.File, string) (*os.File, error)      { return nil, errNoFileIdentity }
func descriptorIdentity(*os.File) (fileIdentity, error)  { return fileIdentity{}, errNoFileIdentity }
func identityAt(*os.File, string) (fileIdentity, error)  { return fileIdentity{}, errNoFileIdentity }
func readLinkAt(*os.File, string) (string, error)        { return "", errNoFileIdentity }
