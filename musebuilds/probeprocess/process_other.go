//go:build !darwin && !linux

package probeprocess

import (
	"context"
	"errors"
	"os"
)

func (OSLauncher) Start(context.Context, *os.File, []string) (Process, error) {
	return nil, errors.New("first-probe execution requires Darwin or Linux")
}
