//go:build !darwin || !cgo

package admission

import (
	"context"
	"errors"
)

func ObserveProcess(context.Context, int) (Process, error) {
	return Process{}, errors.New("process_observer_unsupported")
}

func ObserveKernelProcess(context.Context, int) (KernelProcess, error) {
	return KernelProcess{}, errors.New("process_observer_unsupported")
}
