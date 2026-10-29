//go:build !unix

package sessionmanager

import "context"

func lockControllerEpochs(string) (func(), error) { return nil, ErrControllerEpochUncertain }
func lockControllerEpochsContext(context.Context, string, bool) (func(), error) {
	return nil, ErrControllerEpochUncertain
}
func lockControllerEpochIO(string, bool) (func(), error) { return nil, ErrControllerEpochUncertain }
func lockControllerEpochIOContext(context.Context, string, bool) (func(), error) {
	return nil, ErrControllerEpochUncertain
}
