//go:build unix

package sessionmanager

import (
	"context"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

func lockControllerEpochs(path string) (func(), error) {
	return lockControllerEpochsContext(context.Background(), path, true)
}
func lockControllerEpochsContext(ctx context.Context, path string, exclusive bool) (func(), error) {
	return historyguard.Lock(ctx, path+".fence", exclusive)
}
func lockControllerEpochIO(path string, exclusive bool) (func(), error) {
	return lockControllerEpochIOContext(context.Background(), path, exclusive)
}
func lockControllerEpochIOContext(ctx context.Context, path string, exclusive bool) (func(), error) {
	return historyguard.Lock(ctx, path, exclusive)
}
