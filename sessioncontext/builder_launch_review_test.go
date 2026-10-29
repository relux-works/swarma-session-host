package sessioncontext

import (
	"context"
	"testing"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

func rev6zFactory(t *testing.T, launched *bool, mutate func(sm.ProviderLaunchRequest), host sm.RunnerHost, endpoint *sm.BuilderGatewayEndpoint) builderLaunchFactory {
	return builderLaunchFactory{provider: "codex", launch: func(_ context.Context, request sm.ProviderLaunchRequest) (sm.RunnerHost, error) {
		*endpoint = request.BuilderConfinement.Gateway
		client := sm.NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
		defer client.Disconnect()
		driveOneBuilderRead(t, client)
		*launched = true
		if mutate != nil {
			mutate(request)
		}
		return host, nil
	}}
}

// ZB1: source run evidence turns failed/unknown/foreign during Launch: the
// post-Launch reread must refuse, stop exactly the returned host, close gateway.
func TestRev6ZBRunEvidenceChangedDuringLaunch(t *testing.T) {
	for _, mode := range []string{"failed", "unknown", "deleted", "foreign-owner"} {
		t.Run(mode, func(t *testing.T) {
			m, a, r, runtime, q := newBuilderLaunchFixture(t)
			launched := false
			host := &builderLaunchHost{launched: &launched}
			var endpoint sm.BuilderGatewayEndpoint
			factory := rev6zFactory(t, &launched, func(sm.ProviderLaunchRequest) {
				switch mode {
				case "failed", "unknown":
					a.runs["RUN-boundary"].Status = sm.RunEvidenceStatus(mode)
				case "deleted":
					delete(a.runs, "RUN-boundary")
				case "foreign-owner":
					a.runs["RUN-boundary"].Owner = "TASK-260729-cccccc"
				}
			}, host, &endpoint)
			got, err := m.LaunchBuilderHost(context.Background(), builderLaunchView(m, r, q), factory, q)
			t.Logf("got=%T err=%v stopped=%d observed=%d", got, err, host.stopped, host.observed)
			if err == nil || got != nil || host.stopped != 1 || host.observed != 0 {
				t.Fatalf("changed run evidence accepted or wrong cleanup: %v stop=%d obs=%d", err, host.stopped, host.observed)
			}
			c := sm.NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
			defer c.Disconnect()
			if c.Connect() == nil {
				t.Fatal("gateway still serving")
			}
			if _, _, err := runtime.BuilderAccessTrace(r.ID); err == nil {
				t.Fatal("usable evidence after refusal")
			}
		})
	}
}

