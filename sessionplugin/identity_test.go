package sessionplugin

import (
	"slices"
	"testing"
)

// The standalone engine advertises its neutral identity and no unimplemented
// consumer extension; configured identity is applied to both describe and rows.
func TestEngineConsumerIdentity(t *testing.T) {
	e, _ := testEngine(t)
	description := call(t, e, "describe", map[string]any{}).(map[string]any)
	if description["id"] != "swarma-session-host" {
		t.Fatalf("identity: %#v", description)
	}
	if !slices.Equal(description["capabilities"].([]string), []string{"launch-delegated/1", "admission/1", "status/1", "list/1", "labels/1", "resume/1", "attach/1", "stop/1"}) {
		t.Fatalf("capabilities: %#v", description)
	}
	e.Manager = "example-consumer"
	e.Capabilities = []string{"launch-delegated/1"}
	description = call(t, e, "describe", map[string]any{}).(map[string]any)
	if description["id"] != e.Manager || !slices.Equal(description["capabilities"].([]string), e.Capabilities) {
		t.Fatal("consumer configuration ignored")
	}
	record := decode[Record](t, call(t, e, "launch", request(1)))
	if record.Manager != e.Manager {
		t.Fatalf("record manager %q", record.Manager)
	}
	description["capabilities"].([]string)[0] = "altered"
	if e.Capabilities[0] != "launch-delegated/1" {
		t.Fatal("reply aliases consumer configuration")
	}
}
